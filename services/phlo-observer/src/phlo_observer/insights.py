"""Deterministic insight rules — spec §15.

Every insight is produced by a named, versioned rule over canonical events
and baselines; none require ML (§15.1). Insight lifecycle states are
``open``/``acknowledged``/``resolved``/``suppressed``/``expired`` (§15.3);
a subsequent success event auto-resolves matching open insights.
"""

from __future__ import annotations

import datetime as dt
import statistics
import uuid
from dataclasses import dataclass
from typing import Any

from observe_core.timestamps import parse_rfc3339, utcnow
from sqlalchemy import func, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer import baselines as bl
from phlo_observer import incidents
from phlo_observer import state_engine as se
from phlo_observer.models import Asset, Baseline, Event, Insight

# Rule registry: rule_id -> version. Bump a version when a rule's logic
# changes materially; the pair is stamped on every insight it emits.
RULES: dict[str, int] = {
    "duration-regression": 1,
    "quality-failure": 1,
    "run-failure": 1,
    "freshness-breach": 1,
    "volume-spike": 1,
    "recurring-failure": 1,
}

_DURATION_FACTOR = 2.0
"""duration > 2x rolling median is a regression (spec §15.1)."""
_VOLUME_FACTOR = 3.0
"""scan/row volume increase > 300% of baseline (spec §15.1)."""
_MIN_BASELINE_SAMPLES = 5
"""Fewer samples than this cannot support a statistically sane verdict."""
_CONSECUTIVE_QUALITY_FAILURES = 3

_QUALITY_EVENTS = frozenset({"quality.check", "quality.validate", "dbt.test.execute"})
"""Event names the quality-failure rule evaluates (spec §15.1)."""

_MAX_PRODUCERS = 64
"""Bounded evidence trail per insight: the earliest 64 producing events."""


def _signal_entity(entities: dict[str, str]) -> str | None:
    """Entity a quality signal attaches to — the checked object first."""
    for role in ("asset", "model", "table", "run"):
        if entities.get(role):
            return entities[role]
    return None


@dataclass
class Finding:
    """A rule verdict before persistence."""

    rule_id: str
    kind: str
    severity: str
    title: str
    summary: str
    entity_id: str | None
    evidence_event_ids: list[str]
    recommended_action: str | None = None


async def _baseline(session: AsyncSession, entity_id: str, metric: str) -> Baseline | None:
    return (
        await session.execute(
            select(Baseline).where(Baseline.entity_id == entity_id, Baseline.metric == metric)
        )
    ).scalar_one_or_none()


@dataclass
class BatchState:
    """Preloaded state for one ordered batch of canonical events.

    One bulk load replaces the per-event round trips the insight pass used
    to make: baselines for every (entity, metric) the batch observes, all
    open insights (dedupe + resolution), all open incidents (grouping) and
    reducer state for every touched asset (freshness). The per-event loop
    then runs entirely in memory; ORM mutations flush once at the end.
    """

    baselines: dict[tuple[str, str], Baseline]
    open_by_dedupe: dict[str, Insight]
    open_insights: list[Insight]
    resolved_by_dedupe: dict[str, list[Insight]]
    open_incidents: list[Any]
    asset_states: dict[str, dict[str, Any]]

    @classmethod
    async def load(cls, session: AsyncSession, events: list[dict[str, Any]]) -> BatchState:
        """Bulk-load every row the batch's insight pass can touch.

        Open insights and incidents are scoped to the entities this batch
        can signal: dedupe and resolution both key on the insight's entity,
        and incident grouping only matches insights whose entity is already
        in the incident's entity list. Unscoped loads would read every open
        row in the deployment for every ingest batch.
        """
        keys: set[tuple[str, str]] = set()
        entity_ids: set[str] = set()
        for event in events:
            entity_ids.update(se.event_entities(event).values())
            for ent, metric, _ in bl.observations_of(event):
                keys.add((ent, metric))
                # Metric entities (e.g. ``job://`` duration baselines) can be
                # insight targets too — include them in the load scope.
                entity_ids.add(ent)
        baseline_rows: dict[tuple[str, str], Baseline] = {}
        if keys:
            baseline_rows = {
                (row.entity_id, row.metric): row
                for row in (
                    await session.execute(
                        select(Baseline).where(
                            tuple_(Baseline.entity_id, Baseline.metric).in_(keys)
                        )
                    )
                ).scalars()
            }
        insight_stmt = select(Insight).where(Insight.state == "open")
        resolved_stmt = select(Insight).where(Insight.state == "resolved")
        if entity_ids:
            scope = or_(Insight.entity_id.in_(entity_ids), Insight.entity_id.is_(None))
            insight_stmt = insight_stmt.where(scope)
            resolved_stmt = resolved_stmt.where(scope)
            incident_stmt = select(incidents.Incident).where(incidents.Incident.state == "open")
            dialect = getattr(getattr(session, "bind", None), "dialect", None)
            if dialect is not None and dialect.name == "postgresql":
                # jsonb ?| — incidents whose entity array overlaps the batch.
                incident_stmt = incident_stmt.where(
                    func.jsonb_exists_any(incidents.Incident.entities, sorted(entity_ids))
                )
            # Non-Postgres dialects (dev/test SQLite) keep the full open
            # scan: correct, just not bounded — datasets there stay small.
            open_incidents = list((await session.execute(incident_stmt)).scalars())
        else:
            # A batch with no entities can only dedupe entity-less open
            # insights; nothing it produces can join an existing incident.
            insight_stmt = insight_stmt.where(Insight.entity_id.is_(None))
            resolved_stmt = resolved_stmt.where(Insight.entity_id.is_(None))
            open_incidents = []
        open_insights = list((await session.execute(insight_stmt)).scalars())
        resolved_by_dedupe: dict[str, list[Insight]] = {}
        for row in (await session.execute(resolved_stmt)).scalars():
            if row.dedupe_key:
                resolved_by_dedupe.setdefault(row.dedupe_key, []).append(row)
        return cls(
            baselines=baseline_rows,
            open_by_dedupe={i.dedupe_key: i for i in open_insights if i.dedupe_key},
            open_insights=open_insights,
            resolved_by_dedupe=resolved_by_dedupe,
            open_incidents=open_incidents,
            asset_states=await _load_asset_states(session, events),
        )

    @classmethod
    def empty(cls, asset_states: dict[str, dict[str, Any]] | None = None) -> BatchState:
        """Empty overlays for a rebuild replaying into cleared tables."""
        return cls({}, {}, [], {}, [], asset_states if asset_states is not None else {})

    async def step(
        self,
        session: AsyncSession,
        event: dict[str, Any],
        *,
        on_insight: Any = None,
    ) -> list[Insight]:
        """One event through the insight pipeline, in observed order.

        Evaluate -> record -> group -> resolve -> fold baselines -> fold the
        asset overlay LAST, so an event never judges itself against state it
        created. Every grouping/resolution decision is keyed on the event's
        ``(observed_at, event_id)`` position, so ingest and rebuild converge
        regardless of arrival order.
        """
        created: list[Insight] = []
        key = se.event_key(event)
        findings = await evaluate(
            session, event, asset_states=self.asset_states, baselines=self.baselines
        )
        if findings:
            for insight in await record_findings(
                session,
                event,
                findings,
                open_by_dedupe=self.open_by_dedupe,
                resolved_by_dedupe=self.resolved_by_dedupe,
            ):
                # The finding groups at its own position — replay attaches
                # it while the insight is still open, before any resolver.
                incident = await incidents.group_insight(
                    session,
                    insight,
                    open_incidents=self.open_incidents,
                    signal_key=key,
                )
                # The resolver may already be stored: a success folded on
                # time while this finding arrived late. Replay resolves the
                # insight at that resolver's position — do the same here.
                resolver = await _earliest_resolver(session, insight)
                if resolver is not None:
                    await _resolve_row(
                        session,
                        insight,
                        resolver,
                        open_insights=self.open_insights,
                        open_by_dedupe=self.open_by_dedupe,
                        resolved_by_dedupe=self.resolved_by_dedupe,
                        open_incidents=self.open_incidents,
                        on_insight=on_insight,
                        now=utcnow(),
                    )
                if (
                    insight.state == "open"
                    and insight not in self.open_insights
                    and insight not in session.deleted
                ):
                    self.open_insights.append(insight)
                if on_insight is not None:
                    await on_insight(insight, incident)
                created.append(insight)
        await resolve_for_event(
            session,
            event,
            open_insights=self.open_insights,
            open_by_dedupe=self.open_by_dedupe,
            resolved_by_dedupe=self.resolved_by_dedupe,
            open_incidents=self.open_incidents,
            on_insight=on_insight,
        )
        await bl.update_baselines(session, event, rows=self.baselines)
        asset_eid = se.event_entities(event).get("asset")
        if asset_eid:
            astate = self.asset_states.setdefault(
                asset_eid,
                se.new_asset_state(
                    asset_eid,
                    (event.get("correlation") or {}).get("asset_key")
                    or asset_eid.split("://", 1)[-1],
                ),
            )
            se.apply_asset_event(astate, event)
        return created


async def _load_asset_states(
    session: AsyncSession, events: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    """Seed asset reducer state from rows: one SELECT for the whole batch."""
    ids = {eid for eid in (se.event_entities(e).get("asset") for e in events) if eid}
    if not ids:
        return {}
    existing = {
        a.entity_id: a
        for a in (await session.execute(select(Asset).where(Asset.entity_id.in_(ids)))).scalars()
    }
    states: dict[str, dict[str, Any]] = {}
    for event in events:
        eid = se.event_entities(event).get("asset")
        if not eid or eid in states:
            continue
        row = existing.get(eid)
        if row is not None:
            states[eid] = se.asset_state_from_row(row)
        else:
            corr = event.get("correlation") or {}
            states[eid] = se.new_asset_state(eid, corr.get("asset_key") or eid.split("://", 1)[-1])
    return states


def _metrics_for(event: dict[str, Any]) -> list[tuple[str, str, float]]:
    return bl.observations_of(event)


async def evaluate(
    session: AsyncSession,
    event: dict[str, Any],
    *,
    asset_states: dict[str, dict[str, Any]] | None = None,
    baselines: dict[tuple[str, str], Baseline] | None = None,
) -> list[Finding]:
    """Run every rule against one canonical event; return new findings.

    ``asset_states`` optionally supplies in-memory asset reducer state
    (projection rebuild) so freshness checks read the fold instead of rows
    that have not been flushed yet. ``baselines`` optionally supplies the
    preloaded (entity, metric) -> Baseline map for a whole batch.
    """
    findings: list[Finding] = []
    name = str(event.get("event") or "")
    outcome = event.get("outcome")
    entities = se.event_entities(event)
    entity_id = _signal_entity(entities)
    eid = str(event.get("event_id") or "")
    key = se.event_key(event)

    # Rule: run-failure — a terminal run failure is always an insight.
    if name in se._TERMINAL_RUN_EVENTS and outcome == "failure":
        run_entity = entities.get("run")
        error = event.get("error") or {}
        findings.append(
            Finding(
                rule_id="run-failure",
                kind="recurring_failure"
                if (event.get("attributes") or {}).get("retry_of")
                else "failure",
                severity="critical",
                title=f"Run failed: {run_entity or 'unknown'}",
                summary=str(error.get("message") or "pipeline.run outcome=failure"),
                entity_id=run_entity,
                evidence_event_ids=[eid],
                recommended_action="Inspect the run timeline and failed stage.",
            )
        )

    # Rule: quality-failure — a failed quality signal on a data entity.
    # Covers quality.check/quality.validate and dbt test results; the finding
    # attaches to the checked asset/model/table, not the run.
    if name in _QUALITY_EVENTS and outcome == "failure" and entity_id:
        findings.append(
            Finding(
                rule_id="quality-failure",
                kind="quality_degradation",
                severity="warning",
                title=f"Quality check failed on {entity_id}",
                summary=str((event.get("error") or {}).get("message") or "check failed"),
                entity_id=entity_id,
                evidence_event_ids=[eid],
            )
        )

    # Rule: duration-regression — completed run duration vs rolling median.
    # Rules evaluate the event against the baseline *as of* its observed
    # position, so a late event is judged by the same window a rebuild sees.
    for ent, metric, value in _metrics_for(event):
        base = (
            baselines.get((ent, metric))
            if baselines is not None
            else await _baseline(session, ent, metric)
        )
        window = bl.window_before(base, key)
        n = len(window)
        median = statistics.median(window) if window else None
        base_metric = metric.split("|", 1)[0]
        if (
            n >= _MIN_BASELINE_SAMPLES
            and median
            and base_metric.endswith("duration_ms")
            and value > _DURATION_FACTOR * median
        ):
            findings.append(
                Finding(
                    rule_id="duration-regression",
                    kind="regression",
                    severity="warning",
                    title=f"{metric} regressed on {ent}",
                    summary=(
                        f"observed {value:.0f} vs rolling median {median:.0f} "
                        f"({value / median:.1f}x over {n} samples)"
                    ),
                    entity_id=ent,
                    evidence_event_ids=[eid],
                )
            )
        elif (
            n >= _MIN_BASELINE_SAMPLES
            and median
            and base_metric == "rows"
            and median > 0
            and value > _VOLUME_FACTOR * median
        ):
            findings.append(
                Finding(
                    rule_id="volume-spike",
                    kind="anomaly",
                    severity="warning",
                    # The metric key carries the partition suffix — the title
                    # names which partitioned baseline actually fired.
                    title=f"Volume spike on {ent} ({metric})",
                    summary=(
                        f"observed {value:.0f} rows vs rolling median "
                        f"{median:.0f} ({value / median:.1f}x over {n} samples)"
                    ),
                    entity_id=ent,
                    evidence_event_ids=[eid],
                )
            )

    # Rule: freshness-breach — asset event reporting an SLA that is already
    # exceeded by the gap since last materialization.
    asset_eid = entities.get("asset")
    sla = (event.get("attributes") or {}).get("freshness_sla_seconds")
    if asset_eid and sla:
        if asset_states is not None:
            last = se.materialized_before(asset_states.get(asset_eid) or {}, key)
        else:
            asset = await session.get(Asset, asset_eid)
            last = se.materialized_before(se.asset_state_from_row(asset), key) if asset else None
        observed = se._as_dt(event.get("observed_at"))
        if last is not None and observed is not None:
            gap = (observed - last).total_seconds()
            if gap > float(sla):
                findings.append(
                    Finding(
                        rule_id="freshness-breach",
                        kind="freshness",
                        severity="warning",
                        title=f"Freshness SLA breached on {asset_eid}",
                        summary=(
                            f"last materialized {gap / 3600:.1f}h ago; SLA {float(sla) / 3600:.1f}h"
                        ),
                        entity_id=asset_eid,
                        evidence_event_ids=[eid],
                        recommended_action="Check upstream ingest for this asset.",
                    )
                )
    return findings


def _producer_entry(event: dict[str, Any], finding: Finding) -> dict[str, Any]:
    """One producing event's record: fold-order key + the finding it carried."""
    key = se.event_key(event)
    return {
        "at": key[0].isoformat(),
        "eid": str(event.get("event_id") or ""),
        "finding": {
            "title": finding.title,
            "summary": finding.summary,
            "kind": finding.kind,
            "severity": finding.severity,
            "recommended_action": finding.recommended_action,
        },
    }


def _producers(insight: Insight) -> list[tuple[tuple[Any, str], dict[str, Any]]]:
    """The insight's producing events as sorted ``(key, entry)`` pairs.

    ``attributes["producers"]`` keys every finding by its event's
    ``(observed_at, event_id)`` position, so resolution/dedupe semantics
    follow event order and arrival order cannot change the outcome. Rows
    written before producers existed synthesize one entry anchored at the
    recorded ``observed_at``.
    """
    out: list[tuple[tuple[Any, str], dict[str, Any]]] = []
    for raw in (insight.attributes or {}).get("producers") or []:
        if isinstance(raw, dict):
            key = se._parse_key([raw.get("at"), raw.get("eid")])
            if key is not None:
                out.append((key, raw))
    if out:
        out.sort(key=lambda item: item[0])
        return out
    anchor = (insight.attributes or {}).get("observed_at")
    try:
        at = parse_rfc3339(str(anchor)) if anchor else (insight.created_at or utcnow())
    except (ValueError, TypeError):
        at = insight.created_at or utcnow()
    if getattr(at, "tzinfo", None) is None:
        at = at.replace(tzinfo=dt.UTC)
    first_eid = str((insight.evidence_event_ids or [""])[0])
    return [
        (
            (at, first_eid),
            {
                "at": at.isoformat(),
                "eid": first_eid,
                "finding": {
                    "title": insight.title,
                    "summary": (insight.attributes or {}).get("summary"),
                    "kind": (insight.attributes or {}).get("kind"),
                    "severity": insight.severity,
                    "recommended_action": insight.recommended_action,
                },
            },
        )
    ]


def _apply_producers(
    insight: Insight, producers: list[tuple[tuple[Any, str], dict[str, Any]]], now: Any
) -> None:
    """Rewrite an insight's derived fields from its producer timeline.

    The earliest producer is the insight's canonical description — replay
    creates the insight at that position — while ``last_observed_at`` and
    the evidence list span every retained producer.
    """
    producers = sorted(producers, key=lambda item: item[0])[:_MAX_PRODUCERS]
    (first_key, first), (last_key, _) = producers[0], producers[-1]
    attrs = dict(insight.attributes or {})
    attrs["producers"] = [entry for _, entry in producers]
    attrs["observed_at"] = first_key[0].isoformat()
    if last_key != first_key:
        attrs["last_observed_at"] = last_key[0].isoformat()
    else:
        attrs.pop("last_observed_at", None)
    finding = first.get("finding") or {}
    if finding.get("title"):
        insight.title = finding["title"]
    if finding.get("severity"):
        insight.severity = finding["severity"]
    if finding.get("kind"):
        attrs["kind"] = finding["kind"]
    if finding.get("summary") is not None:
        attrs["summary"] = finding["summary"]
    if finding.get("recommended_action") is not None:
        insight.recommended_action = finding["recommended_action"]
    insight.attributes = attrs
    insight.evidence_event_ids = [entry["eid"] for _, entry in producers]
    insight.updated_at = now


_MIN_KEY = (dt.datetime.min.replace(tzinfo=dt.UTC), "")


def _resolver_key(insight: Insight) -> tuple[Any, str] | None:
    """The resolving event's position, stored when the insight resolved."""
    return se._parse_key((insight.attributes or {}).get("resolved_key"))


def _open_from(insight: Insight) -> tuple[Any, str] | None:
    """The position where this row's open interval starts.

    A dedupe key's timeline partitions at resolver positions: each insight
    row covers ``[open_from, resolver)`` — the first row has no
    ``open_from`` (it starts at the beginning of history) and the open row
    has no resolver. Both keys are recorded so late arrivals can locate the
    interval a replay would have used.
    """
    return se._parse_key((insight.attributes or {}).get("open_from_key"))


async def _chain_rows(
    session: AsyncSession,
    dedupe: str,
    *,
    open_by_dedupe: dict[str, Insight] | None,
    resolved_by_dedupe: dict[str, list[Insight]] | None,
) -> list[Insight]:
    """Every insight row for one dedupe key (its timeline chain)."""
    if open_by_dedupe is None and resolved_by_dedupe is None:
        return list(
            (
                await session.execute(
                    select(Insight).where(
                        Insight.dedupe_key == dedupe,
                        Insight.state.in_(("open", "resolved")),
                    )
                )
            ).scalars()
        )
    rows: list[Insight] = []
    open_row = (open_by_dedupe or {}).get(dedupe)
    if open_row is not None and open_row.state == "open":
        rows.append(open_row)
    rows.extend((resolved_by_dedupe or {}).get(dedupe) or [])
    return [r for r in rows if r not in session.deleted]


def _row_for_position(chain: list[Insight], key: tuple[Any, str]) -> Insight | None:
    """The row whose interval contains ``key`` — where replay lands it.

    Intervals partition on the resolver's observed instant: a resolved row
    holds producers with ``open_from.observed_at < ts <= resolver.observed_at``.
    Resolved rows without a recorded resolver (pre-bookkeeping) cannot be
    placed on the timeline and contain nothing. A key at or before every
    interval start belongs to the chain's first row (replay creates the
    insight there, and later producers join it); a key past every interval
    returns ``None`` — it opens a fresh row.
    """
    best: Insight | None = None
    best_from: tuple[Any, str] | None = None
    first: Insight | None = None
    for row in chain:
        if row.state not in ("open", "resolved"):
            continue
        open_from = _open_from(row)
        resolver = _resolver_key(row) if row.state == "resolved" else None
        if row.state == "resolved" and resolver is None:
            continue  # legacy resolved row — position unknowable
        if first is None or (open_from or _MIN_KEY) < (_open_from(first) or _MIN_KEY):
            first = row
        # Intervals partition on the resolver's observed instant: a finding
        # at the same timestamp lands on the resolved row (timestamps, not
        # event_id tie-breaks — both fold orders converge on it).
        if (open_from is not None and key[0] <= open_from[0]) or (
            resolver is not None and key[0] > resolver[0]
        ):
            continue
        if best is None or (open_from or _MIN_KEY) > (best_from or _MIN_KEY):
            best, best_from = row, open_from
    if best is not None:
        return best
    if first is not None and key[0] <= (_open_from(first) or _MIN_KEY)[0]:
        return first
    return None


async def record_findings(
    session: AsyncSession,
    event: dict[str, Any],
    findings: list[Finding],
    *,
    open_by_dedupe: dict[str, Insight] | None = None,
    resolved_by_dedupe: dict[str, list[Insight]] | None = None,
) -> list[Insight]:
    """Persist findings as insights, deduping on an open insight's key.

    A repeat finding registers a producer on the row whose open interval
    contains its position (evidence, anchors and displayed fields all
    follow the producer timeline) rather than creating a duplicate — the
    dedupe key pins (rule, entity, event name, producer). Returns the
    insight rows (new and refreshed) so callers can group them into
    incidents.

    ``open_by_dedupe``/``resolved_by_dedupe`` optionally supply the batch's
    preloaded rows; new rows are registered into them so repeats inside the
    same batch dedupe identically to repeats across batches.
    """
    touched: list[Insight] = []
    now = utcnow()
    for finding in findings:
        key = se.dedupe_key(
            finding.rule_id,
            finding.entity_id,
            event,
        )
        entry = _producer_entry(event, finding)
        event_k = se.event_key(event)
        chain = await _chain_rows(
            session,
            key,
            open_by_dedupe=open_by_dedupe,
            resolved_by_dedupe=resolved_by_dedupe,
        )
        target = _row_for_position(chain, event_k)
        if target is not None:
            producers = _producers(target)
            if all(pk != event_k for pk, _ in producers):
                producers.append((event_k, entry))
            _apply_producers(target, producers, now)
            touched.append(target)
            continue
        # No row covers this position: a new open insight starts at the
        # latest resolver before it (or the start of history).
        open_from = max(
            (rk for r in chain if (rk := _resolver_key(r)) is not None and rk <= event_k),
            default=None,
        )
        row = Insight(
            insight_id=uuid.uuid4(),
            rule_id=finding.rule_id,
            rule_version=RULES[finding.rule_id],
            title=finding.title,
            severity=finding.severity,
            state="open",
            entity_id=finding.entity_id,
            evidence_event_ids=finding.evidence_event_ids,
            recommended_action=finding.recommended_action,
            recommended_action_verified=0,
            created_at=now,
            updated_at=now,
            dedupe_key=key,
            attributes={},
        )
        session.add(row)
        _apply_producers(row, [(event_k, entry)], now)
        if open_from is not None:
            row.attributes = {**(row.attributes or {}), "open_from_key": se._dump_key(open_from)}
        if open_by_dedupe is not None:
            open_by_dedupe[key] = row
        touched.append(row)
    return touched


def _resolver_view(row: Event) -> dict[str, Any]:
    """Just enough of a stored event for entity/signal computation."""
    return {
        "event_id": str(row.event_id),
        "event": row.event,
        "outcome": row.outcome,
        "observed_at": row.observed_at,
        "correlation": {
            "run_id": row.run_id,
            "asset_key": row.asset_key,
            "table": row.table_name,
            "branch": row.branch,
            "snapshot_id": row.snapshot_id,
        },
        "service": {"name": row.service_name},
        "entities": row.entities or {},
        "source": row.source or {},
    }


_RESOLVER_SCAN = 256
"""Bound on stored events scanned for a retroactive resolver."""


async def _earliest_resolver(session: AsyncSession, insight: Insight) -> tuple[Any, str] | None:
    """Earliest stored success event that resolves ``insight``.

    The resolver may already be folded when a finding arrives late (the
    success was on time, the failure was not). Replay resolves the insight
    at the earliest resolver past its first producer, so scan canonical
    events for that resolver — bounded by ``_RESOLVER_SCAN`` candidates.
    """
    producers = _producers(insight)
    if not producers or not insight.entity_id:
        return None
    first_key = producers[0][0]
    entity_id = insight.entity_id
    if insight.rule_id == "run-failure":
        rid = entity_id.rsplit("/", 1)[-1]
        stmt = select(Event).where(
            Event.event.in_(sorted(se._TERMINAL_RUN_EVENTS)),
            Event.outcome == "success",
            or_(Event.run_id == rid, Event.entities["run"].as_string() == entity_id),
        )

        def _matches(view: dict[str, Any]) -> bool:
            return se.event_entities(view).get("run") == entity_id

    elif insight.rule_id == "quality-failure":
        preds: list[Any] = [
            Event.entities[role].as_string() == entity_id
            for role in ("asset", "model", "table", "run")
        ]
        if entity_id.startswith("asset://"):
            preds.append(Event.asset_key == entity_id[len("asset://") :])
        elif entity_id.startswith("table://"):
            preds.append(Event.table_name == entity_id[len("table://") :])
        elif entity_id.startswith("run://"):
            preds.append(Event.run_id == entity_id.rsplit("/", 1)[-1])
        stmt = select(Event).where(
            Event.event.in_(sorted(_QUALITY_EVENTS)),
            Event.outcome == "success",
            or_(*preds),
        )

        def _matches(view: dict[str, Any]) -> bool:
            return _signal_entity(se.event_entities(view)) == entity_id

    else:
        return None
    # Timestamp-inclusive lower bound: a success observed at the same
    # instant as the first finding resolves it, whichever side of the
    # event_id tie-break it lands on — identical in both fold orders.
    stmt = (
        stmt.where(Event.observed_at >= first_key[0])
        .order_by(Event.observed_at, Event.event_id)
        .limit(_RESOLVER_SCAN)
    )
    for row in (await session.execute(stmt)).scalars():
        view = _resolver_view(row)
        if _matches(view):
            return se.event_key(view)
    return None


async def resolve_for_event(
    session: AsyncSession,
    event: dict[str, Any],
    *,
    open_insights: list[Insight] | None = None,
    open_by_dedupe: dict[str, Insight] | None = None,
    resolved_by_dedupe: dict[str, list[Insight]] | None = None,
    open_incidents: list[Any] | None = None,
    on_insight: Any = None,
) -> int:
    """Auto-resolve open insights when a later success arrives (§15.3).

    A successful quality signal resolves open quality-failure insights on
    the same entity; a successful terminal run resolves open run-failure
    insights on the same run. Resolution keys on event time: an insight is
    resolved only when its earliest producer precedes the success, and
    producing events observed *after* the success move to the dedupe key's
    open successor — a late success cannot silently close findings made
    after it.

    A resolver can itself arrive late: rows already resolved by a
    later-observed success are re-resolved at this event's position when
    its first producer still precedes it, matching replay order.
    """
    name = str(event.get("event") or "")
    if event.get("outcome") != "success":
        return 0
    entities = se.event_entities(event)
    targets: list[tuple[str, str]] = []
    if name in _QUALITY_EVENTS:
        signal_entity = _signal_entity(entities)
        if signal_entity:
            targets.append(("quality-failure", signal_entity))
    if name in se._TERMINAL_RUN_EVENTS and entities.get("run"):
        targets.append(("run-failure", entities["run"]))
    rkey = se.event_key(event)
    resolved = 0
    now = utcnow()
    for rule_id, entity_id in targets:
        if open_insights is None:
            rows = list(
                (
                    await session.execute(
                        select(Insight).where(
                            Insight.rule_id == rule_id,
                            Insight.entity_id == entity_id,
                            Insight.state == "open",
                        )
                    )
                ).scalars()
            )
            resolved_rows = list(
                (
                    await session.execute(
                        select(Insight).where(
                            Insight.rule_id == rule_id,
                            Insight.entity_id == entity_id,
                            Insight.state == "resolved",
                        )
                    )
                ).scalars()
            )
        else:
            rows = [
                i
                for i in open_insights
                if i.state == "open"
                and i.rule_id == rule_id
                and i.entity_id == entity_id
                and i not in session.deleted
            ]
            resolved_rows = [
                r
                for group in (resolved_by_dedupe or {}).values()
                for r in group
                if r.rule_id == rule_id and r.entity_id == entity_id and r not in session.deleted
            ]
        for row in rows + [
            r
            for r in resolved_rows
            if (rk := _resolver_key(r)) is not None
            and (_open_from(r) or _MIN_KEY)[0] < rkey[0] <= rk[0]
        ]:
            if await _resolve_row(
                session,
                row,
                rkey,
                open_insights=open_insights,
                open_by_dedupe=open_by_dedupe,
                resolved_by_dedupe=resolved_by_dedupe,
                open_incidents=open_incidents,
                on_insight=on_insight,
                now=now,
            ):
                resolved += 1
    return resolved


async def _resolve_row(
    session: AsyncSession,
    row: Insight,
    rkey: tuple[Any, str],
    *,
    open_insights: list[Insight] | None,
    open_by_dedupe: dict[str, Insight] | None,
    resolved_by_dedupe: dict[str, list[Insight]] | None,
    open_incidents: list[Any] | None,
    on_insight: Any,
    now: Any,
    force: bool = False,
) -> bool:
    """Resolve ``row`` at resolver position ``rkey``, then renormalize.

    A resolver partitions the dedupe key's timeline at its observed
    instant: producers at or before it stay on the resolved row, later
    producers belong to the interval starting there. ``_normalize_chain``
    re-derives that partition from the recorded keys, so the outcome is
    identical no matter when the resolver itself was folded. ``force``
    re-partitions an already-resolved row whose producer set changed (a
    late finding merged into it).
    """
    producers = _producers(row)
    if not producers or producers[0][0][0] > rkey[0]:
        # The resolver's observed time predates the insight's first signal:
        # at its position in a replay this row does not exist yet, so it
        # resolves an earlier chain row — or nothing — never this one.
        # (Timestamps, not full keys: a success observed at the *same*
        # instant as the failure resolves it — the event_id tie-break is an
        # ordering artifact, and both paths converge on resolution.)
        return False
    prior_resolver = _resolver_key(row)
    if row.state == "resolved":
        if prior_resolver is None:
            return False  # legacy row — cannot place its interval
        if prior_resolver <= rkey and not force:
            return False  # already resolved at or before this resolver
    row.state = "resolved"
    attrs = dict(row.attributes or {})
    attrs["resolved_by"] = rkey[1]
    attrs["resolved_key"] = se._dump_key(rkey)
    row.attributes = attrs
    row.updated_at = now
    if resolved_by_dedupe is not None and row.dedupe_key:
        bucket = resolved_by_dedupe.setdefault(row.dedupe_key, [])
        if row not in bucket:
            bucket.append(row)
    if row.dedupe_key:
        await _normalize_chain(
            session,
            row.dedupe_key,
            open_insights=open_insights,
            open_by_dedupe=open_by_dedupe,
            resolved_by_dedupe=resolved_by_dedupe,
            open_incidents=open_incidents,
            on_insight=on_insight,
            now=now,
        )
    return True


async def _normalize_chain(
    session: AsyncSession,
    dedupe: str,
    *,
    open_insights: list[Insight] | None,
    open_by_dedupe: dict[str, Insight] | None,
    resolved_by_dedupe: dict[str, list[Insight]] | None,
    open_incidents: list[Any] | None,
    on_insight: Any,
    now: Any,
) -> None:
    """Repartition a dedupe key's rows at their resolver positions.

    Resolved rows sort by resolver key into boundaries; row ``i`` covers
    ``(bounds[i-1].ts, bounds[i].ts]`` and the open row covers the rest.
    Every recorded producer is reassigned to the interval containing its
    observed instant — exactly the partition an observed-order replay
    builds, however the resolvers and findings actually arrived. Rows left
    without producers are ones replay never materialized; they are
    deleted. Producers that land on a different row re-group their
    incident memberships at their own positions.
    """
    rows = [
        r
        for r in await _chain_rows(
            session,
            dedupe,
            open_by_dedupe=open_by_dedupe,
            resolved_by_dedupe=resolved_by_dedupe,
        )
        if r not in session.deleted
    ]
    # Unplaceable rows — resolved before resolver bookkeeping (incl. manual
    # transitions, which no rebuild can reproduce) — stay out of the
    # partition and out of the deletion sweep: their producers stay put.
    placeable = [
        r
        for r in rows
        if r.state == "open" or (r.state == "resolved" and _resolver_key(r) is not None)
    ]
    resolved = sorted(
        (r for r in placeable if r.state == "resolved"),
        key=lambda r: _resolver_key(r) or _MIN_KEY,
    )
    open_rows = sorted(
        (r for r in placeable if r.state == "open"),
        key=lambda r: (r.created_at or now, str(r.insight_id)),
    )
    bounds = [rk for r in resolved if (rk := _resolver_key(r)) is not None]
    all_producers: dict[tuple[Any, str], dict[str, Any]] = {}
    placement: dict[tuple[Any, str], Insight] = {}
    for r in placeable:
        for pkey, entry in _producers(r):
            all_producers.setdefault(pkey, entry)
            placement[pkey] = r
    ordered = sorted(all_producers.items())

    assign: dict[Insight, list[tuple[tuple[Any, str], dict[str, Any]]]] = {r: [] for r in resolved}
    open_slot: list[tuple[tuple[Any, str], dict[str, Any]]] = []
    for pkey, entry in ordered:
        target = next(
            (r for r, bound in zip(resolved, bounds, strict=True) if pkey[0] <= bound[0]),
            None,
        )
        if target is None:
            open_slot.append((pkey, entry))
        else:
            assign[target].append((pkey, entry))

    # A boundary that resolves nothing is not a boundary in replay: only
    # resolved rows that actually hold producers survive.
    live_resolved = [r for r in resolved if assign[r]]
    live_bounds = [rk for r in live_resolved if (rk := _resolver_key(r)) is not None]
    open_row: Insight | None = None
    if open_slot:
        open_row = open_rows[0] if open_rows else None
        if open_row is None:
            first_finding = open_slot[0][1].get("finding") or {}
            ref = live_resolved[-1] if live_resolved else None
            open_row = Insight(
                insight_id=uuid.uuid4(),
                rule_id=ref.rule_id if ref else "",
                rule_version=ref.rule_version if ref else "",
                title=str(first_finding.get("title") or (ref.title if ref else "")),
                severity=str(first_finding.get("severity") or (ref.severity if ref else "warn")),
                state="open",
                entity_id=ref.entity_id if ref else None,
                evidence_event_ids=[],
                recommended_action=first_finding.get("recommended_action"),
                recommended_action_verified=0,
                created_at=now,
                updated_at=now,
                dedupe_key=dedupe,
                attributes={},
            )
            session.add(open_row)
        assign[open_row] = open_slot
    survivors = {r for r, ps in assign.items() if ps}

    for r in placeable:
        if r not in survivors:
            await session.delete(r)
            if open_insights is not None and r in open_insights:
                open_insights.remove(r)
            for group in (resolved_by_dedupe or {}).values():
                if r in group:
                    group.remove(r)
            if open_by_dedupe is not None and open_by_dedupe.get(dedupe) is r:
                open_by_dedupe.pop(dedupe, None)

    for i, r in enumerate(live_resolved):
        _set_open_from(r, live_bounds[i - 1] if i else None)
        _apply_producers(r, assign[r], now)
    if open_row is not None:
        _set_open_from(open_row, live_bounds[-1] if live_bounds else None)
        _apply_producers(open_row, open_slot, now)
        if open_by_dedupe is not None:
            open_by_dedupe[dedupe] = open_row
        if open_insights is not None and open_row not in open_insights:
            open_insights.append(open_row)

    # Incident memberships follow producers across rows.
    if open_incidents is not None:
        for pkey, _entry in ordered:
            old = placement.get(pkey)
            new = next(
                (r for r, ms in assign.items() if any(k == pkey for k, _ in ms)),
                None,
            )
            if new is None or old is None or new is old:
                continue
            incidents.detach_member(open_incidents, old, pkey)
            incident = await incidents.group_insight(
                session, new, open_incidents=open_incidents, signal_key=pkey
            )
            if on_insight is not None:
                await on_insight(new, incident)

    if open_row is not None:
        # A resolver may already be stored past the open row's producers —
        # replay would close it there. Recursion terminates: the retro
        # resolver adds a boundary, and the next normalize sees it.
        retro = await _earliest_resolver(session, open_row)
        if retro is not None:
            await _resolve_row(
                session,
                open_row,
                retro,
                open_insights=open_insights,
                open_by_dedupe=open_by_dedupe,
                resolved_by_dedupe=resolved_by_dedupe,
                open_incidents=open_incidents,
                on_insight=on_insight,
                now=now,
            )


def _set_open_from(row: Insight, bound: tuple[Any, str] | None) -> None:
    attrs = dict(row.attributes or {})
    if bound is None:
        attrs.pop("open_from_key", None)
    else:
        attrs["open_from_key"] = se._dump_key(bound)
    row.attributes = attrs
