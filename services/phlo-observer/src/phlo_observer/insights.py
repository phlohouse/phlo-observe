"""Deterministic insight rules — spec §15.

Every insight is produced by a named, versioned rule over canonical events
and baselines; none require ML (§15.1). Insight lifecycle states are
``open``/``acknowledged``/``resolved``/``suppressed``/``expired`` (§15.3);
a subsequent success event auto-resolves matching open insights.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from observe_core.timestamps import utcnow
from sqlalchemy import func, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer import baselines as bl
from phlo_observer import incidents
from phlo_observer import state_engine as se
from phlo_observer.models import Asset, Baseline, Insight

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


def _signal_entity(entities: dict[str, str]) -> str | None:
    """Entity a quality signal attaches to — the checked object first."""
    for role in ("asset", "model", "table", "run"):
        if entities.get(role):
            return entities[role]
    return None


def _fmt_dt(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else value


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
        incident_stmt = select(incidents.Incident).where(incidents.Incident.state == "open")
        if entity_ids:
            insight_stmt = insight_stmt.where(
                or_(Insight.entity_id.in_(entity_ids), Insight.entity_id.is_(None))
            )
            dialect = getattr(getattr(session, "bind", None), "dialect", None)
            if dialect is not None and dialect.name == "postgresql":
                # jsonb ?| — incidents whose entity array overlaps the batch.
                incident_stmt = incident_stmt.where(
                    func.jsonb_exists_any(incidents.Incident.entities, sorted(entity_ids))
                )
            # Non-Postgres dialects (dev/test SQLite) keep the full open
            # scan: correct, just not bounded — datasets there stay small.
        open_insights = list((await session.execute(insight_stmt)).scalars())
        open_incidents = list((await session.execute(incident_stmt)).scalars())
        return cls(
            baselines=baseline_rows,
            open_by_dedupe={i.dedupe_key: i for i in open_insights if i.dedupe_key},
            open_insights=open_insights,
            open_incidents=open_incidents,
            asset_states=await _load_asset_states(session, events),
        )

    @classmethod
    def empty(cls, asset_states: dict[str, dict[str, Any]] | None = None) -> BatchState:
        """Empty overlays for a rebuild replaying into cleared tables."""
        return cls({}, {}, [], [], asset_states if asset_states is not None else {})

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
        created. Ingest and rebuild share this sequence so both converge.
        """
        created: list[Insight] = []
        findings = await evaluate(
            session, event, asset_states=self.asset_states, baselines=self.baselines
        )
        if findings:
            for insight in await record_findings(
                session, event, findings, open_by_dedupe=self.open_by_dedupe
            ):
                if insight not in self.open_insights:
                    self.open_insights.append(insight)
                incident = await incidents.group_insight(
                    session, insight, open_incidents=self.open_incidents
                )
                if on_insight is not None:
                    await on_insight(insight, incident)
                created.append(insight)
        await resolve_for_event(session, event, open_insights=self.open_insights)
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
            states[eid] = {
                "entity_id": row.entity_id,
                "asset_key": row.asset_key,
                "status": row.status,
                "last_materialized_at": row.last_materialized_at,
                "last_event_at": row.last_event_at,
                "freshness_sla_seconds": row.freshness_sla_seconds,
                "attributes": dict(row.attributes or {}),
                "derived_from": list((row.provenance or {}).get("derived_from") or []),
            }
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
    for ent, metric, value in _metrics_for(event):
        base = (
            baselines.get((ent, metric))
            if baselines is not None
            else await _baseline(session, ent, metric)
        )
        base_metric = metric.split("|", 1)[0]
        if (
            base
            and (base.count or 0) >= _MIN_BASELINE_SAMPLES
            and base.median
            and base_metric.endswith("duration_ms")
            and value > _DURATION_FACTOR * base.median
        ):
            findings.append(
                Finding(
                    rule_id="duration-regression",
                    kind="regression",
                    severity="warning",
                    title=f"{metric} regressed on {ent}",
                    summary=(
                        f"observed {value:.0f} vs rolling median {base.median:.0f} "
                        f"({value / base.median:.1f}x over {base.count} samples)"
                    ),
                    entity_id=ent,
                    evidence_event_ids=[eid],
                )
            )
        elif (
            base
            and (base.count or 0) >= _MIN_BASELINE_SAMPLES
            and base.median
            and base_metric == "rows"
            and base.median > 0
            and value > _VOLUME_FACTOR * base.median
        ):
            findings.append(
                Finding(
                    rule_id="volume-spike",
                    kind="anomaly",
                    severity="warning",
                    title=f"Volume spike on {ent}",
                    summary=(
                        f"observed {value:.0f} rows vs rolling median "
                        f"{base.median:.0f} ({value / base.median:.1f}x)"
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
            last = (asset_states.get(asset_eid) or {}).get("last_materialized_at")
        else:
            from phlo_observer.models import Asset  # noqa: PLC0415

            asset = await session.get(Asset, asset_eid)
            last = getattr(asset, "last_materialized_at", None) if asset else None
        observed = event.get("observed_at")
        if last is not None and observed is not None:
            gap = (
                (observed - last).total_seconds()
                if hasattr(observed - last, "total_seconds")
                else None
            )
            if gap is not None and gap > float(sla):
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


async def record_findings(
    session: AsyncSession,
    event: dict[str, Any],
    findings: list[Finding],
    *,
    open_by_dedupe: dict[str, Insight] | None = None,
) -> list[Insight]:
    """Persist findings as insights, deduping on an open insight's key.

    A repeat finding on an already-open insight refreshes ``updated_at`` and
    appends evidence rather than creating a duplicate row — the dedupe key
    pins (rule, entity, event name, producer). Returns the insight rows
    (new and refreshed) so callers can group them into incidents.

    ``open_by_dedupe`` optionally supplies the batch's preloaded open
    insights; new rows are registered into it so repeats inside the same
    batch dedupe identically to repeats across batches.
    """
    touched: list[Insight] = []
    now = utcnow()
    for finding in findings:
        key = se.dedupe_key(
            finding.rule_id,
            finding.entity_id,
            event,
        )
        if open_by_dedupe is None:
            existing = (
                await session.execute(
                    select(Insight).where(Insight.dedupe_key == key, Insight.state == "open")
                )
            ).scalar_one_or_none()
        else:
            existing = open_by_dedupe.get(key)
            if existing is not None and existing.state != "open":
                existing = None
        if existing is not None:
            existing.updated_at = now
            merged = list(existing.evidence_event_ids or [])
            for eid in finding.evidence_event_ids:
                if eid not in merged and len(merged) < 64:
                    merged.append(eid)
            existing.evidence_event_ids = merged
            touched.append(existing)
            continue
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
            attributes={
                "kind": finding.kind,
                "summary": finding.summary,
                # Event-time anchor for incident grouping: replay and
                # incremental ingest must group identically, so the window
                # keys off the producing event, not the wall clock.
                "observed_at": _fmt_dt(event.get("observed_at")),
            },
        )
        session.add(row)
        if open_by_dedupe is not None:
            open_by_dedupe[key] = row
        touched.append(row)
    return touched


async def resolve_for_event(
    session: AsyncSession,
    event: dict[str, Any],
    *,
    open_insights: list[Insight] | None = None,
) -> int:
    """Auto-resolve open insights when a later success arrives (§15.3).

    A successful quality signal resolves open quality-failure insights on
    the same entity; a successful terminal run resolves open run-failure
    insights on the same run. ``open_insights`` optionally supplies the
    batch's preloaded open rows instead of a per-event query.
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
    resolved = 0
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
        else:
            rows = [
                i
                for i in open_insights
                if i.state == "open" and i.rule_id == rule_id and i.entity_id == entity_id
            ]
        for row in rows:
            row.state = "resolved"
            row.updated_at = utcnow()
            resolved += 1
    return resolved
