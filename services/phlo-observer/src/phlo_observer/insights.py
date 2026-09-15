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
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer import baselines as bl
from phlo_observer import state_engine as se
from phlo_observer.models import Baseline, Insight

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


def _metrics_for(event: dict[str, Any]) -> list[tuple[str, str, float]]:
    return bl.observations_of(event)


async def evaluate(session: AsyncSession, event: dict[str, Any]) -> list[Finding]:
    """Run every rule against one canonical event; return new findings."""
    findings: list[Finding] = []
    name = str(event.get("event") or "")
    outcome = event.get("outcome")
    entities = se.event_entities(event)
    entity_id = entities.get("asset") or entities.get("run")
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

    # Rule: quality-failure — failed quality.check on an asset.
    if name == "quality.check" and outcome == "failure" and entity_id:
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
        base = await _baseline(session, ent, metric)
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
    session: AsyncSession, event: dict[str, Any], findings: list[Finding]
) -> list[Insight]:
    """Persist findings as insights, deduping on an open insight's key.

    A repeat finding on an already-open insight refreshes ``updated_at`` and
    appends evidence rather than creating a duplicate row — the dedupe key
    pins (rule, entity, event name, producer). Returns the insight rows
    (new and refreshed) so callers can group them into incidents.
    """
    touched: list[Insight] = []
    now = utcnow()
    for finding in findings:
        key = se.dedupe_key(
            finding.rule_id,
            finding.entity_id,
            event,
        )
        existing = (
            await session.execute(
                select(Insight).where(Insight.dedupe_key == key, Insight.state == "open")
            )
        ).scalar_one_or_none()
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
            attributes={"kind": finding.kind, "summary": finding.summary},
        )
        session.add(row)
        touched.append(row)
    return touched


async def resolve_for_event(session: AsyncSession, event: dict[str, Any]) -> int:
    """Auto-resolve open insights when a later success arrives (§15.3).

    A successful ``quality.check`` resolves open quality-failure insights on
    the same asset; a successful terminal run resolves open run-failure
    insights on the same run.
    """
    name = str(event.get("event") or "")
    if event.get("outcome") != "success":
        return 0
    entities = se.event_entities(event)
    targets: list[tuple[str, str]] = []
    if name == "quality.check" and entities.get("asset"):
        targets.append(("quality-failure", entities["asset"]))
    if name in se._TERMINAL_RUN_EVENTS and entities.get("run"):
        targets.append(("run-failure", entities["run"]))
    resolved = 0
    for rule_id, entity_id in targets:
        rows = (
            (
                await session.execute(
                    select(Insight).where(
                        Insight.rule_id == rule_id,
                        Insight.entity_id == entity_id,
                        Insight.state == "open",
                    )
                )
            )
            .scalars()
            .all()
        )
        for row in rows:
            row.state = "resolved"
            row.updated_at = utcnow()
            resolved += 1
    return resolved
