"""Run timeline projection: ordered events grouped by phase for one run."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import asc, select
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer.models import Event, Run
from phlo_observer.store import _fmt

_PHASE_BY_EVENT_PREFIX = {
    "pipeline.": "pipeline",
    "asset.": "asset",
    "quality.": "quality",
    "table.": "table",
    "wap.": "wap",
    "external.": "external",
}

_LABELS = {
    "pipeline.run": "Run",
    "pipeline.step": "Step",
    "ingestion.load": "Ingest",
    "ingestion.extract": "Extract",
    "asset.materialize": "Materialize",
    "quality.validate": "Validate",
    "quality.check": "Check",
    "wap.branch.create": "Create branch",
    "wap.promote": "Promote",
    "table.commit": "Commit",
    "iceberg.snapshot.create": "Snapshot",
    "dbt.run": "dbt run",
    "dbt.test": "dbt test",
    "external.source_event": "External event",
    "observer.ingest": "Observer ingest",
}


def _phase_for(event_name: str) -> str:
    for prefix, phase in _PHASE_BY_EVENT_PREFIX.items():
        if event_name.startswith(prefix):
            return phase
    return "other"


def _label_for(event_name: str) -> str:
    """Human label for a step; observer owns this derivation (spec §40)."""
    if event_name in _LABELS:
        return _LABELS[event_name]
    tail = event_name.rsplit(".", 1)[-1]
    return tail.replace("_", " ").title() if tail else event_name


def _summary_for(row: Event) -> str | None:
    """One-line step summary derived from the event's own fields."""
    attrs = row.attributes or {}
    if isinstance(row.error, dict) and row.error.get("message"):
        return str(row.error["message"])
    name = row.event
    if name.startswith("ingestion."):
        rows = attrs.get("rows_out", attrs.get("rows_in"))
        if isinstance(rows, int | float):
            return f"{int(rows):,} rows"
    if name == "quality.validate":
        total = attrs.get("checks_total")
        if isinstance(total, int) and total:
            return f"{attrs.get('checks_passed') or 0}/{total} checks passed"
        return attrs.get("suite")
    if name == "quality.check":
        check = attrs.get("check_name")
        verdict = "passed" if attrs.get("passed") else "failed"
        return f"{check} {verdict}" if check else verdict
    if name == "wap.promote":
        branch = attrs.get("branch") or row.branch
        target = attrs.get("target")
        return f"{branch} -> {target}" if branch and target else branch
    if name == "wap.branch.create":
        branch = attrs.get("branch") or row.branch
        return f"branch {branch}" if branch else None
    if name.startswith("asset.") and row.asset_key:
        return row.asset_key
    if name == "pipeline.step":
        return attrs.get("step_key")
    if name == "pipeline.run":
        return attrs.get("job") or row.job_id
    if row.asset_key:
        return row.asset_key
    if row.table_name:
        return row.table_name
    return None


def _event_summary(row: Event) -> dict[str, Any]:
    return {
        "event_id": str(row.event_id),
        "event": row.event,
        "category": row.category,
        "label": _label_for(row.event),
        "summary": _summary_for(row),
        "outcome": row.outcome,
        "severity": row.severity,
        "observed_at": _fmt(row.observed_at),
        "received_at": _fmt(row.received_at),
        "duration_ms": row.duration_ms,
        "correlation": {
            "trace_id": row.trace_id,
            "span_id": row.span_id,
            "asset_key": row.asset_key,
            "partition_key": row.partition_key,
            "branch": row.branch,
        },
        "attributes": row.attributes,
        "error": row.error,
        "correlation_method": row.correlation_method,
    }


# Spec §84's latency target covers runs up to 10,000 events; the cap is set
# there so the tested bound still returns a complete timeline.
_MAX_TIMELINE_EVENTS = 10_000


async def run_timeline(session: AsyncSession, run_id: str) -> dict[str, Any] | None:
    """Return the run projection plus its events grouped into phases.

    Ordering is deterministic: producer ``observed_at`` first, ``event_id``
    (UUIDv7) as the stable tie-breaker. ``None`` when the run is unknown.
    The event list is bounded; ``truncated`` flags runs with more than
    ``_MAX_TIMELINE_EVENTS`` events.
    """
    run = await session.get(Run, run_id)
    stmt = (
        select(Event)
        .where(Event.run_id == run_id)
        .order_by(asc(Event.observed_at), asc(Event.event_id))
        .limit(_MAX_TIMELINE_EVENTS + 1)
    )
    events = list((await session.execute(stmt)).scalars())
    truncated = len(events) > _MAX_TIMELINE_EVENTS
    events = events[:_MAX_TIMELINE_EVENTS]
    if run is None and not events:
        return None
    phases: dict[str, list[dict[str, Any]]] = {}
    for row in events:
        phases.setdefault(_phase_for(row.event), []).append(_event_summary(row))
    steps = [_event_summary(row) for row in events]
    return {
        "run": {
            "run_id": run_id,
            "status": run.status if run else "unknown",
            "job_name": run.job_name if run else None,
            "service_name": run.service_name if run else None,
            "environment": run.environment if run else None,
            "branch": run.branch if run else None,
            "trigger": run.trigger if run else None,
            "started_at": _fmt(run.started_at) if run else None,
            "ended_at": _fmt(run.ended_at) if run else None,
            "duration_ms": run.duration_ms if run else None,
            "event_count": run.event_count if run else len(events),
            "error_count": run.error_count if run else 0,
            "warning_count": run.warning_count if run else 0,
            "asset_count": run.asset_count if run else 0,
            "summary": run.summary if run else {},
        },
        "phases": phases,
        "steps": steps,
        "events": steps,
        "truncated": truncated,
    }


async def event_by_id(session: AsyncSession, event_id: str) -> Event | None:
    """Look up one event by its UUID."""
    try:
        return await session.get(Event, uuid.UUID(event_id))
    except ValueError:
        return None
