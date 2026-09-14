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


def _phase_for(event_name: str) -> str:
    for prefix, phase in _PHASE_BY_EVENT_PREFIX.items():
        if event_name.startswith(prefix):
            return phase
    return "other"


def _event_summary(row: Event) -> dict[str, Any]:
    return {
        "event_id": str(row.event_id),
        "event": row.event,
        "category": row.category,
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


async def run_timeline(session: AsyncSession, run_id: str) -> dict[str, Any] | None:
    """Return the run projection plus its events grouped into phases.

    Ordering is deterministic: producer ``observed_at`` first, ``event_id``
    (UUIDv7) as the stable tie-breaker. ``None`` when the run is unknown.
    """
    run = await session.get(Run, run_id)
    stmt = (
        select(Event)
        .where(Event.run_id == run_id)
        .order_by(asc(Event.observed_at), asc(Event.event_id))
    )
    events = list((await session.execute(stmt)).scalars())
    if run is None and not events:
        return None
    phases: dict[str, list[dict[str, Any]]] = {}
    for row in events:
        phases.setdefault(_phase_for(row.event), []).append(_event_summary(row))
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
        "events": [_event_summary(row) for row in events],
    }


async def event_by_id(session: AsyncSession, event_id: str) -> Event | None:
    """Look up one event by its UUID."""
    try:
        return await session.get(Event, uuid.UUID(event_id))
    except ValueError:
        return None
