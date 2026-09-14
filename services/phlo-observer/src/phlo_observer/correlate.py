"""Deterministic correlation and run-projection maintenance.

Correlation precedence (spec §39):

1. explicit ``run_id`` — confidence 1.0;
2. explicit ``trace_id`` — links the event to the run that already owns that
   trace when one exists;
3. producer-native invocation/run IDs map to ``run_id`` only when the event
   itself carries both (no proximity guessing).

Ambiguous events stay uncorrelated rather than being guessed into a run.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer import metrics
from phlo_observer.models import Event, Run

_TERMINAL_RUN_EVENTS = {"pipeline.run"}
_STATUS_PRECEDENCE = {
    "unknown": 0,
    "running": 1,
    "partial": 2,
    "cancelled": 3,
    "success": 4,
    "failure": 5,
}
_WARNING_SEVERITIES = {"warn", "error", "critical"}


def correlation_method(event: dict[str, Any]) -> str | None:
    """Record how an event was correlated, for observability of correlation."""
    corr = event.get("correlation") or {}
    if corr.get("run_id"):
        metrics.CORRELATION.labels(method="explicit_run_id").inc()
        return "explicit_run_id"
    if corr.get("trace_id"):
        metrics.CORRELATION.labels(method="trace_id").inc()
        return "trace_id"
    if corr.get("invocation_id"):
        metrics.CORRELATION.labels(method="producer_invocation").inc()
        return "producer_invocation"
    metrics.CORRELATION.labels(method="uncorrelated").inc()
    return None


def _resolve_run_id(event: dict[str, Any]) -> str | None:
    corr = event.get("correlation") or {}
    return corr.get("run_id") or None


async def update_run_projection(session: AsyncSession, event: Event) -> None:
    """Upsert the ``runs`` row for an event's run_id.

    Safe for late-arriving events: counters increment, status upgrades to a
    terminal state when an explicit ``pipeline.run`` outcome arrives.
    """
    run_id = event.run_id
    if not run_id:
        return
    run = await session.get(Run, run_id)
    now = event.received_at
    if run is None:
        run = Run(run_id=run_id, status="unknown", updated_at=now, summary={})
        session.add(run)
    run.updated_at = now
    run.event_count = (run.event_count or 0) + 1
    if event.error is not None or event.severity in ("error", "critical"):
        run.error_count = (run.error_count or 0) + 1
    elif event.severity in _WARNING_SEVERITIES:
        run.warning_count = (run.warning_count or 0) + 1
    if event.asset_key:
        assets = set((run.summary or {}).get("asset_keys") or [])
        if event.asset_key not in assets:
            assets.add(event.asset_key)
            run.asset_count = (run.asset_count or 0) + 1
            run.summary = {**(run.summary or {}), "asset_keys": sorted(assets)}
    if event.branch and not run.branch:
        run.branch = event.branch
    for field, value in (
        ("job_name", event.job_id),
        ("service_name", event.service_name),
        ("environment", event.environment),
    ):
        if value and not getattr(run, field):
            setattr(run, field, value)

    if event.event in _TERMINAL_RUN_EVENTS:
        # Explicit run-level signal wins; outcome=unknown means "started".
        run.status = "running" if event.outcome == "unknown" else event.outcome
        run.started_at = event.started_at or run.started_at
        run.ended_at = event.ended_at or event.observed_at or run.ended_at
        run.duration_ms = event.duration_ms or run.duration_ms
        trigger = (event.attributes or {}).get("trigger")
        if trigger:
            run.trigger = trigger
    elif run.status == "unknown" and run.started_at is None:
        run.status = "running"
        run.started_at = event.started_at or event.observed_at

    if run.started_at is None and (event.started_at or event.observed_at):
        run.started_at = event.started_at or event.observed_at


async def link_trace_to_run(session: AsyncSession, event: Event) -> None:
    """When an event has a trace_id but no run_id, reuse an existing run's."""
    if event.run_id or not event.trace_id:
        return
    result = await session.execute(
        select(Event.run_id)
        .where(Event.trace_id == event.trace_id, Event.run_id.is_not(None))
        .limit(1)
    )
    run_id = result.scalar_one_or_none()
    if run_id:
        event.run_id = run_id
        event.correlation_method = "trace_id"
        metrics.CORRELATION.labels(method="trace_id").inc()
