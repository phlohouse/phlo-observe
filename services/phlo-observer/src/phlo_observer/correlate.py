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
from phlo_observer.models import Event


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
        # No metric increment here: correlation_method() already counted this
        # event once under "trace_id" at staging time.
        event.run_id = run_id
        event.correlation_method = "trace_id"
