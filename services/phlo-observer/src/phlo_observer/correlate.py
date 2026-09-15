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

from phlo_observer import metrics


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
