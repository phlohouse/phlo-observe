"""Instantaneous event emission (``event()``) and the internal emit helper."""

from __future__ import annotations

from typing import Any

from observe_core.builder import EventBuilder
from observe_core.models import (
    Category,
    Delivery,
    ErrorInfo,
    Outcome,
    Severity,
    SourceInfo,
)
from observe_core.runtime import get_runtime


def event(
    name: str,
    *,
    category: Category | str | None = None,
    delivery: Delivery | str | None = None,
    severity: Severity | str | None = None,
    outcome: Outcome | str | None = None,
    attributes: dict[str, Any] | None = None,
    correlation: dict[str, Any] | None = None,
    error: ErrorInfo | None = None,
    source: SourceInfo | None = None,
    entities: dict[str, object] | None = None,
    tags: dict[str, object] | None = None,
) -> None:
    """Emit an instantaneous event (no measured duration).

    Example::

        event("wap.promote", category="wap", delivery="critical",
              attributes={"branch": "run/01J", "target": "main"})
    """
    builder = EventBuilder(
        name,
        category=Category(category) if category else Category.OTHER,
        delivery=Delivery(delivery) if delivery else Delivery.TELEMETRY,
        severity=Severity(severity) if severity else None,
        attributes=attributes,
    )
    if correlation:
        builder.set_correlation(**correlation)
    if outcome is not None:
        builder.outcome = Outcome(outcome)
    elif error is not None:
        builder.outcome = Outcome.FAILURE
        if severity is None:
            builder.severity = Severity.ERROR
    if error is not None:
        builder.error = error
    if source is not None:
        builder.source = source
    if entities:
        for role, identifier in entities.items():
            builder.set_entity(role, identifier)
    if tags:
        for key, value in tags.items():
            builder.set_tag(key, value)
    get_runtime().emit(builder, None)


def metric(
    name: str,
    value: float,
    *,
    dimensions: dict[str, str] | None = None,
    aggregate: bool = True,
    flush_after_seconds: float = 60.0,
) -> None:
    """Record a metric sample (spec §7.11 — metrics are event-shaped too).

    With ``aggregate=True`` (default) samples accumulate in the runtime's
    :class:`MetricAggregator`; :func:`flush_metrics` emits one summary event
    per series. With ``aggregate=False`` an immediate ``metric.recorded``
    event is emitted for low-cardinality measurements.
    """
    runtime = get_runtime()
    if aggregate and runtime.aggregator.record(name, float(value), dimensions):
        runtime.stats.incr("aggregated_events")
        return
    # aggregate=False, or the series bound rejected the sample — emit an
    # immediate event instead of silently dropping it.
    attrs: dict[str, Any] = {"metric": name, "value": float(value)}
    if dimensions:
        attrs["dimensions"] = dict(dimensions)
    event(
        "metric.recorded",
        category="metric",
        delivery="telemetry",
        attributes=attrs,
    )


def flush_metrics() -> int:
    """Emit aggregated metric summaries and clear the accumulator.

    Returns the number of series flushed. Call on a cadence (timer, job end)
    or rely on :func:`observe_core.runtime.flush`, which drains pending
    summaries too.
    """
    summaries = get_runtime().aggregator.flush()
    for summary in summaries:
        event(
            "metric.summary",
            category="metric",
            delivery="telemetry",
            attributes=summary,
        )
    return len(summaries)
