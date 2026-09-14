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
    get_runtime().emit(builder, None)
