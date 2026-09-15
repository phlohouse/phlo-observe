"""The event builder yielded by ``observe()`` and consumed by the emit pipeline."""

from __future__ import annotations

import datetime as dt
from typing import Any

from observe_core.models import (
    CORRELATION_KEYS,
    Category,
    Delivery,
    ErrorInfo,
    Outcome,
    Severity,
    SourceInfo,
    severity_at_least,
)

_WARNINGS_KEY = "warnings"
_ANNOTATIONS_KEY = "annotations"
_OBSERVE_META_KEY = "_observe"


class EventBuilder:
    """Accumulates attributes, correlation, warnings and error state.

    This is a deliberately lightweight Python object: setting a field is a dict
    write, with no FFI or I/O on the hot path.
    """

    __slots__ = (
        "_explicit_severity",
        "attributes",
        "capture_stacktrace",
        "category",
        "correlation",
        "delivery",
        "duration_ms",
        "ended_at",
        "entities",
        "error",
        "event",
        "outcome",
        "severity",
        "source",
        "started_at",
        "tags",
    )

    def __init__(
        self,
        event: str,
        *,
        category: Category = Category.OTHER,
        delivery: Delivery = Delivery.TELEMETRY,
        severity: Severity | None = None,
        attributes: dict[str, Any] | None = None,
        capture_stacktrace: bool | None = None,
    ) -> None:
        self.event = event
        self.category = category
        self.delivery = delivery
        self.severity = severity or Severity.INFO
        self._explicit_severity = severity is not None
        self.outcome = Outcome.UNKNOWN
        self.attributes: dict[str, Any] = dict(attributes) if attributes else {}
        self.correlation: dict[str, Any] = {}
        self.error: ErrorInfo | None = None
        self.source: SourceInfo | None = None
        self.entities: dict[str, str] = {}
        """Canonical entity identifiers by role (V2 envelope, spec §9.2)."""
        self.tags: dict[str, str] = {}
        self.started_at: dt.datetime | None = None
        self.ended_at: dt.datetime | None = None
        self.duration_ms: float | None = None
        self.capture_stacktrace = capture_stacktrace
        """Per-operation stacktrace override; None defers to settings."""

    def set(self, **attributes: Any) -> None:
        """Set multiple attribute values at once."""
        self.attributes.update(attributes)

    def set_attribute(self, key: str, value: Any) -> None:
        """Set one attribute."""
        self.attributes[key] = value

    def set_correlation(self, **values: Any) -> None:
        """Set correlation identifiers. Unknown keys go to ``correlation.extra``."""
        for key, value in values.items():
            if value is not None:
                self.correlation[key] = value

    def set_outcome(self, outcome: Outcome | str) -> None:
        """Override the outcome (otherwise inferred from exceptions)."""
        self.outcome = Outcome(outcome)

    def set_severity(self, severity: Severity | str) -> None:
        """Override severity."""
        self.severity = Severity(severity)
        self._explicit_severity = True

    def set_delivery(self, delivery: Delivery | str) -> None:
        """Override the durability class."""
        self.delivery = Delivery(delivery)

    def set_error(self, error: ErrorInfo) -> None:
        """Attach a structured error without marking failure implicitly."""
        self.error = error

    def set_source(self, source: SourceInfo) -> None:
        """Set the canonical source object."""
        self.source = source

    def set_entity(self, role: str, identifier: object) -> None:
        """Record a canonical entity this event involves (spec §9.2/§13).

        ``identifier`` may be an ``EntityId`` or a ``namespace://path`` URI
        string; values are stored verbatim so custom namespaces work.
        """
        self.entities[role] = str(identifier)

    def set_tag(self, key: str, value: object) -> None:
        """Attach a searchable label (spec §23)."""
        self.tags[str(key)] = str(value)

    def add_warning(self, *, code: str, message: str, **details: Any) -> None:
        """Record a structured warning inside event attributes."""
        warnings = self.attributes.setdefault(_WARNINGS_KEY, [])
        warnings.append({"code": code, "message": message, **details})
        if not self._explicit_severity and not severity_at_least(self.severity, Severity.WARN):
            self.severity = Severity.WARN

    def annotate(self, note: str) -> None:
        """Attach a human-readable note (structured attribute, not a log line)."""
        self.attributes.setdefault(_ANNOTATIONS_KEY, []).append(str(note))

    def canonical_correlation(self) -> dict[str, str | None]:
        """Correlation restricted to canonical keys (null-filled)."""
        return {key: _as_optional_str(self.correlation.get(key)) for key in CORRELATION_KEYS}

    def correlation_extra(self) -> dict[str, Any]:
        """Correlation values that are not canonical keys."""
        return {k: v for k, v in self.correlation.items() if k not in CORRELATION_KEYS}


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
