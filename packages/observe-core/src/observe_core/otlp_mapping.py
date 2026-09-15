"""Canonical event <-> OTLP log-record attribute mapping.

Shared by the observe-core OTLP drain (export) and phlo-observer's OTLP
forwarder and ingest adapter (import), so all three encode the envelope the
same way. Attribute names use the ``observe.`` prefix:

- ``observe.<flat field>`` — schema_version, event_id, event, category,
  outcome, severity, delivery, started_at, ended_at, duration_ms,
  observed_at (scalar values);
- ``observe.trace_id`` / ``observe.span_id`` — shortcuts for trace joining;
- ``observe.correlation.<key>`` — every canonical correlation key;
- ``observe.correlation.extra`` — JSON object of non-canonical correlation;
- ``observe.service`` / ``observe.error`` / ``observe.source`` /
  ``observe.attributes`` / ``observe.entities`` / ``observe.tags`` /
  ``observe.contract`` — JSON-encoded envelope sections.

The OTel-native record fields ``traceId``/``spanId`` are separate transport
metadata; these attributes are the canonical-envelope encoding.
"""

from __future__ import annotations

from typing import Any

from observe_core.models import CORRELATION_KEYS
from observe_core.serialization import dumps

ATTR_PREFIX = "observe."
"""Attribute namespace used for canonical-envelope fields in OTLP records."""

_FLAT_FIELDS = (
    "schema_version",
    "event_id",
    "event",
    "category",
    "outcome",
    "severity",
    "delivery",
    "started_at",
    "ended_at",
    "duration_ms",
    "observed_at",
)
"""Envelope fields carried verbatim as ``observe.<name>`` attributes."""

_JSON_SECTIONS = ("service", "error", "source", "attributes", "entities", "tags", "contract")
"""Envelope sections carried as JSON strings under ``observe.<name>``."""


def event_to_otlp_attributes(data: dict[str, Any]) -> dict[str, Any]:
    """Encode a canonical event dict as OTLP record attributes.

    Correlation identifiers are preserved verbatim under
    ``observe.correlation.*`` (plus ``observe.trace_id``/``observe.span_id``
    shortcuts) so events can be joined to real traces and runs downstream.
    """
    attributes: dict[str, Any] = {}
    for key in _FLAT_FIELDS:
        value = data.get(key)
        if value is not None:
            attributes[f"{ATTR_PREFIX}{key}"] = value
    correlation = data.get("correlation") or {}
    for key in CORRELATION_KEYS:
        value = correlation.get(key)
        if value is not None:
            attributes[f"{ATTR_PREFIX}correlation.{key}"] = value
    extra = correlation.get("extra")
    if extra:
        attributes[f"{ATTR_PREFIX}correlation.extra"] = dumps(extra).decode("utf-8")
    if correlation.get("trace_id"):
        attributes[f"{ATTR_PREFIX}trace_id"] = correlation["trace_id"]
    if correlation.get("span_id"):
        attributes[f"{ATTR_PREFIX}span_id"] = correlation["span_id"]
    for section in _JSON_SECTIONS:
        value = data.get(section)
        if value:
            attributes[f"{ATTR_PREFIX}{section}"] = dumps(value).decode("utf-8")
    return attributes
