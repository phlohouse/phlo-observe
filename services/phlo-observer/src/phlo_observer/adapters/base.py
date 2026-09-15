"""Source adapter protocol.

Adapters normalize producer-native payloads into canonical
:class:`EventEnvelope` dicts. A malformed source event must never crash the
observer: ``normalize`` raises :class:`AdapterError` for per-payload failures
and the ingestion layer records them as normalization errors on the raw row.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Protocol

import orjson
from observe_core.ids import new_event_id
from observe_core.models import EventEnvelope
from observe_core.timestamps import utcnow

from phlo_observer import metrics


class AdapterError(Exception):
    """Raised when a source payload cannot be normalized."""

    def __init__(self, message: str, *, code: str = "NORMALIZATION_FAILED") -> None:
        super().__init__(message)
        self.code = code


@dataclass(slots=True)
class RawPayload:
    """An external payload as received, before normalization."""

    producer: str
    source_kind: str
    body: bytes
    content_type: str = "application/json"
    source_version: str | None = None
    keep_payload: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def payload_sha256(self) -> str:
        """SHA-256 of the raw bytes for integrity/conflict checks."""
        return hashlib.sha256(self.body).hexdigest()

    def json(self) -> Any:
        """Parse the body as JSON."""
        return orjson.loads(self.body)


@dataclass(slots=True)
class NormalizedBatch:
    """Result of adapter normalization."""

    events: list[dict[str, Any]] = field(default_factory=list)
    """Canonical event dicts (post-model validation happens at persist)."""
    errors: list[dict[str, Any]] = field(default_factory=list)
    """Per-item failures: ``{"index", "code", "message"}`` dicts."""


class SourceAdapter(Protocol):
    """Normalizes one producer's payloads into canonical events.

    ``keep_payload`` controls whether the raw request body is retained in
    ``raw_events`` for this adapter (spec §36): sensitive sources set it to
    False so only the SHA-256 digest is stored.
    """

    name: str
    version: str
    keep_payload: bool = True

    def can_handle(self, payload: RawPayload) -> bool:
        """Return True when this adapter accepts the payload."""
        ...

    def normalize(self, payload: RawPayload) -> NormalizedBatch:
        """Return normalized events plus per-item errors.

        Raise :class:`AdapterError` only when the payload as a whole cannot be
        interpreted; per-item problems go in ``NormalizedBatch.errors`` so
        valid items still ingest.
        """
        ...


def envelope_for(
    *,
    event: str,
    category: str = "other",
    outcome: str = "success",
    severity: str = "info",
    delivery: str = "telemetry",
    observed_at: Any = None,
    correlation: dict[str, Any] | None = None,
    attributes: dict[str, Any] | None = None,
    source: dict[str, Any] | None = None,
    error: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a canonical event dict through the real model (validation + ids)."""
    envelope = EventEnvelope(
        event_id=new_event_id(),
        event=event,
        category=category,
        outcome=outcome,
        severity=severity,
        delivery=delivery,
        observed_at=observed_at or utcnow(),
        service={"name": (source or {}).get("producer", "external")},
        correlation=correlation or {},
        attributes=attributes or {},
        error=error,
        source=source,
    )
    return envelope.to_canonical_dict()


def record_normalization(adapter: str, status: str) -> None:
    """Metric + no-op shim for normalization bookkeeping."""
    metrics.NORMALIZATION.labels(adapter=adapter, status=status).inc()
