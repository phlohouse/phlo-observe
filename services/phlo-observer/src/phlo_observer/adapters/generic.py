"""Generic source adapter.

Last-resort adapter for ``POST /v1/ingest/generic``: accepts any JSON object,
wraps it in a canonical envelope, preserves the body under
``attributes.payload`` (normalized, redacted downstream) so nothing from the
source system is silently discarded while still producing a queryable event.
"""

from __future__ import annotations

from observe_core.serialization import normalize_value

from phlo_observer.adapters.base import (
    AdapterError,
    NormalizedBatch,
    RawPayload,
    envelope_for,
)


class GenericAdapter:
    """Wraps arbitrary JSON payloads into ``external.<kind>`` events."""

    name = "generic"
    version = "1.0"

    def can_handle(self, payload: RawPayload) -> bool:
        """Accept any JSON object/array payload."""
        try:
            body = payload.json()
        except Exception:
            return False
        return isinstance(body, (dict, list))

    def normalize(self, payload: RawPayload) -> NormalizedBatch:
        """Wrap the payload in one canonical event."""
        try:
            body = payload.json()
        except Exception as exc:
            raise AdapterError(f"payload is not valid JSON: {exc}") from exc
        producer = payload.producer if payload.producer != "generic" else "external"
        return NormalizedBatch(
            events=[
                envelope_for(
                    event="external.source_event",
                    category="other",
                    outcome="unknown",
                    correlation={
                        "run_id": payload.metadata.get("run_id"),
                        "trace_id": payload.metadata.get("trace_id"),
                    },
                    attributes={
                        "producer": payload.producer,
                        "source_kind": payload.source_kind,
                        "payload": normalize_value(body),
                    },
                    source={
                        "producer": producer,
                        "kind": payload.source_kind,
                        "adapter": f"{self.name}.{self.version}",
                    },
                )
            ]
        )
