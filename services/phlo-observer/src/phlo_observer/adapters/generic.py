"""Generic source adapter.

Last-resort adapter for ``POST /v1/ingest/generic``: accepts any JSON object,
wraps it in a canonical envelope, preserves the body under
``attributes.payload`` (normalized, redacted downstream) so nothing from the
source system is silently discarded while still producing a queryable event.
"""

from __future__ import annotations

from observe_core.serialization import normalize_value
from pydantic import ValidationError

from phlo_observer.adapters.base import (
    AdapterError,
    NormalizedBatch,
    RawPayload,
    envelope_for,
)

_CORRELATION_KEYS = (
    "trace_id",
    "span_id",
    "parent_span_id",
    "run_id",
    "job_id",
    "invocation_id",
    "asset_key",
    "partition_key",
    "branch",
    "table",
    "snapshot_id",
    "pipeline",
    "experiment_id",
    "request_id",
)


class GenericAdapter:
    """Wraps arbitrary JSON payloads into ``external.<kind>`` events."""

    name = "generic"
    version = "1.0"
    keep_payload = True

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
        # Correlation: canonical keys found at the payload's top level, with
        # explicit request metadata winning, so generic events still join runs.
        correlation = {
            key: body.get(key)
            for key in _CORRELATION_KEYS
            if isinstance(body, dict) and body.get(key) is not None
        }
        correlation.update({k: v for k, v in payload.metadata.items() if v is not None})
        try:
            event = envelope_for(
                event="external.source_event",
                category="other",
                outcome="unknown",
                correlation=correlation,
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
        except (ValidationError, ValueError) as exc:
            return NormalizedBatch(
                errors=[{"index": 0, "code": "SCHEMA_INVALID", "message": str(exc)}]
            )
        batch = NormalizedBatch()
        batch.add_event(event, index=0)
        return batch
