"""Canonical adapter: payloads already in event-envelope form.

Accepts a single event object or an array; validates each envelope through
the pydantic model so invalid entries surface as per-item errors.
"""

from __future__ import annotations

from observe_core.models import EventEnvelope
from pydantic import ValidationError

from phlo_observer.adapters.base import AdapterError, NormalizedBatch, RawPayload


class CanonicalAdapter:
    """Pass-through normalizer for canonical ``/v1/events`` payloads."""

    name = "canonical"
    version = "1.0"
    keep_payload = True

    def can_handle(self, payload: RawPayload) -> bool:
        """Accept when the body parses to a dict with ``event`` or a list."""
        try:
            body = payload.json()
        except Exception:
            return False
        if isinstance(body, list):
            return True
        return isinstance(body, dict) and "event" in body

    def normalize(self, payload: RawPayload) -> NormalizedBatch:
        """Validate items individually; bad ones become per-item errors."""
        try:
            body = payload.json()
        except Exception as exc:
            raise AdapterError(f"payload is not valid JSON: {exc}") from exc
        items = body if isinstance(body, list) else [body]
        batch = NormalizedBatch()
        for index, item in enumerate(items):
            try:
                event = EventEnvelope.model_validate(item).to_canonical_dict()
            except ValidationError as exc:
                first = exc.errors()[0]
                batch.errors.append(
                    {
                        "index": index,
                        "code": "SCHEMA_INVALID",
                        "message": f"{'.'.join(str(p) for p in first['loc'])}: {first['msg']}",
                    }
                )
                continue
            batch.add_event(event, index=index)
        return batch
