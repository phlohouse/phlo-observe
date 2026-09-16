"""dbt artifact adapter — normalizes ``run_results.json`` documents.

Accepts a raw run_results JSON document, or ``{"run_results": ...,
"manifest": ...}`` for the artifacts endpoint (manifest supplies metadata
only). Mirrors the SDK-side normalization so client and server agree.
"""

from __future__ import annotations

from phlo_observe.integrations.dbt import manifest_metadata, run_results_events
from pydantic import ValidationError

from phlo_observer.adapters.base import (
    AdapterError,
    NormalizedBatch,
    RawPayload,
    envelope_for,
)

# Reuse the SDK's pure-Python parser so client-side and server-side
# normalization stay identical by construction.


class DbtAdapter:
    """Normalizes dbt run_results documents into canonical events."""

    name = "dbt"
    version = "1.0"
    keep_payload = True

    def can_handle(self, payload: RawPayload) -> bool:
        """Accept documents with a dbt-ish ``results``/``metadata`` shape."""
        try:
            body = payload.json()
        except Exception:
            return False
        doc = body.get("run_results", body) if isinstance(body, dict) else None
        return isinstance(doc, dict) and "results" in doc and "metadata" in doc

    def normalize(self, payload: RawPayload) -> NormalizedBatch:
        """Return canonical events for invocation + each node result."""
        try:
            body = payload.json()
        except Exception as exc:
            raise AdapterError(f"payload is not valid JSON: {exc}") from exc
        if not isinstance(body, dict):
            raise AdapterError("dbt payload must be a JSON object")
        doc = body.get("run_results", body)
        manifest = body.get("manifest")
        if not isinstance(doc, dict):
            raise AdapterError("run_results document must be a JSON object")
        try:
            payloads = run_results_events(doc)
        except Exception as exc:
            raise AdapterError(f"invalid run_results document: {exc}") from exc
        extra_meta = manifest_metadata(manifest) if isinstance(manifest, dict) else {}
        batch = NormalizedBatch()
        for index, item in enumerate(payloads):
            attrs = dict(item.get("attributes") or {})
            for key in ("project_name", "adapter_type"):
                if key not in attrs and extra_meta.get(key):
                    attrs[key] = extra_meta[key]
            try:
                event = envelope_for(
                    event=item["event"],
                    category=item.get("category", "other"),
                    outcome=item.get("outcome", "unknown"),
                    severity=item.get("severity")
                    or ("error" if item.get("outcome") == "failure" else "info"),
                    duration_ms=item.get("duration_ms"),
                    correlation=item.get("correlation"),
                    attributes=attrs,
                    entities=item.get("entities"),
                    tags=item.get("tags"),
                    source={
                        "producer": "dbt",
                        "kind": "run_results",
                        "adapter": f"{self.name}.{self.version}",
                    },
                )
            except (ValidationError, ValueError, KeyError) as exc:
                # One malformed result must not reject the batch (spec §35).
                batch.errors.append({"index": index, "code": "SCHEMA_INVALID", "message": str(exc)})
                continue
            batch.add_event(event, index=index)
        return batch
