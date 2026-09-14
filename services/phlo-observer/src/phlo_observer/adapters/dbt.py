"""dbt artifact adapter — normalizes ``run_results.json`` documents.

Accepts a raw run_results JSON document, or ``{"run_results": ...,
"manifest": ...}`` for the artifacts endpoint (manifest supplies metadata
only). Mirrors the SDK-side normalization so client and server agree.
"""

from __future__ import annotations

from typing import Any

from phlo_observe.integrations.dbt import manifest_metadata, run_results_events

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
        body = payload.json()
        doc = body.get("run_results", body)
        manifest = body.get("manifest") if isinstance(body, dict) else None
        try:
            payloads = run_results_events(doc)
        except Exception as exc:
            raise AdapterError(f"invalid run_results document: {exc}") from exc
        extra_meta = manifest_metadata(manifest) if isinstance(manifest, dict) else {}
        events: list[dict[str, Any]] = []
        for item in payloads:
            attrs = dict(item.get("attributes") or {})
            for key in ("project_name", "adapter_type"):
                if key not in attrs and extra_meta.get(key):
                    attrs[key] = extra_meta[key]
            events.append(
                envelope_for(
                    event=item["event"],
                    category=item.get("category", "other"),
                    outcome=item.get("outcome", "unknown"),
                    severity="error" if item.get("outcome") == "failure" else "info",
                    correlation=item.get("correlation"),
                    attributes=attrs,
                    source={
                        "producer": "dbt",
                        "kind": "run_results",
                        "adapter": f"{self.name}.{self.version}",
                    },
                )
            )
        return NormalizedBatch(events=events)
