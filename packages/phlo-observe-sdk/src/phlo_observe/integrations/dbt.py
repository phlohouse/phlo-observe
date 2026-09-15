"""dbt artifact normalization.

Parses ``run_results.json`` and ``manifest.json`` into canonical event
payloads. Pure JSON parsing — no dbt dependency — so this module works
without the ``dbt`` extra. The observer performs the same normalization
server-side for pushed artifacts; this module is for client-side emission.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from observe_core import event
from observe_core.identifiers import model_id, run_id_for
from observe_core.models import SourceInfo

from phlo_observe import events as E

_NODE_TYPE_EVENTS = {
    "model": E.DBT_MODEL_EXECUTE,
    "test": E.DBT_TEST_EXECUTE,
}
_TEST_RESOURCE_TYPES = {"test", "data_test", "schema_test", "unit_test"}


def _load(path_or_dict: str | Path | dict[str, Any]) -> dict[str, Any]:
    if isinstance(path_or_dict, dict):
        return path_or_dict
    return json.loads(Path(path_or_dict).read_text())


def manifest_metadata(path_or_dict: str | Path | dict[str, Any]) -> dict[str, Any]:
    """Extract small identifying metadata from a ``manifest.json``."""
    manifest = _load(path_or_dict)
    metadata = manifest.get("metadata", {})
    return {
        "dbt_version": metadata.get("dbt_version"),
        "project_name": metadata.get("project_name"),
        "adapter_type": metadata.get("adapter_type"),
        "generated_at": metadata.get("generated_at"),
        "invocation_id": metadata.get("invocation_id"),
    }


def run_results_events(path_or_dict: str | Path | dict[str, Any]) -> list[dict[str, Any]]:
    """Convert ``run_results.json`` into canonical event payloads.

    One ``dbt.invocation`` event plus one ``dbt.model.execute`` or
    ``dbt.test.execute`` per node result. Returns dicts ready to emit; use
    :func:`emit_run_results` to emit them directly.
    """
    results_doc = _load(path_or_dict)
    metadata = results_doc.get("metadata", {})
    invocation_id = metadata.get("invocation_id")
    dbt_version = metadata.get("dbt_version")
    elapsed_s = results_doc.get("elapsed_time")

    events: list[dict[str, Any]] = [
        {
            "event": E.DBT_INVOCATION,
            "category": "pipeline",
            "attributes": {
                "invocation_id": invocation_id,
                "dbt_version": dbt_version,
                "elapsed_time_s": elapsed_s,
                "args": results_doc.get("args"),
                "results_count": len(results_doc.get("results", [])),
            },
            "correlation": {"invocation_id": invocation_id},
            "entities": ({"run": str(run_id_for("dbt", invocation_id))} if invocation_id else {}),
        }
    ]

    for result in results_doc.get("results", []):
        unique_id = result.get("unique_id", "")
        resource_type = unique_id.split(".", 1)[0] if "." in unique_id else ""
        is_test = resource_type in _TEST_RESOURCE_TYPES or result.get("unique_id", "").startswith(
            ("test.", "unit_test.", "data_test.")
        )
        name = unique_id.split(".")[-1] if unique_id else None
        adapter_response = result.get("adapter_response") or {}
        failures = result.get("failures")
        attrs: dict[str, Any] = {
            "unique_id": unique_id or None,
            "name": name,
            "status": result.get("status"),
            "execution_time_s": result.get("execution_time"),
            "relation_name": result.get("relation_name"),
            "compiled": result.get("compiled"),
            "failures": failures,
            "rows_affected": adapter_response.get("rows_affected"),
            "adapter_code": adapter_response.get("code"),
            "adapter_message": adapter_response.get("message"),
        }
        outcome = {
            "success": "success",
            "pass": "success",
            "error": "failure",
            "fail": "failure",
            "skipped": "cancelled",
        }.get(str(result.get("status", "")).lower(), "unknown")
        events.append(
            {
                "event": E.DBT_TEST_EXECUTE
                if is_test
                else _NODE_TYPE_EVENTS.get(resource_type, E.DBT_MODEL_EXECUTE),
                "category": "quality" if is_test else "data",
                "outcome": outcome,
                "attributes": {k: v for k, v in attrs.items() if v is not None},
                "correlation": {"invocation_id": invocation_id},
                "entities": (
                    {
                        "run": str(run_id_for("dbt", invocation_id)),
                        **({"model": str(model_id("dbt", name))} if name and not is_test else {}),
                    }
                    if invocation_id
                    else ({"model": str(model_id("dbt", name))} if name and not is_test else {})
                ),
            }
        )
    return events


def emit_run_results(path_or_dict: str | Path | dict[str, Any]) -> int:
    """Emit normalized dbt run-results events through the runtime. Returns count."""
    source = SourceInfo(producer="dbt", kind="run_results")
    payloads = run_results_events(path_or_dict)
    for payload in payloads:
        event(
            payload["event"],
            category=payload.get("category"),
            outcome=payload.get("outcome"),
            attributes=payload.get("attributes"),
            correlation=payload.get("correlation"),
            entities=payload.get("entities"),
            source=source,
        )
    return len(payloads)
