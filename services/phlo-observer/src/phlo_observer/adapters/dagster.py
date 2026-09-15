"""Dagster source adapter.

Accepts Dagster event payloads — a Dagster run/events JSON object or an array
of event records — and normalizes the allowlisted meaningful types:

- run start / success / failure / cancel
- asset materialization
- asset check result
- step start / success / failure

Other Dagster engine events are intentionally not mirrored.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from observe_core.timestamps import parse_rfc3339

from phlo_observer.adapters.base import (
    AdapterError,
    NormalizedBatch,
    RawPayload,
    envelope_for,
)

_ALLOWED_RUN_STATUS = {
    "STARTED": ("pipeline.run", "pipeline", "unknown", "info"),
    "STARTING": ("pipeline.run", "pipeline", "unknown", "info"),
    "SUCCESS": ("pipeline.run", "pipeline", "success", "info"),
    "FAILURE": ("pipeline.run", "pipeline", "failure", "error"),
    "CANCELED": ("pipeline.run", "pipeline", "cancelled", "warn"),
    "CANCELING": ("pipeline.run", "pipeline", "unknown", "info"),
}
_STEP_STATUS = {
    "STEP_START": ("pipeline.step", "pipeline", "unknown", "info"),
    "STEP_SUCCESS": ("pipeline.step", "pipeline", "success", "info"),
    "STEP_FAILURE": ("pipeline.step", "pipeline", "failure", "error"),
}
_MATERIALIZATION = {"ASSET_MATERIALIZATION"}
_CHECK = {"ASSET_CHECK_EVALUATION", "ASSET_CHECK"}
_ALLOWED_TYPES = set(_ALLOWED_RUN_STATUS) | set(_STEP_STATUS) | _MATERIALIZATION | _CHECK


def _event_type(record: dict[str, Any]) -> str | None:
    """Best-effort Dagster event type across common payload shapes."""
    for key in ("event_type", "eventType", "type", "dagster_event_type"):
        value = record.get(key)
        if isinstance(value, str):
            return value.upper()
    dagster_event = record.get("dagster_event")
    if isinstance(dagster_event, dict):
        return _event_type(dagster_event)
    event_specific = record.get("event_specific_data") or record.get("eventSpecificData") or {}
    if isinstance(event_specific, dict):
        nested = event_specific.get("materialization")
        if nested is not None:
            return "ASSET_MATERIALIZATION"
    return None


def _observed_at(record: dict[str, Any]) -> Any:
    raw = record.get("timestamp") or record.get("event_timestamp")
    if isinstance(raw, (int, float)):
        return dt.datetime.fromtimestamp(raw, tz=dt.UTC)
    if isinstance(raw, str):
        try:
            return parse_rfc3339(raw)
        except ValueError:
            return None
    return None


def _asset_key(record: dict[str, Any]) -> str | None:
    key = record.get("asset_key") or record.get("assetKey")
    if isinstance(key, dict):
        path = key.get("path")
        if isinstance(path, list):
            return ".".join(str(p) for p in path)
    if isinstance(key, str):
        return key
    for nested_key in ("event_specific_data", "eventSpecificData"):
        nested = record.get(nested_key)
        if isinstance(nested, dict):
            found = _asset_key(nested)
            if found:
                return found
    return None


class DagsterAdapter:
    """Normalizes Dagster run/step/asset events into canonical events."""

    name = "dagster"
    version = "1.0"

    def can_handle(self, payload: RawPayload) -> bool:
        """Accept Dagster-looking payloads: records with dagster-ish fields."""
        try:
            body = payload.json()
        except Exception:
            return False
        records = body if isinstance(body, list) else [body]
        if not records or not isinstance(records[0], dict):
            return False
        return any(
            k in records[0]
            for k in (
                "run_id",
                "dagster_run_id",
                "event_type",
                "dagster_event",
                "pipeline_name",
                "job_name",
            )
        )

    def normalize(self, payload: RawPayload) -> NormalizedBatch:
        """Normalize allowlisted Dagster records; skip the rest."""
        try:
            body = payload.json()
        except Exception as exc:
            raise AdapterError(f"payload is not valid JSON: {exc}") from exc
        records = body if isinstance(body, list) else [body]
        events: list[dict[str, Any]] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            event_type = _event_type(record)
            if event_type is None or event_type not in _ALLOWED_TYPES:
                continue  # deliberately not mirrored
            run_id = record.get("run_id") or record.get("dagster_run_id")
            job = record.get("job_name") or record.get("pipeline_name")
            source = {
                "producer": "dagster",
                "kind": event_type,
                "adapter": f"{self.name}.{self.version}",
            }
            correlation = {
                "run_id": run_id,
                "job_id": job,
                "asset_key": _asset_key(record),
                "partition_key": record.get("partition") or record.get("partition_key"),
                "pipeline": job,
            }
            if event_type in _MATERIALIZATION:
                events.append(
                    envelope_for(
                        event="asset.materialize",
                        category="data",
                        outcome="success",
                        observed_at=_observed_at(record),
                        correlation=correlation,
                        attributes={
                            "asset_key": _asset_key(record),
                            "dagster_event_type": event_type,
                        },
                        source=source,
                    )
                )
            elif event_type in _CHECK:
                passed = record.get("passed")
                if passed is None:
                    eval_data = record.get("event_specific_data") or {}
                    if isinstance(eval_data, dict):
                        passed = eval_data.get("passed")
                check_name = record.get("check_name") or record.get("check_name_label")
                events.append(
                    envelope_for(
                        event="quality.check",
                        category="quality",
                        outcome="success" if passed else "failure",
                        severity="info" if passed else "error",
                        observed_at=_observed_at(record),
                        correlation=correlation,
                        attributes={"check_name": check_name, "passed": bool(passed)},
                        source=source,
                    )
                )
            else:
                mapping = _ALLOWED_RUN_STATUS.get(event_type) or _STEP_STATUS[event_type]
                name, category, outcome, severity = mapping
                events.append(
                    envelope_for(
                        event=name,
                        category=category,
                        outcome=outcome,
                        severity=severity,
                        observed_at=_observed_at(record),
                        correlation=correlation,
                        attributes={
                            "dagster_event_type": event_type,
                            "step_key": record.get("step_key"),
                            "job_name": job,
                        },
                        source=source,
                    )
                )
        if not events:
            raise AdapterError("payload contained no normalizable Dagster event types")
        return NormalizedBatch(events=events)
