"""OTLP/HTTP JSON adapter — normalizes ``ExportLogsServiceRequest`` payloads.

Each ``logRecord`` becomes one canonical event:

- ``timeUnixNano`` (falling back to ``observedTimeUnixNano``) -> observed_at;
- ``severityNumber``/``severityText`` -> canonical severity;
- ``traceId``/``spanId`` -> correlation.trace_id / correlation.span_id;
- resource ``service.*`` attributes -> canonical service block;
- an ``event.name``/``event`` log attribute matching the event-name pattern
  becomes the canonical event name, otherwise ``otlp.log``;
- remaining record attributes land in ``attributes``; resource attributes are
  preserved under ``resource.*`` keys and the record body under ``body``.

This is the mapping documented for the observe-core OTLP drain (spec §13.4):
events exported as OTel log records re-enter as canonical events here.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

from observe_core.timestamps import utcnow

from phlo_observer.adapters.base import (
    AdapterError,
    NormalizedBatch,
    RawPayload,
    envelope_for,
)

_EVENT_NAME = re.compile(r"^[a-z0-9_]+(\.[a-z0-9_]+)*$")

_SEVERITY_BY_NUMBER = [
    (5, "trace"),
    (9, "debug"),
    (13, "info"),
    (17, "warn"),
    (21, "error"),
    (25, "critical"),
]
_SEVERITY_BY_TEXT = {
    "TRACE": "trace",
    "DEBUG": "debug",
    "INFO": "info",
    "WARN": "warn",
    "ERROR": "error",
    "FATAL": "critical",
}


def _any_value(value: Any) -> Any:
    """Decode an OTLP ``AnyValue`` into a plain Python value."""
    if not isinstance(value, dict):
        return value
    for key in ("stringValue", "string_value"):
        if key in value:
            return value[key]
    for key in ("intValue", "int_value", "doubleValue", "double_value", "boolValue", "bool_value"):
        if key in value:
            return value[key]
    for key in ("bytesValue", "bytes_value"):
        if key in value:
            return {"_encoding": "base64", "bytes": value[key]}
    for key in ("arrayValue", "array_value"):
        if key in value:
            return [_any_value(v) for v in (value[key] or {}).get("values") or []]
    for key in ("kvlistValue", "kvlist_value"):
        if key in value:
            return _kv_list((value[key] or {}).get("values"))
    return None


def _kv_list(items: Any) -> dict[str, Any]:
    """Decode an OTLP ``KeyValue`` list into a dict."""
    out: dict[str, Any] = {}
    if not isinstance(items, list):
        return out
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("key"), str):
            out[item["key"]] = _any_value(item.get("value"))
    return out


def _nanos_to_dt(value: Any) -> Any:
    try:
        nanos = int(value)
    except (TypeError, ValueError):
        return None
    return dt.datetime.fromtimestamp(nanos / 1e9, tz=dt.UTC)


def _severity(record: dict[str, Any]) -> str:
    raw = record.get("severityNumber") or record.get("severity_number")
    try:
        number = int(raw) if raw is not None else 0
    except (TypeError, ValueError):
        number = 0
    if number:
        for bound, name in _SEVERITY_BY_NUMBER:
            if number < bound:
                return name
        return "critical"
    text = str(record.get("severityText") or record.get("severity_text") or "").upper()
    return _SEVERITY_BY_TEXT.get(text[:5].rstrip("0123456789"), "info")


class OtlpAdapter:
    """Normalizes OTLP/HTTP JSON log exports into canonical events."""

    name = "otlp"
    version = "1.0"
    keep_payload = True

    def can_handle(self, payload: RawPayload) -> bool:
        """Accept documents carrying OTLP ``resourceLogs``."""
        try:
            body = payload.json()
        except Exception:
            return False
        return isinstance(body, dict) and ("resourceLogs" in body or "resource_logs" in body)

    def normalize(self, payload: RawPayload) -> NormalizedBatch:
        """Map every logRecord to one canonical event."""
        try:
            body = payload.json()
        except Exception as exc:
            raise AdapterError(f"payload is not valid JSON: {exc}") from exc
        if not isinstance(body, dict):
            raise AdapterError("OTLP payload must be a JSON object")
        resource_logs = body.get("resourceLogs") or body.get("resource_logs")
        if not isinstance(resource_logs, list):
            raise AdapterError("OTLP payload has no resourceLogs")
        events: list[dict[str, Any]] = []
        for resource_log in resource_logs:
            if not isinstance(resource_log, dict):
                continue
            resource = resource_log.get("resource") or {}
            resource_attrs = _kv_list(resource.get("attributes"))
            service_name = str(resource_attrs.get("service.name") or "otlp")
            service_attrs = {
                "name": service_name,
                "version": resource_attrs.get("service.version"),
                "instance_id": resource_attrs.get("service.instance.id"),
            }
            scopes = resource_log.get("scopeLogs") or resource_log.get("scope_logs") or []
            for scope_log in scopes:
                if not isinstance(scope_log, dict):
                    continue
                scope_name = ((scope_log.get("scope") or {}).get("name")) or None
                records = scope_log.get("logRecords") or scope_log.get("log_records") or []
                events.extend(
                    self._record_event(record, resource_attrs, service_attrs, scope_name)
                    for record in records
                    if isinstance(record, dict)
                )
        if not events:
            raise AdapterError("OTLP payload contained no logRecords")
        return NormalizedBatch(events=events)

    def _record_event(
        self,
        record: dict[str, Any],
        resource_attrs: dict[str, Any],
        service_attrs: dict[str, Any],
        scope_name: str | None,
    ) -> dict[str, Any]:
        record_attrs = _kv_list(record.get("attributes"))
        event_name = record_attrs.pop("event.name", None) or record_attrs.pop("event", None)
        if not (isinstance(event_name, str) and _EVENT_NAME.match(event_name)):
            event_name = "otlp.log"
        attributes: dict[str, Any] = {
            f"resource.{k}": v for k, v in resource_attrs.items() if v is not None
        }
        attributes.update(record_attrs)
        if scope_name:
            attributes["otel.scope"] = scope_name
        body = _any_value(record.get("body"))
        if body is not None:
            attributes["body"] = body
        observed = _nanos_to_dt(record.get("timeUnixNano") or record.get("time_unix_nano"))
        if observed is None:
            observed = _nanos_to_dt(
                record.get("observedTimeUnixNano") or record.get("observed_time_unix_nano")
            )
        envelope = envelope_for(
            event=event_name,
            category="other",
            outcome="unknown",
            severity=_severity(record),
            observed_at=observed or utcnow(),
            correlation={
                "trace_id": record.get("traceId") or record.get("trace_id"),
                "span_id": record.get("spanId") or record.get("span_id"),
            },
            attributes=attributes,
            source={
                "producer": "otlp",
                "kind": "logs",
                "adapter": f"{self.name}.{self.version}",
            },
        )
        envelope["service"] = {k: v for k, v in service_attrs.items() if v is not None}
        if not envelope["service"].get("name"):
            envelope["service"]["name"] = "otlp"
        return envelope
