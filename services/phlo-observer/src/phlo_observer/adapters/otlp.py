"""OTLP/HTTP JSON adapter — normalizes ``ExportLogsServiceRequest`` payloads.

Two record shapes are handled:

- **observe-core encoded records** (marked by an ``observe.event_id``
  attribute) restore the canonical envelope from ``observe.*`` attributes —
  correlation, event id/name, timing, severity, service, error, source and
  attributes — so events exported by the observe-core OTLP drain (or this
  observer's own OTLP forwarder) round-trip with run/trace correlation intact.
- **plain OTel records** normalize generically: ``timeUnixNano`` (falling
  back to ``observedTimeUnixNano``) -> observed_at;
  ``severityNumber``/``severityText`` -> canonical severity;
  ``traceId``/``spanId`` -> correlation.trace_id / correlation.span_id
  (with ``observe.*`` attribute fallbacks); resource ``service.*`` attributes
  -> canonical service block; an ``event.name``/``event`` log attribute
  matching the event-name pattern becomes the canonical event name, otherwise
  ``otlp.log``; remaining record attributes land in ``attributes``, resource
  attributes under ``resource.*`` keys, and the record body under ``body``.

This is the mapping documented for the observe-core OTLP drain (spec §13.4):
events exported as OTel log records re-enter as canonical events here.
"""

from __future__ import annotations

import datetime as dt
import re
from typing import Any

import orjson
from observe_core.ids import new_event_id
from observe_core.models import (
    CORRELATION_KEYS,
    ContractRef,
    ErrorInfo,
    EventEnvelope,
    ServiceInfo,
    SourceInfo,
)
from observe_core.timestamps import parse_rfc3339, utcnow

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
    "CRITICAL": "critical",
    "FATAL": "critical",
}

_SERVICE_FIELDS = frozenset(ServiceInfo.model_fields)
_SOURCE_FIELDS = frozenset(SourceInfo.model_fields)
_ERROR_FIELDS = frozenset(ErrorInfo.model_fields)
_CONTRACT_FIELDS = frozenset(ContractRef.model_fields)


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
    return _SEVERITY_BY_TEXT.get(text, _SEVERITY_BY_TEXT.get(text[:5].rstrip("0123456789"), "info"))


def _json_section(value: Any) -> Any:
    """Decode a JSON-encoded ``observe.<section>`` attribute (string or dict)."""
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    try:
        return orjson.loads(value)
    except orjson.JSONDecodeError:
        return None


def _dt_attr(value: Any) -> Any:
    """Parse an ``observe.*`` timestamp attribute (RFC3339 string) safely."""
    if isinstance(value, str):
        try:
            return parse_rfc3339(value)
        except ValueError:
            return None
    return value if isinstance(value, dt.datetime) else None


def _num_attr(value: Any) -> float | None:
    """Coerce an ``observe.*`` numeric attribute; OTLP int64s arrive as strings."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _event_name(candidate: Any) -> str | None:
    """Return ``candidate`` when it is a valid canonical event name."""
    if isinstance(candidate, str) and _EVENT_NAME.match(candidate):
        return candidate
    return None


def _trace_correlation(record: dict[str, Any], record_attrs: dict[str, Any]) -> dict[str, Any]:
    """Trace/span ids from OTel-native fields, then ``observe.*`` attributes."""
    return {
        "trace_id": record.get("traceId")
        or record.get("trace_id")
        or record_attrs.get("observe.correlation.trace_id")
        or record_attrs.get("observe.trace_id"),
        "span_id": record.get("spanId")
        or record.get("span_id")
        or record_attrs.get("observe.correlation.span_id")
        or record_attrs.get("observe.span_id"),
    }


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
        indices: list[int] = []
        errors: list[dict[str, Any]] = []
        position = 0
        for resource_log in resource_logs:
            if not isinstance(resource_log, dict):
                continue
            resource = resource_log.get("resource") or {}
            resource_attrs = _kv_list(resource.get("attributes"))
            service_attrs = {
                "name": str(resource_attrs.get("service.name") or "otlp"),
                "version": resource_attrs.get("service.version"),
                "instance_id": resource_attrs.get("service.instance.id"),
                "environment": resource_attrs.get("deployment.environment"),
            }
            scopes = resource_log.get("scopeLogs") or resource_log.get("scope_logs") or []
            for scope_log in scopes:
                if not isinstance(scope_log, dict):
                    continue
                scope_name = ((scope_log.get("scope") or {}).get("name")) or None
                records = scope_log.get("logRecords") or scope_log.get("log_records") or []
                for record in records:
                    index, position = position, position + 1
                    if not isinstance(record, dict):
                        continue
                    try:
                        events.append(
                            self._record_event(record, resource_attrs, service_attrs, scope_name)
                        )
                        indices.append(index)
                    except Exception as exc:
                        # One bad record must not reject the batch; the
                        # envelope model re-validates restored fields anyway.
                        errors.append(
                            {
                                "index": index,
                                "code": "SCHEMA_INVALID",
                                "message": f"logRecord {index} cannot be normalized: {exc}",
                            }
                        )
        if not events and not errors:
            raise AdapterError("OTLP payload contained no logRecords")
        return NormalizedBatch(events=events, errors=errors, indices=indices)

    def _record_event(
        self,
        record: dict[str, Any],
        resource_attrs: dict[str, Any],
        service_attrs: dict[str, Any],
        scope_name: str | None,
    ) -> dict[str, Any]:
        record_attrs = _kv_list(record.get("attributes"))
        if "observe.event_id" in record_attrs:
            return self._canonical_event(
                record, record_attrs, resource_attrs, service_attrs, scope_name
            )
        event_name = _event_name(record_attrs.pop("event.name", None)) or _event_name(
            record_attrs.pop("event", None)
        )
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
            event=event_name or "otlp.log",
            category="other",
            outcome="unknown",
            severity=_severity(record),
            observed_at=observed or utcnow(),
            correlation=_trace_correlation(record, record_attrs),
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

    def _canonical_event(
        self,
        record: dict[str, Any],
        attrs: dict[str, Any],
        resource_attrs: dict[str, Any],
        service_attrs: dict[str, Any],
        scope_name: str | None,
    ) -> dict[str, Any]:
        """Rebuild an observe-core encoded envelope from ``observe.*`` attrs.

        The encoded event id, correlation, timing and structured sections are
        authoritative; OTel-native record fields fill any gaps. The envelope
        model re-validates every restored value, so malformed metadata cannot
        smuggle a non-canonical event through.
        """
        correlation: dict[str, Any] = {}
        for key in CORRELATION_KEYS:
            value = attrs.get(f"observe.correlation.{key}")
            if value is not None:
                correlation[key] = str(value)
        for key, value in _trace_correlation(record, attrs).items():
            correlation.setdefault(key, value)
        extra = _json_section(attrs.get("observe.correlation.extra"))
        if isinstance(extra, dict):
            correlation["extra"] = extra

        service = _json_section(attrs.get("observe.service"))
        if isinstance(service, dict):
            service = {
                key: value
                for key, value in service.items()
                if key in _SERVICE_FIELDS and value is not None
            }
        else:
            service = {k: v for k, v in service_attrs.items() if v is not None}
        if not service.get("name"):
            service["name"] = "otlp"

        source = _json_section(attrs.get("observe.source"))
        if isinstance(source, dict):
            source = {
                key: value
                for key, value in source.items()
                if key in _SOURCE_FIELDS and value is not None
            }
        else:
            source = {"kind": "logs"}
        source["adapter"] = f"{self.name}.{self.version}"
        source.setdefault("producer", "otlp")

        error = _json_section(attrs.get("observe.error"))
        if isinstance(error, dict):
            error = {
                key: value
                for key, value in error.items()
                if key in _ERROR_FIELDS and value is not None
            }
            if not error.get("message"):
                error = None
        else:
            error = None

        entities = _json_section(attrs.get("observe.entities"))
        entities = dict(entities) if isinstance(entities, dict) else {}
        tags = _json_section(attrs.get("observe.tags"))
        tags = dict(tags) if isinstance(tags, dict) else {}
        contract = _json_section(attrs.get("observe.contract"))
        contract = (
            {key: value for key, value in contract.items() if key in _CONTRACT_FIELDS}
            if isinstance(contract, dict)
            else None
        )

        attributes = _json_section(attrs.get("observe.attributes"))
        attributes = dict(attributes) if isinstance(attributes, dict) else {}
        # Anything not part of the observe.* encoding (collector-added
        # metadata) is preserved alongside the restored attributes.
        for key, value in attrs.items():
            if not key.startswith("observe.") and key not in ("event.name", "event"):
                attributes.setdefault(key, value)
        for key, value in resource_attrs.items():
            if value is not None and key not in (
                "service.name",
                "service.version",
                "service.instance.id",
                "deployment.environment",
            ):
                attributes.setdefault(f"resource.{key}", value)
        if scope_name:
            attributes["otel.scope"] = scope_name

        body = _any_value(record.get("body"))
        event_name = (
            _event_name(attrs.get("observe.event"))
            or _event_name(body)
            or _event_name(attrs.get("event.name"))
            or "otlp.log"
        )
        observed = (
            _dt_attr(attrs.get("observe.observed_at"))
            or _nanos_to_dt(record.get("timeUnixNano") or record.get("time_unix_nano"))
            or _nanos_to_dt(
                record.get("observedTimeUnixNano") or record.get("observed_time_unix_nano")
            )
            or utcnow()
        )
        severity_fields = (
            record.get("severityNumber")
            or record.get("severity_number")
            or record.get("severityText")
            or record.get("severity_text")
        )
        severity = (
            _severity(record) if severity_fields else str(attrs.get("observe.severity") or "info")
        )
        envelope = EventEnvelope(
            schema_version=str(attrs.get("observe.schema_version") or "1.0"),
            event_id=str(attrs.get("observe.event_id") or new_event_id()),
            event=event_name,
            category=str(attrs.get("observe.category") or "other"),
            outcome=str(attrs.get("observe.outcome") or "unknown"),
            severity=severity,
            delivery=str(attrs.get("observe.delivery") or "telemetry"),
            started_at=_dt_attr(attrs.get("observe.started_at")),
            ended_at=_dt_attr(attrs.get("observe.ended_at")),
            duration_ms=_num_attr(attrs.get("observe.duration_ms")),
            observed_at=observed,
            service=service,
            correlation=correlation,
            attributes=attributes,
            error=error,
            source=source,
            entities=entities,
            tags=tags,
            contract=contract,
        )
        return envelope.to_canonical_dict()
