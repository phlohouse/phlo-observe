"""Optional forwarding of normalized events to an OTLP/HTTP endpoint.

Speaks OTLP/HTTP JSON: canonical events are exported as log records
(``resourceLogs``) so a real OpenTelemetry Collector ``otlp`` receiver accepts
them. ``endpoint`` may be the collector base URL (``http://host:4318``) or the
full logs URL — a bare base gets ``/v1/logs`` appended, matching the OTel SDK
convention for ``OTEL_EXPORTER_OTLP_ENDPOINT``.

The caller schedules this as a background task: forwarding must never delay
or fail ingestion. Outcomes are counted on ``phlo_observer_export_total``.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any
from urllib.parse import urlsplit

import httpx
from observe_core.timestamps import parse_rfc3339

from phlo_observer import __version__, metrics

logger = logging.getLogger("phlo_observer.forward")

_TIMEOUT = httpx.Timeout(5.0)
_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)

# OTLP severity numbers (logs data model): DEBUG=5 INFO=9 WARN=13 ERROR=17.
_SEVERITY_NUMBER = {
    "debug": 5,
    "info": 9,
    "warn": 13,
    "error": 17,
    "critical": 21,
}
_SEVERITY_TEXT = {
    "debug": "DEBUG",
    "info": "INFO",
    "warn": "WARN",
    "error": "ERROR",
    "critical": "FATAL",
}


def _logs_url(endpoint: str) -> str:
    """Resolve the logs URL: bare collector bases get ``/v1/logs`` appended."""
    if urlsplit(endpoint).path.rstrip("/"):
        return endpoint
    return f"{endpoint.rstrip('/')}/v1/logs"


def _unix_nano(value: Any) -> str:
    """RFC3339/datetime -> unix nanos as a decimal string (proto3 JSON int64)."""
    if isinstance(value, str):
        value = parse_rfc3339(value)
    if not isinstance(value, dt.datetime):
        return "0"
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.UTC)
    delta = value - _EPOCH
    return str(
        delta.days * 86_400_000_000_000 + delta.seconds * 1_000_000_000 + delta.microseconds * 1_000
    )


def _otlp_value(value: Any) -> dict[str, Any]:
    """Encode a Python value as an OTLP AnyValue."""
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, dict):
        return {
            "kvlistValue": {
                "values": [
                    {"key": str(k), "value": _otlp_value(v)}
                    for k, v in value.items()
                    if v is not None
                ]
            }
        }
    if isinstance(value, (list, tuple)):
        return {"arrayValue": {"values": [_otlp_value(v) for v in value]}}
    if value is None:
        return {}
    return {"stringValue": str(value)}


def _flatten(prefix: str, value: Any, out: list[dict[str, Any]]) -> None:
    """Append ``prefix.<nested>`` attributes; dicts flatten one level at a time."""
    if value is None:
        return
    if isinstance(value, dict):
        for key, nested in value.items():
            _flatten(f"{prefix}.{key}", nested, out)
        return
    out.append({"key": prefix, "value": _otlp_value(value)})


def _log_record(event: dict[str, Any], received_ns: str) -> dict[str, Any]:
    """Map one canonical event dict onto an OTLP log record."""
    attrs: list[dict[str, Any]] = []
    for key in ("event_id", "schema_version", "category", "outcome", "delivery"):
        _flatten(f"observe.{key}", event.get(key), attrs)
    _flatten("observe.duration_ms", event.get("duration_ms"), attrs)
    for key in ("started_at", "ended_at"):
        value = event.get(key)
        if isinstance(value, dt.datetime):
            value = value.isoformat()
        _flatten(f"observe.{key}", value, attrs)
    _flatten("correlation", event.get("correlation"), attrs)
    _flatten("attributes", event.get("attributes"), attrs)
    _flatten("error", event.get("error"), attrs)
    _flatten("source", event.get("source"), attrs)
    severity = str(event.get("severity") or "info")
    return {
        "timeUnixNano": _unix_nano(event.get("observed_at")),
        "observedTimeUnixNano": received_ns,
        "severityNumber": _SEVERITY_NUMBER.get(severity, 9),
        "severityText": _SEVERITY_TEXT.get(severity, "INFO"),
        "body": {"stringValue": str(event.get("event") or "")},
        "attributes": attrs,
    }


def otlp_logs_payload(events: list[dict[str, Any]], received_ns: str) -> dict[str, Any]:
    """Build the ``resourceLogs`` request, grouping records by service."""
    resources: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for event in events:
        service = event.get("service") or {}
        key = (
            str(service.get("name") or "unknown"),
            str(service.get("version") or ""),
            str(service.get("environment") or ""),
        )
        resources.setdefault(key, []).append(_log_record(event, received_ns))
    return {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": name}},
                        {"key": "service.version", "value": {"stringValue": version}},
                        {
                            "key": "deployment.environment",
                            "value": {"stringValue": environment},
                        },
                    ]
                },
                "scopeLogs": [
                    {
                        "scope": {"name": "phlo-observer", "version": __version__},
                        "logRecords": records,
                    }
                ],
            }
            for (name, version, environment), records in resources.items()
        ]
    }


async def forward_events(events: list[dict[str, Any]], endpoint: str) -> None:
    """POST canonical events as OTLP/HTTP JSON logs; failures are logged+counted."""
    if not events:
        return
    url = _logs_url(endpoint)
    received_ns = _unix_nano(dt.datetime.now(dt.UTC))
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(url, json=otlp_logs_payload(events, received_ns))
        ok = resp.status_code < 400
        metrics.EXPORT.labels(destination="otlp", status="success" if ok else "error").inc(
            len(events)
        )
        if not ok:
            logger.warning("OTLP forward to %s returned %s", url, resp.status_code)
    except Exception:
        metrics.EXPORT.labels(destination="otlp", status="error").inc(len(events))
        logger.exception("OTLP forward to %s failed", url)
