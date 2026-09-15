"""OTLP ingest of observe-core encoded records (spec §13.4 / §50 pattern 2).

Events exported through the observe-core OTLP drain or the observer's own
OTLP forwarder carry the full canonical envelope in ``observe.*`` record
attributes. Re-ingesting such a record must restore correlation (run_id,
trace/span), the original event id/name, timing, severity, service, error,
source and attributes — otherwise Collector-routed events silently lose run
correlation and become new, uncorrelated events.
"""

from __future__ import annotations

import uuid
from typing import Any

import orjson
import pytest
from httpx import AsyncClient
from observe_core.otlp_mapping import event_to_otlp_attributes
from phlo_observer.adapters import ADAPTERS, RawPayload

_SEVERITY_NUMBER = {
    "trace": 1,
    "debug": 5,
    "info": 9,
    "warn": 13,
    "error": 17,
    "critical": 24,
}


def _any(value: Any) -> dict[str, Any]:
    """Encode a Python value as an OTLP AnyValue (as a collector forwards)."""
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    return {"stringValue": str(value)}


def observe_record(event: dict[str, Any], **record_overrides: Any) -> dict[str, Any]:
    """Encode a canonical event dict as an OTLP logRecord, drain-style."""
    record = {
        "timeUnixNano": "1735689601500000000",
        "severityNumber": _SEVERITY_NUMBER[event.get("severity", "info")],
        "severityText": str(event.get("severity", "info")).upper(),
        "body": {"stringValue": event["event"]},
        "attributes": [
            {"key": key, "value": _any(value)}
            for key, value in event_to_otlp_attributes(event).items()
        ],
    }
    record.update(record_overrides)
    return record


def otlp_body(*records: dict[str, Any], resource_attrs: list | None = None) -> dict[str, Any]:
    """Wrap logRecords in a resourceLogs export document."""
    return {
        "resourceLogs": [
            {
                "resource": {"attributes": resource_attrs or []},
                "scopeLogs": [{"logRecords": list(records)}],
            }
        ]
    }


def normalize(body: dict[str, Any]):
    payload = RawPayload(producer="otlp", source_kind="otlp", body=orjson.dumps(body))
    return ADAPTERS["otlp"].normalize(payload)


def canonical_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "event": "pipeline.run",
        "category": "pipeline",
        "outcome": "failure",
        "severity": "error",
        "delivery": "critical",
        "started_at": "2025-01-01T00:00:00Z",
        "ended_at": "2025-01-01T00:00:01.5Z",
        "duration_ms": 1500.0,
        "observed_at": "2025-01-01T00:00:01.5Z",
        "service": {"name": "etl", "version": "2.0", "environment": "prod"},
        "correlation": {
            "run_id": "run-otlp-1",
            "job_id": "nightly",
            "invocation_id": "inv-9",
            "asset_key": "staging.orders",
            "partition_key": "2025-01-01",
            "branch": "main",
            "table": "orders",
            "snapshot_id": "snap-2",
            "pipeline": "etl",
            "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
            "span_id": "00f067aa0ba902b7",
            "extra": {"attempt": 2},
        },
        "attributes": {"rows_out": 42},
        "error": {"message": "load failed", "code": "LOAD_FAILED", "retryable": True},
        "source": {"producer": "phlo-observe", "kind": "sdk"},
    }
    event.update(overrides)
    return event


def test_observe_core_record_restores_envelope() -> None:
    event = canonical_event()
    batch = normalize(otlp_body(observe_record(event)))
    assert not batch.errors
    (restored,) = batch.events

    assert restored["event_id"] == event["event_id"]
    assert restored["event"] == "pipeline.run"
    assert restored["category"] == "pipeline"
    assert restored["outcome"] == "failure"
    assert restored["severity"] == "error"
    assert restored["delivery"] == "critical"
    assert restored["started_at"] == "2025-01-01T00:00:00.000Z"
    assert restored["ended_at"] == "2025-01-01T00:00:01.500Z"
    assert restored["duration_ms"] == 1500.0
    assert restored["observed_at"] == "2025-01-01T00:00:01.500Z"
    assert restored["service"] == {
        "name": "etl",
        "version": "2.0",
        "instance_id": None,
        "environment": "prod",
        "host": None,
    }
    corr = restored["correlation"]
    for key, value in event["correlation"].items():
        assert corr[key] == value, key
    assert restored["attributes"]["rows_out"] == 42
    assert restored["error"]["message"] == "load failed"
    assert restored["error"]["code"] == "LOAD_FAILED"
    assert restored["error"]["retryable"] is True
    assert restored["source"]["producer"] == "phlo-observe"
    assert restored["source"]["adapter"] == "otlp.1.0"


def test_event_name_restored_when_body_is_not_a_name() -> None:
    """Collectors may replace the body; ``observe.event`` still restores it."""
    event = canonical_event()
    record = observe_record(event, body={"stringValue": "Something happened"})
    batch = normalize(otlp_body(record))
    (restored,) = batch.events
    assert restored["event"] == "pipeline.run"


def test_record_level_trace_fields_fill_missing_correlation() -> None:
    """A collector that sets traceId/spanId still joins the same trace."""
    event = canonical_event()
    record = observe_record(
        event, traceId="ff92f3577b34da6a3ce929d0e0e4ffff", spanId="11f067aa0ba902ff"
    )
    # Remove the encoded trace/span so only record-level fields remain.
    record["attributes"] = [
        kv
        for kv in record["attributes"]
        if kv["key"]
        not in (
            "observe.correlation.trace_id",
            "observe.correlation.span_id",
            "observe.trace_id",
            "observe.span_id",
        )
    ]
    batch = normalize(otlp_body(record))
    (restored,) = batch.events
    assert restored["correlation"]["trace_id"] == "ff92f3577b34da6a3ce929d0e0e4ffff"
    assert restored["correlation"]["span_id"] == "11f067aa0ba902ff"
    assert restored["correlation"]["run_id"] == "run-otlp-1"


def test_malformed_observe_metadata_is_per_item_error() -> None:
    """Bogus observe.* values reject only their own record (spec §35)."""
    good = observe_record(canonical_event())
    bad = observe_record(canonical_event())
    for kv in bad["attributes"]:
        if kv["key"] == "observe.category":
            kv["value"] = {"stringValue": "not-a-category"}
    batch = normalize(otlp_body(bad, good))
    assert len(batch.events) == 1
    assert batch.events[0]["event"] == "pipeline.run"
    assert len(batch.errors) == 1
    assert batch.errors[0]["index"] == 0
    assert batch.errors[0]["code"] == "SCHEMA_INVALID"
    assert batch.indices == [1]


def test_plain_otlp_record_still_normalizes_generically() -> None:
    """Non-observe records keep the generic mapping untouched."""
    body = otlp_body(
        {
            "timeUnixNano": "1735689600000000000",
            "severityNumber": 9,
            "body": {"stringValue": "hello"},
            "traceId": "aa" * 16,
            "attributes": [{"key": "rows", "value": {"intValue": "5"}}],
        },
        resource_attrs=[{"key": "service.name", "value": {"stringValue": "loader"}}],
    )
    batch = normalize(body)
    (event,) = batch.events
    assert event["event"] == "otlp.log"
    assert event["category"] == "other"
    assert event["correlation"]["trace_id"] == "aa" * 16
    assert event["attributes"]["rows"] == "5"
    assert event["attributes"]["body"] == "hello"
    assert event["service"]["name"] == "loader"
    # Plain records get a fresh id; only observe-encoded ones restore theirs.
    uuid.UUID(event["event_id"])


@pytest.mark.asyncio
async def test_ingested_observe_record_joins_run(client: AsyncClient) -> None:
    """End-to-end: an observe-core OTLP record restores run correlation."""
    run_id = f"otlp-run-{uuid.uuid4().hex[:8]}"
    event = canonical_event(
        event="pipeline.step",
        outcome="success",
        severity="info",
        delivery="telemetry",
        correlation={"run_id": run_id, "asset_key": "staging.orders"},
    )
    resp = await client.post("/v1/ingest/otlp", json=otlp_body(observe_record(event)))
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 1

    stored = await client.get(f"/v1/events/{event['event_id']}")
    assert stored.status_code == 200
    item = stored.json()
    assert item["correlation"]["run_id"] == run_id
    assert item["correlation"]["asset_key"] == "staging.orders"
    assert item["event"] == "pipeline.step"

    run = await client.get(f"/v1/runs/{run_id}")
    assert run.status_code == 200
    assert run.json()["run_id"] == run_id
    assert run.json()["event_count"] >= 1
