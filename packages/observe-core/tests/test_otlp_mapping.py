"""Canonical event -> OTLP attribute mapping shared by the drain and forwarder."""

from __future__ import annotations

import orjson
import pytest
from observe_core.otlp_mapping import event_to_otlp_attributes


def _event() -> dict:
    return {
        "schema_version": "1.0",
        "event_id": "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b",
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
            "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
            "span_id": "00f067aa0ba902b7",
            "parent_span_id": "00f067aa0ba90200",
            "run_id": "run-7",
            "job_id": "nightly",
            "invocation_id": "inv-3",
            "asset_key": "staging.orders",
            "partition_key": "2025-01-01",
            "branch": "main",
            "table": "orders",
            "snapshot_id": "snap-1",
            "pipeline": "etl",
            "experiment_id": "exp-9",
            "request_id": "req-1",
            "extra": {"attempt": 2},
        },
        "attributes": {"rows_out": 42},
        "error": {"message": "load failed", "code": "LOAD_FAILED", "retryable": True},
        "source": {"producer": "phlo-observe", "kind": "sdk"},
    }


def test_flat_fields_and_shortcuts():
    attrs = event_to_otlp_attributes(_event())
    assert attrs["observe.event_id"] == "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b"
    assert attrs["observe.event"] == "pipeline.run"
    assert attrs["observe.category"] == "pipeline"
    assert attrs["observe.outcome"] == "failure"
    assert attrs["observe.severity"] == "error"
    assert attrs["observe.delivery"] == "critical"
    assert attrs["observe.duration_ms"] == 1500.0
    assert attrs["observe.observed_at"] == "2025-01-01T00:00:01.5Z"
    assert attrs["observe.trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert attrs["observe.span_id"] == "00f067aa0ba902b7"


def test_every_correlation_key_is_carried():
    attrs = event_to_otlp_attributes(_event())
    corr = _event()["correlation"]
    for key, value in corr.items():
        if key == "extra":
            continue
        assert attrs[f"observe.correlation.{key}"] == value
    assert orjson.loads(attrs["observe.correlation.extra"]) == {"attempt": 2}


def test_structured_sections_are_json():
    attrs = event_to_otlp_attributes(_event())
    assert orjson.loads(attrs["observe.service"]) == _event()["service"]
    assert orjson.loads(attrs["observe.error"])["message"] == "load failed"
    assert orjson.loads(attrs["observe.source"])["producer"] == "phlo-observe"
    assert orjson.loads(attrs["observe.attributes"]) == {"rows_out": 42}


def test_minimal_event_emits_only_present_fields():
    attrs = event_to_otlp_attributes(
        {
            "event_id": "e",
            "event": "application.log",
            "observed_at": "2025-01-01T00:00:00Z",
        }
    )
    assert attrs == {
        "observe.event_id": "e",
        "observe.event": "application.log",
        "observe.observed_at": "2025-01-01T00:00:00Z",
    }


def test_drain_log_record_carries_correlation():
    """The OTel drain encodes correlation into record attributes."""
    pytest.importorskip("opentelemetry._logs")
    from observe_core.drains.otlp import _HAS_OTEL, OtlpDrain

    if not _HAS_OTEL:
        pytest.skip("observe-core[otlp] extra not installed")
    drain = OtlpDrain("http://localhost:4318/v1/logs")
    try:
        record = drain._to_log_record(_event())
    finally:
        drain.close()
    assert record.body == "pipeline.run"
    assert record.severity_number.value == 17
    assert dict(record.attributes)["observe.correlation.run_id"] == "run-7"
    assert dict(record.attributes)["observe.event_id"] == _event()["event_id"]
