"""Event envelope model tests: validation, timing consistency, serialization."""

from __future__ import annotations

import datetime as dt

import pytest
from observe_core.models import (
    Category,
    Correlation,
    Delivery,
    ErrorInfo,
    EventEnvelope,
    Outcome,
    Severity,
    SourceInfo,
)
from observe_core.timestamps import UTC, format_rfc3339, utcnow
from pydantic import ValidationError


def _envelope(**overrides) -> EventEnvelope:
    base = {
        "event_id": "01JTEST0000000000000000000",
        "event": "asset.materialize",
        "category": "data",
        "outcome": "success",
        "severity": "info",
        "delivery": "telemetry",
        "observed_at": utcnow(),
        "service": {"name": "test"},
    }
    base.update(overrides)
    return EventEnvelope(**base)


def test_minimal_envelope_serializes():
    env = _envelope()
    data = env.to_canonical_dict()
    assert data["schema_version"] == "2.0"
    # Empty V2 extensions stay out of the wire format (V1 byte-compat).
    assert "entities" not in data
    assert "tags" not in data
    assert "contract" not in data
    assert data["event"] == "asset.materialize"
    assert data["correlation"]["run_id"] is None
    assert data["error"] is None
    assert data["source"] is None
    assert data["attributes"] == {}


def test_timestamps_serialize_as_rfc3339_z():
    env = _envelope(observed_at=dt.datetime(2026, 9, 14, 21, 15, 11, 970000, tzinfo=UTC))
    assert env.to_canonical_dict()["observed_at"] == "2026-09-14T21:15:11.970Z"


def test_json_bytes_round_trip():
    env = _envelope()
    parsed = EventEnvelope.from_json(env.to_json_bytes())
    assert parsed.event_id == env.event_id
    assert parsed.event == env.event


def test_duration_consistency_enforced():
    start = utcnow()
    with pytest.raises(ValidationError, match="duration_ms"):
        _envelope(started_at=start, ended_at=start + dt.timedelta(seconds=2), duration_ms=500)


def test_duration_consistent_passes():
    start = utcnow()
    env = _envelope(
        started_at=start, ended_at=start + dt.timedelta(milliseconds=1500), duration_ms=1500.4
    )
    assert env.duration_ms == pytest.approx(1500.4)


def test_invalid_event_name_rejected():
    for bad in ("Asset.Materialize", "has space", "x..y", ".lead", "trail.", "has/slash"):
        with pytest.raises(ValidationError):
            _envelope(event=bad)


def test_invalid_schema_version_rejected():
    with pytest.raises(ValidationError):
        _envelope(schema_version="3.0")
    for valid in ("1.0", "1.1", "2.0", "2.1"):
        env = _envelope(schema_version=valid)
        assert env.schema_version == valid


def test_enums():
    assert Category.WAP == "wap"
    assert Outcome.PARTIAL == "partial"
    assert Severity.CRITICAL == "critical"
    assert Delivery.CRITICAL == "critical"


def test_correlation_extra():
    corr = Correlation(run_id="R1-child", root_run_id="R1", extra={"dagster_run": "xyz"})
    assert corr.canonical_dict()["run_id"] == "R1-child"
    assert corr.canonical_dict()["root_run_id"] == "R1"
    assert "dagster_run" not in corr.canonical_dict()
    assert corr.extra == {"dagster_run": "xyz"}


def test_error_info():
    err = ErrorInfo(code="X", message="boom", why="because", fix="do y", retryable=False)
    data = err.model_dump(mode="json")
    assert data["code"] == "X"
    assert data["message"] == "boom"


def test_source_info():
    src = SourceInfo(producer="dagster", kind="ASSET_MATERIALIZATION", adapter="dagster.v1")
    assert src.producer == "dagster"


def test_format_rfc3339_naive_assumed_utc():
    naive = dt.datetime(2026, 1, 1, 0, 0, 0)
    assert format_rfc3339(naive) == "2026-01-01T00:00:00.000Z"
