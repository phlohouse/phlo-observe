"""Typed settings, env vars, discriminated drain config."""

from __future__ import annotations

import pytest
from observe_core.config import (
    ConsoleDrainConfig,
    HttpDrainConfig,
    JsonlDrainConfig,
    MemoryDrainConfig,
    ObserveSettings,
)
from pydantic import ValidationError


def test_defaults():
    s = ObserveSettings(service_name="svc")
    assert s.queue_capacity == 10_000
    assert s.batch_size == 100
    assert s.flush_interval_ms == 1_000
    assert s.drop_policy == "drop_newest"
    assert s.spool_enabled is True


def test_env_prefix(monkeypatch):
    monkeypatch.setenv("OBSERVE_SERVICE_NAME", "from-env")
    monkeypatch.setenv("OBSERVE_ENVIRONMENT", "staging")
    s = ObserveSettings()
    assert s.service_name == "from-env"
    assert s.environment == "staging"


def test_env_drain_shorthand(monkeypatch):
    monkeypatch.setenv("OBSERVE_DRAINS", "memory,jsonl")
    monkeypatch.setenv("OBSERVE_JSONL_PATH", "/tmp/env-events.jsonl")
    s = ObserveSettings(service_name="x")
    assert isinstance(s.drains[0], MemoryDrainConfig)
    assert isinstance(s.drains[1], JsonlDrainConfig)
    assert str(s.drains[1].path) == "/tmp/env-events.jsonl"


def test_env_drain_shorthand_http_requires_endpoint(monkeypatch):
    monkeypatch.setenv("OBSERVE_DRAINS", "http")
    monkeypatch.delenv("OBSERVE_HTTP_ENDPOINT", raising=False)
    with pytest.raises(Exception) as excinfo:
        ObserveSettings(service_name="x")
    # pydantic-settings wraps the shorthand error; check the cause chain
    assert "OBSERVE_HTTP_ENDPOINT" in str(excinfo.value.__cause__ or excinfo.value)


def test_env_drain_shorthand_http_token_and_api_key(monkeypatch):
    """Both OBSERVE_HTTP_TOKEN and OBSERVE_HTTP_API_KEY reach the drain."""
    monkeypatch.setenv("OBSERVE_DRAINS", "http")
    monkeypatch.setenv("OBSERVE_HTTP_ENDPOINT", "https://o.test/v1/events")
    monkeypatch.setenv("OBSERVE_HTTP_API_KEY", "key-abc")
    s = ObserveSettings(service_name="x")
    assert isinstance(s.drains[0], HttpDrainConfig)
    assert s.drains[0].api_key == "key-abc"
    assert s.drains[0].token is None

    monkeypatch.setenv("OBSERVE_HTTP_TOKEN", "tok-xyz")
    monkeypatch.delenv("OBSERVE_HTTP_API_KEY")
    s = ObserveSettings(service_name="x")
    assert s.drains[0].token == "tok-xyz"
    assert s.drains[0].api_key is None


def test_drain_discriminated_union():
    s = ObserveSettings(
        service_name="x",
        drains=[
            {"type": "console"},
            {"type": "jsonl", "path": "/tmp/e.jsonl"},
            {"type": "http", "endpoint": "https://o.test"},
            {"type": "memory"},
        ],
    )
    assert isinstance(s.drains[0], ConsoleDrainConfig)
    assert isinstance(s.drains[1], JsonlDrainConfig)
    assert isinstance(s.drains[2], HttpDrainConfig)
    assert isinstance(s.drains[3], MemoryDrainConfig)


def test_invalid_drain_type():
    with pytest.raises(ValidationError):
        ObserveSettings(service_name="x", drains=[{"type": "bogus"}])


def test_sampling_rate_bounds():
    with pytest.raises(ValidationError):
        ObserveSettings(service_name="x", sampling_telemetry_rate=1.5)
    with pytest.raises(ValidationError):
        ObserveSettings(service_name="x", sampling_debug_rate=-0.1)
    s = ObserveSettings(service_name="x", sampling_debug_rate=0.25)
    assert s.resolved_debug_rate() == 0.25


def test_drop_policy_literal():
    s = ObserveSettings(service_name="x", drop_policy="drop_oldest")
    assert s.drop_policy == "drop_oldest"
    with pytest.raises(ValidationError):
        ObserveSettings(service_name="x", drop_policy="explode")


def test_positive_integer_validation():
    with pytest.raises(ValidationError):
        ObserveSettings(service_name="x", queue_capacity=0)
    with pytest.raises(ValidationError):
        ObserveSettings(service_name="x", max_event_bytes=-1)
