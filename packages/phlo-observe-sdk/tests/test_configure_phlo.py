"""configure_phlo: SDK quick-start wiring onto observe-core."""

from __future__ import annotations

import pytest
from observe_core import ObserveSettings, shutdown
from observe_core.config import HttpDrainConfig
from observe_core.drains.http import HttpDrain
from observe_core.drains.memory import MemoryDrain
from phlo_observe import configure_phlo


@pytest.fixture(autouse=True)
def _clean_observe_env(monkeypatch):
    """Keep ambient OBSERVE_* env from leaking into settings under test."""
    for var in (
        "OBSERVE_DRAINS",
        "OBSERVE_HTTP_ENDPOINT",
        "OBSERVE_HTTP_TOKEN",
        "OBSERVE_HTTP_API_KEY",
    ):
        monkeypatch.delenv(var, raising=False)


def test_configure_phlo_appends_http_drain_for_endpoint():
    rt = configure_phlo(
        service_name="sdk-test",
        drains=[{"type": "memory"}],
        spool_enabled=False,
        observer_endpoint="https://observer.test/v1/events",
        api_key="test-key",
    )
    try:
        kinds = [type(d) for d in rt.drains]
        assert MemoryDrain in kinds
        assert HttpDrain in kinds
        http = next(d for d in rt.drains if isinstance(d, HttpDrain))
        assert http.endpoint == "https://observer.test/v1/events"
        assert http._client.headers["x-api-key"] == "test-key"
    finally:
        shutdown()


def test_configure_phlo_env_endpoint(monkeypatch):
    monkeypatch.setenv("OBSERVE_HTTP_ENDPOINT", "https://env.test/v1/events")
    monkeypatch.setenv("OBSERVE_HTTP_TOKEN", "env-token")
    rt = configure_phlo(service_name="sdk-test", drains=[], spool_enabled=False)
    try:
        http = next(d for d in rt.drains if isinstance(d, HttpDrain))
        assert http.endpoint == "https://env.test/v1/events"
        assert http._client.headers["authorization"] == "Bearer env-token"
    finally:
        shutdown()


def test_configure_phlo_dedupes_existing_endpoint():
    rt = configure_phlo(
        ObserveSettings(
            service_name="sdk-test",
            spool_enabled=False,
            drains=[
                HttpDrainConfig(endpoint="https://dup.test/v1/events", token="t"),
            ],
        ),
        observer_endpoint="https://dup.test/v1/events",
    )
    try:
        assert sum(isinstance(d, HttpDrain) for d in rt.drains) == 1
    finally:
        shutdown()


def test_configure_phlo_no_endpoint_keeps_drains():
    rt = configure_phlo(service_name="sdk-test", drains=[{"type": "memory"}], spool_enabled=False)
    try:
        assert not any(isinstance(d, HttpDrain) for d in rt.drains)
    finally:
        shutdown()


def test_configure_phlo_settings_roundtrip_revalidated():
    """A settings object with drains folds into overrides and re-validates."""
    rt = configure_phlo(
        ObserveSettings(service_name="sdk-test", spool_enabled=False, drains=[]),
        drains=[{"type": "memory"}],
    )
    try:
        assert any(isinstance(d, MemoryDrain) for d in rt.drains)
        assert rt.settings.service_name == "sdk-test"
    finally:
        shutdown()


def test_configure_phlo_registers_phlo_terminal_events():
    """Phlo run boundaries (pipeline.run et al.) reach the tail sampler."""
    rt = configure_phlo(service_name="sdk-test", drains=[], spool_enabled=False, tail_sampling=True)
    try:
        assert rt._tail is not None
        assert {"pipeline.run", "dlt.pipeline.run", "dbt.invocation"} <= set(
            rt._tail.terminal_events
        )
    finally:
        shutdown()


def test_configure_phlo_tail_terminal_events_union_with_callers():
    rt = configure_phlo(
        service_name="sdk-test",
        drains=[],
        spool_enabled=False,
        tail_sampling=True,
        tail_terminal_events=["custom.done"],
    )
    try:
        assert rt._tail is not None
        assert {"pipeline.run", "custom.done"} <= set(rt._tail.terminal_events)
    finally:
        shutdown()
