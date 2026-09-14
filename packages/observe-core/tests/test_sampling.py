"""Deterministic sampling tests."""

from __future__ import annotations

from observe_core import event, flush
from observe_core.models import Delivery
from observe_core.sampling import Sampler


def test_critical_never_sampled():
    sampler = Sampler(debug_rate=0.0, telemetry_rate=0.0)
    assert sampler.should_keep(Delivery.CRITICAL, "wap.promote", "r1")


def test_zero_and_full_rates():
    sampler = Sampler(debug_rate=0.0, telemetry_rate=1.0)
    assert not sampler.should_keep(Delivery.DEBUG, "x", "k")
    assert sampler.should_keep(Delivery.TELEMETRY, "x", "k")


def test_deterministic_same_key():
    sampler = Sampler(debug_rate=0.5, telemetry_rate=0.5)
    results = {sampler.should_keep(Delivery.DEBUG, "pipeline.step", "run-9") for _ in range(50)}
    assert len(results) == 1  # same input -> same decision


def test_partial_rate_keeps_some():
    sampler = Sampler(debug_rate=0.5, telemetry_rate=1.0)
    kept = sum(sampler.should_keep(Delivery.DEBUG, "pipeline.step", f"run-{i}") for i in range(200))
    assert 40 < kept < 160  # rough half


def test_debug_sampling_end_to_end(make_runtime):
    rt = make_runtime(sampling_debug_rate=0.0, environment="production")
    drain = rt.drains[0]
    for _ in range(20):
        event("application.log", delivery="debug")
    flush(2.0)
    assert drain.events == []
    stats = rt.stats.snapshot()
    assert stats["dropped_sampled"] == 20


def test_production_default_debug_rate():
    from observe_core.config import ObserveSettings

    assert ObserveSettings(environment="production").resolved_debug_rate() == 0.1
    assert ObserveSettings(environment="development").resolved_debug_rate() == 1.0
