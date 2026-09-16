"""SDK application-side overhead benchmarks (hardening item 2).

Measures the dimensions not covered by test_core_perf.py: context
propagation, sampler decision cost, enqueue under queue pressure, and
the caller-visible cost when the exporter is unavailable. The question
each asserts: telemetry never becomes the bottleneck of the app it
observes.
"""

from __future__ import annotations

import os
import statistics
import time
from collections.abc import Iterator
from typing import Any

import pytest
from observe_core import ObserveSettings, configure, event, observe, shutdown
from observe_core.context import bind_context
from observe_core.models import Delivery, Outcome, Severity
from observe_core.propagation import decode_context, encode_context
from observe_core.runtime import Runtime
from observe_core.sampling import PolicySampler, Sampler, SamplingContext

pytestmark = pytest.mark.performance

_LATENCY_FACTOR = 3.0 if os.environ.get("CI") else 1.0
_THROUGHPUT_FACTOR = 0.4 if os.environ.get("CI") else 1.0


def _us(limit: float) -> float:
    return limit * _LATENCY_FACTOR


def _per_s(floor: float) -> float:
    return floor * _THROUGHPUT_FACTOR


@pytest.fixture
def runtime(tmp_path: Any) -> Iterator[Runtime]:
    rt = configure(
        ObserveSettings(
            service_name="sdk-bench",
            environment="test",
            drains=[{"type": "memory"}],
            spool_enabled=False,
            queue_capacity=200_000,
            spool_dir=tmp_path / "spool",
        )
    )
    yield rt
    shutdown(timeout=5.0)


def _median_us(samples: list[int]) -> float:
    return statistics.median(samples) / 1e3


def test_context_bind_overhead(runtime: Runtime) -> None:
    """bind_context + observe inside it: ambient correlation is cheap."""
    samples: list[int] = []
    for i in range(2000):
        t0 = time.perf_counter_ns()
        with bind_context(run_id=f"r-{i}"), observe("perf.ctx"):
            pass
        samples.append(time.perf_counter_ns() - t0)
    print(f"\nbind+observe: median={_median_us(samples):.1f}µs")
    assert _median_us(samples) < _us(200.0)


def test_context_token_encode_decode() -> None:
    """Propagation token encode/decode: subprocess boundary cost."""
    ctx = {"run_id": "r-123", "trace_id": "t-abc", "job": "daily"}
    n = 10_000
    t0 = time.perf_counter()
    for _ in range(n):
        decode_context(encode_context(ctx))
    rate = n / (time.perf_counter() - t0)
    print(f"\npropagation token roundtrip: {rate:.0f}/s")
    assert rate >= _per_s(20_000)


def test_sampler_decision_overhead() -> None:
    """PolicySampler decide() cost with rules configured."""
    base = Sampler(debug_rate=1.0, telemetry_rate=1.0)
    sampler = PolicySampler.from_settings(
        base,
        [
            {"name": "metrics-down", "event": "metric.*", "rate": 0.1},
            {"name": "drop-debug", "severity": "debug", "rate": 0.0},
        ],
    )
    ctx = SamplingContext(
        event="pipeline.step",
        delivery=Delivery.TELEMETRY,
        severity=Severity.INFO,
        outcome=Outcome.SUCCESS,
        duration_ms=None,
        service="sdk-bench",
        environment="test",
        sample_key="run-1",
    )
    n = 20_000
    t0 = time.perf_counter()
    for _ in range(n):
        sampler.decide(ctx)
    rate = n / (time.perf_counter() - t0)
    print(f"\nsampler decide: {rate:.0f}/s")
    assert rate >= _per_s(20_000)


def test_enqueue_under_queue_pressure(tmp_path: Any) -> None:
    """Full queue: enqueue must stay bounded (drop path), never block."""
    import threading
    from collections.abc import Sequence

    from observe_core.drains.base import CanonicalEvent

    gate = threading.Event()

    class _BlockingDrain:
        name = "blocking"
        is_remote = False

        def emit_batch(self, events: Sequence[CanonicalEvent]) -> None:
            gate.wait(30.0)  # hold the worker so the queue fills

        def emit_raw(self, payloads: Sequence[bytes]) -> None:
            gate.wait(30.0)

        def flush(self) -> None:
            pass

        def close(self) -> None:
            pass

    rt = configure(
        ObserveSettings(
            service_name="sdk-pressure",
            environment="test",
            drains=[{"type": "memory"}],
            spool_enabled=False,
            queue_capacity=1_000,
            spool_dir=tmp_path / "spool",
        )
    )
    rt.drains.clear()
    rt.drains.append(_BlockingDrain())
    try:
        for _ in range(2_000):  # overfill: worker is parked on the gate
            event("perf.pressure")
        samples: list[int] = []
        n = 5_000
        t0 = time.perf_counter()
        for _ in range(n):
            s = time.perf_counter_ns()
            event("perf.pressure")
            samples.append(time.perf_counter_ns() - s)
        elapsed = time.perf_counter() - t0
        rate = n / elapsed
        print(f"\nenqueue under pressure: median={_median_us(samples):.1f}µs rate={rate:.0f}/s")
        # The drop path is a bounded counter increment — never a block.
        assert _median_us(samples) < _us(500.0)
        assert rate >= _per_s(5_000)
    finally:
        gate.set()
        shutdown(timeout=5.0)


def test_exporter_unavailable_overhead(tmp_path: Any) -> None:
    """Failing drain: caller-side emit cost stays bounded, errors isolated."""
    import httpx
    from observe_core.drains.http import HttpDrain

    drain = HttpDrain(
        "http://127.0.0.1:1/unreachable",
        token="t",
        max_attempts=1,
        read_timeout_s=0.2,
        backoff_base_ms=1.0,
    )
    drain._client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    rt = configure(
        ObserveSettings(
            service_name="sdk-down",
            environment="test",
            drains=[],
            spool_enabled=False,
            queue_capacity=10_000,
            spool_dir=tmp_path / "spool",
        )
    )
    rt.drains.clear()
    rt.drains.append(drain)
    try:
        samples: list[int] = []
        n = 2_000
        for _ in range(n):
            s = time.perf_counter_ns()
            event("perf.exporter_down")
            samples.append(time.perf_counter_ns() - s)
        print(f"\nemit with dead exporter: median={_median_us(samples):.1f}µs")
        # Enqueue stays cheap; the drain error is the worker's problem.
        assert _median_us(samples) < _us(500.0)
    finally:
        drain.close()
        shutdown(timeout=5.0)
