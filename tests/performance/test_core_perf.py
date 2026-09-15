"""observe-core client-side performance benchmarks (spec §84).

Each test measures one required dimension and asserts the V1 target where the
spec defines one; where the spec asks for a measurement only, a regression
floor is asserted and the measured rate is printed.
"""

from __future__ import annotations

import os
import statistics
import time
from collections.abc import Iterator
from typing import Any

import pytest
from observe_core import ObserveSettings, configure, event, observe, shutdown
from observe_core.builder import EventBuilder
from observe_core.runtime import Runtime

pytestmark = pytest.mark.performance

# Shared CI runners have contended, variable CPU: thresholds stay meaningful
# for regressions but tolerate slower shared hardware.
_LATENCY_FACTOR = 3.0 if os.environ.get("CI") else 1.0
_THROUGHPUT_FACTOR = 0.4 if os.environ.get("CI") else 1.0


def _us(limit: float) -> float:
    return limit * _LATENCY_FACTOR


def _per_s(floor: float) -> float:
    return floor * _THROUGHPUT_FACTOR


@pytest.fixture
def runtime(tmp_path: Any) -> Iterator[Runtime]:
    """A configured runtime with an in-memory drain (no drain I/O)."""
    rt = configure(
        ObserveSettings(
            service_name="perf-bench",
            environment="test",
            drains=[{"type": "memory"}],
            spool_enabled=False,
            queue_capacity=200_000,
            spool_dir=tmp_path / "spool",
        )
    )
    yield rt
    shutdown(timeout=5.0)


def _percentile(samples: list[int], pct: float) -> float:
    ordered = sorted(samples)
    return ordered[max(0, int(len(ordered) * pct) - 1)] / 1e3  # ns -> µs


def test_observe_operation_overhead(runtime: Runtime) -> None:
    """Empty ``with observe(...)`` — target: median bookkeeping < 100µs."""
    samples: list[int] = []
    for _ in range(2000):
        t0 = time.perf_counter_ns()
        with observe("perf.op"):
            pass
        samples.append(time.perf_counter_ns() - t0)
    median_us = statistics.median(samples) / 1e3
    p95_us = _percentile(samples, 0.95)
    print(f"\nobserve() overhead: median={median_us:.1f}µs p95={p95_us:.1f}µs")
    assert median_us < _us(100.0), (
        f"median overhead {median_us:.1f}µs exceeds {_us(100.0):.0f}µs target"
    )


def test_event_scalar_attributes_overhead(runtime: Runtime) -> None:
    """Event with 10 scalar attributes — same < 100µs bookkeeping target."""
    attrs = {f"k{i}": i for i in range(10)}
    samples: list[int] = []
    for _ in range(2000):
        t0 = time.perf_counter_ns()
        event("perf.scalar", attributes=attrs)
        samples.append(time.perf_counter_ns() - t0)
    median_us = statistics.median(samples) / 1e3
    print(f"\nscalar attrs emit: median={median_us:.1f}µs")
    assert median_us < _us(100.0), (
        f"median overhead {median_us:.1f}µs exceeds {_us(100.0):.0f}µs target"
    )


def test_event_nested_attributes_overhead(runtime: Runtime) -> None:
    """Event with nested attributes — < 100µs bookkeeping target."""
    attrs = {
        "query": {"sql": "select 1", "tables": ["a", "b"], "stats": {"rows": 5}},
        "result": {"columns": [{"name": "c", "type": "int"}] * 4},
    }
    samples: list[int] = []
    for _ in range(2000):
        t0 = time.perf_counter_ns()
        event("perf.nested", attributes=attrs)
        samples.append(time.perf_counter_ns() - t0)
    median_us = statistics.median(samples) / 1e3
    print(f"\nnested attrs emit: median={median_us:.1f}µs")
    assert median_us < _us(100.0), (
        f"median overhead {median_us:.1f}µs exceeds {_us(100.0):.0f}µs target"
    )


def test_enqueue_p95_and_throughput(runtime: Runtime) -> None:
    """Targets: p95 enqueue < 1ms, sustained >= 10,000 events/sec."""
    n = 30_000
    samples: list[int] = []
    t0 = time.perf_counter()
    for _ in range(n):
        s = time.perf_counter_ns()
        event("perf.enqueue")
        samples.append(time.perf_counter_ns() - s)
    elapsed = time.perf_counter() - t0
    p95_us = _percentile(samples, 0.95)
    rate = n / elapsed
    print(f"\nenqueue: p95={p95_us:.1f}µs rate={rate:.0f}/s")
    assert p95_us < _us(1000.0), f"p95 enqueue {p95_us:.1f}µs exceeds {_us(1000.0):.0f}µs target"
    assert rate >= _per_s(10_000), f"enqueue rate {rate:.0f}/s below {_per_s(10_000):.0f}/s target"


def test_serialization_throughput(runtime: Runtime) -> None:
    """Finalize+serialize throughput through the real pipeline path."""
    builder_attrs = {"a": 1, "b": "x", "nested": {"c": [1, 2, 3]}}
    n = 10_000
    t0 = time.perf_counter()
    for _ in range(n):
        builder = EventBuilder("perf.ser", attributes=dict(builder_attrs))
        event_obj = runtime._finalize(builder, None)
        assert event_obj is not None
    elapsed = time.perf_counter() - t0
    rate = n / elapsed
    print(f"\nfinalize+serialize: {rate:.0f} events/s")
    assert rate >= _per_s(2_000), f"serialization rate {rate:.0f}/s is pathologically slow"


def test_http_drain_throughput() -> None:
    """Batch HTTP drain throughput (serialization + request path, mock transport)."""
    import httpx
    from observe_core.drains.base import CanonicalEvent
    from observe_core.drains.http import HttpDrain
    from observe_core.models import Delivery
    from observe_core.serialization import dumps

    drain = HttpDrain("http://observer.test/v1/events", token="t", max_attempts=1)
    requests: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.content or b"")
        return httpx.Response(202, json={"accepted": 1, "rejected": 0})

    drain._client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        payload = dumps({"event": "perf.http", "attributes": {"k": "v"}})
        batch = [CanonicalEvent(data={}, payload=payload, delivery=Delivery.TELEMETRY)] * 100
        n_batches = 50
        t0 = time.perf_counter()
        for _ in range(n_batches):
            drain.emit_batch(batch)
        elapsed = time.perf_counter() - t0
        rate = (n_batches * len(batch)) / elapsed
        print(f"\nhttp drain: {rate:.0f} events/s ({n_batches} batches of 100)")
        assert rate >= _per_s(1_000), f"drain throughput {rate:.0f}/s is pathologically slow"
        assert requests and requests[0].startswith(b"[")
    finally:
        drain.close()
