"""Regression tests for the V2 hardening review findings.

Each test names the failure it pins: a fixed bug stays fixed.
"""

from __future__ import annotations

import threading
import time

import pytest
from observe_core import (
    bind_context,
    event,
    flush,
    metric,
    observe,
    shutdown,
)
from observe_core.aggregate import MetricAggregator
from observe_core.backends import WorkerBackend, _FlushRequest
from observe_core.drains.base import CanonicalEvent
from observe_core.drains.memory import MemoryDrain
from observe_core.emit import flush_metrics
from observe_core.models import Delivery
from observe_core.stats import TelemetryStats
from observe_core.tail import TailSampler


class _E:
    """Minimal CanonicalEvent stand-in for tail tests."""

    def __init__(self, data):
        self.data = data
        self.delivery = Delivery.TELEMETRY


def _canonical(data: dict, delivery: Delivery = Delivery.TELEMETRY) -> CanonicalEvent:
    return CanonicalEvent(data=data, payload=b"{}", delivery=delivery)


# -- tail sampler -------------------------------------------------------------


def test_tail_bound_releases_once_and_keeps_chunking():
    """The boundary event must not be emitted twice, and the run must keep
    buffering in bounded chunks instead of switching to unbounded pass-through.
    """
    stats = TelemetryStats()
    emitted = []
    tail = TailSampler(min_duration_ms=30_000, max_runs=100, max_run_events=2, stats=stats)
    corr = {"correlation": {"run_id": "r-1"}}

    for i in range(3):  # third event hits the bound
        tail.process(_E({**corr, "event": "run.step", "i": i}), emitted.append)
    # Bound hit: the buffered chunk (events 0,1,2) released exactly once each.
    assert [e.data["i"] for e in emitted] == [0, 1, 2]

    # Buffering resumes: a fresh chunk accumulates rather than passing through.
    tail.process(_E({**corr, "event": "run.step", "i": 3}), emitted.append)
    assert [e.data["i"] for e in emitted] == [0, 1, 2]  # i=3 still buffered
    tail.process(_E({**corr, "event": "run.step", "i": 4}), emitted.append)
    tail.process(_E({**corr, "event": "run.step", "i": 5}), emitted.append)
    assert [e.data["i"] for e in emitted] == [0, 1, 2, 3, 4, 5]
    assert stats.snapshot().get("tail_released", 0) == 2


def test_tail_bound_event_is_not_duplicated():
    """Direct regression for the double-emit: N inputs produce N outputs."""
    emitted = []
    tail = TailSampler(min_duration_ms=30_000, max_runs=100, max_run_events=1)
    corr = {"correlation": {"run_id": "r-x"}}
    for i in range(4):
        tail.process(_E({**corr, "event": "run.step", "i": i}), emitted.append)
    emitted_ids = [id(e) for e in emitted]
    assert len(emitted_ids) == len(set(emitted_ids))  # no object emitted twice
    assert [e.data["i"] for e in emitted] == [0, 1, 2, 3]


# -- metric aggregation ---------------------------------------------------------


def test_aggregator_reservoir_is_bounded():
    """Values past the sample cap are reservoir-sampled; counters stay exact."""
    agg = MetricAggregator(max_samples=100)
    for i in range(5_000):
        agg.record("lat", float(i))
    summaries = agg.flush()
    (s,) = summaries
    assert s["count"] == 5_000
    assert s["sum"] == sum(range(5_000))
    assert s["min"] == 0.0
    assert s["max"] == 4_999.0
    assert s["mean"] == pytest.approx(2_499.5)
    # Percentiles come from the bounded reservoir, not the full population.
    assert 0 <= s["p50"] <= 4_999


def test_aggregator_reservoir_memory_bound():
    """The stored sample list itself stays at max_samples."""
    agg = MetricAggregator(max_samples=64)
    for i in range(10_000):
        agg.record("lat", float(i))
    (bucket,) = agg._buckets.values()
    assert len(bucket.values) == 64


def test_aggregator_series_context_round_trips():
    """Series key + summary payload keep correlation/entities/tags."""
    agg = MetricAggregator()
    agg.record(
        "rows",
        10.0,
        {"t": "x"},
        correlation={"run_id": "r1", "ignored": None},
        entities={"asset": "asset://a"},
        tags={"team": "data"},
    )
    agg.record("rows", 99.0)  # distinct series: no context
    summaries = agg.flush()
    assert len(summaries) == 2
    ctx = next(s for s in summaries if s["sum"] == 10.0)
    plain = next(s for s in summaries if s["sum"] == 99.0)
    # The context-carrying series recorded only the first sample.
    assert ctx.get("correlation") == {"run_id": "r1"}
    assert ctx.get("entities") == {"asset": "asset://a"}
    assert ctx.get("tags") == {"team": "data"}
    assert ctx["dimensions"] == {"t": "x"}
    assert "correlation" not in plain and "entities" not in plain


def test_aggregator_series_bound_rejects():
    """Past max_series a sample is rejected so the caller can fall back."""
    agg = MetricAggregator(max_series=1)
    assert agg.record("a", 1.0) is True
    assert agg.record("b", 1.0) is False


def test_runtime_flush_drains_metric_summaries(captured):
    """Runtime.flush() must drain pending summaries, not just the queue."""
    _, drain = captured
    metric("rows.written", 5.0, entities={"asset": "asset://a"})
    assert flush(2.0) is True
    summaries = [e.data for e in drain.events if e.data["event"] == "metric.summary"]
    assert len(summaries) == 1
    assert summaries[0]["entities"] == {"asset": "asset://a"}


def test_shutdown_drains_metric_summaries(make_runtime):
    """Shutdown emits pending summaries before closing the backend."""
    rt = make_runtime()
    drain = rt.drains[0]
    assert isinstance(drain, MemoryDrain)
    metric("rows.written", 7.0)
    shutdown(2.0)
    summaries = [e.data for e in drain.events if e.data["event"] == "metric.summary"]
    assert len(summaries) == 1
    assert summaries[0]["attributes"]["count"] == 1


def test_metric_cadence_auto_flushes(captured):
    """flush_after_seconds=0 means every record() drains what is due."""
    _, drain = captured
    metric("m", 1.0, flush_after_seconds=0.0)
    flush(2.0)
    summaries = [e.data for e in drain.events if e.data["event"] == "metric.summary"]
    assert summaries  # drained without an explicit flush_metrics()


def test_metric_summary_carries_correlation(captured):
    """A run-correlated metric series must emit its summary with run_id so
    observer baselines can attach it to the run entity."""
    _, drain = captured
    metric("m", 3.0, correlation={"run_id": "r-9"})
    assert flush_metrics() == 1
    flush(2.0)
    (s,) = [e.data for e in drain.events if e.data["event"] == "metric.summary"]
    assert s["correlation"]["run_id"] == "r-9"


# -- critical admission ---------------------------------------------------------


class _StallDrain(MemoryDrain):
    """Drain that blocks inside emit_batch until released."""

    def __init__(self, gate: threading.Event) -> None:
        super().__init__()
        self.gate = gate
        self.entered = 0

    def emit_batch(self, events) -> None:
        self.entered += 1
        self.gate.wait(timeout=10)


def _stalled_backend(make_runtime, gate: threading.Event, **kw) -> WorkerBackend:
    """A WorkerBackend whose single worker is wedged inside a slow drain."""
    kw.setdefault("queue_capacity", 5)
    kw.setdefault("worker_count", 1)
    rt = make_runtime(**kw)
    backend = rt.backend
    assert isinstance(backend, WorkerBackend)
    stall = _StallDrain(gate)
    rt.drains.clear()
    rt.drains.append(stall)
    backend.delivery.drains = rt.drains
    event("warmup.fill")  # occupies the worker inside emit_batch
    deadline = time.time() + 2
    while stall.entered == 0 and time.time() < deadline:
        time.sleep(0.005)
    assert stall.entered == 1
    return backend


def test_critical_spool_failure_reports_dropped(make_runtime):
    """A critical event that cannot be spooled must not report accepted."""
    gate = threading.Event()
    backend = _stalled_backend(make_runtime, gate, queue_capacity=1)
    try:
        backend._queue.put_nowait(_canonical({"event": "filler"}))
        # Spool is disabled -> spool_event returns False -> honest rejection.
        result = backend.emit(_canonical({"event": "crit"}, Delivery.CRITICAL))
        assert result.accepted is False
        assert result.dropped is True
        assert result.reason == "queue_full_spool_unavailable"
        assert backend.stats.snapshot().get("spool_errors", 0) >= 1
    finally:
        gate.set()
        backend.close(2.0)


def test_critical_spool_success_reports_spooled(make_runtime, tmp_path):
    gate = threading.Event()
    backend = _stalled_backend(
        make_runtime,
        gate,
        queue_capacity=1,
        spool_enabled=True,
        spool_dir=tmp_path / "sp",
    )
    try:
        backend._queue.put_nowait(_canonical({"event": "filler"}))
        result = backend.emit(_canonical({"event": "crit"}, Delivery.CRITICAL))
        assert result.accepted is True
        assert result.spooled is True
    finally:
        gate.set()
        backend.close(2.0)


def test_eviction_scan_is_bounded(make_runtime):
    """The evictor never scans past _EVICT_SCAN_LIMIT items.

    Fill the queue beyond the window with criticals and put one telemetry
    item past the window's reach: only the oldest in-window critical is
    evicted; the out-of-window telemetry item is never touched.
    """
    gate = threading.Event()
    backend = _stalled_backend(make_runtime, gate, queue_capacity=300, drop_policy="drop_oldest")
    try:
        for i in range(300):
            backend._queue.put_nowait(
                _canonical(
                    {"event": "tail" if i == 299 else f"c{i}"},
                    Delivery.TELEMETRY if i == 299 else Delivery.CRITICAL,
                )
            )
        backend.delivery.spool_event = lambda ev: True  # type: ignore[method-assign]
        evicted = backend._evict_oldest_event()
        # The telemetry item sits beyond the scan window, so the oldest
        # critical inside the window is the victim (spooled by the caller).
        assert evicted is not None
        assert evicted.delivery == Delivery.CRITICAL
        assert evicted.data["event"] == "c0"
        assert backend._queue.qsize() == 299
    finally:
        gate.set()
        backend.close(2.0)


def test_eviction_never_takes_sentinels(make_runtime):
    """_FlushRequest sentinels survive eviction inside the scan window."""
    gate = threading.Event()
    backend = _stalled_backend(make_runtime, gate, queue_capacity=4, drop_policy="drop_oldest")
    try:
        sentinel = _FlushRequest()
        backend._queue.put_nowait(sentinel)
        backend._queue.put_nowait(_canonical({"event": "t1"}))
        backend._queue.put_nowait(_canonical({"event": "t2"}))
        backend._queue.put_nowait(_canonical({"event": "t3"}))
        evicted = backend._evict_oldest_event()
        assert evicted is not None and evicted.data["event"] == "t1"
        remaining = [backend._queue.get_nowait() for _ in range(3)]
        assert sentinel in remaining
    finally:
        gate.set()
        backend.close(2.0)


# -- producer namespacing -------------------------------------------------------


def test_ambient_producer_namespaces_derived_ids(captured):
    """bind_context(producer=...) applies to events that declare no source."""
    _, drain = captured
    with bind_context(producer="dagster", run_id="r-1"):
        event("pipeline.step")
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    assert ev["source"]["producer"] == "dagster"


def test_explicit_producer_wins_over_ambient(captured):
    _, drain = captured
    with bind_context(producer="dagster"):
        event("pipeline.step", producer="custom")
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    assert ev["source"]["producer"] == "custom"


def test_observe_scope_producer_kwarg(captured):
    _, drain = captured
    with observe("pipeline.run", producer="dbt", correlation={"run_id": "r-7"}):
        pass
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    assert ev["source"]["producer"] == "dbt"


def test_producer_not_leaked_into_correlation_extra(captured):
    """The bound producer must not also land in correlation.extra."""
    _, drain = captured
    with bind_context(producer="dagster", tenant="acme"):
        event("pipeline.step")
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    extra = (ev.get("correlation") or {}).get("extra") or {}
    assert "producer" not in extra
    assert extra.get("tenant") == "acme"


# -- OTLP encode side ------------------------------------------------------------


def test_otlp_mapping_encodes_v2_sections():
    """entities/tags/contract are encoded into observe.* attributes."""
    import orjson
    from observe_core.otlp_mapping import event_to_otlp_attributes

    canonical = {
        "schema_version": "2.0",
        "event_id": "01JEXAMPLE",
        "event": "asset.materialize",
        "category": "data",
        "outcome": "success",
        "severity": "info",
        "delivery": "telemetry",
        "observed_at": "2026-01-05T06:00:00Z",
        "service": {"name": "svc"},
        "correlation": {"run_id": "r1"},
        "entities": {"asset": "asset://a", "run": "run://dagster/r1"},
        "tags": {"team": "data", "env": "prod"},
        "contract": {"schema_id": "asset/v1", "schema_hash": "abc"},
        "attributes": {"rows": 10},
    }
    attrs = event_to_otlp_attributes(canonical)
    assert orjson.loads(attrs["observe.entities"]) == canonical["entities"]
    assert orjson.loads(attrs["observe.tags"]) == canonical["tags"]
    assert orjson.loads(attrs["observe.contract"]) == canonical["contract"]
