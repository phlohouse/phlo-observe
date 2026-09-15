"""Bounded queue, backpressure policies, oversized events, stats."""

from __future__ import annotations

import threading
import time
from collections.abc import Sequence

from observe_core import event, flush, observe, shutdown
from observe_core.drains.base import CanonicalEvent
from observe_core.drains.memory import MemoryDrain
from observe_core.runtime import Runtime


class _SlowDrain(MemoryDrain):
    """Drain that blocks inside emit_batch until released."""

    def __init__(self, gate: threading.Event) -> None:
        super().__init__()
        self.gate = gate
        self.entered = 0

    def emit_batch(self, events: Sequence[CanonicalEvent]) -> None:
        self.entered += 1
        self.gate.wait(timeout=10)


def _stall_runtime(make_runtime, gate: threading.Event, **kw) -> Runtime:
    """Runtime whose worker is wedged inside a slow drain."""
    kw.setdefault("queue_capacity", 5)
    rt = make_runtime(**kw)
    slow = _SlowDrain(gate)
    rt.drains.clear()
    rt.drains.append(slow)
    event("warmup.fill")  # occupies the worker inside emit_batch
    deadline = time.time() + 2
    while slow.entered == 0 and time.time() < deadline:
        time.sleep(0.005)
    assert slow.entered == 1
    return rt


def test_queue_full_telemetry_dropped(make_runtime):
    gate = threading.Event()
    rt = _stall_runtime(make_runtime, gate, drop_policy="drop_newest")
    try:
        for _ in range(20):
            event("application.log", delivery="telemetry")
        stats = rt.stats.snapshot()
        assert stats["dropped_telemetry"] >= 14  # 5 queued + 1 in-flight
        assert stats["enqueued"] == 6  # warmup + 5
    finally:
        gate.set()
        shutdown(2.0)


def test_queue_full_drop_oldest_evicts(make_runtime):
    gate = threading.Event()
    rt = _stall_runtime(make_runtime, gate, drop_policy="drop_oldest")
    try:
        for i in range(10):
            event("application.log", attributes={"i": i})
        stats = rt.stats.snapshot()
        assert stats["dropped_telemetry"] >= 5
    finally:
        gate.set()
        shutdown(2.0)


def test_drop_oldest_preserves_flush_sentinels(make_runtime):
    """A queued _FlushRequest must survive drop_oldest eviction.

    Regression: previously drop_oldest evicted the queue head even when it was
    a control sentinel, which could hang flush()/shutdown() under pressure.
    """
    from observe_core.runtime import _FlushRequest

    gate = threading.Event()
    rt = _stall_runtime(make_runtime, gate, drop_policy="drop_oldest")
    try:
        req = _FlushRequest()
        rt._queue.put_nowait(req)
        for _ in range(20):
            event("application.log", delivery="telemetry")
        # evictions happened, but the sentinel is still queued
        assert req in rt._queue.queue
        assert not req.done.is_set()
        gate.set()
        assert req.done.wait(5.0), "flush sentinel was lost to drop_oldest"
    finally:
        gate.set()
        shutdown(2.0)


def test_critical_event_spooled_when_queue_full(make_runtime, tmp_path):
    gate = threading.Event()
    rt = _stall_runtime(make_runtime, gate, spool_enabled=True, spool_dir=tmp_path / "spool")
    try:
        for _ in range(10):
            event("application.log")
        event("wap.promote", delivery="critical")
        stats = rt.stats.snapshot()
        assert stats["spooled_events"] == 1
        assert rt.spool is not None
        assert rt.spool.pending_bytes() > 0
    finally:
        gate.set()
        shutdown(2.0)


def test_oversized_event_truncated(make_runtime):
    rt = make_runtime(max_event_bytes=4_096)
    drain = rt.drains[0]
    with observe("ingestion.load") as evt:
        evt.set(payload="x" * 100_000)
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    assert ev["attributes"]["_observe"]["truncated"] is True
    assert "attributes.payload" in ev["attributes"]["_observe"]["fields"]
    assert rt.stats.snapshot()["truncated_events"] >= 1


def test_still_oversized_dropped(make_runtime):
    rt = make_runtime(max_event_bytes=600)
    drain = rt.drains[0]
    event("application.log", attributes={"blob": "x" * 100_000})
    flush(2.0)
    assert drain.events == []
    assert rt.stats.snapshot()["dropped_oversized"] == 1


def test_stats_surface_counters(captured):
    rt, _drain = captured
    event("application.start")
    flush(2.0)
    stats = rt.stats.snapshot()
    assert stats["emitted_events"] == 1
    assert stats["drain_errors"] == 0
    assert stats["flushes"] >= 1
