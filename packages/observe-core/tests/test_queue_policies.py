"""Bounded queue, backpressure policies, oversized events, stats."""

from __future__ import annotations

import threading
import time
from collections.abc import Sequence

import pytest
from observe_core import TelemetryError, event, flush, observe, shutdown
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


class _FirstCallSlowDrain(MemoryDrain):
    """Block only the first worker delivery, allowing the second to run."""

    def __init__(self, gate: threading.Event, entered: threading.Event) -> None:
        super().__init__()
        self.gate = gate
        self.entered = entered
        self.calls = 0

    def emit_batch(self, events: Sequence[CanonicalEvent]) -> None:
        self.calls += 1
        if self.calls == 1:
            self.entered.set()
            self.gate.wait(timeout=10)
        super().emit_batch(events)


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
    from observe_core.backends import _FlushRequest

    gate = threading.Event()
    rt = _stall_runtime(make_runtime, gate, drop_policy="drop_oldest")
    try:
        req = _FlushRequest()
        rt._backend._queue.put_nowait(req)
        for _ in range(20):
            event("application.log", delivery="telemetry")
        # evictions happened, but the sentinel is still queued
        assert req in rt._backend._queue.queue
        assert not req.done.is_set()
        gate.set()
        assert req.done.wait(5.0), "flush sentinel was lost to drop_oldest"
    finally:
        gate.set()
        shutdown(2.0)


def test_flush_waits_for_all_workers_and_supports_concurrent_calls(make_runtime):
    """Flush barriers include an event held by another worker."""
    gate = threading.Event()
    entered = threading.Event()
    rt = make_runtime(worker_count=2, queue_capacity=20, flush_interval_ms=10)
    slow = _FirstCallSlowDrain(gate, entered)
    rt.drains.clear()
    rt.drains.append(slow)
    rt._backend.delivery.drains = rt.drains
    try:
        event("warmup.fill")
        assert entered.wait(2.0)
        event("application.log")

        results: list[bool] = []
        threads = [threading.Thread(target=lambda: results.append(flush(0.1))) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(1.0)
        assert results == [False, False]

        gate.set()
        results.clear()
        threads = [threading.Thread(target=lambda: results.append(flush(2.0))) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(2.0)
        assert results == [True, True]
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


def test_drop_oldest_prefers_evicting_telemetry(make_runtime, tmp_path):
    """Under drop_oldest, a queued critical event survives telemetry pressure."""
    gate = threading.Event()
    rt = _stall_runtime(
        make_runtime,
        gate,
        drop_policy="drop_oldest",
        queue_capacity=3,
        spool_enabled=True,
        spool_dir=tmp_path / "spool",
    )
    try:
        event("wap.promote", delivery="critical")  # queued first, has room
        for _ in range(5):
            event("application.log")
        queued = [
            item.event for item in rt._backend._queue.queue if isinstance(item, CanonicalEvent)
        ]
        assert "wap.promote" in queued, "critical event was evicted over telemetry"
        stats = rt.stats.snapshot()
        assert stats["spooled_events"] == 0
        assert stats["dropped_telemetry"] >= 3
    finally:
        gate.set()
        shutdown(2.0)


def test_drop_oldest_spools_evicted_critical(make_runtime, tmp_path):
    """Regression: evicting a queued critical event must spool it, not drop it.

    Previously an evicted critical was counted as dropped_telemetry and lost.
    """
    gate = threading.Event()
    rt = _stall_runtime(
        make_runtime,
        gate,
        drop_policy="drop_oldest",
        queue_capacity=3,
        spool_enabled=True,
        spool_dir=tmp_path / "spool",
    )
    try:
        for i in range(3):
            event("wap.promote", delivery="critical", attributes={"i": i})
        # Queue now holds only criticals; one more event evicts the oldest.
        event("application.log")
        stats = rt.stats.snapshot()
        assert stats["spooled_events"] == 1
        assert rt.spool is not None
        bodies = b"\n".join(
            line for seg in rt.spool._segments() for line in seg.read_bytes().splitlines()
        )
        assert b"wap.promote" in bodies
        assert stats["dropped_telemetry"] == 0
    finally:
        gate.set()
        shutdown(2.0)


def test_remote_drain_failure_spools_criticals(make_runtime, tmp_path):
    """A failing remote drain spools criticals (spool_on_failure default on)."""
    gate = threading.Event()
    rt = _stall_runtime(make_runtime, gate, spool_enabled=True, spool_dir=tmp_path / "spool")
    gate.set()

    class FailingRemote(MemoryDrain):
        is_remote = True

        def emit_batch(self, events):
            raise RuntimeError("observer down")

    rt.drains.clear()
    rt.drains.append(FailingRemote())
    try:
        event("wap.promote", delivery="critical")
        event("application.log")
        flush(3.0)
        stats = rt.stats.snapshot()
        assert stats["drain_errors"] >= 1
        assert stats["spooled_events"] == 1
    finally:
        shutdown(2.0)


def test_spool_on_failure_disabled_skips_spooling(make_runtime, tmp_path):
    """HttpDrainConfig.spool_on_failure=False must not spool on drain failure."""
    gate = threading.Event()
    rt = _stall_runtime(make_runtime, gate, spool_enabled=True, spool_dir=tmp_path / "spool")
    gate.set()

    class FailingRemote(MemoryDrain):
        is_remote = True
        spool_on_failure = False

        def emit_batch(self, events):
            raise RuntimeError("observer down")

    rt.drains.clear()
    rt.drains.append(FailingRemote())
    try:
        event("wap.promote", delivery="critical")
        flush(3.0)
        stats = rt.stats.snapshot()
        assert stats["drain_errors"] >= 1
        assert stats["spooled_events"] == 0
    finally:
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


def test_still_oversized_raises_when_telemetry_required(make_runtime):
    """Regression: telemetry_required must fail closed on oversized events.

    The raise inside ``_finalize`` was previously swallowed by ``emit``'s
    catch-all (which only re-raised under ``fail_fast``), so an oversized
    event silently vanished even with telemetry_required on.
    """
    rt = make_runtime(telemetry_required=True, fail_fast=False, max_event_bytes=600)
    with pytest.raises(TelemetryError, match="exceeds size limit"):
        event("application.log", attributes={"blob": "x" * 100_000})
    assert rt.stats.snapshot()["dropped_oversized"] == 1


def test_drop_oldest_preserves_queue_order(make_runtime):
    """Regression: eviction must not reorder survivors.

    Previously held items were re-queued at the tail, moving criticals
    dequeued during the scan behind events that were never dequeued.
    """
    gate = threading.Event()
    rt = _stall_runtime(make_runtime, gate, drop_policy="drop_oldest", queue_capacity=4)
    try:
        event("wap.promote", delivery="critical", attributes={"tag": "c0"})
        for i in range(3):
            event("application.log", attributes={"tag": f"t{i}"})
        # Queue full at [c0, t0, t1, t2]; emitting t3 evicts t0, the oldest
        # non-critical. c0 must stay ahead of t1/t2, not slide to the tail.
        event("application.log", attributes={"tag": "t3"})
        tags = [
            item.data["attributes"]["tag"]
            for item in rt._backend._queue.queue
            if isinstance(item, CanonicalEvent)
        ]
        assert tags == ["c0", "t1", "t2", "t3"]
    finally:
        gate.set()
        shutdown(2.0)


def test_stats_surface_counters(captured):
    rt, _drain = captured
    event("application.start")
    flush(2.0)
    stats = rt.stats.snapshot()
    assert stats["emitted_events"] == 1
    assert stats["drain_errors"] == 0
    assert stats["flushes"] >= 1
