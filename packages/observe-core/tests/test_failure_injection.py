"""Failure injection (spec §82): worker/drain faults and shutdown under load.

Covered elsewhere: observer unavailable/HTTP timeout/429/500 (test_http_drain),
spool full and corrupt segments (test_spool), queue policies
(test_queue_policies), auth (observer test_auth).
"""

from __future__ import annotations

import time

from observe_core import event, flush, shutdown
from observe_core.drains.memory import MemoryDrain
from observe_core.runtime import Runtime


class _FlakyDrain(MemoryDrain):
    """Raises for the first ``fail_for`` batches, then records normally."""

    def __init__(self, fail_for: int) -> None:
        super().__init__()
        self.fail_for = fail_for
        self.calls = 0

    def emit_batch(self, events):
        self.calls += 1
        if self.calls <= self.fail_for:
            raise RuntimeError("injected drain failure")
        super().emit_batch(events)


def test_worker_survives_drain_exceptions(make_runtime):
    """A drain raising inside the worker loop must not kill the worker."""
    rt = make_runtime(flush_interval_ms=20, batch_size=1)
    drain = _FlakyDrain(fail_for=3)
    rt.drains.clear()
    rt.drains.append(drain)
    for _ in range(6):
        event("application.log")
    flush(3.0)
    stats = rt.stats.snapshot()
    assert stats["drain_errors"] == 3
    assert len(drain.events) == 3  # first 3 batches lost; later ones delivered
    assert rt.workers_alive()


def test_worker_survives_flaky_drain_under_load(make_runtime):
    """Intermittent drain failures don't wedge the pipeline permanently."""
    rt = make_runtime(flush_interval_ms=20, batch_size=1)
    drain = _FlakyDrain(fail_for=5)
    rt.drains.clear()
    rt.drains.append(drain)
    for i in range(10):
        event("application.log", attributes={"i": i})
    flush(3.0)
    assert len(drain.events) == 5
    assert rt.stats.snapshot()["drain_errors"] == 5


def test_shutdown_drains_queued_events(make_runtime):
    """Process shutdown with queued events still delivers them (bounded)."""
    rt = make_runtime(queue_capacity=500, flush_interval_ms=20)
    drain = rt.drains[0]
    for i in range(120):  # exceeds a single batch_size=100
        event("application.log", attributes={"i": i})
    shutdown(5.0)
    assert len(drain.events) == 120


def test_shutdown_with_slow_drain_is_bounded(make_runtime):
    """A drain stuck in emit_batch cannot hang shutdown forever."""

    class StuckDrain(MemoryDrain):
        def emit_batch(self, events):
            time.sleep(60)  # pragma: no cover - interrupted by worker timeout

    rt = make_runtime(flush_interval_ms=20)
    stuck = StuckDrain()
    rt.drains.clear()
    rt.drains.append(stuck)
    for _ in range(3):
        event("application.log")
    start = time.monotonic()
    shutdown(1.0)
    elapsed = time.monotonic() - start
    assert elapsed < 30, "shutdown did not respect its bound"


def test_events_after_shutdown_are_dropped_not_raised(make_runtime):
    """Emitting on a closed runtime counts a drop instead of raising."""
    from observe_core.builder import EventBuilder

    rt = make_runtime()
    rt.shutdown(2.0)  # instance-level: the global still references this one
    rt.emit(EventBuilder("application.log"))
    assert rt.stats.snapshot()["dropped_closed"] == 1


def test_reconfigure_resets_cleanly(make_runtime):
    """configure() twice: second runtime independent of the first."""
    rt1 = make_runtime()
    drain1 = rt1.drains[0]
    event("application.start")
    flush(2.0)
    assert len(drain1.events) == 1
    rt2 = make_runtime()
    drain2 = rt2.drains[0]
    assert drain2 is not drain1
    assert isinstance(rt2, Runtime)
    event("application.start")
    flush(2.0)
    assert len(drain2.events) == 1
    assert len(drain1.events) == 1  # first runtime no longer receives
