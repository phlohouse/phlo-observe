"""Pluggable runtime backends (spec §7.5).

The public API — ``observe()``, ``event()``, ``configure()`` — never depends
on how events are queued or exported. A :class:`RuntimeBackend` owns
transport after the runtime has produced a canonical event::

    class RuntimeBackend(Protocol):
        name: str
        def emit(self, event: CanonicalEvent) -> EmitResult: ...
        def flush(self, timeout: float | None = None) -> FlushResult: ...
        def health(self) -> BackendHealth: ...
        def close(self, timeout: float | None = None) -> None: ...

Three backends ship with observe-core:

- :class:`WorkerBackend` — bounded queue + background drain workers (the
  production default; same machinery as V1);
- :class:`SyncBackend` — delivers each event to drains inline, on the
  caller's thread (tests, CLIs, constrained contexts);
- :class:`CaptureBackend` — retains events in a bounded in-memory buffer for
  assertions, without touching drains.
"""

from __future__ import annotations

import contextlib
import queue
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from observe_core.drains.base import CanonicalEvent, Drain
from observe_core.models import Delivery
from observe_core.stats import TelemetryStats

if TYPE_CHECKING:
    from observe_core.config import ObserveSettings
    from observe_core.spool import Spool


def _diag(message: str) -> None:
    """Minimal stderr diagnostic that never raises."""
    with contextlib.suppress(Exception):
        sys.stderr.write(f"[observe-core] {message}\n")


@dataclass(frozen=True)
class EmitResult:
    """Outcome of handing one canonical event to a backend."""

    accepted: bool
    """The backend took responsibility for the event."""
    enqueued: bool = False
    """The event entered an async queue (worker backend)."""
    spooled: bool = False
    """The event went to the critical-event spool instead of the queue."""
    dropped: bool = False
    reason: str | None = None


@dataclass(frozen=True)
class FlushResult:
    """Outcome of a bounded flush."""

    ok: bool
    pending: int = 0


@dataclass
class BackendHealth:
    """Transport-level health reported by a backend (spec §7.8)."""

    backend: str
    queue_depth: int = 0
    queue_capacity: int = 0
    workers_alive: bool = True
    last_export_ok_at: float | None = None
    """Wall-clock timestamp of the last successful drain delivery."""
    last_export_error: str | None = None
    endpoints: list[str] = field(default_factory=list)
    """Configured remote drain endpoints (for health surfaces)."""


@runtime_checkable
class RuntimeBackend(Protocol):
    """Transport contract between the emission pipeline and the drains."""

    name: str

    def emit(self, event: CanonicalEvent) -> EmitResult:
        """Accept one canonical event.

        May raise ``TelemetryError`` only in fail-closed
        (``telemetry_required``) mode.
        """
        ...

    def flush(self, timeout: float | None = None) -> FlushResult:
        """Bounded wait until queued events reach drains."""
        ...

    def health(self) -> BackendHealth:
        """Current transport health."""
        ...

    def close(self, timeout: float | None = None) -> None:
        """Stop workers, flush drains, release resources. Idempotent."""
        ...


_STOP = object()


class _FlushRequest:
    __slots__ = ("done",)

    def __init__(self) -> None:
        self.done = threading.Event()


class DrainDelivery:
    """Shared drain fan-out: batch delivery, failure isolation, critical spool.

    Used by the worker backend per batch and the sync backend per event so
    both transports share identical drain-error and spool-on-failure
    semantics.
    """

    def __init__(
        self,
        drains: list[Drain],
        stats: TelemetryStats,
        spool: Spool | None,
        *,
        telemetry_required: bool = False,
        replay_interval_s: float = 30.0,
    ) -> None:
        self.drains = drains
        self.stats = stats
        self.spool = spool
        self.telemetry_required = telemetry_required
        self.replay_interval_s = replay_interval_s
        self.last_ok_at: float | None = None
        self.last_error: str | None = None
        self._last_replay = 0.0

    def endpoints(self) -> list[str]:
        """Remote drain endpoints, for health surfaces."""
        return [
            str(getattr(drain, "endpoint", getattr(drain, "name", "?")))
            for drain in self.drains
            if getattr(drain, "is_remote", False)
        ]

    def spool_event(self, event: CanonicalEvent) -> bool:
        """Append a critical event to the spool; count failures."""
        from observe_core.runtime import TelemetryError  # noqa: PLC0415

        if self.spool is not None and self.spool.append(event.payload):
            self.stats.incr("spooled_events")
            return True
        self.stats.incr("spool_errors")
        _diag(f"critical event {event.event!r} could not be queued or spooled")
        if self.telemetry_required:
            raise TelemetryError("critical event could not be queued or spooled")
        return False

    def deliver(self, batch: list[CanonicalEvent]) -> None:
        """Emit a batch to every drain; isolate per-drain failures."""
        if not batch:
            return
        for drain in self.drains:
            try:
                drain.emit_batch(batch)
                self.stats.incr("emitted_events", len(batch))
                self.last_ok_at = time.time()
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as error:  # drain isolation
                self.stats.incr("drain_errors")
                self.last_error = str(error)
                _diag(f"drain {getattr(drain, 'name', '?')} failed: {error}")
                # Spool criticals when a remote drain rejects them and that
                # drain opted in (HttpDrainConfig.spool_on_failure). Local
                # drain failures are counted but never spooled: the spool
                # replays to remote drains only.
                wants_spool = getattr(drain, "spool_on_failure", True)
                if getattr(drain, "is_remote", False) and wants_spool:
                    for event in batch:
                        if event.delivery == Delivery.CRITICAL:
                            self.spool_event(event)
        self.stats.incr("emitted_batches")

    def flush_drains(self) -> None:
        """Flush every drain, isolating failures to diagnostics."""
        for drain in self.drains:
            try:
                drain.flush()
            except Exception as error:
                _diag(f"drain {getattr(drain, 'name', '?')} flush failed: {error}")

    def close_drains(self) -> None:
        """Close every drain, isolating failures to diagnostics."""
        for drain in self.drains:
            try:
                drain.close()
            except Exception as error:
                _diag(f"drain {getattr(drain, 'name', '?')} close failed: {error}")

    def maybe_replay(self) -> None:
        """Replay pending spool segments to remote drains, on an interval."""
        if self.spool is None or self.spool.pending_segments() == 0:
            return
        now = time.monotonic()
        if now - self._last_replay < self.replay_interval_s:
            return
        self._last_replay = now
        for drain in self.drains:
            if getattr(drain, "is_remote", False):
                try:
                    self.spool.replay(drain)
                except (KeyboardInterrupt, SystemExit):
                    raise
                except BaseException as error:
                    _diag(f"spool replay via {drain.name} failed: {error}")


class WorkerBackend:
    """Bounded queue + background workers; the production transport."""

    name = "worker"

    def __init__(
        self,
        settings: ObserveSettings,
        stats: TelemetryStats,
        spool: Spool | None,
        drains: list[Drain],
    ) -> None:
        self.settings = settings
        self.stats = stats
        self.delivery = DrainDelivery(
            drains,
            stats,
            spool,
            telemetry_required=settings.telemetry_required,
            replay_interval_s=settings.spool_replay_interval_s,
        )
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=settings.queue_capacity)
        self._stop = threading.Event()
        self._closed = False
        self._workers = [
            threading.Thread(target=self._worker_loop, name=f"observe-core-{i}", daemon=True)
            for i in range(max(1, settings.worker_count))
        ]
        for worker in self._workers:
            worker.start()

    # -- admission -------------------------------------------------------------

    def emit(self, event: CanonicalEvent) -> EmitResult:
        """Enqueue a canonical event, applying the configured drop policy."""
        try:
            self._queue.put_nowait(event)
            self.stats.incr("enqueued")
            return EmitResult(accepted=True, enqueued=True)
        except queue.Full:
            pass
        if event.delivery == Delivery.CRITICAL:
            self.delivery.spool_event(event)
            return EmitResult(accepted=True, spooled=True)
        if self.settings.drop_policy == "drop_oldest":
            evicted = self._evict_oldest_event()
            if evicted is not None:
                if evicted.delivery == Delivery.CRITICAL:
                    # Critical events are never silently dropped: an evicted
                    # one goes to the spool instead (spec §12.1).
                    self.delivery.spool_event(evicted)
                else:
                    self.stats.incr(
                        "dropped_debug"
                        if evicted.delivery == Delivery.DEBUG
                        else "dropped_telemetry"
                    )
            try:
                self._queue.put_nowait(event)
                self.stats.incr("enqueued")
                return EmitResult(accepted=True, enqueued=True)
            except queue.Full:
                if event.delivery == Delivery.CRITICAL:
                    self.delivery.spool_event(event)
                    return EmitResult(accepted=True, spooled=True)
                self._drop_noncritical(event.delivery)
                return EmitResult(accepted=False, dropped=True, reason="queue_full")
        self._drop_noncritical(event.delivery)
        return EmitResult(accepted=False, dropped=True, reason="queue_full")

    def _drop_noncritical(self, delivery: Delivery) -> None:
        from observe_core.runtime import TelemetryError  # noqa: PLC0415

        field_name = "dropped_debug" if delivery == Delivery.DEBUG else "dropped_telemetry"
        self.stats.incr(field_name)
        if self.settings.telemetry_required:
            raise TelemetryError("observe queue is full")

    def _evict_oldest_event(self) -> CanonicalEvent | None:
        """Remove one queued event to make room, preferring non-critical ones.

        Control sentinels (``_FlushRequest``/``_STOP``) are never evicted:
        dropping one would hang ``flush()``/``close()``. Critical events
        are evicted only when the queue holds nothing else; the caller spools
        them rather than counting a drop. Items pulled during the scan are
        re-queued in their original order.
        """
        held: list[Any] = []
        evicted: CanonicalEvent | None = None
        oldest_critical: CanonicalEvent | None = None
        # Drain the whole queue so survivors can be re-queued in their
        # original order; stopping at the first eviction would move held items
        # behind everything never dequeued.
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if isinstance(item, CanonicalEvent):
                if item.delivery == Delivery.CRITICAL:
                    if oldest_critical is None:
                        oldest_critical = item
                elif evicted is None:
                    evicted = item
                    continue
            held.append(item)
        if evicted is None and oldest_critical is not None:
            # Queue holds only criticals/sentinels: the oldest critical makes
            # room and is spooled by the caller. Compare by identity — two
            # canonical events may be equal by value.
            for i, item in enumerate(held):
                if item is oldest_critical:
                    del held[i]
                    break
            evicted = oldest_critical
        for item in held:
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                # A racing producer refilled the slot between the scan and the
                # re-put. Sentinels must never be lost; criticals go to the
                # spool; anything else counts as a normal drop.
                if isinstance(item, CanonicalEvent):
                    if item.delivery == Delivery.CRITICAL:
                        self.delivery.spool_event(item)
                    else:
                        self.stats.incr(
                            "dropped_debug"
                            if item.delivery == Delivery.DEBUG
                            else "dropped_telemetry"
                        )
                else:
                    with contextlib.suppress(queue.Full):
                        self._queue.put(item, timeout=0.5)
        return evicted

    # -- worker ------------------------------------------------------------------

    def _worker_loop(self) -> None:
        batch: list[CanonicalEvent] = []
        while True:
            deadline = time.monotonic() + self.settings.flush_interval_ms / 1000.0
            while len(batch) < self.settings.batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = self._queue.get(timeout=remaining)
                except queue.Empty:
                    break
                if item is _STOP:
                    self.delivery.deliver(batch)
                    self.delivery.flush_drains()
                    return
                if isinstance(item, _FlushRequest):
                    self.delivery.deliver(batch)
                    batch = []
                    self.delivery.flush_drains()
                    self.stats.incr("flushes")
                    item.done.set()
                    continue
                batch.append(item)
            if batch:
                self.delivery.deliver(batch)
                batch = []
            self.delivery.maybe_replay()
            if self._stop.is_set() and self._queue.empty():
                self.delivery.flush_drains()
                return

    # -- lifecycle ---------------------------------------------------------------

    def queue_depth(self) -> int:
        """Events currently waiting in the queue."""
        return self._queue.qsize()

    def workers_alive(self) -> bool:
        """True while every worker thread runs its drain loop."""
        return all(worker.is_alive() for worker in self._workers)

    def flush(self, timeout: float | None = None) -> FlushResult:
        """Bounded wait until queued events have been handed to drains."""
        if self._closed:
            return FlushResult(ok=True)
        budget = 5.0 if timeout is None else timeout
        deadline = time.monotonic() + budget
        requests = []
        for _ in self._workers:
            req = _FlushRequest()
            try:
                self._queue.put(req, timeout=max(0.0, deadline - time.monotonic()))
            except queue.Full:
                return FlushResult(ok=False, pending=self._queue.qsize())
            requests.append(req)
        remaining = deadline - time.monotonic()
        ok = all(req.done.wait(max(0.0, remaining)) for req in requests)
        return FlushResult(ok=ok, pending=self._queue.qsize())

    def health(self) -> BackendHealth:
        """Queue depth, worker liveness and last export outcome."""
        return BackendHealth(
            backend=self.name,
            queue_depth=self._queue.qsize(),
            queue_capacity=self.settings.queue_capacity,
            workers_alive=self.workers_alive(),
            last_export_ok_at=self.delivery.last_ok_at,
            last_export_error=self.delivery.last_error,
            endpoints=self.delivery.endpoints(),
        )

    def close(self, timeout: float | None = None) -> None:
        """Stop workers, flush drains and release resources. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        budget = 5.0 if timeout is None else timeout
        deadline = time.monotonic() + budget
        for _ in self._workers:
            try:
                self._queue.put(_STOP, timeout=max(0.05, deadline - time.monotonic()))
            except queue.Full:
                break
        per_worker = max(0.05, (deadline - time.monotonic()) / len(self._workers))
        for worker in self._workers:
            worker.join(timeout=per_worker)
        self.delivery.close_drains()


class SyncBackend:
    """Delivers each event to drains on the caller's thread.

    For tests, CLIs and contexts where a background worker is unwanted.
    Events still flow through the full runtime pipeline — redaction,
    sampling, size limits — only the transport is synchronous.
    """

    name = "sync"

    def __init__(
        self,
        settings: ObserveSettings,
        stats: TelemetryStats,
        spool: Spool | None,
        drains: list[Drain],
    ) -> None:
        self.delivery = DrainDelivery(
            drains,
            stats,
            spool,
            telemetry_required=settings.telemetry_required,
            replay_interval_s=settings.spool_replay_interval_s,
        )
        self.stats = stats
        self._closed = False

    def emit(self, event: CanonicalEvent) -> EmitResult:
        """Deliver the event to drains on the calling thread."""
        if self._closed:
            return EmitResult(accepted=False, dropped=True, reason="closed")
        self.stats.incr("enqueued")
        self.delivery.deliver([event])
        self.delivery.maybe_replay()
        return EmitResult(accepted=True)

    def flush(self, timeout: float | None = None) -> FlushResult:
        """Flush drains; the transport itself never holds events."""
        self.delivery.flush_drains()
        self.stats.incr("flushes")
        return FlushResult(ok=True)

    def health(self) -> BackendHealth:
        """Always-alive transport; reports drain delivery status."""
        return BackendHealth(
            backend=self.name,
            workers_alive=True,
            last_export_ok_at=self.delivery.last_ok_at,
            last_export_error=self.delivery.last_error,
            endpoints=self.delivery.endpoints(),
        )

    def close(self, timeout: float | None = None) -> None:
        """Flush and close the drains. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self.delivery.flush_drains()
        self.delivery.close_drains()


class CaptureBackend:
    """Retains events in a bounded in-memory buffer; drains are unused.

    For tests and embedders that assert on canonical envelopes. ``events``
    returns captured payloads oldest-first; ``clear()`` resets the buffer.
    ``capacity`` bounds memory (spec §7.5/§39); overflow drops oldest.
    """

    name = "capture"

    def __init__(self, capacity: int = 10_000, stats: TelemetryStats | None = None) -> None:
        self.capacity = capacity
        self.stats = stats or TelemetryStats()
        self._events: deque[CanonicalEvent] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._closed = False

    def emit(self, event: CanonicalEvent) -> EmitResult:
        """Retain the event in the bounded buffer; overflow drops oldest."""
        if self._closed:
            return EmitResult(accepted=False, dropped=True, reason="closed")
        with self._lock:
            overflow = len(self._events) == self.capacity
            self._events.append(event)
        self.stats.incr("enqueued")
        if overflow:
            self.stats.incr(
                "dropped_debug" if event.delivery == Delivery.DEBUG else "dropped_telemetry"
            )
        return EmitResult(accepted=True)

    @property
    def events(self) -> list[CanonicalEvent]:
        """Captured events, oldest first."""
        with self._lock:
            return list(self._events)

    def payloads(self) -> list[dict[str, Any]]:
        """Captured canonical payloads (``orjson``-decoded)."""
        import orjson  # noqa: PLC0415

        return [orjson.loads(event.payload) for event in self.events]

    def clear(self) -> None:
        """Drop all captured events."""
        with self._lock:
            self._events.clear()

    def flush(self, timeout: float | None = None) -> FlushResult:
        """No-op: nothing is buffered for delivery."""
        self.stats.incr("flushes")
        return FlushResult(ok=True)

    def health(self) -> BackendHealth:
        """Buffer depth against the configured capacity."""
        with self._lock:
            depth = len(self._events)
        return BackendHealth(
            backend=self.name,
            queue_depth=depth,
            queue_capacity=self.capacity,
            workers_alive=True,
        )

    def close(self, timeout: float | None = None) -> None:
        """Mark the backend closed; subsequent emits are rejected."""
        self._closed = True
