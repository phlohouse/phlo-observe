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
        self._destination_spools: dict[str, Spool] = {}
        self._destination_lock = threading.Lock()

    @staticmethod
    def _destination_identity(drain: Drain) -> str:
        """Return a stable identity without putting the endpoint in filenames."""
        configured = getattr(drain, "spool_identity", None)
        if configured:
            return str(configured)
        return f"{getattr(drain, 'name', type(drain).__name__)}\0{getattr(drain, 'endpoint', '')}"

    def _spool_for(self, drain: Drain) -> Spool | None:
        if self.spool is None:
            return None
        identity = self._destination_identity(drain)
        with self._destination_lock:
            if identity not in self._destination_spools:
                destination = getattr(self.spool, "destination", None)
                if not callable(destination):
                    return None
                try:
                    self._destination_spools[identity] = destination(identity)
                except OSError as error:
                    self.stats.incr("spool_errors")
                    _diag(f"destination spool unavailable: {error}")
                    return None
            return self._destination_spools[identity]

    def endpoints(self) -> list[str]:
        """Remote drain endpoints, for health surfaces."""
        return [
            str(getattr(drain, "endpoint", getattr(drain, "name", "?")))
            for drain in self.drains
            if getattr(drain, "is_remote", False)
        ]

    def spool_event(self, event: CanonicalEvent, drain: Drain | None = None) -> bool:
        """Append a critical event to the spool; count failures."""
        from observe_core.runtime import TelemetryError  # noqa: PLC0415

        destinations = (
            [drain]
            if drain is not None
            else [item for item in self.drains if getattr(item, "is_remote", False)]
        )
        if destinations:
            results = [
                target is not None and target.append(event.payload)
                for item in destinations
                for target in [self._spool_for(item)]
            ]
            accepted = all(results)
        else:
            accepted = self.spool is not None and self.spool.append(event.payload)
        if accepted:
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
                            self.spool_event(event, drain)
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
        if self.spool is None:
            return
        now = time.monotonic()
        if now - self._last_replay < self.replay_interval_s:
            return
        self._last_replay = now
        for drain in self.drains:
            if getattr(drain, "is_remote", False):
                try:
                    target = self._spool_for(drain)
                    if target is not None and target.pending_segments():
                        target.replay(drain)
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

    def _spool_critical(self, event: CanonicalEvent) -> EmitResult:
        """Spool a critical event; the result reflects what actually happened."""
        if self.delivery.spool_event(event):
            return EmitResult(accepted=True, spooled=True)
        return EmitResult(accepted=False, dropped=True, reason="queue_full_spool_unavailable")

    def emit(self, event: CanonicalEvent) -> EmitResult:
        """Enqueue a canonical event, applying the configured drop policy."""
        try:
            self._queue.put_nowait(event)
            self.stats.incr("enqueued")
            return EmitResult(accepted=True, enqueued=True)
        except queue.Full:
            pass
        if event.delivery == Delivery.CRITICAL:
            return self._spool_critical(event)
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
                    return self._spool_critical(event)
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

    _EVICT_SCAN_LIMIT = 256
    """Max queue items inspected per eviction.

    Unbounded head-to-tail scans make every emit O(capacity) once the queue
    saturates — the drop policy then amplifies the pressure it exists to
    absorb. The window preserves the preference order (non-critical before
    critical) over the oldest part of the queue where eviction matters most.
    """

    def _evict_oldest_event(self) -> CanonicalEvent | None:
        """Remove one queued event to make room, preferring non-critical ones.

        Control sentinels (``_FlushRequest``/``_STOP``) are never evicted:
        dropping one would hang ``flush()``/``close()``. Critical events
        are evicted only when the scanned window holds nothing else; the
        caller spools them rather than counting a drop.

        The scan runs in place on the queue's own deque under its mutex:
        survivors keep their positions, so eviction never reorders the
        stream, and a racing producer cannot interleave between a pull and
        a re-put (the get/re-put dance could drop a sentinel).
        """
        with self._queue.mutex:
            items = self._queue.queue
            evicted_index: int | None = None
            critical_index: int | None = None
            for i in range(min(len(items), self._EVICT_SCAN_LIMIT)):
                item = items[i]
                if not isinstance(item, CanonicalEvent):
                    continue
                if item.delivery == Delivery.CRITICAL:
                    if critical_index is None:
                        critical_index = i
                else:
                    evicted_index = i
                    break
            index = evicted_index if evicted_index is not None else critical_index
            if index is None:
                return None
            evicted = items[index]
            del items[index]
            self._queue.not_full.notify()
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
