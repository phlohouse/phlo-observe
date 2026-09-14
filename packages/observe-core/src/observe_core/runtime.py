"""The emission runtime: bounded queue, background workers, drain fan-out.

Completed event flow (per spec §11)::

    builder -> merge context -> normalize -> enrich -> redact
      -> validate limits -> sample -> enqueue -> worker batch -> drains

Nothing on the application path performs network I/O: ``emit`` only builds the
canonical event and enqueues it.
"""

from __future__ import annotations

import atexit
import contextlib
import logging
import queue
import sys
import threading
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from observe_core.config import (
    ConsoleDrainConfig,
    HttpDrainConfig,
    JsonlDrainConfig,
    MemoryDrainConfig,
    ObserveSettings,
    OtlpDrainConfig,
)
from observe_core.context import ambient_correlation, ambient_extra, ambient_service
from observe_core.drains.base import CanonicalEvent, Drain
from observe_core.drains.console import ConsoleDrain
from observe_core.drains.http import HttpDrain
from observe_core.drains.jsonl import JsonlDrain
from observe_core.drains.memory import MemoryDrain
from observe_core.errors import ObservedError, error_info_from_exception
from observe_core.ids import new_event_id
from observe_core.models import (
    CORRELATION_KEYS,
    Correlation,
    Delivery,
    EventEnvelope,
    Outcome,
    Severity,
)
from observe_core.redaction import Redactor
from observe_core.sampling import Sampler
from observe_core.serialization import dumps, normalize_value
from observe_core.spool import Spool
from observe_core.stats import TelemetryStats
from observe_core.timestamps import utcnow

if TYPE_CHECKING:
    from observe_core.builder import EventBuilder
    from observe_core.config import ObserveSettings
    from observe_core.enrich import Enricher

_log = logging.getLogger("observe_core")

_STOP = object()
_TRUNCATE_FIELD_BYTES = 4 * 1024
_TRUNCATE_STACKTRACE_BYTES = 16 * 1024


def _opt_str(value: Any) -> str | None:
    return None if value is None else str(value)


class TelemetryError(Exception):
    """Raised in ``telemetry_required`` mode when an event cannot be accepted."""


class _FlushRequest:
    __slots__ = ("done",)

    def __init__(self) -> None:
        self.done = threading.Event()


def _diag(message: str) -> None:
    """Minimal stderr diagnostic that never raises."""
    with contextlib.suppress(Exception):
        sys.stderr.write(f"[observe-core] {message}\n")


class Runtime:
    """Owns the queue, workers, drains, spool, redactor and sampler."""

    def __init__(self, settings: ObserveSettings, enrichers: Iterable[Enricher] = ()) -> None:
        self.settings = settings
        self.stats = TelemetryStats()
        self.enrichers: list[Enricher] = list(enrichers)
        self.redactor = Redactor(
            extra_keys=settings.redact_keys,
            key_patterns=settings.redact_key_patterns,
            path_rules=settings.redact_paths,
            value_patterns=settings.redact_value_patterns,
            max_depth=settings.max_depth,
            enabled=settings.redaction_enabled,
        )
        self.sampler = Sampler(
            debug_rate=settings.resolved_debug_rate(),
            telemetry_rate=settings.sampling_telemetry_rate,
        )
        self.spool = (
            Spool(
                settings.resolved_spool_dir(),
                max_bytes=settings.spool_max_bytes,
                segment_max_bytes=settings.spool_segment_max_bytes,
                on_full=settings.spool_on_full,
                stats=self.stats,
            )
            if settings.spool_enabled
            else None
        )
        self.drains: list[Drain] = [self._build_drain(cfg) for cfg in settings.drains]
        self._queue: queue.Queue[Any] = queue.Queue(maxsize=settings.queue_capacity)
        self._stop = threading.Event()
        self._closed = False
        self._last_replay = 0.0
        self._workers = [
            threading.Thread(target=self._worker_loop, name=f"observe-core-{i}", daemon=True)
            for i in range(max(1, settings.worker_count))
        ]
        for worker in self._workers:
            worker.start()

    # -- construction ---------------------------------------------------------

    @staticmethod
    def _build_drain(cfg: Any) -> Drain:
        if isinstance(cfg, ConsoleDrainConfig):
            return ConsoleDrain(
                stream=sys.stderr if cfg.stream == "stderr" else sys.stdout,
                color=cfg.color,
                show_error_details=cfg.show_error_details,
            )
        if isinstance(cfg, JsonlDrainConfig):
            return JsonlDrain(
                cfg.path,
                max_bytes=cfg.max_bytes,
                backup_count=cfg.backup_count,
                fsync=cfg.fsync,
            )
        if isinstance(cfg, HttpDrainConfig):
            return HttpDrain(
                cfg.endpoint,
                token=cfg.token,
                api_key=cfg.api_key,
                api_key_header=cfg.api_key_header,
                headers=cfg.headers,
                connect_timeout_s=cfg.connect_timeout_s,
                read_timeout_s=cfg.read_timeout_s,
                max_attempts=cfg.max_attempts,
                backoff_base_ms=cfg.backoff_base_ms,
                backoff_cap_ms=cfg.backoff_cap_ms,
                gzip_threshold_bytes=cfg.gzip_threshold_bytes,
            )
        if isinstance(cfg, OtlpDrainConfig):
            from observe_core.drains.otlp import OtlpDrain  # noqa: PLC0415 - optional deps

            return OtlpDrain(cfg.endpoint, headers=cfg.headers)
        if isinstance(cfg, MemoryDrainConfig):
            return MemoryDrain()
        raise TypeError(f"unsupported drain config: {cfg!r}")

    # -- emit path --------------------------------------------------------------

    def emit(self, builder: EventBuilder, exc: BaseException | None = None) -> None:
        """Finalize the builder and enqueue the canonical event.

        Never raises (unless ``fail_fast``/``telemetry_required`` are set);
        telemetry must not take down the workload.
        """
        if not self.settings.enabled:
            self.stats.incr("dropped_disabled")
            return
        if self._closed:
            self.stats.incr("dropped_closed")
            return
        try:
            event = self._finalize(builder, exc)
        except Exception as error:  # fail open by default
            if self.settings.fail_fast:
                raise
            self.stats.incr("worker_errors")
            _diag(f"event finalization failed for {builder.event!r}: {error}")
            return
        if event is None:
            return
        if not self.sampler.should_keep(event.delivery, event.event, self._sample_key(event.data)):
            self.stats.incr("dropped_sampled")
            return
        self._offer(event)

    @staticmethod
    def _sample_key(data: dict[str, Any]) -> str | None:
        corr = data.get("correlation") or {}
        for key in ("run_id", "trace_id", "invocation_id", "request_id"):
            if corr.get(key):
                return str(corr[key])
        return None

    def _finalize(self, builder: EventBuilder, exc: BaseException | None) -> CanonicalEvent | None:
        if exc is not None:
            if builder.outcome == Outcome.UNKNOWN:
                builder.outcome = Outcome.FAILURE
            if not builder._explicit_severity:
                builder.severity = Severity.ERROR
            if builder.error is None:
                builder.error = error_info_from_exception(
                    exc,
                    include_traceback=self.settings.capture_stacktrace
                    and not isinstance(exc, ObservedError),
                )
        elif builder.outcome == Outcome.UNKNOWN:
            builder.outcome = Outcome.SUCCESS

        for enricher in self.enrichers:
            try:
                enricher.enrich(builder)
            except Exception as error:  # bad enricher must not kill emit
                self.stats.incr("worker_errors")
                _diag(f"enricher {type(enricher).__name__} failed: {error}")

        merged_corr = {**ambient_correlation(), **builder.correlation}
        correlation = Correlation(
            **{k: _opt_str(merged_corr.get(k)) for k in CORRELATION_KEYS},
            extra=normalize_value(
                {
                    **ambient_extra(),
                    **{k: v for k, v in merged_corr.items() if k not in CORRELATION_KEYS},
                },
                max_depth=self.settings.max_depth,
            ),
        )
        ambient_svc = ambient_service()
        envelope = EventEnvelope(
            event_id=new_event_id(),
            event=builder.event,
            category=builder.category,
            outcome=builder.outcome,
            severity=builder.severity,
            delivery=builder.delivery,
            started_at=builder.started_at,
            ended_at=builder.ended_at,
            duration_ms=builder.duration_ms,
            observed_at=utcnow(),
            service={
                "name": ambient_svc.get("service_name") or self.settings.service_name,
                "version": ambient_svc.get("service_version") or self.settings.service_version,
                "instance_id": ambient_svc.get("instance_id") or self.settings.instance_id,
                "environment": ambient_svc.get("environment") or self.settings.environment,
                "host": ambient_svc.get("host") or self.settings.host,
            },
            correlation=correlation,
            attributes=normalize_value(builder.attributes, max_depth=self.settings.max_depth),
            error=builder.error,
            source=builder.source,
        )
        data = envelope.to_canonical_dict()
        self.redactor.redact_event(data)
        payload = dumps(data)
        if len(payload) > self.settings.max_event_bytes:
            data = self._truncate(data)
            payload = dumps(data)
        if len(payload) > self.settings.max_event_bytes:
            self.stats.incr("dropped_oversized")
            _diag(
                f"dropped oversized event {builder.event!r} "
                f"({len(payload)}B > {self.settings.max_event_bytes}B)"
            )
            if self.settings.telemetry_required:
                raise TelemetryError(f"event {builder.event!r} exceeds size limit")
            return None
        return CanonicalEvent(data=data, payload=payload, delivery=envelope.delivery)

    def _truncate(self, data: dict[str, Any]) -> dict[str, Any]:
        """Truncate known-large optional fields; mark ``_observe.truncated``."""
        truncated_fields: list[str] = []
        attributes = data.get("attributes")
        if isinstance(attributes, dict):
            for key, value in list(attributes.items()):
                if isinstance(value, str) and len(value.encode()) > _TRUNCATE_FIELD_BYTES:
                    attributes[key] = value[: _TRUNCATE_FIELD_BYTES // 4] + "…[truncated]"
                    truncated_fields.append(f"attributes.{key}")
        error = data.get("error")
        if isinstance(error, dict):
            stack = error.get("stacktrace")
            if isinstance(stack, str) and len(stack.encode()) > _TRUNCATE_STACKTRACE_BYTES:
                error["stacktrace"] = stack[:_TRUNCATE_STACKTRACE_BYTES] + "\n…[truncated]"
                truncated_fields.append("error.stacktrace")
        if truncated_fields:
            meta = data.setdefault("attributes", {}).setdefault("_observe", {})
            meta["truncated"] = True
            meta["fields"] = truncated_fields
            self.stats.incr("truncated_events")
        return data

    # -- queue -----------------------------------------------------------------

    def _offer(self, event: CanonicalEvent) -> None:
        try:
            self._queue.put_nowait(event)
            self.stats.incr("enqueued")
            return
        except queue.Full:
            pass
        if event.delivery == Delivery.CRITICAL:
            self._spool(event)
            return
        if self.settings.drop_policy == "drop_oldest":
            try:
                evicted = self._queue.get_nowait()
            except queue.Empty:
                evicted = None
            if evicted is not None and isinstance(evicted, CanonicalEvent):
                self.stats.incr(
                    "dropped_debug" if evicted.delivery == Delivery.DEBUG else "dropped_telemetry"
                )
            try:
                self._queue.put_nowait(event)
                self.stats.incr("enqueued")
            except queue.Full:
                self._drop_noncritical(event.delivery)
        else:
            self._drop_noncritical(event.delivery)

    def _drop_noncritical(self, delivery: Delivery) -> None:
        field = "dropped_debug" if delivery == Delivery.DEBUG else "dropped_telemetry"
        self.stats.incr(field)
        if self.settings.telemetry_required:
            raise TelemetryError("observe queue is full")

    def _spool(self, event: CanonicalEvent) -> None:
        if self.spool is not None and self.spool.append(event.payload):
            self.stats.incr("spooled_events")
            return
        self.stats.incr("spool_errors")
        _diag(f"critical event {event.event!r} could not be queued or spooled")
        if self.settings.telemetry_required:
            raise TelemetryError("critical event could not be queued or spooled")

    # -- worker -----------------------------------------------------------------

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
                    self._fan_out(batch)
                    self._flush_drains()
                    return
                if isinstance(item, _FlushRequest):
                    self._fan_out(batch)
                    batch = []
                    self._flush_drains()
                    self.stats.incr("flushes")
                    item.done.set()
                    continue
                batch.append(item)
            if batch:
                self._fan_out(batch)
                batch = []
            self._maybe_replay()
            if self._stop.is_set() and self._queue.empty():
                self._flush_drains()
                return

    def _fan_out(self, batch: list[CanonicalEvent]) -> None:
        if not batch:
            return
        for drain in self.drains:
            try:
                drain.emit_batch(batch)
                self.stats.incr("emitted_events", len(batch))
            except Exception as error:  # drain isolation
                self.stats.incr("drain_errors")
                _diag(f"drain {getattr(drain, 'name', '?')} failed: {error}")
                for event in batch:
                    if event.delivery == Delivery.CRITICAL:
                        self._spool(event)
        self.stats.incr("emitted_batches")

    def _flush_drains(self) -> None:
        for drain in self.drains:
            try:
                drain.flush()
            except Exception as error:
                _diag(f"drain {getattr(drain, 'name', '?')} flush failed: {error}")

    def _maybe_replay(self) -> None:
        if self.spool is None or self.spool.pending_segments() == 0:
            return
        now = time.monotonic()
        if now - self._last_replay < self.settings.spool_replay_interval_s:
            return
        self._last_replay = now
        for drain in self.drains:
            if getattr(drain, "is_remote", False):
                try:
                    self.spool.replay(drain)
                except Exception as error:
                    _diag(f"spool replay via {drain.name} failed: {error}")

    # -- lifecycle ----------------------------------------------------------------

    def queue_depth(self) -> int:
        """Current number of events waiting in the queue."""
        return self._queue.qsize()

    def flush(self, timeout: float = 5.0) -> bool:
        """Wait until queued events have been handed to drains. Bounded."""
        if self._closed:
            return True
        deadline = time.monotonic() + timeout
        requests = []
        for _ in self._workers:
            req = _FlushRequest()
            try:
                self._queue.put(req, timeout=max(0.0, deadline - time.monotonic()))
            except queue.Full:
                return False
            requests.append(req)
        remaining = deadline - time.monotonic()
        return all(req.done.wait(max(0.0, remaining)) for req in requests)

    def shutdown(self, timeout: float = 5.0) -> None:
        """Stop workers, flush drains and release resources. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        deadline = time.monotonic() + timeout
        for _ in self._workers:
            try:
                self._queue.put(_STOP, timeout=max(0.05, deadline - time.monotonic()))
            except queue.Full:
                break
        per_worker = max(0.05, (deadline - time.monotonic()) / len(self._workers))
        for worker in self._workers:
            worker.join(timeout=per_worker)
        for drain in self.drains:
            try:
                drain.close()
            except Exception as error:
                _diag(f"drain {getattr(drain, 'name', '?')} close failed: {error}")


# -- module-level runtime management ------------------------------------------

_runtime: Runtime | None = None
_runtime_lock = threading.Lock()
_pending_enrichers: list[Enricher] = []
_atexit_registered = False
_last_stats: dict[str, Any] = {}


def get_runtime() -> Runtime:
    """Return the active runtime, lazily creating a default-configured one."""
    global _runtime  # noqa: PLW0603 - process-wide singleton by design
    if _runtime is None:
        with _runtime_lock:
            if _runtime is None:
                _runtime = _new_runtime(ObserveSettings())
    return _runtime


def _new_runtime(settings: ObserveSettings, enrichers: list[Enricher] | None = None) -> Runtime:
    """Build and start a runtime, registering atexit flush once."""
    global _atexit_registered  # noqa: PLW0603 - one-shot registration flag
    all_enrichers = [*_pending_enrichers, *(enrichers or [])]
    _pending_enrichers.clear()
    runtime = Runtime(settings, enrichers=all_enrichers)
    if not _atexit_registered:
        atexit.register(_atexit_shutdown)
        _atexit_registered = True
    return runtime


def configure(
    settings: ObserveSettings | None = None,
    *,
    enrichers: list[Enricher] | None = None,
    **overrides: Any,
) -> Runtime:
    """(Re)configure the global runtime.

    Safe to call repeatedly: the previous runtime is shut down first. Keyword
    arguments are forwarded to :class:`ObserveSettings`.
    """
    global _runtime  # noqa: PLW0603 - process-wide singleton by design

    if settings is None:
        settings = ObserveSettings(**overrides)
    elif overrides:
        raise ValueError("pass either an ObserveSettings instance or keyword overrides")
    with _runtime_lock:
        if _runtime is not None:
            _runtime.shutdown(timeout=settings.shutdown_timeout_s)
        _runtime = _new_runtime(settings, enrichers)
        return _runtime


def add_enricher(enricher: Enricher) -> None:
    """Register an enricher; applies to the current and future runtimes."""
    with _runtime_lock:
        _pending_enrichers.append(enricher)
        if _runtime is not None:
            _runtime.enrichers.append(enricher)


def flush(timeout: float = 5.0) -> bool:
    """Flush the global runtime (bounded)."""
    if _runtime is None:
        return True
    return _runtime.flush(timeout)


def shutdown(timeout: float = 5.0) -> None:
    """Shut down the global runtime. Safe to call repeatedly."""
    global _runtime, _last_stats  # noqa: PLW0603 - process-wide singleton by design
    with _runtime_lock:
        if _runtime is not None:
            _runtime.shutdown(timeout)
            _last_stats = get_stats()
            _runtime = None


def _atexit_shutdown() -> None:
    with contextlib.suppress(Exception):  # atexit must never raise
        shutdown(timeout=2.0)


def get_stats() -> dict[str, Any]:
    """Snapshot of internal counters (drops, spool, drain errors, flushes).

    Counters remain readable after :func:`shutdown` via the last snapshot.
    """
    rt = _runtime
    if rt is None:
        return dict(_last_stats)
    snapshot = rt.stats.snapshot()
    snapshot["queue_depth"] = rt.queue_depth()
    if rt.spool is not None:
        snapshot["spool_bytes"] = rt.spool.pending_bytes()
        snapshot["spool_segments"] = rt.spool.pending_segments()
    return snapshot
