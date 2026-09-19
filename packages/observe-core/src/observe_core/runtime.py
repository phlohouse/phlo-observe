"""The emission runtime: finalize events, then hand them to a backend.

Completed event flow (per spec §11)::

    builder -> merge context -> normalize -> enrich -> redact
      -> validate limits -> sample -> backend.emit -> drains

Nothing on the application path performs network I/O with the default
:class:`~observe_core.backends.WorkerBackend`: ``emit`` only builds the
canonical event and enqueues it. ``ObserveSettings.runtime_backend`` selects
``worker`` (default), ``sync`` or ``capture`` transports; a custom
:class:`~observe_core.backends.RuntimeBackend` may be passed to
:func:`configure` via ``backend=``.
"""

from __future__ import annotations

import atexit
import contextlib
import logging
import sys
import threading
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from observe_core.aggregate import MetricAggregator
from observe_core.backends import (
    BackendHealth,
    CaptureBackend,
    RuntimeBackend,
    SyncBackend,
    WorkerBackend,
)
from observe_core.builder import EventBuilder
from observe_core.config import (
    ConsoleDrainConfig,
    HttpDrainConfig,
    JsonlDrainConfig,
    MemoryDrainConfig,
    ObserveSettings,
    OtlpDrainConfig,
)
from observe_core.context import (
    ambient_correlation,
    ambient_extra,
    ambient_producer,
    ambient_service,
    operation_correlation,
)
from observe_core.drains.base import CanonicalEvent, Drain
from observe_core.drains.console import ConsoleDrain
from observe_core.drains.http import HttpDrain
from observe_core.drains.jsonl import JsonlDrain
from observe_core.drains.memory import MemoryDrain
from observe_core.errors import ObservedError, error_info_from_exception
from observe_core.ids import new_event_id
from observe_core.models import (
    CORRELATION_KEYS,
    Category,
    ContractRef,
    Correlation,
    Delivery,
    ErrorInfo,
    EventEnvelope,
    Outcome,
    Severity,
    SourceInfo,
)
from observe_core.redaction import Redactor
from observe_core.sampling import PolicySampler, Sampler, SamplingContext
from observe_core.serialization import dumps, normalize_value
from observe_core.spool import Spool
from observe_core.stats import TelemetryStats
from observe_core.tail import TailSampler
from observe_core.timestamps import utcnow

if TYPE_CHECKING:
    from observe_core.config import ObserveSettings
    from observe_core.enrich import Enricher

_log = logging.getLogger("observe_core")

_TRUNCATE_FIELD_BYTES = 4 * 1024
_TRUNCATE_STACKTRACE_BYTES = 16 * 1024


def _opt_str(value: Any) -> str | None:
    return None if value is None else str(value)


class TelemetryError(Exception):
    """Raised in ``telemetry_required`` mode when an event cannot be accepted."""


def _diag(message: str) -> None:
    """Minimal stderr diagnostic that never raises."""
    with contextlib.suppress(Exception):
        sys.stderr.write(f"[observe-core] {message}\n")


class Runtime:
    """Owns the pipeline (finalize/redact/sample) and the transport backend."""

    def __init__(
        self,
        settings: ObserveSettings,
        enrichers: Iterable[Enricher] = (),
        *,
        backend: RuntimeBackend | None = None,
    ) -> None:
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
        self.policy_sampler: PolicySampler | None = (
            PolicySampler.from_settings(self.sampler, settings.sampling_policy)
            if settings.sampling_policy
            else None
        )
        self._tail: TailSampler | None = (
            TailSampler(
                min_duration_ms=settings.tail_min_duration_ms,
                max_runs=settings.tail_max_runs,
                terminal_events=settings.tail_terminal_events,
                stats=self.stats,
            )
            if settings.tail_sampling
            else None
        )
        self._tail_poll = 0
        self.aggregator = MetricAggregator()
        self.spool: Spool | None = None
        if settings.spool_enabled:
            try:
                self.spool = Spool(
                    settings.resolved_spool_dir(),
                    max_bytes=settings.spool_max_bytes,
                    segment_max_bytes=settings.spool_segment_max_bytes,
                    on_full=settings.spool_on_full,
                    stats=self.stats,
                )
            except OSError as exc:
                # A spool that cannot be created (read-only filesystem, no
                # writable home) must degrade, not crash the host application:
                # critical events then count as spool_errors like any other
                # spool write failure.
                self.stats.incr("spool_errors")
                _log.warning("critical-event spool unavailable: %s", exc)
                _diag(f"critical-event spool unavailable: {exc}")
        self.drains: list[Drain] = [self._build_drain(cfg) for cfg in settings.drains]
        if backend is not None:
            self._backend = backend
        elif settings.runtime_backend == "sync":
            self._backend: RuntimeBackend = SyncBackend(
                settings, self.stats, self.spool, self.drains
            )
        elif settings.runtime_backend == "capture":
            self._backend = CaptureBackend(capacity=settings.queue_capacity, stats=self.stats)
        else:
            self._backend = WorkerBackend(settings, self.stats, self.spool, self.drains)
        self._closed = False

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
                spool_on_failure=cfg.spool_on_failure,
            )
        if isinstance(cfg, OtlpDrainConfig):
            from observe_core.drains.otlp import OtlpDrain  # noqa: PLC0415 - optional deps

            return OtlpDrain(cfg.endpoint, headers=cfg.headers)
        if isinstance(cfg, MemoryDrainConfig):
            return MemoryDrain()
        raise TypeError(f"unsupported drain config: {cfg!r}")

    def add_drain(self, drain: Drain) -> None:
        """Attach a consumer-provided drain instance to this runtime.

        ``settings.drains`` covers the drains observe-core builds from
        config; this is the extension point for application-provided
        :class:`Drain` implementations. The drain joins batch delivery
        immediately — the worker and sync backends read the same list —
        and is flushed and closed with the configured drains on shutdown.
        It replays spool segments only if it marks itself ``is_remote``.
        Call it before emitting for deterministic drain ordering.
        """
        if not isinstance(drain, Drain):
            raise TypeError(f"drain must implement the Drain protocol, got {type(drain).__name__}")
        self.drains.append(drain)

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
            # TelemetryError is only raised when telemetry_required is set;
            # it must propagate even when fail_fast is off, or the fail-closed
            # contract silently degrades to drop-on-floor for oversized events.
            # ContractViolation is raised only in strict mode — likewise an
            # explicit opt-in contract that must not silently degrade.
            from observe_core.contracts import ContractViolation  # noqa: PLC0415

            if self.settings.fail_fast or isinstance(error, TelemetryError | ContractViolation):
                raise
            self.stats.incr("worker_errors")
            _diag(f"event finalization failed for {builder.event!r}: {error}")
            return
        if event is None:
            return
        if self.policy_sampler is not None:
            backend_health = self._backend.health()
            service_info = event.data.get("service") or {}
            outcome = Outcome(event.outcome)
            ctx = SamplingContext(
                event=event.event,
                delivery=event.delivery,
                severity=Severity(event.severity),
                outcome=outcome,
                duration_ms=event.data.get("duration_ms"),
                service=service_info.get("name"),
                environment=service_info.get("environment"),
                sample_key=self._sample_key(event.data),
                queue_depth=backend_health.queue_depth,
                queue_capacity=backend_health.queue_capacity,
                recent_error_rate=self.policy_sampler.recent_error_rate(),
            )
            decision = self.policy_sampler.decide(ctx)
            self.policy_sampler.note_outcome(outcome)
            if not decision.keep:
                self.stats.incr("dropped_sampled")
                return
        elif not self.sampler.should_keep(
            event.delivery, event.event, self._sample_key(event.data)
        ):
            self.stats.incr("dropped_sampled")
            return
        if self._tail is not None:
            self._tail.process(event, self._backend.emit)
            # Sweep orphaned run buffers occasionally so crashed producers do
            # not pin events forever (bounded: at most every 64 emits).
            self._tail_poll = (self._tail_poll + 1) % 64
            if self._tail_poll == 0:
                self._tail.flush_expired(self._backend.emit)
            return
        self._backend.emit(event)

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
                capture = builder.capture_stacktrace
                if capture is None:
                    capture = self.settings.capture_stacktrace
                builder.error = error_info_from_exception(
                    exc,
                    include_traceback=capture and not isinstance(exc, ObservedError),
                )
        elif builder.outcome == Outcome.UNKNOWN:
            builder.outcome = Outcome.SUCCESS

        for enricher in self.enrichers:
            try:
                enricher.enrich(builder)
            except Exception as error:  # bad enricher must not kill emit
                self.stats.incr("worker_errors")
                _diag(f"enricher {type(enricher).__name__} failed: {error}")

        # Correlation precedence (spec §10.5): explicit event values win over
        # the active operation context, which wins over bound ambient context.
        # ``observe()`` blocks already fold the parent chain into
        # ``builder.correlation`` at enter time; this merge is what lets a bare
        # ``event()`` inside a block inherit run_id/trace_id/span coordinates.
        merged_corr = {
            **ambient_correlation(),
            **operation_correlation(),
            **builder.correlation,
        }
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
        # Normalize and redact arbitrary correlation values before the
        # correlation model adds its envelope depth to the traversal path.
        self.redactor.redact_value(correlation.extra, path=("correlation", "extra"))
        ambient_svc = ambient_service()
        # Contract validation (spec §7.2): ``warn`` (the production default)
        # records violations under attributes._observe without failing
        # application work; ``strict`` raises ContractViolation from
        # ``validate_event_attributes``.
        contract_spec = None
        if self.settings.contract_validation != "off":
            from observe_core.contracts import get_contract  # noqa: PLC0415

            contract_spec = get_contract(builder.event)
            if contract_spec is not None:
                violations = contract_spec.validate_attributes(builder.attributes)
                if violations:
                    self.stats.incr("contract_violations")
                    if self.settings.contract_validation == "strict":
                        from observe_core.contracts import (  # noqa: PLC0415
                            ContractViolation,
                        )

                        raise ContractViolation(
                            f"event {builder.event!r} violates contract "
                            f"{contract_spec.schema_id}: {'; '.join(violations)}"
                        )
                    meta = builder.attributes.setdefault("_observe", {})
                    meta.setdefault("contract_violations", []).extend(violations)
        error = builder.error
        if isinstance(error, ErrorInfo):
            # ``error.details`` accepts arbitrary values; normalize them like
            # attributes so one unserializable detail cannot drop the event —
            # critical failure records must not vanish over a bad detail.
            details = normalize_value(error.details, max_depth=self.settings.max_depth)
            self.redactor.redact_value(details, path=("error", "details"))
            error = error.model_copy(update={"details": details})
        elif isinstance(error, dict):
            # Callers may assign a raw error dict; normalize it whole.
            error = normalize_value(error, max_depth=self.settings.max_depth)
            self.redactor.redact_value(error, path=("error",))
        contract_ref = None
        if contract_spec is not None:
            contract_ref = ContractRef(
                name=contract_spec.name,
                version=contract_spec.version,
                schema_id=contract_spec.schema_id,
                schema_hash=contract_spec.schema_hash,
            )
        # Producer identity: an explicit ``source.producer`` wins; otherwise an
        # ambient ``bind_context(producer=...)`` applies so integration scopes
        # namespace derived entities consistently (``run://dagster/<id>``
        # rather than ``run://<service.name>/<id>``).
        source = builder.source
        bound_producer = ambient_producer()
        if bound_producer is not None and (source is None or not source.producer):
            source = (
                source.model_copy(update={"producer": bound_producer})
                if source is not None
                else SourceInfo(producer=bound_producer)
            )
        attributes = normalize_value(builder.attributes, max_depth=self.settings.max_depth)
        self.redactor.redact_value(attributes, path=("attributes",))
        entities = normalize_value(builder.entities, max_depth=2)
        self.redactor.redact_value(entities, path=("entities",))
        tags = normalize_value(builder.tags, max_depth=2)
        self.redactor.redact_value(tags, path=("tags",))
        envelope = EventEnvelope(
            schema_version=self.settings.envelope_version,
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
            attributes=attributes,
            error=error,
            source=source,
            entities=entities,
            tags=tags,
            contract=contract_ref,
        )
        data = envelope.to_canonical_dict()
        extra_paths: tuple[tuple[str, ...], ...] = ()
        if contract_spec is not None and contract_spec.sensitive_fields:
            # Contract-declared sensitive fields redact regardless of key-name
            # heuristics (spec §7.2/§34).
            extra_paths = tuple(("attributes", name) for name in contract_spec.sensitive_fields)
        self.redactor.redact_event(data, extra_paths=extra_paths)
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

    # -- backend-facing helpers --------------------------------------------------

    @property
    def backend(self) -> RuntimeBackend:
        """The transport backend in use (``worker``, ``sync`` or ``capture``)."""
        return self._backend

    def queue_depth(self) -> int:
        """Current number of events waiting in the backend's queue."""
        queue_depth = getattr(self._backend, "queue_depth", None)
        if callable(queue_depth):
            return int(queue_depth())
        return self._backend.health().queue_depth

    def workers_alive(self) -> bool:
        """True while the backend's workers (if any) are running."""
        workers_alive = getattr(self._backend, "workers_alive", None)
        if callable(workers_alive):
            return bool(workers_alive())
        return self._backend.health().workers_alive

    def emit_metric_summaries(self) -> int:
        """Drain the aggregator and emit each pending ``metric.summary`` event.

        Returns the number of series emitted. Summaries carry their series
        correlation/entities/tags so observers can attach them to the right
        entity baselines.
        """
        summaries = self.aggregator.flush()
        for summary in summaries:
            correlation = summary.pop("correlation", None)
            entities = summary.pop("entities", None)
            tags = summary.pop("tags", None)
            builder = EventBuilder(
                "metric.summary",
                category=Category.METRIC,
                delivery=Delivery.TELEMETRY,
                attributes=summary,
            )
            if correlation:
                builder.set_correlation(**correlation)
            for role, identifier in (entities or {}).items():
                builder.set_entity(role, identifier)
            for key, value in (tags or {}).items():
                builder.set_tag(key, value)
            self.emit(builder, None)
        return len(summaries)

    def flush(self, timeout: float = 5.0) -> bool:
        """Wait until queued events have been handed to drains. Bounded.

        Pending metric summaries are emitted first so a ``flush()`` call
        drains every kind of pending telemetry, not just the queue.
        """
        if self._closed:
            return True
        self.emit_metric_summaries()
        if self._tail is not None:
            self._tail.flush_all(self._backend.emit)
        return self._backend.flush(timeout).ok

    def health(self) -> dict[str, Any]:
        """SDK health surface (spec §7.8): queue, counters, spool, backend.

        Applications may expose this directly without emitting recursive
        telemetry.
        """
        backend_health: BackendHealth = self._backend.health()
        stats = self.stats.snapshot()
        return {
            "backend": backend_health.backend,
            "queue_depth": backend_health.queue_depth,
            "queue_capacity": backend_health.queue_capacity,
            "workers_alive": backend_health.workers_alive,
            "enqueued": stats.get("enqueued", 0),
            "emitted": stats.get("emitted_events", 0),
            "dropped": sum(
                stats.get(name, 0)
                for name in (
                    "dropped_debug",
                    "dropped_telemetry",
                    "dropped_sampled",
                    "dropped_oversized",
                    "dropped_disabled",
                    "dropped_closed",
                )
            ),
            "spooled_events": stats.get("spooled_events", 0),
            "spool_bytes": self.spool.pending_bytes() if self.spool else 0,
            "tail_buffered_runs": self._tail.buffered_runs() if self._tail else 0,
            "aggregated_series": self.aggregator.series_count(),
            "last_export_ok_at": backend_health.last_export_ok_at,
            "last_export_error": backend_health.last_export_error,
            "exporter_endpoints": backend_health.endpoints,
        }

    def shutdown(self, timeout: float = 5.0) -> None:
        """Stop the backend and release resources. Idempotent.

        Pending metric summaries and tail-sampled buffers are emitted before
        the backend closes so shutdown does not silently lose telemetry.
        """
        if self._closed:
            return
        self.emit_metric_summaries()
        if self._tail is not None:
            self._tail.flush_all(self._backend.emit)
        self._closed = True
        self._backend.close(timeout)


# -- module-level runtime management ------------------------------------------

_runtime: Runtime | None = None
_runtime_lock = threading.Lock()
_pending_enrichers: list[Enricher] = []
_pending_drains: list[Drain] = []
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


def _new_runtime(
    settings: ObserveSettings,
    enrichers: list[Enricher] | None = None,
    *,
    backend: RuntimeBackend | None = None,
) -> Runtime:
    """Build and start a runtime, registering atexit flush once."""
    global _atexit_registered  # noqa: PLW0603 - one-shot registration flag
    all_enrichers = [*_pending_enrichers, *(enrichers or [])]
    _pending_enrichers.clear()
    runtime = Runtime(settings, enrichers=all_enrichers, backend=backend)
    for drain in _pending_drains:
        runtime.add_drain(drain)
    _pending_drains.clear()
    if not _atexit_registered:
        atexit.register(_atexit_shutdown)
        _atexit_registered = True
    return runtime


def configure(
    settings: ObserveSettings | None = None,
    *,
    enrichers: list[Enricher] | None = None,
    backend: RuntimeBackend | None = None,
    **overrides: Any,
) -> Runtime:
    """(Re)configure the global runtime.

    Safe to call repeatedly: the previous runtime is shut down first. Keyword
    arguments are forwarded to :class:`ObserveSettings`. ``backend`` accepts a
    custom :class:`RuntimeBackend`; otherwise
    ``ObserveSettings.runtime_backend`` selects ``worker``/``sync``/``capture``.
    """
    global _runtime  # noqa: PLW0603 - process-wide singleton by design

    if settings is None:
        settings = ObserveSettings(**overrides)
    elif overrides:
        raise ValueError("pass either an ObserveSettings instance or keyword overrides")
    with _runtime_lock:
        if _runtime is not None:
            _runtime.shutdown(timeout=settings.shutdown_timeout_s)
        _runtime = _new_runtime(settings, enrichers, backend=backend)
        return _runtime


def add_enricher(enricher: Enricher) -> None:
    """Register an enricher; applies to the current and future runtimes."""
    with _runtime_lock:
        _pending_enrichers.append(enricher)
        if _runtime is not None:
            _runtime.enrichers.append(enricher)


def add_drain(drain: Drain) -> None:
    """Register a drain instance; applies to the current and next runtime.

    This is the programmatic counterpart of ``OBSERVE_DRAINS``: where a
    configured drain name cannot express what the application needs — a
    drain carrying its own presentation config, streams or credentials —
    the application builds the :class:`Drain` itself and registers it here
    or on the :class:`Runtime` returned by :func:`configure`. The instance
    is shared: one registered before :func:`configure` attaches to the
    next runtime built.
    """
    if not isinstance(drain, Drain):
        raise TypeError(f"drain must implement the Drain protocol, got {type(drain).__name__}")
    with _runtime_lock:
        _pending_drains.append(drain)
        if _runtime is not None:
            _runtime.drains.append(drain)


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
