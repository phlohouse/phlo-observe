"""OTLP drain: export canonical events to an OpenTelemetry collector.

Each canonical event becomes an OTel log record: the event name is the body,
severity maps onto OTel severity numbers, and the remaining envelope fields are
record attributes. Correlation identifiers are preserved verbatim under
``phlo.correlation.*`` (plus ``phlo.trace_id``/``phlo.span_id`` shortcuts) so
events can be joined to real traces downstream.

Requires the ``observe-core[otlp]`` extra.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from observe_core.drains.base import CanonicalEvent, DrainFailure
from observe_core.models import Severity
from observe_core.serialization import dumps, loads

try:  # optional extra: observe-core[otlp]
    from opentelemetry._logs.severity import (  # ty: ignore[unresolved-import]
        SeverityNumber,
    )
    from opentelemetry.exporter.otlp.proto.http._log_exporter import (  # ty: ignore[unresolved-import]
        OTLPLogExporter,
    )
    from opentelemetry.sdk._logs import (  # ty: ignore[unresolved-import]
        LoggerProvider,
        LogRecord,
    )
    from opentelemetry.sdk._logs.export import (  # ty: ignore[unresolved-import]
        BatchLogRecordProcessor,
    )

    _HAS_OTEL = True
except ImportError:  # pragma: no cover - depends on installed extras
    _HAS_OTEL = False

_OTEL_SEVERITY = {
    Severity.TRACE: 1,
    Severity.DEBUG: 5,
    Severity.INFO: 9,
    Severity.WARN: 13,
    Severity.ERROR: 17,
    Severity.CRITICAL: 24,
}

_FLAT_FIELDS = (
    "schema_version",
    "event_id",
    "category",
    "outcome",
    "delivery",
    "started_at",
    "ended_at",
    "duration_ms",
    "observed_at",
)


class OtlpDrain:
    """Export canonical events as OTel log records via OTLP/HTTP."""

    name = "otlp"
    is_remote = True

    def __init__(
        self,
        endpoint: str | None = None,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        if not _HAS_OTEL:
            raise DrainFailure(
                "OTLP drain requires the 'observe-core[otlp]' extra: pip install observe-core[otlp]"
            )

        self._severity_number = SeverityNumber
        self._log_record_cls = LogRecord
        exporter = OTLPLogExporter(endpoint=endpoint, headers=headers or {})
        self._provider = LoggerProvider()
        self._provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
        self._logger = self._provider.get_logger("observe-core")

    def emit_batch(self, events: Sequence[CanonicalEvent]) -> None:
        """Emit each event as one OTel log record."""
        for item in events:
            self._logger.emit(self._to_log_record(item.data))

    def emit_raw(self, payloads: Sequence[bytes]) -> None:
        """Replay pre-serialized payloads by re-parsing them."""
        for payload in payloads:
            self._logger.emit(self._to_log_record(loads(payload)))

    def _to_log_record(self, data: dict[str, Any]) -> Any:
        attributes: dict[str, Any] = {}
        for key in _FLAT_FIELDS:
            value = data.get(key)
            if value is not None:
                attributes[f"phlo.{key}"] = value
        correlation = data.get("correlation") or {}
        for key, value in correlation.items():
            if value is None or key == "extra":
                continue
            attributes[f"phlo.correlation.{key}"] = value
        if correlation.get("trace_id"):
            attributes["phlo.trace_id"] = correlation["trace_id"]
        if correlation.get("span_id"):
            attributes["phlo.span_id"] = correlation["span_id"]
        for section in ("service", "error", "source", "attributes"):
            value = data.get(section)
            if value:
                attributes[f"phlo.{section}"] = dumps(value).decode("utf-8")

        severity = Severity(data.get("severity", "info"))
        return self._log_record_cls(
            severity_text=severity.value.upper(),
            severity_number=self._severity_number(_OTEL_SEVERITY[severity]),
            body=data.get("event"),
            attributes=attributes,
        )

    def flush(self) -> None:
        """Force-flush pending log records."""
        self._provider.force_flush()

    def close(self) -> None:
        """Flush and shut down the exporter."""
        try:
            self._provider.force_flush()
        finally:
            self._provider.shutdown()
