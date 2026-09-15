"""OTLP drain: export canonical events to an OpenTelemetry collector.

Each canonical event becomes an OTel log record: the event name is the body,
severity maps onto OTel severity numbers, and the remaining envelope fields are
record attributes encoded by :mod:`observe_core.otlp_mapping`. Correlation
identifiers are preserved verbatim under ``observe.correlation.*`` (plus
``observe.trace_id``/``observe.span_id`` shortcuts) so events can be joined to
real traces downstream.

Requires the ``observe-core[otlp]`` extra.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from observe_core.drains.base import CanonicalEvent, DrainFailure, PermanentDrainFailure
from observe_core.models import Severity
from observe_core.otlp_mapping import event_to_otlp_attributes
from observe_core.serialization import loads

try:  # optional extra: observe-core[otlp]
    from opentelemetry._logs import LogRecord
    from opentelemetry._logs.severity import SeverityNumber
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

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
            try:
                data = loads(payload)
            except Exception as exc:
                raise PermanentDrainFailure(f"spooled payload is not parseable: {exc}") from exc
            self._logger.emit(self._to_log_record(data))

    def _to_log_record(self, data: dict[str, Any]) -> Any:
        severity = Severity(data.get("severity", "info"))
        return self._log_record_cls(
            severity_text=severity.value.upper(),
            severity_number=self._severity_number(_OTEL_SEVERITY[severity]),
            body=data.get("event"),
            attributes=event_to_otlp_attributes(data),
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
