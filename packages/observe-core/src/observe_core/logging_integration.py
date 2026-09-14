"""Optional integration with the stdlib :mod:`logging` package.

Two opt-in directions:

1. :class:`CorrelationFilter` attaches active correlation identifiers to log
   records (``record.run_id``, ``record.trace_id``, ...) for formatters to use.
2. :class:`ObservedLogHandler` converts selected log records into
   ``application.log`` events.

Both are opt-in to avoid feedback loops and duplicate telemetry; records from
observe-core's own loggers are always ignored by the handler.
"""

from __future__ import annotations

import logging
from typing import Any

from observe_core.context import ambient_correlation
from observe_core.emit import event
from observe_core.models import Category, Delivery, Severity

_INTERNAL_LOGGERS = ("observe_core",)


class CorrelationFilter(logging.Filter):
    """Injects active correlation IDs onto log records.

    Usage::

        handler.addFilter(CorrelationFilter())
        formatter = logging.Formatter("%(run_id)s %(message)s")
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Attach ``run_id``/``trace_id``/``span_id``/``job_id`` to the record."""
        corr = ambient_correlation()
        for key in ("run_id", "trace_id", "span_id", "job_id", "asset_key"):
            setattr(record, key, corr.get(key) or "-")
        return True


_LEVEL_TO_SEVERITY = {
    logging.DEBUG: Severity.DEBUG,
    logging.INFO: Severity.INFO,
    logging.WARNING: Severity.WARN,
    logging.ERROR: Severity.ERROR,
    logging.CRITICAL: Severity.CRITICAL,
}


class ObservedLogHandler(logging.Handler):
    """Converts stdlib log records into ``application.log`` events."""

    def __init__(
        self,
        level: int = logging.WARNING,
        delivery: Delivery = Delivery.TELEMETRY,
    ) -> None:
        super().__init__(level)
        self.delivery = delivery

    def emit(self, record: logging.LogRecord) -> None:
        """Convert the record to an ``application.log`` event."""
        if record.name.startswith(_INTERNAL_LOGGERS):
            return
        try:
            severity = _severity_for(record.levelno)
            attributes: dict[str, Any] = {
                "logger": record.name,
                "level": record.levelname,
                "message": record.getMessage(),
            }
            if record.exc_info and record.exc_info[1] is not None:
                exc = record.exc_info[1]
                attributes["exception_type"] = type(exc).__qualname__
                attributes["exception_message"] = str(exc)
            event(
                "application.log",
                category=Category.APPLICATION,
                severity=severity,
                delivery=self.delivery,
                attributes=attributes,
                outcome="failure" if severity in (Severity.ERROR, Severity.CRITICAL) else None,
            )
        except Exception:  # logging must never break the app
            self.handleError(record)


def _severity_for(levelno: int) -> Severity:
    for threshold in sorted(_LEVEL_TO_SEVERITY, reverse=True):
        if levelno >= threshold:
            return _LEVEL_TO_SEVERITY[threshold]
    return Severity.TRACE
