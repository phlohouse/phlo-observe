"""Phlo logging bridge.

A thin helper over stdlib logging that:

- emits normal application logs to a stream handler;
- binds run/trace correlation IDs onto every record via ``CorrelationFilter``;
- never installs duplicate handlers;
- does not impose a logging framework — it composes with stdlib logging.
"""

from __future__ import annotations

import logging
import sys
from typing import TextIO

from observe_core.logging_integration import CorrelationFilter

DEFAULT_FORMAT = (
    "%(asctime)s %(levelname)-8s %(name)s %(message)s run=%(run_id)s trace=%(trace_id)s"
)

_HANDLER_MARKER = "_phlo_observe_handler"


def configure_logging(
    *,
    level: int | str = logging.INFO,
    fmt: str = DEFAULT_FORMAT,
    stream: TextIO | None = None,
    logger: logging.Logger | None = None,
) -> logging.Logger:
    """Attach a correlation-aware stream handler to ``logger`` (default: root).

    Idempotent: repeated calls reuse the handler already installed by this
    function rather than stacking duplicates.
    """
    target = logger if logger is not None else logging.getLogger()
    target.setLevel(level)
    for handler in target.handlers:
        if getattr(handler, _HANDLER_MARKER, False):
            return target
    handler = logging.StreamHandler(stream or sys.stderr)
    setattr(handler, _HANDLER_MARKER, True)
    handler.setFormatter(logging.Formatter(fmt))
    handler.addFilter(CorrelationFilter())
    target.addHandler(handler)
    return target
