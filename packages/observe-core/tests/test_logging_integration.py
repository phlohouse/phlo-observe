"""Optional stdlib logging bridge."""

from __future__ import annotations

import logging

from observe_core import flush
from observe_core.drains.memory import MemoryDrain
from observe_core.logging_integration import CorrelationFilter, ObservedLogHandler
from observe_core.runtime import Runtime


def _events(drain: MemoryDrain) -> list[dict]:
    flush(2.0)
    return [e.data for e in drain.events]


def test_log_records_become_events(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    logger = logging.getLogger("test.bridge")
    handler = ObservedLogHandler(level=logging.WARNING)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        logger.info("info ignored")  # below handler level
        logger.warning("warn me")
        logger.error("bad thing")
    finally:
        logger.removeHandler(handler)
    events = _events(drain)
    names = [e["event"] for e in events]
    assert names == ["application.log", "application.log"]
    by_message = {e["attributes"]["message"]: e for e in events}
    assert "info ignored" not in by_message
    assert by_message["warn me"]["severity"] == "warn"
    assert by_message["bad thing"]["severity"] == "error"
    assert by_message["bad thing"]["outcome"] == "failure"
    assert by_message["warn me"]["attributes"]["logger"] == "test.bridge"


def test_exception_details_captured(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    logger = logging.getLogger("test.bridge.exc")
    handler = ObservedLogHandler(level=logging.ERROR)
    logger.addHandler(handler)
    try:
        try:
            raise ValueError("kaput")
        except ValueError:
            logger.exception("exploded")
    finally:
        logger.removeHandler(handler)
    (ev,) = _events(drain)
    assert ev["attributes"]["exception_type"] == "ValueError"
    assert ev["attributes"]["exception_message"] == "kaput"


def test_internal_loggers_ignored(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    logger = logging.getLogger("observe_core.internal")
    handler = ObservedLogHandler(level=logging.DEBUG)
    logger.addHandler(handler)
    try:
        logger.error("internal noise")
    finally:
        logger.removeHandler(handler)
    assert _events(drain) == []


def test_correlation_filter_attaches_ids(captured: tuple[Runtime, MemoryDrain]):
    _, _ = captured
    from observe_core import bind_context

    record = logging.LogRecord(
        name="x",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="m",
        args=(),
        exc_info=None,
    )
    with bind_context(run_id="R77", trace_id="t9"):
        CorrelationFilter().filter(record)
    assert record.run_id == "R77"
    assert record.trace_id == "t9"
    assert record.span_id == "-"


def test_correlation_filter_sees_operation_context(
    captured: tuple[Runtime, MemoryDrain],
):
    """Log records inside an ``observe()`` block carry its correlation."""
    from observe_core import observe

    _, _ = captured
    record = logging.LogRecord(
        name="x",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="m",
        args=(),
        exc_info=None,
    )
    with observe("pipeline.run", correlation={"run_id": "R5"}) as evt:
        CorrelationFilter().filter(record)
        op_span = evt.correlation["span_id"]
    assert record.run_id == "R5"
    assert record.span_id == op_span
    assert record.trace_id != "-"
