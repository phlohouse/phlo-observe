"""observe() lifecycle: sync/async, decorator, exceptions, nesting."""

from __future__ import annotations

import asyncio

import pytest
from observe_core import ObservedError, event, flush, observe
from observe_core.drains.memory import MemoryDrain
from observe_core.runtime import Runtime


def _data(drain: MemoryDrain) -> list[dict]:
    """Flush the pipeline and return canonical event dicts."""
    flush(2.0)
    return [e.data for e in drain.events]


def test_success_lifecycle(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with observe("asset.materialize", category="data") as evt:
        evt.set(rows_out=98)
    (ev,) = _data(drain)
    assert ev["event"] == "asset.materialize"
    assert ev["category"] == "data"
    assert ev["outcome"] == "success"
    assert ev["severity"] == "info"
    assert ev["duration_ms"] is not None
    assert ev["attributes"]["rows_out"] == 98
    assert ev["error"] is None


async def test_async_lifecycle(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    async with observe("query.execute", category="query") as evt:
        await asyncio.sleep(0)
        evt.set(rows=1)
    (ev,) = _data(drain)
    assert ev["outcome"] == "success"
    assert ev["attributes"]["rows"] == 1


def test_exception_marks_failure_and_reraises(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with pytest.raises(ValueError, match="boom"), observe("transform.execute") as evt:
        evt.set(step="a")
        raise ValueError("boom")
    (ev,) = _data(drain)
    assert ev["outcome"] == "failure"
    assert ev["severity"] == "error"
    assert ev["error"]["exception_type"] == "ValueError"
    assert ev["error"]["message"] == "boom"
    assert ev["error"]["stacktrace"]  # capture_stacktrace default true


def test_observed_error_fields_propagate(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    err = ObservedError(
        "quality failed", code="QUALITY_CHECK_FAILED", why="8 nulls", fix="fix source"
    )
    with pytest.raises(ObservedError), observe("quality.validate", category="quality"):
        raise err
    (ev,) = _data(drain)
    assert ev["error"]["code"] == "QUALITY_CHECK_FAILED"
    assert ev["error"]["why"] == "8 nulls"
    assert ev["error"]["fix"] == "fix source"
    assert ev["outcome"] == "failure"


def test_capture_stacktrace_off_per_operation(captured: tuple[Runtime, MemoryDrain]):
    """observe(capture_stacktrace=False) overrides the enabled global default."""
    _, drain = captured
    with pytest.raises(ValueError), observe("transform.execute", capture_stacktrace=False):
        raise ValueError("boom")
    (ev,) = _data(drain)
    assert ev["error"]["exception_type"] == "ValueError"
    assert ev["error"]["stacktrace"] is None


def test_capture_stacktrace_on_per_operation(make_runtime):
    """observe(capture_stacktrace=True) overrides a disabled global default."""
    rt = make_runtime(capture_stacktrace=False)
    drain = rt.drains[0]
    with pytest.raises(ValueError), observe("transform.execute", capture_stacktrace=True):
        raise ValueError("boom")
    (ev,) = _data(drain)
    assert ev["error"]["stacktrace"] is not None


def test_duration_matches_wall_timestamps(captured: tuple[Runtime, MemoryDrain]):
    """duration_ms must satisfy the envelope invariant (ended - started)."""
    from observe_core.models import DURATION_TOLERANCE_MS
    from observe_core.timestamps import parse_rfc3339

    _, drain = captured
    with observe("ingestion.load"):
        pass
    (ev,) = _data(drain)
    wall_ms = (
        parse_rfc3339(ev["ended_at"]) - parse_rfc3339(ev["started_at"])
    ).total_seconds() * 1000.0
    assert abs(wall_ms - ev["duration_ms"]) <= DURATION_TOLERANCE_MS


def test_overrides(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with observe("ingestion.load", delivery="critical", severity="warn") as evt:
        evt.set_outcome("partial")
        evt.set_delivery("telemetry")
    (ev,) = _data(drain)
    assert ev["outcome"] == "partial"
    assert ev["delivery"] == "telemetry"
    assert ev["severity"] == "warn"


def test_warnings_and_annotations(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with observe("transform.execute") as evt:
        evt.add_warning(code="SKEW", message="partition skew detected")
        evt.annotate("first pass complete")
    (ev,) = _data(drain)
    assert ev["attributes"]["warnings"][0]["code"] == "SKEW"
    assert ev["attributes"]["annotations"] == ["first pass complete"]
    assert ev["severity"] == "warn"  # warning bumps severity


def test_nested_operations_link_spans(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with observe("pipeline.run", category="pipeline") as outer:
        outer.set_correlation(run_id="R9")
        with observe("pipeline.step") as inner:
            inner.set(step=1)
    inner_ev, outer_ev = _data(drain)  # inner completes first
    assert inner_ev["correlation"]["parent_span_id"] == outer_ev["correlation"]["span_id"]
    assert inner_ev["correlation"]["trace_id"] == outer_ev["correlation"]["trace_id"]
    assert inner_ev["correlation"]["run_id"] == "R9"  # inherited from parent op


def test_decorator_sync(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured

    @observe("transform.execute")
    def transform(x: int) -> int:
        return x * 2

    assert transform(3) == 6
    assert transform.__name__ == "transform"
    (ev,) = _data(drain)
    assert ev["event"] == "transform.execute"
    assert ev["outcome"] == "success"


async def test_decorator_async(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured

    @observe("ingestion.extract")
    async def extract() -> int:
        await asyncio.sleep(0)
        return 42

    assert await extract() == 42
    (ev,) = _data(drain)
    assert ev["outcome"] == "success"


def test_instantaneous_event(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    event(
        "wap.promote",
        category="wap",
        delivery="critical",
        attributes={"branch": "run/R1", "target": "main"},
    )
    (ev,) = _data(drain)
    assert ev["event"] == "wap.promote"
    assert ev["delivery"] == "critical"
    assert ev["started_at"] is None
    assert ev["duration_ms"] is None
    assert ev["attributes"]["target"] == "main"


def test_exception_inside_async_op_reraises(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured

    async def run():
        async with observe("ingestion.extract"):
            raise KeyError("k")

    with pytest.raises(KeyError):
        asyncio.run(run())
    (ev,) = _data(drain)
    assert ev["outcome"] == "failure"
