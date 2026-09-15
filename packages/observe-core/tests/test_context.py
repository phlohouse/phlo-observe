"""Context propagation: binding, precedence, asyncio isolation, threads."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

from observe_core import (
    bind_context,
    bind_context_token,
    clear_context,
    configure,
    event,
    get_context,
    observe,
    propagate,
)
from observe_core.drains.memory import MemoryDrain
from observe_core.runtime import Runtime


def _data(drain: MemoryDrain) -> list[dict]:
    """Flush the pipeline and return canonical event dicts."""
    from observe_core import flush

    flush(2.0)
    return [e.data for e in drain.events]


def test_bind_context_sets_correlation(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with bind_context(run_id="R1", pipeline="daily"):
        event("pipeline.step")
    (ev,) = _data(drain)
    assert ev["correlation"]["run_id"] == "R1"
    assert ev["correlation"]["pipeline"] == "daily"


def test_context_restores_after_block(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with bind_context(run_id="R1"):
        pass
    event("pipeline.step")
    (ev,) = _data(drain)
    assert ev["correlation"]["run_id"] is None


def test_token_binding(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    bound = bind_context_token(run_id="TOK1")
    event("pipeline.step")
    bound.reset()
    (ev,) = _data(drain)
    assert ev["correlation"]["run_id"] == "TOK1"


def test_explicit_overrides_ambient(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with bind_context(run_id="ambient"):
        event("pipeline.step", correlation={"run_id": "explicit"})
    (ev,) = _data(drain)
    assert ev["correlation"]["run_id"] == "explicit"


def test_service_defaults_from_settings(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    event("application.start")
    (ev,) = _data(drain)
    assert ev["service"]["name"] == "test-service"
    assert ev["service"]["environment"] == "test"


def test_ambient_service_overrides_settings(make_runtime):
    rt = make_runtime(service_name="settings-name")
    drain = rt.drains[0]
    with bind_context(service_name="ambient-name"):
        event("application.start")
    (ev,) = _data(drain)
    assert ev["service"]["name"] == "ambient-name"


def test_unknown_context_keys_go_to_extra(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with bind_context(run_id="R1", custom_thing="abc"):
        event("pipeline.step")
    (ev,) = _data(drain)
    assert ev["correlation"]["extra"]["custom_thing"] == "abc"
    assert "custom_thing" not in {k for k in ev["correlation"] if k != "extra"}


def test_get_and_clear_context():
    with bind_context(run_id="R1"):
        assert get_context()["run_id"] == "R1"
        clear_context()
        assert get_context() == {}


async def test_context_isolated_across_async_tasks(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured

    async def worker(i: int) -> None:
        with bind_context(run_id=f"run-{i}"):
            await asyncio.sleep(0.001)
            event("pipeline.step")

    await asyncio.gather(*(worker(i) for i in range(20)))
    seen = {ev["correlation"]["run_id"] for ev in _data(drain)}
    assert seen == {f"run-{i}" for i in range(20)}


async def test_hundred_concurrent_tasks_isolated(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured

    async def worker(i: int) -> None:
        with bind_context(run_id=f"r{i}", request_id=f"req{i}"):
            await asyncio.sleep(0)
            with observe("pipeline.step") as evt:
                evt.set(i=i)
            event("application.log")

    await asyncio.gather(*(worker(i) for i in range(100)))
    events = _data(drain)
    assert len(events) == 200
    for ev in events:
        i = ev["correlation"]["run_id"]
        assert ev["correlation"]["request_id"] == f"req{i[1:]}"


def test_thread_propagation_via_propagate(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with bind_context(run_id="threaded"):
        ctx = propagate()
        with ThreadPoolExecutor(1) as pool:
            pool.submit(ctx.run, lambda: event("pipeline.step")).result()
    (ev,) = _data(drain)
    assert ev["correlation"]["run_id"] == "threaded"


def test_threads_without_propagation_are_isolated(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with bind_context(run_id="not-in-thread"), ThreadPoolExecutor(1) as pool:
        pool.submit(lambda: event("pipeline.step")).result()
    (ev,) = _data(drain)
    assert ev["correlation"]["run_id"] is None


def test_multiple_producer_threads(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with ThreadPoolExecutor(8) as pool:
        futures = [
            pool.submit(lambda i=i: event("pipeline.step", attributes={"i": i})) for i in range(50)
        ]
        for f in futures:
            f.result()
    assert len(_data(drain)) == 50


def test_configure_resets_and_replaces(make_runtime):
    rt1 = make_runtime(service_name="one")
    rt2 = make_runtime(service_name="two")
    assert rt1 is not rt2
    event("application.start")
    assert _data(rt2.drains[0])[0]["service"]["name"] == "two"


def test_shutdown_idempotent(make_runtime):
    make_runtime()
    from observe_core import shutdown

    shutdown(1.0)
    shutdown(1.0)


def test_workers_alive_reports_thread_state(make_runtime):
    rt = make_runtime()
    assert rt.workers_alive()
    rt.shutdown(timeout=2.0)
    assert not rt.workers_alive()


def test_configure_with_kwargs():
    rt = configure(service_name="kwarg-service", drains=[{"type": "memory"}])
    try:
        assert rt.settings.service_name == "kwarg-service"
    finally:
        from observe_core import shutdown

        shutdown(1.0)
