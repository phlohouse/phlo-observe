"""Cross-instance SSE fan-out via Postgres LISTEN/NOTIFY (hardening item 8).

Two observer replicas share one database. A subscriber on replica B must
receive the stream messages committed through replica A — otherwise a
load-balanced live tail silently misses most events.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
import workloads as wl
from phlo_observer.notify import NotifyBridge, pack_notify
from phlo_observer.store import persist_events
from phlo_observer.stream import StreamHub
from sqlalchemy import text

asyncio_only = pytest.mark.asyncio


def test_pack_notify_caps_oversized_batches() -> None:
    """Payloads stay under Postgres's NOTIFY byte limit at any batch size."""
    messages = [{"kind": "run.changed", "data": {"run_id": "x" * 200}} for _ in range(500)]
    packed = pack_notify("inst", messages)
    assert packed is not None
    assert len(packed.encode()) <= 8000
    assert pack_notify("inst", []) is None


def test_bridge_disables_cleanly_on_non_asyncpg_dsn() -> None:
    """A psycopg/SQLite DSN cannot LISTEN: the bridge must refuse once at
    startup rather than reconnect-loop forever on a URL asyncpg will never
    parse — a previous version logged warnings in a hot loop."""
    for url in (
        "postgresql+psycopg://u:p@h/db",
        "sqlite+aiosqlite:///tmp/x.db",
        "postgresql+pg8000://u:p@h/db",
    ):
        bridge = NotifyBridge(url, StreamHub(), instance_id="x")
        bridge.start()  # must not spawn a task
        assert bridge._task is None


@asyncio_only
async def test_notify_reaches_other_replica(session_factory: Any, database_url: str) -> None:
    """Ingest on instance A -> subscriber on instance B's hub sees it."""
    hub_b = StreamHub()
    bridge = NotifyBridge(database_url, hub_b, instance_id="replica-b")
    bridge.start()
    queue = hub_b.subscribe()
    try:
        events = wl.dagster_run(f"notify-{uuid.uuid4().hex[:8]}", wl.T0)
        async with session_factory() as session, session.begin():
            await persist_events(session, events, instance_id="replica-a")
        kinds = set()
        deadline = asyncio.get_running_loop().time() + 10.0
        while asyncio.get_running_loop().time() < deadline:
            try:
                message = await asyncio.wait_for(
                    queue.get(), timeout=deadline - asyncio.get_running_loop().time()
                )
            except TimeoutError:
                break
            kinds.add(message["kind"])
            if "run.changed" in kinds:
                break
        assert "run.changed" in kinds, "replica B never saw replica A's stream"
    finally:
        await bridge.stop()


@asyncio_only
async def test_notify_skips_own_instance(session_factory: Any, database_url: str) -> None:
    """A bridge never republishes its own instance's messages back locally."""
    hub = StreamHub()
    bridge = NotifyBridge(database_url, hub, instance_id="replica-self")
    bridge.start()
    queue = hub.subscribe()
    try:
        events = wl.dagster_run(f"self-{uuid.uuid4().hex[:8]}", wl.T0)
        async with session_factory() as session, session.begin():
            await persist_events(session, events, instance_id="replica-self")
        try:
            message = await asyncio.wait_for(queue.get(), timeout=3.0)
            pytest.fail(f"own-instance message leaked back: {message}")
        except TimeoutError:
            pass  # correct: nothing arrived via the bridge
    finally:
        await bridge.stop()


@asyncio_only
async def test_notify_reconnects_after_connection_drop(
    session_factory: Any, database_url: str
) -> None:
    """A killed LISTEN backend must re-listen: cross-instance SSE survives
    Postgres-side termination, not just clean shutdown. The bridge names
    its backend ``phlo-observer-notify-*`` so it is visible (and killable)
    in ``pg_stat_activity``.
    """
    hub_b = StreamHub()
    bridge = NotifyBridge(database_url, hub_b, instance_id="replica-b")
    bridge.start()
    queue = hub_b.subscribe()
    loop = asyncio.get_running_loop()

    async def _bridge_pids() -> list[int]:
        async with session_factory() as session:
            rows = (
                await session.execute(
                    text(
                        "select pid from pg_stat_activity "
                        "where application_name like 'phlo-observer-notify-%'"
                    )
                )
            ).all()
        return [r[0] for r in rows]

    async def _wait_listener(deadline: float, *, exclude: set[int] | None = None) -> int:
        while loop.time() < deadline:
            for pid in await _bridge_pids():
                if not exclude or pid not in exclude:
                    return pid
            await asyncio.sleep(0.1)
        return -1

    try:
        pid = await _wait_listener(loop.time() + 10.0)
        assert pid > 0, "bridge never established its LISTEN backend"

        # Kill the backend server-side; the bridge must notice and re-listen.
        async with session_factory() as session:
            await session.execute(text("select pg_terminate_backend(:pid)"), {"pid": pid})
        new_pid = await _wait_listener(loop.time() + 15.0, exclude={pid})
        assert new_pid > 0, "bridge never re-listened after its backend died"

        events = wl.dagster_run(f"notify-rc-{uuid.uuid4().hex[:8]}", wl.T0)
        async with session_factory() as session, session.begin():
            await persist_events(session, events, instance_id="replica-a")
        kinds = set()
        deadline = loop.time() + 10.0
        while loop.time() < deadline:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=deadline - loop.time())
            except TimeoutError:
                break
            kinds.add(message["kind"])
            if "run.changed" in kinds:
                break
        assert "run.changed" in kinds, "replica B saw nothing after reconnect"
    finally:
        await bridge.stop()


@asyncio_only
async def test_local_notification_waits_for_committed_state(session_factory, make_event):
    from phlo_observer.models import Run

    hub = StreamHub()
    queue = hub.subscribe()
    async with session_factory() as session, session.begin():
        await persist_events(session, [make_event(correlation={"run_id": "committed"})], stream=hub)
        assert queue.empty()
        async with session_factory() as reader:
            assert await reader.get(Run, "committed") is None
    assert queue.get_nowait()["data"]["run_id"] == "committed"
    async with session_factory() as reader:
        assert await reader.get(Run, "committed") is not None


@asyncio_only
async def test_local_notification_discards_rollback_and_allows_session_reuse(
    session_factory, make_event
):
    hub = StreamHub()
    queue = hub.subscribe()
    async with session_factory() as session:
        async with session.begin():
            await persist_events(
                session, [make_event(correlation={"run_id": "rolled-back"})], stream=hub
            )
            await session.rollback()
        assert queue.empty()
        async with session.begin():
            await persist_events(
                session, [make_event(correlation={"run_id": "survives"})], stream=hub
            )
    assert queue.get_nowait()["data"]["run_id"] == "survives"
    assert queue.empty()


@asyncio_only
async def test_local_notification_discards_enclosing_savepoint_rollback(
    session_factory, make_event
):
    hub = StreamHub()
    queue = hub.subscribe()
    async with session_factory() as session, session.begin():
        await persist_events(session, [make_event(correlation={"run_id": "survives"})], stream=hub)
        savepoint = await session.begin_nested()
        await persist_events(
            session, [make_event(correlation={"run_id": "rolled-back"})], stream=hub
        )
        await savepoint.rollback()
        assert queue.empty()
    assert queue.get_nowait()["data"]["run_id"] == "survives"
    assert queue.empty()


@asyncio_only
@pytest.mark.parametrize("rollback", [False, True])
async def test_alerts_wait_for_outer_commit(session_factory, monkeypatch, rollback):
    from phlo_observer import alerts
    from phlo_observer.models import Insight

    delivered = []
    tasks = set()
    alerts._last_sent.clear()

    async def capture(url, kind, payload):
        async with session_factory() as reader:
            assert await reader.get(Insight, uuid.UUID(payload["insight_id"])) is not None
        delivered.append(payload)

    monkeypatch.setattr(alerts, "_post", capture)
    async with session_factory() as session, session.begin():
        await persist_events(
            session,
            wl.dagster_run("alert-commit", wl.T0, outcome="failure"),
            alert_urls=["https://alerts.example.test"],
            alert_tasks=tasks,
        )
        await asyncio.sleep(0)
        assert not tasks
        assert not delivered
        assert not alerts._last_sent
        if rollback:
            await session.rollback()
    while tasks:
        await asyncio.gather(*list(tasks))
    assert bool(delivered) is not rollback
    assert bool(alerts._last_sent) is not rollback


@asyncio_only
async def test_projection_rollback_discards_pending_notifications(session_factory, monkeypatch):
    from phlo_observer import alerts, notify

    hub = StreamHub()
    queue = hub.subscribe()
    tasks = set()
    alerts._last_sent.clear()

    async def fail_final_notification(session, payload):
        raise RuntimeError("fail after local notification intents were collected")

    monkeypatch.setattr(notify, "emit_notify", fail_final_notification)
    events = wl.dagster_run("projection-rollback", wl.T0, outcome="failure")
    async with session_factory() as session, session.begin():
        result = await persist_events(
            session,
            events,
            stream=hub,
            instance_id="test",
            alert_urls=["https://alerts.example.test"],
            alert_tasks=tasks,
        )
        assert result.accepted == len(events)
    await asyncio.sleep(0)
    assert queue.empty()
    assert not tasks
    assert not alerts._last_sent
