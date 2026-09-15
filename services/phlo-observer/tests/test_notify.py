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

asyncio_only = pytest.mark.asyncio


def test_pack_notify_caps_oversized_batches() -> None:
    """Payloads stay under Postgres's NOTIFY byte limit at any batch size."""
    messages = [{"kind": "run.changed", "data": {"run_id": "x" * 200}} for _ in range(500)]
    packed = pack_notify("inst", messages)
    assert packed is not None
    assert len(packed.encode()) <= 8000
    assert pack_notify("inst", []) is None


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
