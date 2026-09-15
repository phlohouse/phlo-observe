"""Failure-injection tests for the observer (hardening item 4).

Proves the failure modes the spec cares about: mid-batch crashes leave no
partial state, projection failures are fail-open (events stay durable and
rebuildable), retention can run during ingest, restarts preserve state,
and unknown schema versions are rejected rather than silently stored.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest
import workloads as wl
from httpx import ASGITransport, AsyncClient
from phlo_observer import projections
from phlo_observer.models import Event, Run
from phlo_observer.retention import run_retention_once
from phlo_observer.settings import ObserverSettings
from phlo_observer.store import persist_events
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

pytestmark = pytest.mark.asyncio


async def test_aborted_transaction_leaves_no_partial_state(
    session_factory: Any, make_event: Any
) -> None:
    """A crash mid-batch (session closed without commit) stores nothing."""
    events = wl.dagster_run(f"atomic-{uuid.uuid4().hex[:8]}", wl.T0)
    session = session_factory()
    await session.begin()
    await persist_events(session, events)
    # Simulated crash: the transaction is abandoned, never committed.
    await session.rollback()
    await session.close()
    async with session_factory() as check:
        stored = await check.scalar(select(func.count()).select_from(Event))
        runs = await check.scalar(select(func.count()).select_from(Run))
    assert stored == 0
    assert runs == 0


async def test_projection_failure_is_fail_open(
    session_factory: Any, make_event: Any, monkeypatch: Any
) -> None:
    """A projection crash must not reject durable events (spec §12.4)."""
    events = wl.dagster_run(f"fo-{uuid.uuid4().hex[:8]}", wl.T0)

    async def _boom(session: AsyncSession, rows: list[Event]) -> None:
        raise RuntimeError("projection exploded")

    monkeypatch.setattr(projections, "apply_events_batch", _boom)
    async with session_factory() as session, session.begin():
        result = await persist_events(session, events)
    assert result.accepted == len(events)
    monkeypatch.undo()
    # Events are durable; derived state can be rebuilt after the incident.
    async with session_factory() as session, session.begin():
        await projections.rebuild_projections(session)
    async with session_factory() as session:
        run_id = next(e["correlation"]["run_id"] for e in events if e.get("correlation"))
        run = await session.scalar(select(Run).where(Run.run_id == run_id))
    assert run is not None
    assert run.event_count == len(events)


async def test_retention_during_ingest(
    client: AsyncClient, session_factory: Any, database_url: str
) -> None:
    """Retention sweeping concurrently with ingest: both complete."""
    settings = ObserverSettings(database_url=database_url)
    seeded = wl.mixed_history(seed=5, days=2, daily_runs=2)
    await client.post("/v1/events", json=seeded)
    incoming = wl.dagster_run(f"during-ret-{uuid.uuid4().hex[:8]}", wl.T0)

    retention = asyncio.create_task(run_retention_once(session_factory, settings))
    resp = await client.post("/v1/events", json=incoming)
    report = await retention
    assert resp.status_code == 202
    assert resp.json()["accepted"] == len(incoming)
    assert not report.skipped


async def test_restart_preserves_state_and_dedup(
    session_factory: Any, database_url: str, make_event: Any
) -> None:
    """A fresh app instance over the same DB sees prior state + dedupes."""
    from phlo_observer.app import create_app

    events = wl.dagster_run(f"restart-{uuid.uuid4().hex[:8]}", wl.T0)
    app1 = create_app(ObserverSettings(database_url=database_url))
    app1.state.session_factory = session_factory
    async with AsyncClient(transport=ASGITransport(app=app1), base_url="http://a") as c1:
        r1 = await c1.post("/v1/events", json=events)
        assert r1.json()["accepted"] == len(events)
    # "Restart": new app, same database and session factory.
    app2 = create_app(ObserverSettings(database_url=database_url))
    app2.state.session_factory = session_factory
    async with AsyncClient(transport=ASGITransport(app=app2), base_url="http://b") as c2:
        run = (await c2.get(f"/v1/runs/{events[0]['correlation']['run_id']}")).json()
        assert run["event_count"] == len(events)
        r2 = await c2.post("/v1/events", json=events)
        assert r2.json()["duplicates"] == len(events)
        assert r2.json()["accepted"] == 0


async def test_unknown_schema_version_rejected(client: AsyncClient, make_event: Any) -> None:
    """schema_version outside 1.x/2.x is rejected, never silently stored.

    An all-bad batch 422s; a mixed batch returns 202 with per-item errors
    so one malformed envelope cannot take down the good events around it.
    """
    bad = make_event(schema_version="99.0")
    resp = await client.post("/v1/events", json=[bad])
    assert resp.status_code == 422
    good = make_event()
    mixed = await client.post("/v1/events", json=[good, bad])
    assert mixed.status_code == 202
    body = mixed.json()
    assert body["accepted"] == 1
    assert body["rejected"] == 1


async def test_late_arriving_run_events_still_reconstruct(
    client: AsyncClient, session_factory: Any
) -> None:
    """Terminal event arriving before its steps (out of order) reconstructs."""
    events = wl.dagster_run(f"ooo-{uuid.uuid4().hex[:8]}", wl.T0, outcome="success")
    reordered = list(reversed(events))  # terminal event first
    resp = await client.post("/v1/events", json=reordered)
    assert resp.json()["accepted"] == len(events)
    run_id = events[0]["correlation"]["run_id"]
    run = (await client.get(f"/v1/runs/{run_id}")).json()
    assert run["event_count"] == len(events)
    assert run["status"] == "success"
