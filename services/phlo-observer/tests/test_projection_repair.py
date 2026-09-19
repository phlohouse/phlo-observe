"""Projection gaps survive acceptance, restarts, and unsuccessful repairs."""

from __future__ import annotations

import uuid

import pytest
from phlo_observer import projections
from phlo_observer.models import Event, ProjectionFailure, Run
from phlo_observer.repair import repair_projections
from phlo_observer.store import persist_events
from sqlalchemy import delete, func, select

pytestmark = pytest.mark.asyncio


async def test_failed_projection_is_visible_and_explicit_repair_clears_it(
    client, session_factory, make_event, monkeypatch
):
    async def fail(session, rows):
        raise RuntimeError("synthetic sensitive exception text")

    event = make_event(correlation={"run_id": "repair-me"})
    with monkeypatch.context() as patch:
        patch.setattr(projections, "apply_events_batch", fail)
        response = await client.post("/v1/events", json=event)
    assert response.status_code == 202
    assert response.json()["accepted"] == 1
    status = (await client.get("/v2/projections/status")).json()
    assert status["status"] == "degraded"
    assert status["pending_batches"] == status["pending_events"] == 1
    assert status["oldest_failure_at"]
    async with session_factory() as session:
        assert await session.get(Run, "repair-me") is None
    # A duplicate delivery neither creates another marker nor repairs the gap.
    assert (await client.post("/v1/events", json=event)).json()["duplicates"] == 1
    assert (await client.get("/v2/projections/status")).json()["pending_batches"] == 1
    async with session_factory() as session:
        failure = (await session.execute(select(ProjectionFailure))).scalar_one()
        assert failure.error_type == "RuntimeError"
        assert failure.event_ids == [event["event_id"]]
    metrics = (await client.get("/metrics")).text
    assert "phlo_observer_projection_pending_batches 1.0" in metrics
    assert "phlo_observer_projection_pending_events 1.0" in metrics
    # A fresh session represents the maintenance process after a restart.
    async with session_factory() as session, session.begin():
        report = await repair_projections(session)
    assert report["repaired_batches"] == report["repaired_events"] == 1
    async with session_factory() as session:
        assert (await session.get(Run, "repair-me")).event_count == 1
    assert (await client.get("/v2/projections/status")).json()["status"] == "current"
    assert "phlo_observer_projection_pending_batches 0.0" in (await client.get("/metrics")).text
    async with session_factory() as session, session.begin():
        assert await repair_projections(session) == {"repaired_batches": 0, "repaired_events": 0}


async def test_rolled_back_ingest_does_not_create_repair_record(
    session_factory, make_event, monkeypatch
):
    async def fail(session, rows):
        raise RuntimeError("projection failure")

    monkeypatch.setattr(projections, "apply_events_batch", fail)
    async with session_factory() as session, session.begin():
        await persist_events(session, [make_event()])
        await session.rollback()
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(ProjectionFailure)) == 0
        assert await session.scalar(select(func.count()).select_from(Event)) == 0


async def test_repair_failure_keeps_durable_marker(session_factory, make_event, monkeypatch):
    from phlo_observer import repair

    async def fail(session, rows):
        raise RuntimeError("projection failure")

    with monkeypatch.context() as patch:
        patch.setattr(projections, "apply_events_batch", fail)
        async with session_factory() as session, session.begin():
            await persist_events(session, [make_event()])

    async def broken_rebuild(session):
        raise RuntimeError("ambiguous lifecycle history")

    monkeypatch.setattr(repair, "rebuild_projections", broken_rebuild)
    with pytest.raises(RuntimeError, match="ambiguous"):
        async with session_factory() as session, session.begin():
            await repair_projections(session)
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(ProjectionFailure)) == 1


async def test_repair_does_not_acknowledge_expired_evidence(
    session_factory, make_event, monkeypatch
):
    async def fail(session, rows):
        raise RuntimeError("projection failure")

    with monkeypatch.context() as patch:
        patch.setattr(projections, "apply_events_batch", fail)
        async with session_factory() as session, session.begin():
            await persist_events(session, [make_event()])
    async with session_factory() as session, session.begin():
        await session.execute(delete(Event))
    with pytest.raises(ValueError, match="expired events"):
        async with session_factory() as session, session.begin():
            await repair_projections(session)
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(ProjectionFailure)) == 1


async def test_repair_leaves_failure_committed_after_snapshot_pending(
    session_factory, make_event, monkeypatch
):
    from observe_core.timestamps import utcnow
    from phlo_observer import repair

    async def fail(session, rows):
        raise RuntimeError("projection failure")

    with monkeypatch.context() as patch:
        patch.setattr(projections, "apply_events_batch", fail)
        async with session_factory() as session, session.begin():
            await persist_events(session, [make_event()])

    original = repair.rebuild_projections
    late_id = uuid.uuid4()

    async def rebuild_with_late_failure(session):
        # A projection savepoint rollback releases its advisory lock. Its
        # outer transaction may commit a failure record during maintenance.
        async with session_factory() as writer, writer.begin():
            writer.add(
                ProjectionFailure(
                    failure_id=late_id,
                    occurred_at=utcnow(),
                    event_ids=[],
                    event_count=0,
                    error_type="LateFailure",
                )
            )
        return await original(session)

    monkeypatch.setattr(repair, "rebuild_projections", rebuild_with_late_failure)
    async with session_factory() as session, session.begin():
        report = await repair_projections(session)
    assert report["repaired_batches"] == 1
    async with session_factory() as session:
        assert (await session.execute(select(ProjectionFailure.failure_id))).scalars().all() == [
            late_id
        ]


async def test_linked_trace_rollback_preserves_events_and_marker(
    client, session_factory, make_event, monkeypatch
):
    async def fail(session, rows):
        raise RuntimeError("failure after trace linking mutates canonical rows")

    monkeypatch.setattr(projections, "apply_events_batch", fail)
    trace = "a" * 32
    explicit = make_event(correlation={"run_id": "linked", "trace_id": trace})
    orphan = make_event(correlation={"trace_id": trace})
    result = await client.post("/v1/events", json=[explicit, orphan])
    assert result.status_code == 202
    assert result.json()["accepted"] == 2
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Event)) == 2
        failure = (await session.execute(select(ProjectionFailure))).scalar_one()
        assert set(failure.event_ids) == {explicit["event_id"], orphan["event_id"]}
