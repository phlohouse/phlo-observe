"""Recovery and lifecycle proofs (hardening item 9).

Archive -> delete -> restore -> rebuild must return the deployment to the
same derived state the live ingest produced, and retention must stay
correct and fast at realistic volumes.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import uuid
from typing import Any

import pytest
import workloads as wl
from observe_core.timestamps import utcnow
from phlo_observer.cli import app as cli_app
from phlo_observer.models import Event, RawEvent
from phlo_observer.retention import run_retention_once
from phlo_observer.settings import ObserverSettings
from sqlalchemy import func, select, text
from test_equivalence import _snapshot, diff_snapshots
from typer.testing import CliRunner

pytestmark = pytest.mark.asyncio


async def test_archive_delete_restore_rebuild_roundtrip(
    session_factory: Any, database_url: str, tmp_path: Any, monkeypatch: Any
) -> None:
    """Restored canonical events rebuild to the same derived state."""
    from phlo_observer.store import persist_events

    events = wl.mixed_history(days=2, seed=11)
    async with session_factory() as session, session.begin():
        await persist_events(session, events)
    before = await _snapshot(session_factory)
    assert before["runs"], "workload produced no runs"

    out = tmp_path / "events.jsonl"
    monkeypatch.setenv("PHLO_OBSERVER_DATABASE_URL", database_url)
    result = await asyncio.to_thread(CliRunner().invoke, cli_app, ["archive", str(out)])
    assert result.exit_code == 0, result.output

    async with session_factory() as session, session.begin():
        for table in (
            "observe_insights",
            "observe_incidents",
            "observe_baselines",
            "observe_relationships",
            "observe_assets",
            "observe_entities",
            "runs",
            "events",
        ):
            await session.execute(text(f"delete from {table}"))  # noqa: S608

    result = await asyncio.to_thread(CliRunner().invoke, cli_app, ["restore", str(out)])
    assert result.exit_code == 0, result.output

    result = await asyncio.to_thread(CliRunner().invoke, cli_app, ["rebuild-projections"])
    assert result.exit_code == 0, result.output

    after = await _snapshot(session_factory)
    diff = diff_snapshots(before, after)
    assert not diff, f"restored+rebuilt state diverged:\n{diff}"


async def test_retention_at_volume(session_factory: Any) -> None:
    """Retention stays correct over tens of thousands of aged rows."""
    settings = ObserverSettings(
        event_retention_days=30, run_retention_days=30, raw_event_ttl_hours=0
    )
    old = wl.T0 - dt.timedelta(days=400)
    fresh = utcnow()

    def _row(i: int, when: dt.datetime, run: str) -> Event:
        return Event(
            event_id=uuid.uuid4(),
            schema_version="2.0",
            event="run.step.completed",
            category="pipeline",
            outcome="success",
            severity="info",
            delivery="telemetry",
            observed_at=when,
            received_at=when,
            run_id=run,
            attributes={},
        )

    async with session_factory() as session, session.begin():
        session.add_all([_row(i, old + dt.timedelta(seconds=i), f"old-{i}") for i in range(2000)])
        session.add_all([_row(i, fresh + dt.timedelta(seconds=i), f"new-{i}") for i in range(500)])
        session.add_all(
            [
                RawEvent(
                    id=uuid.uuid4(),
                    received_at=old,
                    producer="test",
                    source_kind="http",
                    payload={"stale": True},
                    payload_sha256="0" * 64,
                    expires_at=old,
                )
                for _ in range(1000)
            ]
        )
    report = await run_retention_once(session_factory, settings)
    assert report.events == 2000
    assert report.raw_events == 1000
    async with session_factory() as session:
        remaining = await session.scalar(select(func.count()).select_from(Event))
        assert remaining == 500


async def test_v1_envelope_still_ingests(client: Any, make_event: Any) -> None:
    """V1 single-event and batch shapes remain accepted after V2 landed."""
    response = await client.post("/v1/events", json=make_event())
    assert response.status_code == 202
    assert response.json()["accepted"] == 1
    batch = [make_event(), make_event()]
    response = await client.post("/v1/events", json=batch)
    assert response.status_code == 202
    assert response.json()["accepted"] == 2
