"""Health, readiness, metrics, retention, and migration tests."""

from __future__ import annotations

import datetime as dt
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient
from observe_core.timestamps import utcnow
from phlo_observer.models import Event, RawEvent, Run
from phlo_observer.retention import run_retention_once
from phlo_observer.settings import ObserverSettings
from sqlalchemy import text

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


@pytest.mark.asyncio
async def test_healthz(client: AsyncClient) -> None:
    resp = await client.get("/health/live")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_readyz_reports_schema(client: AsyncClient) -> None:
    resp = await client.get("/health/ready")
    assert resp.status_code == 200
    # schema created via metadata in tests (no alembic_version row)
    assert resp.json()["status"] == "ready"


@pytest.mark.asyncio
async def test_readyz_reports_schema_incompatible(database_url: str, session_factory: Any) -> None:
    """Reachable DB with no events table -> 503, never a bare 500."""
    from httpx import ASGITransport
    from phlo_observer.app import create_app
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    async with engine.begin() as conn:
        await conn.execute(text("drop table if exists events cascade"))
    await engine.dispose()
    app = create_app(ObserverSettings(database_url=database_url))
    app.state.session_factory = session_factory
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        resp = await c.get("/health/ready")
        assert resp.status_code == 503
        assert resp.json()["database"] == "schema_incompatible"


@pytest.mark.asyncio
async def test_metrics_endpoint(client: AsyncClient, make_event: Any) -> None:
    await client.post("/v1/events", json=make_event())
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    text_body = resp.text
    assert "phlo_observer_ingest_events_total" in text_body
    assert "phlo_observer_events_stored_total" in text_body
    assert "phlo_observer_http_requests_total" in text_body
    assert "phlo_observer_queue_depth" in text_body


@pytest.mark.asyncio
async def test_retention_cleanup(session_factory: Any, database_url: str, make_event: Any) -> None:
    settings = ObserverSettings(
        database_url=database_url,
        raw_retention_days=14,
        event_retention_days=90,
        run_retention_days=365,
    )
    old = utcnow() - dt.timedelta(days=400)
    raw_id = uuid.uuid4()
    linked_event_id = uuid.uuid4()
    async with session_factory() as session, session.begin():
        session.add(
            RawEvent(
                id=raw_id,
                received_at=old,
                producer="p",
                source_kind="k",
                content_type="application/json",
                payload={},
                payload_sha256="x" * 64,
                expires_at=old,  # expired
            )
        )
        session.add(
            Event(
                event_id=uuid.uuid4(),
                schema_version="1.0",
                event="pipeline.run",
                category="pipeline",
                outcome="success",
                severity="info",
                delivery="telemetry",
                observed_at=old,
                received_at=old,
                attributes={},
                source={},
            )
        )
        # A fresh event still referencing the expired raw row — the realistic
        # case: raw retention (14d) expires before event retention (90d).
        # Flush the parent first, mirroring store_raw() -> persist_events().
        await session.flush()
        session.add(
            Event(
                event_id=linked_event_id,
                schema_version="1.0",
                event="pipeline.step",
                category="pipeline",
                outcome="success",
                severity="info",
                delivery="telemetry",
                observed_at=utcnow(),
                received_at=utcnow(),
                attributes={},
                source={},
                payload={},
                raw_event_id=raw_id,
            )
        )
        session.add(Run(run_id="old-run", status="success", updated_at=old, summary={}))
    report = await run_retention_once(session_factory, settings)
    assert report.raw_events == 1
    assert report.events == 1
    assert report.runs == 1
    # The linked event survives; ON DELETE SET NULL clears its reference.
    async with session_factory() as session:
        surviving = await session.get(Event, linked_event_id)
        assert surviving is not None
        assert surviving.raw_event_id is None


@pytest.mark.asyncio
async def test_retention_terminal_insights_incidents(
    session_factory: Any, database_url: str
) -> None:
    """Terminal insights/incidents age out; open ones are live signal."""
    from phlo_observer.models import Incident, Insight

    settings = ObserverSettings(database_url=database_url, run_retention_days=365)
    old = utcnow() - dt.timedelta(days=400)

    def _insight(state: str) -> Insight:
        return Insight(
            insight_id=uuid.uuid4(),
            rule_id="r",
            rule_version=1,
            title="t",
            severity="warn",
            state=state,
            evidence_event_ids=[],
            evidence_metric_ids=[],
            created_at=old,
            updated_at=old,
            attributes={},
        )

    def _incident(state: str) -> Incident:
        return Incident(
            incident_id=uuid.uuid4(),
            title="i",
            state=state,
            severity="warn",
            entities=[],
            insight_ids=[],
            timeline={},
            impact={},
            attributes={},
            updated_at=old,
        )

    async with session_factory() as session, session.begin():
        session.add(_insight("resolved"))
        session.add(_insight("open"))
        session.add(_incident("resolved"))
        session.add(_incident("open"))
    report = await run_retention_once(session_factory, settings)
    assert report.insights == 1
    assert report.incidents == 1
    async with session_factory() as session:
        remaining_i = (
            await session.execute(text("select count(*) from observe_insights"))
        ).scalar()
        remaining_c = (
            await session.execute(text("select count(*) from observe_incidents"))
        ).scalar()
    assert remaining_i == 1
    assert remaining_c == 1


@pytest.mark.asyncio
async def test_retention_skips_when_lock_held(session_factory: Any, database_url: str) -> None:
    """A second instance's pass must skip while another holds the advisory lock."""
    settings = ObserverSettings(database_url=database_url)
    async with session_factory() as holder, holder.begin():
        await holder.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": 0x70686C6F})
        report = await run_retention_once(session_factory, settings)
    assert report.skipped
    assert report.events == 0
    # After the holder commits, a fresh pass runs normally.
    report = await run_retention_once(session_factory, settings)
    assert not report.skipped


@pytest.mark.asyncio
async def test_alembic_upgrade_and_downgrade(database_url: str) -> None:
    """Real migration: empty database -> head -> down to base -> head again."""
    import sys

    env = dict(os.environ, PHLO_OBSERVER_DATABASE_URL=database_url)

    def run_alembic(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "phlo_observer.cli", *args],
            env=env,
            capture_output=True,
            text=True,
            cwd=MIGRATIONS_DIR.parent,
        )

    # drop everything so the migration starts from an empty database
    from phlo_observer.models import Base
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.execute(text("drop table if exists alembic_version"))
    await engine.dispose()

    result = run_alembic("migrate")
    assert result.returncode == 0, result.stderr
    result = run_alembic("downgrade", "base")
    assert result.returncode == 0, result.stderr
    result = run_alembic("migrate")
    assert result.returncode == 0, result.stderr
