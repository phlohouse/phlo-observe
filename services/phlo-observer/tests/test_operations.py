"""Health, readiness, metrics, retention, and migration tests."""

from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
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
async def test_json_endpoints_enforce_body_limit(session_factory: Any, database_url: str) -> None:
    """Regression: JSON-parsing endpoints must enforce ``max_body_bytes``.

    The middleware only checks the declared Content-Length, so a chunked
    body without one used to reach ``request.json()`` — which buffers the
    stream unboundedly. All JSON endpoints now read through ``_body()``.
    """
    from collections.abc import AsyncIterator

    from phlo_observer.app import create_app

    app = create_app(ObserverSettings(database_url=database_url, max_body_bytes=64))
    app.state.session_factory = session_factory

    async def _chunks() -> AsyncIterator[bytes]:
        yield b'{"run_a": "'
        yield b"x" * 128
        yield b'"}'

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        # No Content-Length -> middleware cannot reject; the endpoint's own
        # bounded reader must.
        resp = await c.post("/v2/query/compare-runs", content=_chunks())
        assert resp.status_code == 413
        # Declared oversize is still rejected up front.
        big = await c.post("/v2/query/compare-runs", content=b"x" * 256)
        assert big.status_code == 413
        # In-bounds bodies still reach the endpoint's own validation.
        missing = await c.post("/v2/query/compare-runs", json={"run_a": "a"})
        assert missing.status_code == 400
        unknown = await c.post("/v2/query/compare-runs", json={"run_a": "a", "run_b": "b"})
        assert unknown.status_code == 404
        # Malformed and non-object bodies are client errors, not 500s.
        assert (await c.post("/v2/query/compare-runs", content=b"not json")).status_code == 400
        assert (await c.post("/v2/query/compare-runs", json=[1, 2])).status_code == 400


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
async def test_retention_uses_received_at_not_observed_at(
    session_factory: Any, database_url: str
) -> None:
    """Regression: the retention clock is ``received_at`` (server time).

    A clock-skewed producer must not cause an event to expire early or
    to outlive retention: ``observed_at`` is untrusted input.
    """
    settings = ObserverSettings(database_url=database_url, event_retention_days=30)
    now = utcnow()
    old = now - dt.timedelta(days=400)

    def _event(observed: dt.datetime, received: dt.datetime) -> Event:
        return Event(
            event_id=uuid.uuid4(),
            schema_version="1.0",
            event="pipeline.step",
            category="pipeline",
            outcome="success",
            severity="info",
            delivery="telemetry",
            observed_at=observed,
            received_at=received,
            attributes={},
            source={},
        )

    skewed_old = _event(observed=old, received=now)  # far-past producer clock
    skewed_future = _event(observed=now, received=old)  # freshly-observed, stale receive
    async with session_factory() as session, session.begin():
        session.add(skewed_old)
        session.add(skewed_future)
    report = await run_retention_once(session_factory, settings)
    assert report.events == 1
    async with session_factory() as session:
        assert await session.get(Event, skewed_old.event_id) is not None
        assert await session.get(Event, skewed_future.event_id) is None


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


@pytest.mark.asyncio
async def test_alembic_0006_rewrites_baseline_samples(database_url: str) -> None:
    """0006 upgrades legacy ``[value, ...]`` samples to keyed triples and the
    downgrade restores them — values and order survive the round trip."""
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

    from phlo_observer.models import Base
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.execute(text("drop table if exists alembic_version"))
    result = run_alembic("migrate", "0005")
    assert result.returncode == 0, result.stderr

    async with engine.begin() as conn:
        await conn.execute(
            text(
                "insert into observe_baselines "
                "(entity_id, metric, samples, count, mean, updated_at) "
                "values ('e', 'm', '[1.5, 2.5]', 2, 2.0, now())"
            )
        )

    result = run_alembic("migrate", "0006")
    assert result.returncode == 0, result.stderr
    async with engine.begin() as conn:
        samples = (await conn.execute(text("select samples from observe_baselines"))).scalar_one()
    assert [s[2] for s in samples] == [1.5, 2.5]
    assert all(len(s) == 3 and s[0].endswith("Z") for s in samples)

    result = run_alembic("downgrade", "0005")
    assert result.returncode == 0, result.stderr
    async with engine.begin() as conn:
        samples = (await conn.execute(text("select samples from observe_baselines"))).scalar_one()
    assert samples == [1.5, 2.5]
    await engine.dispose()


@pytest.mark.asyncio
async def test_alembic_0007_collapses_duplicate_open_insights(database_url: str) -> None:
    """0007 merges duplicate open rows into the earliest keeper before
    creating the partial unique index — producers and evidence unioned,
    losers suppressed with a ``deduped_into`` pointer, resolved rows and
    NULL-dedupe rows untouched."""
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

    from phlo_observer.models import Base
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.execute(text("drop table if exists alembic_version"))
    result = run_alembic("migrate", "0006")
    assert result.returncode == 0, result.stderr

    keeper = "11111111-1111-4111-8111-111111111111"
    loser = "22222222-2222-4222-8222-222222222222"
    resolved = "33333333-3333-4333-8333-333333333333"
    async with engine.begin() as conn:
        for iid, state, created, attrs, evidence in [
            (
                keeper,
                "open",
                dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
                {"producers": [{"at": "2025-01-01T00:00:00Z", "eid": "e-1"}]},
                '["e-1"]',
            ),
            (
                loser,
                "open",
                dt.datetime(2025, 1, 2, tzinfo=dt.UTC),
                {"producers": [{"at": "2025-01-02T00:00:00Z", "eid": "e-2"}]},
                '["e-2"]',
            ),
            (resolved, "resolved", dt.datetime(2024, 12, 31, tzinfo=dt.UTC), {}, "[]"),
        ]:
            await conn.execute(
                text(
                    "insert into observe_insights (insight_id, rule_id, rule_version,"
                    " title, severity, state, entity_id, evidence_event_ids,"
                    " evidence_metric_ids, recommended_action_verified, dedupe_key,"
                    " attributes, created_at, updated_at)"
                    " values (:iid, 'run-failure', 1, 't', 'warning', :state,"
                    " 'asset://a', cast(:evidence as jsonb), '[]'::jsonb, 0, 'dk',"
                    " cast(:attrs as jsonb), :created, :created)"
                ),
                {
                    "iid": iid,
                    "state": state,
                    "created": created,
                    "attrs": json.dumps(attrs),
                    "evidence": evidence,
                },
            )
        # An open row with NULL dedupe_key must survive the index.
        await conn.execute(
            text(
                "insert into observe_insights (insight_id, rule_id, rule_version,"
                " title, severity, state, entity_id, evidence_event_ids,"
                " evidence_metric_ids, recommended_action_verified, dedupe_key,"
                " attributes, created_at, updated_at)"
                " values ('44444444-4444-4444-8444-444444444444', 'run-failure', 1,"
                " 't', 'warning', 'open', 'asset://b', '[]'::jsonb, '[]'::jsonb, 0,"
                " NULL, '{}'::jsonb, now(), now())"
            )
        )

    result = run_alembic("migrate", "0007")
    assert result.returncode == 0, result.stderr

    async with engine.begin() as conn:
        rows = (
            (
                await conn.execute(
                    text(
                        "select insight_id, state, attributes, evidence_event_ids"
                        " from observe_insights where dedupe_key = 'dk'"
                        " order by created_at"
                    )
                )
            )
            .mappings()
            .all()
        )
        assert len(rows) == 3
        res, kept, dup = rows  # ordered by created_at: resolved predates both
        assert str(res["insight_id"]) == resolved
        assert res["state"] == "resolved"
        assert str(kept["insight_id"]) == keeper
        assert kept["state"] == "open"
        assert kept["attributes"]["dedupe_merged"] is True
        assert {p["eid"] for p in kept["attributes"]["producers"]} == {"e-1", "e-2"}
        assert set(kept["evidence_event_ids"]) == {"e-1", "e-2"}
        assert str(dup["insight_id"]) == loser
        assert dup["state"] == "suppressed"
        assert dup["attributes"]["deduped_into"] == keeper

        # The partial unique index now rejects a second open row on 'dk'.
        from sqlalchemy.exc import IntegrityError

        with pytest.raises(IntegrityError):
            async with engine.begin() as conn2:
                await conn2.execute(
                    text(
                        "insert into observe_insights (insight_id, rule_id,"
                        " rule_version, title, severity, state, entity_id,"
                        " evidence_event_ids, evidence_metric_ids,"
                        " recommended_action_verified, dedupe_key, attributes,"
                        " created_at, updated_at)"
                        " values ('55555555-5555-4555-8555-555555555555',"
                        " 'run-failure', 1, 't', 'warning', 'open', 'asset://a',"
                        " '[]'::jsonb, '[]'::jsonb, 0, 'dk', '{}'::jsonb,"
                        " now(), now())"
                    )
                )

    result = run_alembic("downgrade", "0006")
    assert result.returncode == 0, result.stderr
    result = run_alembic("migrate")
    assert result.returncode == 0, result.stderr
    await engine.dispose()
