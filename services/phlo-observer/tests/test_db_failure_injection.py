"""Observer failure injection (spec §82): database down and reconnect.

Covered elsewhere: malformed raw events (test_ingest/test_validation),
auth token failures (test_auth), worker/drain faults on the emit side
(packages/observe-core/tests/test_failure_injection.py).
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from phlo_observer.app import create_app
from phlo_observer.settings import ObserverSettings
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

_DEAD_URL = "postgresql+asyncpg://phlo:phlo@localhost:1/phlo_test"


async def _dead_app() -> Any:
    """App whose session factory points at a closed port."""
    engine = create_async_engine(_DEAD_URL, pool_pre_ping=True)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    application = create_app(
        ObserverSettings(database_url=_DEAD_URL, ingest_tokens="", read_tokens="")
    )
    application.state.session_factory = factory
    application.state.engine = engine
    return application


@pytest.mark.asyncio
async def test_database_unavailable_readiness(make_event: Any) -> None:
    """Database down: readiness reports not_ready, liveness stays up."""
    app = await _dead_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        live = await c.get("/health/live")
        assert live.status_code == 200
        ready = await c.get("/health/ready")
        assert ready.status_code == 503
        assert ready.json()["database"] == "unreachable"


@pytest.mark.asyncio
async def test_database_unavailable_ingest_errors_not_hangs(make_event: Any) -> None:
    """Database down: ingestion returns a 5xx, never an accepted response."""
    app = await _dead_app()
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://t") as c:
        resp = await c.post("/v1/events", json=make_event())
        assert resp.status_code >= 500


@pytest.mark.asyncio
async def test_database_reconnect_recovers(
    database_url: str,
    engine: Any,
    make_event: Any,
) -> None:
    """Server-side connection loss must not permanently break ingestion.

    ``pg_terminate_backend`` kills this engine's pooled connections
    server-side (matched by ``application_name``); ``pool_pre_ping`` then
    reconnects them transparently on the next checkout.
    """
    marker = f"phlo_reconnect_{uuid.uuid4().hex[:8]}"
    cut_engine = create_async_engine(
        database_url,
        pool_pre_ping=True,
        connect_args={"server_settings": {"application_name": marker}},
    )
    factory = async_sessionmaker(cut_engine, expire_on_commit=False)
    app = create_app(ObserverSettings(database_url=database_url, ingest_tokens="", read_tokens=""))
    app.state.session_factory = factory
    app.state.engine = cut_engine
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            first = await c.post("/v1/events", json=make_event())
            assert first.status_code == 202

            # Hold one pooled connection as the victim while a second
            # connection terminates its backend server-side. When the victim
            # is returned, the pool holds a dead connection.
            victim = await cut_engine.connect()
            async with cut_engine.connect() as conn:
                result = await conn.exec_driver_sql(
                    "select pg_terminate_backend(pid) from pg_stat_activity "
                    "where application_name = $1 and pid <> pg_backend_pid()",
                    (marker,),
                )
                terminated = result.fetchall()
                assert terminated, "expected at least one pooled backend to terminate"
            await victim.close()  # dead connection goes back into the pool

            second = await c.post("/v1/events", json=make_event())
            assert second.status_code == 202
            stored = await c.get("/v1/events")
            assert len(stored.json()["items"]) == 2
    finally:
        await cut_engine.dispose()


@pytest.mark.asyncio
async def test_timeline_bounded(
    make_event: Any, session_factory: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Run timelines cap their event list and flag truncation."""
    import phlo_observer.timeline as timeline_mod
    from phlo_observer.store import persist_events

    monkeypatch.setattr(timeline_mod, "_MAX_TIMELINE_EVENTS", 3)
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    events = [
        make_event(correlation={"run_id": run_id}, event_id=str(uuid.uuid4())) for _ in range(6)
    ]
    async with session_factory() as session, session.begin():
        await persist_events(session, events)
    async with session_factory() as session:
        timeline = await timeline_mod.run_timeline(session, run_id)
    assert timeline is not None
    assert timeline["truncated"] is True
    assert len(timeline["events"]) == 3
