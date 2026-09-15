"""End-to-end: observe-core HTTP drain -> phlo-observer -> query API.

Runs a real uvicorn server on a loopback socket against real Postgres
(PHLO_OBSERVER_TEST_DATABASE_URL): emission, HTTP transport, normalization,
correlation, persistence, and read-back all go over the wire.
"""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
import uvicorn
from httpx import AsyncClient
from observe_core.config import HttpDrainConfig
from observe_core.config import ObserveSettings as CoreSettings
from observe_core.runtime import configure, flush, shutdown
from phlo_observer.app import create_app
from phlo_observer.models import Base
from phlo_observer.settings import ObserverSettings
from sqlalchemy.ext.asyncio import create_async_engine

TEST_DATABASE_URL = os.environ.get(
    "PHLO_OBSERVER_TEST_DATABASE_URL",
    "postgresql+asyncpg://phlo:phlo@localhost:5432/phlo_observer_test",
)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _reachable(url: str) -> bool:
    probe = create_async_engine(url)
    try:
        async with probe.connect() as conn:
            await conn.exec_driver_sql("select 1")
        return True
    except Exception:
        return False
    finally:
        await probe.dispose()


@pytest.fixture(scope="module")
def observer_url() -> Iterator[str]:
    """A live observer HTTP server for the whole module."""
    if not asyncio.run(_reachable(TEST_DATABASE_URL)):
        pytest.skip("PostgreSQL not reachable")
    settings = ObserverSettings(database_url=TEST_DATABASE_URL)
    app = create_app(settings)
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 15
    while not server.started:
        if time.monotonic() > deadline:
            pytest.fail("observer server did not start")
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


@pytest_asyncio.fixture
async def db_reset() -> AsyncIterator[None]:
    """Fresh schema per test (server is module-scoped, schema is not)."""
    engine = create_async_engine(TEST_DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


@pytest_asyncio.fixture
async def client(observer_url: str) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(base_url=observer_url) as c:
        yield c


@pytest.mark.asyncio
async def test_emit_to_observer_roundtrip(
    observer_url: str, db_reset: None, client: AsyncClient
) -> None:
    """observe() -> real http drain -> ingest -> /v1/events shows the event."""
    run_id = f"e2e-{uuid.uuid4().hex[:8]}"
    settings = CoreSettings(
        service_name="e2e-test",
        drains=[HttpDrainConfig(endpoint=f"{observer_url}/v1/events")],
    )
    try:
        configure(settings)
        from observe_core.operation import observe

        with observe("pipeline.run", correlation={"run_id": run_id}):
            pass
        assert flush(5.0)
    finally:
        shutdown()

    resp = await client.get("/v1/events", params={"run_id": run_id})
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0]["event"] == "pipeline.run"
    assert items[0]["service"]["name"] == "e2e-test"

    run = (await client.get(f"/v1/runs/{run_id}")).json()
    assert run["status"] == "success"
    assert run["event_count"] == 1


@pytest.mark.asyncio
async def test_dagster_adapter_flow(db_reset: None, client: AsyncClient) -> None:
    """Source adapters normalize foreign payloads into queryable events."""
    dagster_payload = [
        {"run_id": "dg-1", "job_name": "etl", "event_type": "STARTED"},
        {
            "run_id": "dg-1",
            "job_name": "etl",
            "event_type": "ASSET_MATERIALIZATION",
            "asset_key": {"path": ["staging", "orders"]},
        },
        {"run_id": "dg-1", "job_name": "etl", "event_type": "SUCCESS"},
    ]
    resp = await client.post("/v1/ingest/dagster", json=dagster_payload)
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 3

    timeline = (await client.get("/v1/runs/dg-1/timeline")).json()
    assert timeline["run"]["status"] == "success"
    assert timeline["run"]["event_count"] == 3
    assert timeline["run"]["asset_count"] == 1
    assert timeline["run"]["job_name"] == "etl"


@pytest.mark.asyncio
async def test_partial_batch_errors(db_reset: None, client: AsyncClient) -> None:
    """Mixed valid/invalid canonical batch: valid stored, invalid reported."""
    good = {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "event": "pipeline.run",
        "category": "pipeline",
        "outcome": "success",
        "severity": "info",
        "delivery": "telemetry",
        "observed_at": "2025-01-01T00:00:00Z",
        "service": {"name": "it"},
        "correlation": {},
        "attributes": {},
    }
    bad = dict(good)
    bad["event_id"] = str(uuid.uuid4())
    del bad["observed_at"]
    resp = await client.post("/v1/events", json=[good, bad])
    body = resp.json()
    assert resp.status_code == 202
    assert body["accepted"] == 1
    assert body["rejected"] == 1
    assert body["errors"][0]["code"] == "SCHEMA_INVALID"
    assert body["errors"][0]["index"] == 1
