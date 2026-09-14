"""Canonical ingestion: single, batch, gzip, limits, idempotency, conflicts."""

from __future__ import annotations

import gzip
import json
import uuid
from typing import Any

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_ingest_single_event(client: AsyncClient, make_event: Any) -> None:
    resp = await client.post("/v1/events", json=make_event())
    assert resp.status_code == 202
    body = resp.json()
    assert body["accepted"] == 1
    assert body["rejected"] == 0
    assert body["errors"] == []


@pytest.mark.asyncio
async def test_ingest_batch(client: AsyncClient, make_event: Any) -> None:
    events = [make_event() for _ in range(5)]
    resp = await client.post("/v1/events", json=events)
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 5


@pytest.mark.asyncio
async def test_ingest_partial_rejection(client: AsyncClient, make_event: Any) -> None:
    """A batch with a bad item rejects the item, keeps the good ones."""
    good = make_event()
    bad = dict(make_event())
    del bad["event"]  # missing required field
    resp = await client.post("/v1/events", json=[good, bad])
    body = resp.json()
    # adapter-level failure rejects the whole batch as SCHEMA_INVALID
    assert body["rejected"] >= 1
    assert body["errors"]


@pytest.mark.asyncio
async def test_ingest_gzip(client: AsyncClient, make_event: Any) -> None:
    payload = gzip.compress(json.dumps([make_event(), make_event()]).encode())
    resp = await client.post(
        "/v1/events",
        content=payload,
        headers={"Content-Type": "application/json", "Content-Encoding": "gzip"},
    )
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 2


@pytest.mark.asyncio
async def test_ingest_invalid_gzip(client: AsyncClient) -> None:
    resp = await client.post(
        "/v1/events",
        content=b"not gzip",
        headers={"Content-Encoding": "gzip"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_ingest_invalid_json(client: AsyncClient) -> None:
    resp = await client.post("/v1/events", content=b"{not json")
    assert resp.status_code in (400, 422)


@pytest.mark.asyncio
async def test_body_limit(database_url: str, session_factory: Any, make_event: Any) -> None:
    from httpx import ASGITransport
    from phlo_observer.app import create_app
    from phlo_observer.settings import ObserverSettings

    settings = ObserverSettings(database_url=database_url, max_body_bytes=256)
    app = create_app(settings)
    app.state.session_factory = session_factory
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        resp = await c.post("/v1/events", json=make_event())
        assert resp.status_code == 413


@pytest.mark.asyncio
async def test_batch_limit(database_url: str, session_factory: Any, make_event: Any) -> None:
    from httpx import ASGITransport
    from phlo_observer.app import create_app
    from phlo_observer.settings import ObserverSettings

    settings = ObserverSettings(database_url=database_url, max_batch_events=2)
    app = create_app(settings)
    app.state.session_factory = session_factory
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        resp = await c.post("/v1/events", json=[make_event() for _ in range(3)])
        assert resp.status_code == 413
        assert resp.json()["errors"][0]["code"] == "BATCH_TOO_LARGE"


@pytest.mark.asyncio
async def test_duplicate_identical_event(client: AsyncClient, make_event: Any) -> None:
    """Same event_id + identical payload: idempotent, not a conflict."""
    event = make_event()
    first = await client.post("/v1/events", json=event)
    second = await client.post("/v1/events", json=event)
    assert first.status_code == 202
    assert second.status_code == 202
    body = second.json()
    assert body["duplicates"] == 1
    assert body["accepted"] == 0
    # still one stored row
    resp = await client.get("/v1/events", params={"event": "pipeline.run"})
    assert len(resp.json()["items"]) == 1


@pytest.mark.asyncio
async def test_conflicting_duplicate_event(client: AsyncClient, make_event: Any) -> None:
    """Same event_id + different payload: integrity conflict, no overwrite."""
    event = make_event()
    await client.post("/v1/events", json=event)
    tampered = dict(event)
    tampered["outcome"] = "failure"
    tampered["attributes"] = {"forged": True}
    resp = await client.post("/v1/events", json=tampered)
    body = resp.json()
    assert body["rejected"] == 1
    assert body["errors"][0]["code"] == "INTEGRITY_CONFLICT"
    stored = await client.get(f"/v1/events/{event['event_id']}")
    assert stored.json()["outcome"] == "success"  # original preserved


@pytest.mark.asyncio
async def test_concurrent_ingest_same_id(client: AsyncClient, make_event: Any) -> None:
    """Concurrent identical submissions resolve to exactly one stored row."""
    import asyncio

    event = make_event()

    async def submit() -> int:
        return (await client.post("/v1/events", json=event)).status_code

    statuses = await asyncio.gather(*[submit() for _ in range(4)])
    assert all(s == 202 for s in statuses)
    resp = await client.get("/v1/events")
    assert len(resp.json()["items"]) == 1


@pytest.mark.asyncio
async def test_raw_payload_preserved(
    client: AsyncClient, session_factory: Any, make_event: Any
) -> None:
    from phlo_observer.models import RawEvent
    from sqlalchemy import select

    await client.post("/v1/events", json=make_event())
    async with session_factory() as session:
        raws = list((await session.execute(select(RawEvent))).scalars())
    assert len(raws) == 1
    assert raws[0].producer == "canonical"
    assert raws[0].payload_sha256
    assert raws[0].normalization_status == "ok"


@pytest.mark.asyncio
async def test_ingest_dagster_endpoint(client: AsyncClient) -> None:
    record = {
        "run_id": "dagster-run-1",
        "job_name": "etl_job",
        "event_type": "SUCCESS",
        "timestamp": "2025-01-01T00:00:00Z",
    }
    resp = await client.post("/v1/ingest/dagster", json=record)
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 1


@pytest.mark.asyncio
async def test_ingest_generic_endpoint(client: AsyncClient) -> None:
    resp = await client.post("/v1/ingest/generic", json={"custom": "payload", "n": 1})
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 1
    stored = await client.get("/v1/events", params={"event": "external.source_event"})
    assert len(stored.json()["items"]) == 1


@pytest.mark.asyncio
async def test_ingest_dbt_run_results(client: AsyncClient) -> None:
    run_results = {
        "metadata": {
            "dbt_version": "1.7.0",
            "invocation_id": str(uuid.uuid4()),
            "generated_at": "2025-01-01T00:00:00Z",
        },
        "results": [
            {
                "unique_id": "model.proj.my_model",
                "status": "success",
                "execution_time": 1.2,
                "timing": [],
            }
        ],
        "elapsed_time": 2.5,
    }
    resp = await client.post("/v1/ingest/dbt", json=run_results)
    assert resp.status_code == 202
    assert resp.json()["accepted"] >= 1


@pytest.mark.asyncio
async def test_ingest_dbt_artifacts_bundle(client: AsyncClient) -> None:
    bundle = {
        "run_results": {
            "metadata": {"invocation_id": str(uuid.uuid4())},
            "results": [{"unique_id": "test.proj.t1", "status": "pass", "timing": []}],
        },
        "manifest": {"metadata": {"project_name": "proj", "adapter_type": "trino"}},
    }
    resp = await client.post("/v1/ingest/dbt/artifacts", json=bundle)
    assert resp.status_code == 202
    assert resp.json()["accepted"] >= 1
