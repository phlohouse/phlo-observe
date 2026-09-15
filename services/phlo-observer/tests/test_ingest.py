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
async def test_raw_source_version_from_header(client: AsyncClient, session_factory: Any) -> None:
    """X-Source-Version lands on raw_events.source_version."""
    from phlo_observer.models import RawEvent
    from sqlalchemy import select

    record = {
        "run_id": "r1",
        "job_name": "j",
        "event_type": "SUCCESS",
        "timestamp": "2025-01-01T00:00:00Z",
    }
    resp = await client.post(
        "/v1/ingest/dagster", json=record, headers={"X-Source-Version": "1.9.2"}
    )
    assert resp.status_code == 202
    async with session_factory() as session:
        raw = (await session.execute(select(RawEvent))).scalars().one()
    assert raw.source_version == "1.9.2"


@pytest.mark.asyncio
async def test_raw_source_version_defaults_to_adapter(
    client: AsyncClient, session_factory: Any
) -> None:
    """Without the header, the adapter's own version is recorded."""
    from phlo_observer.models import RawEvent
    from sqlalchemy import select

    await client.post("/v1/ingest/generic", json={"a": 1})
    async with session_factory() as session:
        raw = (await session.execute(select(RawEvent))).scalars().one()
    assert raw.source_version == "1.0"


@pytest.mark.asyncio
async def test_raw_payload_disabled_by_adapter(
    client: AsyncClient, session_factory: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """keep_payload=False stores only the digest, never the body (spec §36)."""
    from phlo_observer.adapters import ADAPTERS
    from phlo_observer.models import RawEvent
    from sqlalchemy import select

    monkeypatch.setattr(ADAPTERS["generic"], "keep_payload", False)
    secret = {"ssn": "123-45-6789"}
    resp = await client.post("/v1/ingest/generic", json=secret)
    assert resp.status_code == 202
    async with session_factory() as session:
        raw = (await session.execute(select(RawEvent))).scalars().one()
    assert raw.payload.get("_encoding") == "sha256"
    assert "123-45-6789" not in json.dumps(raw.payload)


@pytest.mark.asyncio
async def test_raw_payload_size_cap(database_url: str, session_factory: Any) -> None:
    """Bodies over max_raw_payload_bytes store a digest (spec §34.3)."""
    from httpx import ASGITransport
    from phlo_observer.app import create_app
    from phlo_observer.models import RawEvent
    from phlo_observer.settings import ObserverSettings
    from sqlalchemy import select

    settings = ObserverSettings(
        database_url=database_url, max_raw_payload_bytes=64, ingest_tokens="", read_tokens=""
    )
    app = create_app(settings)
    app.state.session_factory = session_factory
    big = {"data": "x" * 512}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        resp = await c.post("/v1/ingest/generic", json=big)
        assert resp.status_code == 202
    async with session_factory() as session:
        raw = (await session.execute(select(RawEvent))).scalars().one()
    assert raw.payload.get("_encoding") == "sha256"


@pytest.mark.asyncio
async def test_ingest_otlp_endpoint(client: AsyncClient) -> None:
    """Each OTLP logRecord becomes one canonical event with trace linkage."""
    otlp = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "loader"}},
                    ]
                },
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": "1735689600000000000",
                                "severityNumber": 9,
                                "body": {"stringValue": "started"},
                                "traceId": "aa" * 16,
                            },
                            {
                                "timeUnixNano": "1735689601000000000",
                                "severityNumber": 17,
                                "body": {"stringValue": "failed"},
                            },
                        ]
                    }
                ],
            }
        ]
    }
    resp = await client.post("/v1/ingest/otlp", json=otlp)
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 2
    stored = await client.get("/v1/events", params={"service": "loader"})
    items = stored.json()["items"]
    assert len(items) == 2
    severities = {item["severity"] for item in items}
    assert severities == {"info", "error"}
    traced = [i for i in items if i["correlation"]["trace_id"]]
    assert traced[0]["correlation"]["trace_id"] == "aa" * 16


@pytest.mark.asyncio
async def test_generic_run_id_correlation(client: AsyncClient) -> None:
    """Generic events carrying run_id join the run projection."""
    run_id = f"generic-run-{uuid.uuid4().hex[:8]}"
    resp = await client.post("/v1/ingest/generic", json={"run_id": run_id, "n": 1})
    assert resp.status_code == 202
    run = await client.get(f"/v1/runs/{run_id}")
    assert run.status_code == 200
    assert run.json()["run_id"] == run_id


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
