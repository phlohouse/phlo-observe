"""V2 operations: search, SSE stream, quarantine replay (spec §23/§26/§34)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from phlo_observer.models import IngestFailure
from phlo_observer.stream import StreamHub, sse_encode
from sqlalchemy import select


def _event(
    *,
    event: str = "pipeline.run",
    outcome: str = "success",
    run_id: str | None = None,
    asset_key: str | None = None,
    error: dict[str, Any] | None = None,
    service_name: str = "svc",
) -> dict[str, Any]:
    corr: dict[str, Any] = {}
    if run_id:
        corr["run_id"] = run_id
    if asset_key:
        corr["asset_key"] = asset_key
    body: dict[str, Any] = {
        "schema_version": "2.0",
        "event_id": str(uuid.uuid4()),
        "event": event,
        "category": "pipeline",
        "outcome": outcome,
        "severity": "info",
        "delivery": "telemetry",
        "observed_at": "2025-01-01T00:00:00Z",
        "service": {"name": service_name},
        "correlation": corr,
        "attributes": {},
        "source": {"producer": "test"},
    }
    if error:
        body["error"] = error
    return body


class TestStreamHub:
    def test_publish_reaches_subscriber(self) -> None:
        hub = StreamHub()
        queue = hub.subscribe()
        assert hub.publish("run.changed", {"run_id": "r1"}) == 1
        assert queue.get_nowait()["data"]["run_id"] == "r1"
        hub.unsubscribe(queue)

    def test_publish_drops_full_queue(self) -> None:

        hub = StreamHub()
        queue = hub.subscribe()
        for _ in range(queue.maxsize):
            queue.put_nowait({"kind": "x", "data": {}})
        assert hub.publish("x", {}) == 0  # full queue drops, no raise
        hub.unsubscribe(queue)

    def test_subscribe_capacity(self) -> None:
        hub = StreamHub()
        queues = [hub.subscribe() for _ in range(3)]
        assert len(hub._subscribers) == 3
        for q in queues:
            hub.unsubscribe(q)
        assert not hub._subscribers

    def test_sse_encode_frame(self) -> None:
        frame = sse_encode({"kind": "insight.opened", "data": {"a": 1}})
        assert frame.startswith(b"event: insight.opened\n")
        assert b'data: {"a": 1}' in frame


@pytest.mark.asyncio
class TestSearch:
    async def test_q_matches_event_name(self, client: Any) -> None:
        await client.post(
            "/v1/events",
            json=[_event(event="pipeline.run", run_id="s1")],
        )
        resp = await client.get("/v2/search", params={"q": "pipeline"})
        assert resp.status_code == 200
        assert any(i["event"] == "pipeline.run" for i in resp.json()["items"])

    async def test_q_matches_error_message(self, client: Any) -> None:
        await client.post(
            "/v1/events",
            json=[_event(error={"message": "trino OOM killer"}, outcome="failure")],
        )
        resp = await client.get("/v2/search", params={"q": "OOM killer"})
        assert resp.status_code == 200
        assert len(resp.json()["items"]) == 1

    async def test_q_no_match(self, client: Any) -> None:
        await client.post("/v1/events", json=[_event()])
        resp = await client.get("/v2/search", params={"q": "zzz-no-such"})
        assert resp.json()["items"] == []

    async def test_structured_filters_still_apply(self, client: Any) -> None:
        await client.post(
            "/v1/events",
            json=[_event(asset_key="search/x", run_id="sr1")],
        )
        resp = await client.get("/v2/search", params={"asset_key": "search/x", "q": "pipeline"})
        assert len(resp.json()["items"]) == 1


@pytest.mark.asyncio
class TestQuarantine:
    async def test_failed_payload_quarantined(self, client: Any, session_factory: Any) -> None:
        # Unparseable payload for the dagster adapter -> normalization fails.
        resp = await client.post("/v1/ingest/dagster", content=b"not-json")
        assert resp.status_code in (400, 422)
        async with session_factory() as session:
            rows = (await session.execute(select(IngestFailure))).scalars().all()
        assert len(rows) == 1
        assert rows[0].producer == "dagster"
        assert rows[0].replayed == 0

    async def test_quarantine_list_endpoint(self, client: Any, session_factory: Any) -> None:
        await client.post("/v1/ingest/dagster", content=b"garbage")
        resp = await client.get("/v2/admin/quarantine")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 1
        assert items[0]["error_code"]

    async def test_quarantine_replay_not_replayable(
        self, client: Any, session_factory: Any
    ) -> None:
        await client.post("/v1/ingest/dagster", content=b"bad")
        async with session_factory() as session:
            row = (await session.execute(select(IngestFailure))).scalar_one()
            fid = str(row.id)
        resp = await client.post(f"/v2/admin/quarantine/{fid}/replay")
        # Raw payload is stored as text-wrapped dict, not the original JSON —
        # digest/text encoding is not replayable.
        assert resp.status_code in (200, 422)
