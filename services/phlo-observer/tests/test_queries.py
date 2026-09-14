"""Query API: filters, cursor pagination, runs, timeline, late arrivals."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from httpx import AsyncClient


async def _post(client: AsyncClient, event: dict[str, Any]) -> dict[str, Any]:
    resp = await client.post("/v1/events", json=event)
    assert resp.status_code == 202, resp.text
    return resp.json()


@pytest.mark.asyncio
async def test_filter_by_run_id(client: AsyncClient, make_event: Any) -> None:
    run = f"run-{uuid.uuid4().hex[:8]}"
    await _post(client, make_event(correlation={"run_id": run}))
    await _post(client, make_event(event="asset.materialize", correlation={"run_id": "other"}))
    resp = await client.get("/v1/events", params={"run_id": run})
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0]["correlation"]["run_id"] == run


@pytest.mark.asyncio
async def test_filter_by_outcome_and_event(client: AsyncClient, make_event: Any) -> None:
    await _post(client, make_event(outcome="failure", event="quality.check"))
    await _post(client, make_event(outcome="success"))
    resp = await client.get("/v1/events", params={"outcome": "failure", "event": "quality.check"})
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0]["outcome"] == "failure"


@pytest.mark.asyncio
async def test_cursor_pagination(client: AsyncClient, make_event: Any) -> None:
    for i in range(5):
        await _post(
            client,
            make_event(observed_at=f"2025-01-01T00:00:0{i}Z"),
        )
    seen: list[str] = []
    cursor = None
    while True:
        params: dict[str, Any] = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        resp = await client.get("/v1/events", params=params)
        body = resp.json()
        seen.extend(item["event_id"] for item in body["items"])
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert len(seen) == 5
    assert len(set(seen)) == 5  # no page overlap


@pytest.mark.asyncio
async def test_get_event_by_id(client: AsyncClient, make_event: Any) -> None:
    event = make_event()
    await _post(client, event)
    resp = await client.get(f"/v1/events/{event['event_id']}")
    assert resp.status_code == 200
    assert resp.json()["event_id"] == event["event_id"]


@pytest.mark.asyncio
async def test_get_event_404(client: AsyncClient) -> None:
    resp = await client.get(f"/v1/events/{uuid.uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_run_projection_and_timeline(client: AsyncClient, make_event: Any) -> None:
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    await _post(
        client,
        make_event(
            event="pipeline.run",
            outcome="unknown",
            observed_at="2025-01-01T00:00:00Z",
            correlation={"run_id": run_id},
            attributes={"trigger": "schedule"},
        ),
    )
    await _post(
        client,
        make_event(
            event="asset.materialize",
            category="data",
            observed_at="2025-01-01T00:00:05Z",
            correlation={"run_id": run_id, "asset_key": "dbt.stg_orders"},
        ),
    )
    await _post(
        client,
        make_event(
            event="pipeline.run",
            outcome="success",
            observed_at="2025-01-01T00:00:10Z",
            ended_at="2025-01-01T00:00:10Z",
            duration_ms=10000,
            correlation={"run_id": run_id},
        ),
    )
    run = (await client.get(f"/v1/runs/{run_id}")).json()
    assert run["status"] == "success"
    assert run["event_count"] == 3
    assert run["asset_count"] == 1
    assert run["trigger"] == "schedule"

    timeline = (await client.get(f"/v1/runs/{run_id}/timeline")).json()
    assert timeline["run"]["run_id"] == run_id
    assert len(timeline["events"]) == 3
    assert "pipeline" in timeline["phases"]
    assert "asset" in timeline["phases"]
    # deterministic ordering by observed_at
    times = [e["observed_at"] for e in timeline["events"]]
    assert times == sorted(times)


@pytest.mark.asyncio
async def test_late_arriving_event_updates_run(client: AsyncClient, make_event: Any) -> None:
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    await _post(
        client,
        make_event(
            event="pipeline.run",
            outcome="success",
            correlation={"run_id": run_id},
        ),
    )
    # late event for the same run arrives after the terminal signal
    await _post(
        client,
        make_event(
            event="quality.check",
            outcome="failure",
            severity="error",
            correlation={"run_id": run_id, "asset_key": "a.b"},
            error={"exception_type": "CheckError", "message": "null rate exceeded"},
        ),
    )
    run = (await client.get(f"/v1/runs/{run_id}")).json()
    assert run["status"] == "success"  # terminal status preserved
    assert run["event_count"] == 2
    assert run["error_count"] == 1


@pytest.mark.asyncio
async def test_uncorrelated_event_stays_uncorrelated(client: AsyncClient, make_event: Any) -> None:
    event_id = str(uuid.uuid4())
    await _post(client, make_event(event_id=event_id))
    stored = (await client.get(f"/v1/events/{event_id}")).json()
    assert stored["correlation"]["run_id"] is None
    assert stored["correlation_method"] is None


@pytest.mark.asyncio
async def test_trace_id_links_to_run(client: AsyncClient, make_event: Any) -> None:
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    trace = uuid.uuid4().hex
    await _post(
        client,
        make_event(correlation={"run_id": run_id, "trace_id": trace}),
    )
    await _post(
        client,
        make_event(event="asset.materialize", correlation={"trace_id": trace}),
    )
    resp = await client.get("/v1/events", params={"trace_id": trace})
    items = resp.json()["items"]
    assert len(items) == 2
    linked = next(i for i in items if i["event"] == "asset.materialize")
    assert linked["correlation"]["run_id"] == run_id
    assert linked["correlation_method"] == "trace_id"


@pytest.mark.asyncio
async def test_list_runs(client: AsyncClient, make_event: Any) -> None:
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    await _post(client, make_event(correlation={"run_id": run_id}, outcome="unknown"))
    resp = await client.get("/v1/runs", params={"status": "running"})
    ids = [r["run_id"] for r in resp.json()["items"]]
    assert run_id in ids


@pytest.mark.asyncio
async def test_run_404(client: AsyncClient) -> None:
    assert (await client.get("/v1/runs/nonexistent")).status_code == 404
    assert (await client.get("/v1/runs/nonexistent/timeline")).status_code == 404
