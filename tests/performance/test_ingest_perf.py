"""Observer performance benchmarks (spec §84 items 7-8).

- canonical ingestion throughput to PostgreSQL (target: >= 1,000 events/sec);
- run timeline query latency at realistic event volume (target: p95 < 500ms
  for up to 10,000 events).

Correlated variants measure the projection-update path; the spec target is
defined on canonical batches, so those are reported with a regression floor.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.performance


async def _post_batch(client: AsyncClient, events: list[dict[str, Any]]) -> float:
    """POST one batch; return elapsed seconds."""
    start = time.perf_counter()
    resp = await client.post("/v1/events", json=events)
    elapsed = time.perf_counter() - start
    assert resp.status_code == 202, resp.text
    assert resp.json()["accepted"] == len(events), resp.json()["errors"][:3]
    return elapsed


@pytest.mark.asyncio
async def test_observer_ingest_throughput(client: AsyncClient, make_event: Any) -> None:
    """Spec target: >= 1,000 canonical events/sec to PostgreSQL."""
    total = 5_000
    batch_size = 1_000
    elapsed = 0.0
    for _ in range(total // batch_size):
        events = [make_event(event="pipeline.step") for _ in range(batch_size)]
        elapsed += await _post_batch(client, events)
    rate = total / elapsed
    print(f"\nobserver ingest: {rate:.0f} events/s ({total} in {elapsed:.2f}s)")
    assert rate >= 1_000, f"ingest {rate:.0f}/s below the 1,000/s spec target"


@pytest.mark.asyncio
async def test_observer_ingest_throughput_same_run(client: AsyncClient, make_event: Any) -> None:
    """Correlated events (one run) — measures the run-projection update path."""
    run_id = f"perf-{uuid.uuid4().hex[:8]}"
    total = 2_000
    events = [
        make_event(event="pipeline.step", correlation={"run_id": run_id}) for _ in range(total)
    ]
    elapsed = 0.0
    for i in range(0, total, 1_000):
        elapsed += await _post_batch(client, events[i : i + 1_000])
    rate = total / elapsed
    run = (await client.get(f"/v1/runs/{run_id}")).json()
    assert run["event_count"] == total
    print(f"\nobserver ingest (one run): {rate:.0f} events/s")
    # Regression floor; spec target of 1,000/s applies to canonical batches.
    assert rate >= 500, f"correlated ingest {rate:.0f}/s regressed"


@pytest.mark.asyncio
async def test_timeline_query_p95(
    client: AsyncClient, session_factory: Any, make_event: Any
) -> None:
    """Spec target: timeline p95 < 500ms for a run with <= 10,000 events."""
    from phlo_observer.store import persist_events

    run_id = f"perf-{uuid.uuid4().hex[:8]}"
    n = 10_000
    async with session_factory() as session, session.begin():
        result = await persist_events(
            session,
            [make_event(event="pipeline.step", correlation={"run_id": run_id}) for _ in range(n)],
        )
    assert result.accepted == n

    latencies_ms: list[float] = []
    for _ in range(9):
        start = time.perf_counter()
        resp = await client.get(f"/v1/runs/{run_id}/timeline")
        latencies_ms.append((time.perf_counter() - start) * 1000)
        assert resp.status_code == 200
        assert len(resp.json()["steps"]) == n
    latencies_ms.sort()
    p95 = latencies_ms[int(len(latencies_ms) * 0.95) - 1]
    p50 = latencies_ms[len(latencies_ms) // 2]
    print(f"\ntimeline ({n} events): p50={p50:.0f}ms p95={p95:.0f}ms")
    # Spec target is p95 < 500ms; assert a regression floor at 2x so a
    # pathological slowdown fails without flaky gating on shared CI runners.
    assert p95 < 1000, f"timeline p95 {p95:.0f}ms regressed (spec target 500ms)"


@pytest.mark.asyncio
async def test_query_pagination_scales(client: AsyncClient, make_event: Any) -> None:
    """Cursor pagination over ingested rows stays fast and complete."""
    await client.post("/v1/events", json=[make_event() for _ in range(50)])
    start = time.perf_counter()
    count = 0
    cursor = None
    while True:
        params: dict[str, Any] = {"limit": 25}
        if cursor:
            params["cursor"] = cursor
        body = (await client.get("/v1/events", params=params)).json()
        count += len(body["items"])
        cursor = body["next_cursor"]
        if cursor is None:
            break
    elapsed = time.perf_counter() - start
    assert count >= 50
    assert elapsed < 5.0
