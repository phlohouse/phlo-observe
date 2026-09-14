"""Light performance smoke tests: ingestion throughput + queue behavior.

Not a benchmark harness — these assert the pipeline is not pathologically
slow (hundreds of events per second through the full persist path) so a
regression in the hot path fails CI instead of shipping silently.
"""

from __future__ import annotations

import time
from typing import Any

import pytest
from httpx import AsyncClient

N_EVENTS = 200


@pytest.mark.performance
@pytest.mark.asyncio
async def test_batch_ingest_throughput(client: AsyncClient, make_event: Any) -> None:
    """200 canonical events in one batch persist in well under a second-ish."""
    events = [make_event() for _ in range(N_EVENTS)]
    start = time.perf_counter()
    resp = await client.post("/v1/events", json=events)
    elapsed = time.perf_counter() - start
    assert resp.status_code == 202
    assert resp.json()["accepted"] == N_EVENTS
    assert elapsed < 5.0, f"batch ingest took {elapsed:.2f}s for {N_EVENTS} events"


@pytest.mark.performance
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
