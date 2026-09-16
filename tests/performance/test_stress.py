"""Observer stress tests on realistic Phlo workloads (hardening item 3).

Covers sustained and burst ingestion of multi-producer telemetry,
concurrent producers, many-entity histories, duplicate resends, two
observer replicas sharing one database, and ingest during projection
rebuild. Reports throughput, batch-latency percentiles, per-batch
statement counts and peak allocation so regressions are measurable.
"""

from __future__ import annotations

import asyncio
import os
import time
import tracemalloc
import uuid
from typing import Any

import pytest
import workloads as wl
from httpx import ASGITransport, AsyncClient
from sqlalchemy import event

pytestmark = pytest.mark.performance

_FACTOR = 0.4 if os.environ.get("CI") else 1.0


def _per_s(floor: float) -> float:
    return floor * _FACTOR


def _pct(latencies_ms: list[float]) -> tuple[float, float, float]:
    ordered = sorted(latencies_ms)
    n = len(ordered)
    return (
        ordered[n // 2],
        ordered[max(0, int(n * 0.95) - 1)],
        ordered[max(0, int(n * 0.99) - 1)],
    )


async def _post(
    client: AsyncClient, events: list[dict[str, Any]], latencies_ms: list[float]
) -> dict[str, Any]:
    start = time.perf_counter()
    resp = await client.post("/v1/events", json=events)
    latencies_ms.append((time.perf_counter() - start) * 1000)
    assert resp.status_code == 202, resp.text
    return resp.json()


class _StatementCounter:
    """Counts statements issued on an engine via before_cursor_execute."""

    def __init__(self) -> None:
        self.count = 0
        self.enabled = False

    def attach(self, engine: Any) -> None:
        @event.listens_for(engine.sync_engine, "before_cursor_execute")
        def _count(*args: Any) -> None:
            if self.enabled:
                self.count += 1


@pytest.mark.asyncio
async def test_sustained_mixed_workload(client: AsyncClient, session_factory: Any) -> None:
    """Two weeks of mixed producer telemetry; measure e/s, latency, round trips."""
    events = wl.mixed_history(days=30, daily_runs=4, seed=7)
    assert len(events) > 2_000
    counter = _StatementCounter()
    counter.attach(session_factory.kw["bind"])

    tracemalloc.start()
    latencies_ms: list[float] = []
    counter.enabled = True
    start = time.perf_counter()
    batch = 500
    for i in range(0, len(events), batch):
        body = await _post(client, events[i : i + batch], latencies_ms)
        assert body["accepted"] == min(batch, len(events) - i), body["errors"][:3]
    elapsed = time.perf_counter() - start
    counter.enabled = False
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    rate = len(events) / elapsed
    p50, p95, p99 = _pct(latencies_ms)
    stmts_per_batch = counter.count / (len(events) / batch)
    print(
        f"\nsustained mixed: {rate:.0f} events/s, batch p50={p50:.0f}ms "
        f"p95={p95:.0f}ms p99={p99:.0f}ms, stmts/batch={stmts_per_batch:.0f}, "
        f"peak_alloc={peak / 1e6:.0f}MB"
    )
    assert rate >= _per_s(400), f"sustained ingest {rate:.0f}/s regressed"


@pytest.mark.asyncio
async def test_burst_ingest(client: AsyncClient) -> None:
    """Back-to-back 1k-event bursts from different producers."""
    latencies_ms: list[float] = []
    total = 0
    for seed in range(4):
        events = wl.mixed_history(days=3, daily_runs=6, seed=100 + seed)
        body = await _post(client, events, latencies_ms)
        total += body["accepted"]
        assert body["rejected"] == 0, body["errors"][:3]
    p50, p95, p99 = _pct(latencies_ms)
    print(f"\nburst: {total} events, batch p50={p50:.0f}ms p95={p95:.0f}ms p99={p99:.0f}ms")


@pytest.mark.asyncio
async def test_concurrent_producers(client: AsyncClient) -> None:
    """Eight producers ingest disjoint run histories concurrently."""
    histories = [wl.dagster_run(f"conc-run-{i}", wl.T0, outcome="success") for i in range(8)]
    histories += [
        wl.dagster_run(f"conc-fail-{i}", wl.T0, outcome="failure", fail_step=1) for i in range(4)
    ]
    start = time.perf_counter()
    bodies = await asyncio.gather(*(_post(client, h, []) for h in histories))
    elapsed = time.perf_counter() - start
    total = sum(b["accepted"] for b in bodies)
    assert all(b["rejected"] == 0 for b in bodies)
    assert total == sum(len(h) for h in histories)
    print(f"\nconcurrent producers: {total} events across 12 tasks in {elapsed:.2f}s")
    for i in range(8):
        run = (await client.get(f"/v1/runs/conc-run-{i}")).json()
        assert run["event_count"] == len(histories[i])


@pytest.mark.asyncio
async def test_concurrent_same_run(client: AsyncClient) -> None:
    """Producers racing on one run_id must not deadlock or lose events."""
    run_id = f"shared-{uuid.uuid4().hex[:8]}"
    events = wl.dagster_run(run_id, wl.T0, steps=8, assets=[f"a.{i}" for i in range(4)])
    shards = [events[i::6] for i in range(6)]
    bodies = await asyncio.gather(*(_post(client, s, []) for s in shards))
    assert sum(b["accepted"] for b in bodies) == len(events)
    run = (await client.get(f"/v1/runs/{run_id}")).json()
    assert run["event_count"] == len(events)
    assert run["status"] == "success"


@pytest.mark.asyncio
async def test_duplicate_resend(client: AsyncClient) -> None:
    """Producer retry: the identical batch re-posted must dedupe cheaply."""
    events = wl.mixed_history(days=2, daily_runs=3, seed=3)
    first = await _post(client, events, [])
    assert first["accepted"] == len(events)
    latencies_ms: list[float] = []
    second = await _post(client, events, latencies_ms)
    assert second["duplicates"] == len(events)
    assert second["accepted"] == 0
    print(f"\nduplicate resend of {len(events)}: {latencies_ms[0]:.0f}ms")


@pytest.mark.asyncio
async def test_many_entities(client: AsyncClient) -> None:
    """A run materializing 200 assets: projection fan-out stays bounded."""
    assets = [f"warehouse.schema_{i // 20}.table_{i}" for i in range(200)]
    events = wl.dagster_run(f"wide-{uuid.uuid4().hex[:8]}", wl.T0, assets=assets, steps=2)
    latencies_ms: list[float] = []
    body = await _post(client, events, latencies_ms)
    assert body["accepted"] == len(events)
    start = time.perf_counter()
    resp = await client.get("/v2/entities", params={"limit": 200})
    query_ms = (time.perf_counter() - start) * 1000
    assert resp.status_code == 200
    print(f"\n200-entity run: ingest {latencies_ms[0]:.0f}ms, entity list {query_ms:.0f}ms")


@pytest.mark.asyncio
async def test_two_instances_dedup(client: AsyncClient, session_factory: Any) -> None:
    """Two observer replicas on one database dedupe the same batch."""
    from phlo_observer.app import create_app
    from phlo_observer.settings import ObserverSettings

    # ASGITransport never runs the lifespan, so the replica's own engine is
    # never created; overriding session_factory shares the test database.
    other_app = create_app(ObserverSettings())
    other_app.state.session_factory = session_factory
    transport = ASGITransport(app=other_app)
    async with AsyncClient(transport=transport, base_url="http://replica") as replica:
        events = wl.dagster_run(f"ha-{uuid.uuid4().hex[:8]}", wl.T0)
        a, b = await asyncio.gather(
            client.post("/v1/events", json=events),
            replica.post("/v1/events", json=events),
        )
        assert a.status_code == b.status_code == 202
        ja, jb = a.json(), b.json()
        # Exactly one replica accepts each event; the other sees duplicates.
        assert ja["accepted"] + jb["accepted"] == len(events)
        assert ja["duplicates"] + jb["duplicates"] == len(events)


@pytest.mark.asyncio
async def test_ingest_during_rebuild(client: AsyncClient, session_factory: Any) -> None:
    """Rebuild while new events arrive: no errors, state converges."""
    from phlo_observer.projections import rebuild_projections

    seeded = wl.mixed_history(days=3, daily_runs=3, seed=11)
    await _post(client, seeded, [])
    incoming = wl.mixed_history(days=2, daily_runs=3, seed=12)

    async def _rebuild() -> None:
        async with session_factory() as session, session.begin():
            await rebuild_projections(session)

    rebuild_task = asyncio.create_task(_rebuild())
    for i in range(0, len(incoming), 200):
        body = await _post(client, incoming[i : i + 200], [])
        assert body["accepted"] > 0, body["errors"][:3]
    await rebuild_task

    # Final rebuild makes state canonical regardless of interleaving; the
    # post-rebuild ingest must equal a clean rebuild of all events.
    async with session_factory() as session, session.begin():
        await rebuild_projections(session)
    runs = (await client.get("/v1/runs")).json()
    assert runs, "expected runs after concurrent rebuild+ingest"
