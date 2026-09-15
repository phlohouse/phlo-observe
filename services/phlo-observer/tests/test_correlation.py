"""Correlation validation (hardening item 6).

Telemetry is associated with runs/assets by declared evidence only:
explicit run_id, trace_id reuse, or producer invocation markers — never
by proximity guessing. Weak evidence stays weak; nothing is manufactured.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import workloads as wl
from httpx import AsyncClient
from phlo_observer.models import Event, Relationship
from sqlalchemy import select

pytestmark = pytest.mark.asyncio


def _corr_event(
    run_id: str | None = None, trace_id: str | None = None, **kw: Any
) -> dict[str, Any]:
    corr: dict[str, Any] = {}
    if run_id:
        corr["run_id"] = run_id
    if trace_id:
        corr["trace_id"] = trace_id
    corr.update(kw)
    return wl._event("pipeline.step", wl.T0, correlation=corr)


async def _stored(session_factory: Any, event_id: str) -> Event | None:
    async with session_factory() as session:
        return await session.get(Event, uuid.UUID(event_id))


async def test_trace_only_event_joins_owning_run(client: AsyncClient, session_factory: Any) -> None:
    """An event with trace_id but no run_id adopts the trace owner's run."""
    run_id = f"trace-run-{uuid.uuid4().hex[:8]}"
    owner = _corr_event(run_id=run_id, trace_id="tr-shared")
    orphan = _corr_event(trace_id="tr-shared")
    await client.post("/v1/events", json=[owner, orphan])
    row = await _stored(session_factory, orphan["event_id"])
    assert row is not None
    assert row.run_id == run_id
    assert row.correlation_method == "trace_id"


async def test_trace_to_nowhere_stays_uncorrelated(
    client: AsyncClient, session_factory: Any
) -> None:
    """A trace_id no run has claimed must not fabricate a run link."""
    ev = _corr_event(trace_id=f"tr-{uuid.uuid4().hex[:8]}")
    await client.post("/v1/events", json=[ev])
    row = await _stored(session_factory, ev["event_id"])
    assert row is not None
    assert row.run_id is None


async def test_invocation_id_records_method_without_guessing(
    client: AsyncClient, session_factory: Any
) -> None:
    """producer_invocation is recorded but no run_id is invented."""
    ev = _corr_event(invocation_id=f"inv-{uuid.uuid4().hex[:8]}")
    await client.post("/v1/events", json=[ev])
    row = await _stored(session_factory, ev["event_id"])
    assert row is not None
    assert row.correlation_method == "producer_invocation"
    assert row.run_id is None


async def test_explicit_run_id_wins_over_trace(client: AsyncClient, session_factory: Any) -> None:
    """Declared run_id outranks a trace_id pointing at a different run."""
    owner = _corr_event(run_id="real-run", trace_id="tr-x")
    ev = _corr_event(run_id="declared-run", trace_id="tr-x")
    await client.post("/v1/events", json=[owner, ev])
    row = await _stored(session_factory, ev["event_id"])
    assert row is not None
    assert row.run_id == "declared-run"
    assert row.correlation_method == "explicit_run_id"


async def test_ambiguous_trace_first_owner_wins(client: AsyncClient, session_factory: Any) -> None:
    """Two runs claiming one trace: the orphan binds deterministically."""
    a = _corr_event(run_id="run-a", trace_id="tr-dupe")
    b = _corr_event(run_id="run-b", trace_id="tr-dupe")
    orphan = _corr_event(trace_id="tr-dupe")
    await client.post("/v1/events", json=[a, b, orphan])
    row = await _stored(session_factory, orphan["event_id"])
    assert row is not None
    # Deterministic by run_id ordering — the answer must not depend on plan.
    assert row.run_id == "run-a"


async def test_ambiguous_trace_stable_across_batches(
    client: AsyncClient, session_factory: Any
) -> None:
    """A later orphan of a multi-run trace resolves to the same owner."""
    trace = f"tr-{uuid.uuid4().hex[:8]}"
    b = _corr_event(run_id="run-b", trace_id=trace)
    a = _corr_event(run_id="run-a", trace_id=trace)
    await client.post("/v1/events", json=[b, a])  # insert order != id order
    orphan = _corr_event(trace_id=trace)
    await client.post("/v1/events", json=[orphan])
    row = await _stored(session_factory, orphan["event_id"])
    assert row is not None
    assert row.run_id == "run-a"


async def test_edges_only_from_declared_entities(client: AsyncClient, session_factory: Any) -> None:
    """Bare telemetry (service only, no run/entities) produces no edges."""
    ev = wl._event("pipeline.step", wl.T0)
    await client.post("/v1/events", json=[ev])
    async with session_factory() as session:
        edges = list((await session.execute(select(Relationship))).scalars())
    assert edges == []


async def test_run_id_correlation_yields_service_edge(
    client: AsyncClient, session_factory: Any
) -> None:
    """run_id in correlation + declared service -> honest explicit edge."""
    run_id = f"edge-{uuid.uuid4().hex[:8]}"
    ev = _corr_event(run_id=run_id)
    await client.post("/v1/events", json=[ev])
    async with session_factory() as session:
        edges = list((await session.execute(select(Relationship))).scalars())
    assert len(edges) == 1
    assert edges[0].relationship_type == "executes"
    assert edges[0].to_entity == f"run://phlo/{run_id}"
    assert edges[0].confidence == 1.0
    assert edges[0].method == "explicit"


async def test_run_asset_partition_association(client: AsyncClient, session_factory: Any) -> None:
    """Partitioned materialization links run->asset with the partition tag."""
    events = wl.dagster_run(
        f"part-{uuid.uuid4().hex[:8]}",
        wl.T0,
        assets=["analytics.orders"],
        partitions=["2026-01-05"],
        steps=1,
    )
    await client.post("/v1/events", json=events)
    async with session_factory() as session:
        edges = list((await session.execute(select(Relationship))).scalars())
        events_stored = list(
            (
                await session.execute(select(Event).where(Event.partition_key == "2026-01-05"))
            ).scalars()
        )
    assert edges, "expected at least one declared edge"
    assert all(e.confidence == 1.0 for e in edges)
    assert events_stored, "partition_key must land on the stored row"
