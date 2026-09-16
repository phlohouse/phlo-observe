"""Order-convergence regression tests (hardening review fixes).

Late and out-of-order arrivals previously diverged incremental state from
a rebuild replaying the same events in observed order — the exact class of
bug the "incremental == rebuild" invariant exists to forbid. Every scenario
here posts a hostile arrival order in *separate* batches (the divergence
lives across request boundaries, not within one sorted batch), snapshots
all derived state, rebuilds, and compares. Semantic assertions pin the
expected value; the snapshot comparison proves full-state convergence.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

import pytest
import workloads as wl
from httpx import AsyncClient
from phlo_observer.models import Asset, Baseline, Entity, Insight
from phlo_observer.projections import rebuild_projections
from sqlalchemy import select
from test_equivalence import diff_snapshots, snapshot_state

pytestmark = pytest.mark.asyncio

_UID = uuid.uuid4().hex[:6]


async def _post(client: AsyncClient, *batches: list[dict[str, Any]]) -> None:
    """Post each batch as its own request: arrival order across boundaries."""
    for batch in batches:
        resp = await client.post("/v1/events", json=batch)
        assert resp.status_code == 202, resp.text[:300]
        assert resp.json()["rejected"] == 0


async def _snap(factory: Any) -> dict[str, Any]:
    async with factory() as session:
        return await snapshot_state(session)


async def _converged(
    client: AsyncClient, session_factory: Any, *batches: list[dict[str, Any]]
) -> tuple[dict[str, Any], dict[str, Any]]:
    await _post(client, *batches)
    before = await _snap(session_factory)
    async with session_factory() as session, session.begin():
        await rebuild_projections(session)
    after = await _snap(session_factory)
    return before, after


async def test_late_failure_after_success_converges(
    client: AsyncClient, session_factory: Any
) -> None:
    """Review repro 1: failure@t1 arriving after success@t2 left the asset
    ``failing`` incrementally but ``recovering`` on rebuild — and the
    incremental answer was semantically wrong (the newest *observed*
    outcome was success)."""
    asset = f"analytics.ord_{_UID}"
    ok = wl._event(
        "asset.materialize",
        wl.T0 + dt.timedelta(hours=2),
        category="data",
        outcome="success",
        correlation={"asset_key": asset},
        entities={"asset": f"asset://{asset}"},
        attributes={"asset_key": asset},
    )
    fail = wl._event(
        "asset.materialize",
        wl.T0 + dt.timedelta(hours=1),
        category="data",
        outcome="failure",
        correlation={"asset_key": asset},
        entities={"asset": f"asset://{asset}"},
        attributes={"asset_key": asset},
        error={"exception_type": "MaterializeError", "message": "boom"},
    )
    before, after = await _converged(client, session_factory, [ok], [fail])
    diff = diff_snapshots(before, after)
    assert before == after, f"divergence:\n{diff}"
    async with session_factory() as session:
        row = await session.get(Asset, f"asset://{asset}")
    assert row is not None
    # Newest observed outcome is the success: failing-then-recovering, not
    # stuck at failing because the failure arrived second.
    assert row.status == "recovering"


async def test_late_branch_create_does_not_regress_state(
    client: AsyncClient, session_factory: Any
) -> None:
    """Review repro 2: a late ``wap.branch.create`` rewound the branch
    entity's ``state`` from ``promoted`` to ``open`` incrementally."""
    branch = f"wap/late_{_UID}"
    promote = wl._event(
        "wap.promote",
        wl.T0 + dt.timedelta(hours=2),
        category="storage",
        outcome="success",
        correlation={"branch": branch},
        entities={"branch": f"branch://nessie/{branch}"},
        attributes={"target": "main"},
    )
    create = wl._event(
        "wap.branch.create",
        wl.T0 + dt.timedelta(hours=1),
        category="storage",
        outcome="success",
        correlation={"branch": branch},
        entities={"branch": f"branch://nessie/{branch}"},
        attributes={"base_branch": "main"},
    )
    before, after = await _converged(client, session_factory, [promote], [create])
    diff = diff_snapshots(before, after)
    assert before == after, f"divergence:\n{diff}"
    async with session_factory() as session:
        row = await session.get(Entity, f"branch://nessie/{branch}")
    assert row is not None
    assert (row.attributes or {}).get("state") == "promoted"


async def test_late_metric_sample_inserts_in_place(
    client: AsyncClient, session_factory: Any
) -> None:
    """A metric event observed mid-history but arriving late must land in
    the baseline's observed-order position, not at the tail."""
    entity = f"asset://analytics.rows_{_UID}"
    values = [100.0, 102.0, 98.0, 101.0, 99.0]
    on_time = wl.metric_samples("asset", entity, "rows", values, wl.T0)
    late = wl.metric_samples("asset", entity, "rows", [10_000.0], wl.T0 + dt.timedelta(hours=3))
    before, after = await _converged(client, session_factory, on_time, late)
    diff = diff_snapshots(before, after)
    assert before == after, f"divergence:\n{diff}"
    async with session_factory() as session:
        row = (
            await session.execute(
                select(Baseline).where(Baseline.entity_id == entity, Baseline.metric == "rows")
            )
        ).scalar_one()
    samples = [s[2] for s in row.samples]
    assert samples.count(10_000.0) == 1
    assert row.count == len(values) + 1


async def test_resolver_folded_before_finding_converges(
    client: AsyncClient, session_factory: Any
) -> None:
    """The success (resolver) arrives on time; the failure finding arrives
    late. Replay resolves the insight at the resolver — incremental ingest
    must land the same resolved row, not a duplicate open insight."""
    asset = f"analytics.q_{_UID}"
    ent = f"asset://{asset}"
    fail = wl._event(
        "quality.check",
        wl.T0,
        category="quality",
        outcome="failure",
        entities={"asset": ent},
        error={"exception_type": "CheckError", "message": "nulls"},
    )
    ok = wl._event(
        "quality.check",
        wl.T0 + dt.timedelta(hours=1),
        category="quality",
        outcome="success",
        entities={"asset": ent},
    )
    # Resolver first, then the late finding.
    before, after = await _converged(client, session_factory, [ok], [fail])
    diff = diff_snapshots(before, after)
    assert before == after, f"divergence:\n{diff}"
    async with session_factory() as session:
        rows = list(
            (await session.execute(select(Insight).where(Insight.entity_id == ent))).scalars()
        )
    assert len(rows) == 1
    assert rows[0].state == "resolved"


async def test_late_finding_merges_into_resolved_interval(
    client: AsyncClient, session_factory: Any
) -> None:
    """Findings straddling a resolver converge to replay's chain: the
    dedupe key ends with one resolved row holding the pre-resolver finding
    and one open row holding the post-resolver one — no matter which order
    the three events arrive in."""
    asset = f"analytics.chain_{_UID}"
    ent = f"asset://{asset}"
    f1 = wl._event(
        "quality.check",
        wl.T0,
        category="quality",
        outcome="failure",
        entities={"asset": ent},
        error={"exception_type": "CheckError", "message": "nulls"},
    )
    s2 = wl._event(
        "quality.check",
        wl.T0 + dt.timedelta(hours=2),
        category="quality",
        outcome="success",
        entities={"asset": ent},
    )
    f3 = wl._event(
        "quality.check",
        wl.T0 + dt.timedelta(hours=4),
        category="quality",
        outcome="failure",
        entities={"asset": ent},
        error={"exception_type": "CheckError", "message": "nulls"},
    )
    # Hostile arrival: latest finding first, resolver second, earliest last.
    before, after = await _converged(client, session_factory, [f3], [s2], [f1])
    diff = diff_snapshots(before, after)
    assert before == after, f"divergence:\n{diff}"
    async with session_factory() as session:
        rows = list(
            (await session.execute(select(Insight).where(Insight.entity_id == ent))).scalars()
        )
    states = sorted(r.state for r in rows)
    assert states == ["open", "resolved"]


async def test_late_nonterminal_event_sets_run_started(
    client: AsyncClient, session_factory: Any
) -> None:
    """A run's first-observed event contributes ``observed_at`` to
    ``started_at`` when it carries no ``started_at`` — even when that event
    arrives after later-observed ones."""
    run_id = f"run-latestart-{_UID}"
    terminal = wl._event(
        "pipeline.run",
        wl.T0 + dt.timedelta(hours=2),
        outcome="success",
        correlation={"run_id": run_id, "job_id": "etl"},
        started_at=wl.T0 + dt.timedelta(hours=2),
        ended_at=wl.T0 + dt.timedelta(hours=2, minutes=30),
        duration_ms=1_800_000,
    )
    early = wl._event(
        "pipeline.step",
        wl.T0 + dt.timedelta(hours=1),
        outcome="success",
        correlation={"run_id": run_id, "job_id": "etl"},
    )
    before, after = await _converged(client, session_factory, [terminal], [early])
    diff = diff_snapshots(before, after)
    assert before == after, f"divergence:\n{diff}"
    started = before["runs"][run_id]["started_at"]
    assert started == wl.T0 + dt.timedelta(hours=1)


async def test_same_instant_resolvers_converge(client: AsyncClient, session_factory: Any) -> None:
    """Two successes observed at the finding's exact instant: whichever
    folds first resolves it, and a later-arriving smaller-key resolver
    repartitions the row — replay lands on the earliest resolver every
    time. Producers sharing the resolver's timestamp never leave the
    resolved row empty (the IndexError the stress suite found)."""
    asset = f"analytics.sameinst_{_UID}"
    ent = f"asset://{asset}"
    t = wl.T0
    s_lo = wl._event(
        "quality.check",
        t,
        category="quality",
        outcome="success",
        entities={"asset": ent},
        event_id="00000000-0000-0000-0000-000000000001",
    )
    fail = wl._event(
        "quality.check",
        t,
        category="quality",
        outcome="failure",
        entities={"asset": ent},
        error={"exception_type": "CheckError", "message": "nulls"},
        event_id="55555555-5555-5555-5555-555555555555",
    )
    s_hi = wl._event(
        "quality.check",
        t,
        category="quality",
        outcome="success",
        entities={"asset": ent},
        event_id="ffffffff-ffff-ffff-ffff-ffffffffffff",
    )
    # Hostile arrival: the later-keyed resolver first, then the finding,
    # then the earlier-keyed resolver — forcing a repartition where every
    # producer shares the resolver's timestamp.
    before, after = await _converged(client, session_factory, [s_hi], [fail], [s_lo])
    diff = diff_snapshots(before, after)
    assert before == after, f"divergence:\n{diff}"
    async with session_factory() as session:
        rows = list(
            (await session.execute(select(Insight).where(Insight.entity_id == ent))).scalars()
        )
    assert len(rows) == 1
    assert rows[0].state == "resolved"
    assert (rows[0].attributes or {}).get("resolved_by") == s_lo["event_id"]


async def test_small_batches_shuffle_converges(client: AsyncClient, session_factory: Any) -> None:
    """The scenario the old suite never produced: same-entity inversions
    split across request batches. Small pages force late events to fold
    after later-observed ones throughout the history."""
    events = wl.inject_out_of_order(wl.mixed_history(seed=23, days=2, daily_runs=2), seed=5)
    counts = await wl.post_all(client, events, batch=7)
    assert counts["rejected"] == 0
    before = await _snap(session_factory)
    async with session_factory() as session, session.begin():
        await rebuild_projections(session)
    after = await _snap(session_factory)
    diff = diff_snapshots(before, after)
    assert before == after, f"small-batch divergence:\n{diff}"


async def test_critical_insights_group_convergently_out_of_order(
    client: AsyncClient, session_factory: Any
) -> None:
    """Incident membership keys on event position: two critical run-failure
    insights on the same entity arrive out of order and group exactly as a
    replay groups them."""
    run_id = f"run-inc-{_UID}"
    run_ent = f"run://dagster/{run_id}"
    f1 = wl._event(
        "pipeline.run",
        wl.T0,
        outcome="failure",
        correlation={"run_id": run_id, "job_id": "etl"},
        entities={"run": run_ent},
        started_at=wl.T0,
        ended_at=wl.T0 + dt.timedelta(minutes=5),
        error={"exception_type": "RunFailure", "message": "failed"},
    )
    f2 = wl._event(
        "pipeline.run",
        wl.T0 + dt.timedelta(minutes=10),
        outcome="failure",
        correlation={"run_id": f"{run_id}-retry", "job_id": "etl"},
        entities={"run": run_ent},
        started_at=wl.T0 + dt.timedelta(minutes=8),
        ended_at=wl.T0 + dt.timedelta(minutes=10),
        error={"exception_type": "RunFailure", "message": "failed again"},
    )
    before, after = await _converged(client, session_factory, [f2], [f1])
    diff = diff_snapshots(before, after)
    assert before == after, f"divergence:\n{diff}"
