"""Projection equivalence: incremental state must equal rebuild state.

Canonical events are the source of truth. For every derived table —
runs, entities, relationships, assets, baselines, insights, incidents —
the state produced by incremental ingest must match the state produced
by ``rebuild_projections`` replaying the same events in observed order.

Volatile fields (uuid primary keys, wall-clock timestamps, dedupe order)
are normalized before comparison; semantic content must match exactly.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest
import workloads as wl
from httpx import AsyncClient
from phlo_observer.models import (
    Asset,
    Baseline,
    Entity,
    Incident,
    Insight,
    Relationship,
    Run,
)
from phlo_observer.projections import rebuild_projections
from sqlalchemy import select


def _prov(prov: Any) -> dict[str, Any]:
    prov = prov or {}
    return {
        "rule": prov.get("rule"),
        "rule_version": prov.get("rule_version"),
        "derived_from": sorted(prov.get("derived_from") or []),
    }


async def snapshot_state(session: Any) -> dict[str, Any]:
    """Canonical dump of every derived table, volatile fields removed."""
    snap: dict[str, Any] = {}

    snap["runs"] = {
        r.run_id: {
            "status": r.status,
            "job_name": r.job_name,
            "service_name": r.service_name,
            "environment": r.environment,
            "started_at": r.started_at,
            "ended_at": r.ended_at,
            "duration_ms": r.duration_ms,
            "trigger": r.trigger,
            "event_count": r.event_count,
            "error_count": r.error_count,
            "warning_count": r.warning_count,
            "branch": r.branch,
            "asset_count": r.asset_count,
            "asset_keys": sorted((r.summary or {}).get("asset_keys") or []),
            "provenance": _prov(r.provenance),
            # Fold bookkeeping is deterministic event-position data: two
            # converged states must carry identical keys or the NEXT late
            # event would fold them apart.
            "fold_state": r.fold_state or {},
        }
        for r in (await session.execute(select(Run))).scalars()
    }

    snap["entities"] = {
        e.entity_id: {
            "kind": e.kind,
            "display_name": e.display_name,
            "first_seen_at": e.first_seen_at,
            "last_seen_at": e.last_seen_at,
            "attributes": e.attributes or {},
            "provenance": _prov(e.provenance),
            "fold_state": e.fold_state or {},
        }
        for e in (await session.execute(select(Entity))).scalars()
    }

    snap["relationships"] = {
        (r.from_entity, r.to_entity, r.relationship_type): {
            "method": r.method,
            "confidence": r.confidence,
            "source_event_ids": sorted(r.source_event_ids or []),
            "fold_state": r.fold_state or {},
        }
        for r in (await session.execute(select(Relationship))).scalars()
    }

    snap["assets"] = {
        a.entity_id: {
            "asset_key": a.asset_key,
            "status": a.status,
            "last_materialized_at": a.last_materialized_at,
            "last_event_at": a.last_event_at,
            "freshness_sla_seconds": a.freshness_sla_seconds,
            "attributes": a.attributes or {},
            "provenance": _prov(a.provenance),
            "fold_state": a.fold_state or {},
        }
        for a in (await session.execute(select(Asset))).scalars()
    }

    snap["baselines"] = {
        (b.entity_id, b.metric): {
            "count": b.count,
            "median": b.median,
            "mad": b.mad,
            "mean": b.mean,
            "p10": b.p10,
            "p90": b.p90,
            "samples": sorted(b.samples or []),
        }
        for b in (await session.execute(select(Baseline))).scalars()
    }

    def _pos_key(i: Any, name: str) -> Any:
        raw = (i.attributes or {}).get(name)
        return tuple(raw) if isinstance(raw, list) else raw

    snap["insights"] = {
        # A dedupe key owns a *chain* of rows partitioned at resolver
        # positions, so identity is (dedupe, open_from, resolved): all
        # event positions, stable across incremental and rebuild.
        (
            i.dedupe_key or str(i.insight_id),
            _pos_key(i, "open_from_key"),
            _pos_key(i, "resolved_key"),
        ): {
            "rule_id": i.rule_id,
            "rule_version": i.rule_version,
            "title": i.title,
            "severity": i.severity,
            "state": i.state,
            "entity_id": i.entity_id,
            "evidence": sorted(str(e) for e in i.evidence_event_ids or []),
            "attributes": i.attributes or {},
        }
        for i in (await session.execute(select(Insight))).scalars()
    }

    # Incident insight_ids hold uuids; map them onto insight dedupe keys so
    # incidents compare semantically across runs of the same scenario. The
    # member records inside attributes carry the same volatile uuids.
    insight_key_by_id = {
        str(i.insight_id): i.dedupe_key or str(i.insight_id)
        for i in (await session.execute(select(Insight))).scalars()
    }

    def _incident_attrs(c: Any) -> dict[str, Any]:
        attrs = dict(c.attributes or {})
        members = attrs.get("members")
        if isinstance(members, list):
            attrs["members"] = [
                {**m, "iid": insight_key_by_id.get(str(m.get("iid")), m.get("iid"))}
                for m in members
                if isinstance(m, dict)
            ]
        return attrs

    snap["incidents"] = {
        # Entity-set alone is not unique: two incidents can legitimately
        # share it, and keying on it alone would silently merge them.
        (tuple(sorted(c.entities or [])), c.title, c.state): {
            "state": c.state,
            "severity": c.severity,
            "title": c.title,
            "insight_keys": sorted(insight_key_by_id.get(i, i) for i in c.insight_ids or []),
            "entities": sorted(c.entities or []),
            "attributes": _incident_attrs(c),
        }
        for c in (await session.execute(select(Incident))).scalars()
    }
    return snap


def diff_snapshots(a: dict[str, Any], b: dict[str, Any]) -> str:
    """Human-readable description of the first divergences found."""
    out: list[str] = []
    for table, value in a.items():
        other = b.get(table) or {}
        if value == other:
            continue
        ak, bk = set(value), set(other)
        out.extend(f"{table}: only-in-incremental {k}" for k in sorted(ak - bk, key=str))
        out.extend(f"{table}: only-in-rebuild {k}" for k in sorted(bk - ak, key=str))
        out.extend(
            f"{table}: {k}: {value[k]} != {other[k]}"
            for k in sorted(ak & bk, key=str)
            if value[k] != other[k]
        )
    return "\n".join(out[:40])


async def _snapshot(client_session_factory: Any) -> dict[str, Any]:
    async with client_session_factory() as session:
        return await snapshot_state(session)


async def _ingest_and_rebuild(
    client: AsyncClient,
    session_factory: Any,
    events: list[dict[str, Any]],
    batch: int = 200,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Ingest arrival-order events, snapshot, rebuild, snapshot again."""
    counts = await wl.post_all(client, events, batch=batch)
    assert counts["rejected"] == 0
    before = await _snapshot(session_factory)
    async with session_factory() as session, session.begin():
        await rebuild_projections(session)
    after = await _snapshot(session_factory)
    return before, after


@pytest.mark.asyncio
class TestEquivalence:
    async def test_mixed_history_in_order(self, client: AsyncClient, session_factory: Any) -> None:
        events = wl.mixed_history(seed=7, days=4, daily_runs=3)
        before, after = await _ingest_and_rebuild(client, session_factory, events)
        diff = diff_snapshots(before, after)
        assert before == after, f"in-order divergence:\n{diff}"

    async def test_out_of_order_events(self, client: AsyncClient, session_factory: Any) -> None:
        events = wl.inject_out_of_order(wl.mixed_history(seed=11, days=3), seed=1)
        before, after = await _ingest_and_rebuild(client, session_factory, events)
        diff = diff_snapshots(before, after)
        assert before == after, f"out-of-order divergence:\n{diff}"

    async def test_late_events(self, client: AsyncClient, session_factory: Any) -> None:
        events = wl.inject_late(
            wl.mixed_history(seed=13, days=3, daily_runs=2), seed=2, fraction=0.1
        )
        before, after = await _ingest_and_rebuild(client, session_factory, events)
        diff = diff_snapshots(before, after)
        assert before == after, f"late-event divergence:\n{diff}"

    async def test_duplicate_events(self, client: AsyncClient, session_factory: Any) -> None:
        events = wl.inject_duplicates(
            wl.mixed_history(seed=17, days=2, daily_runs=2), seed=3, fraction=0.15
        )
        counts = await wl.post_all(client, events)
        before = await _snapshot(session_factory)
        assert counts["accepted"] + counts["rejected"] <= len(events)
        async with session_factory() as session, session.begin():
            await rebuild_projections(session)
        after = await _snapshot(session_factory)
        diff = diff_snapshots(before, after)
        assert before == after, f"duplicate-event divergence:\n{diff}"

    async def test_large_history_many_entities(
        self, client: AsyncClient, session_factory: Any
    ) -> None:
        """Wider topology: many runs, branches, partitions, tables."""
        rng_events: list[dict[str, Any]] = []
        import datetime as dt

        for i in range(12):
            t = wl.T0 + dt.timedelta(hours=i * 3)
            rng_events += wl.dagster_run(
                f"run-{i}",
                t,
                assets=[f"analytics.asset_{i % 4}", f"analytics.wide_{i}"],
                outcome="failure" if i % 5 == 0 else "success",
            )
            rng_events += wl.wap_branch_lifecycle(f"wap-{i}", f"b/{i}", t + dt.timedelta(hours=1))
        rng_events.sort(key=lambda e: (e["observed_at"], e["event_id"]))
        before, after = await _ingest_and_rebuild(client, session_factory, rng_events)
        diff = diff_snapshots(before, after)
        assert before == after, f"wide-topology divergence:\n{diff}"

    async def test_scoped_rebuild_preserves_shared_asset(
        self, client: AsyncClient, session_factory: Any
    ) -> None:
        """Regression: ``--run`` rebuild must not regress assets other runs touched.

        Run A materializes a shared asset early; run B materializes it later.
        Rebuilding run A alone previously deleted the asset row and rewrote it
        from A's events only — wiping B's contribution. The asset must be
        re-derived from its full event history.
        """
        shared = "analytics.shared"
        events = wl.dagster_run("run-early", wl.T0, assets=[shared], steps=1, duration_ms=60_000)
        events += wl.dagster_run(
            "run-late",
            wl.T0 + dt.timedelta(hours=6),
            assets=[shared],
            steps=1,
            duration_ms=60_000,
        )
        counts = await wl.post_all(client, events)
        assert counts["rejected"] == 0

        async with session_factory() as session, session.begin():
            await rebuild_projections(session, run_id="run-early")

        async with session_factory() as session:
            asset = await session.get(Asset, f"asset://{shared}")
            assert asset is not None
            late_events = [e for e in events if e["correlation"].get("asset_key") == shared]
            late_materialize = max(
                dt.datetime.fromisoformat(e["observed_at"].replace("Z", "+00:00"))
                for e in late_events
                if e["event"] == "asset.materialize" and e["outcome"] == "success"
            )
            assert asset.last_materialized_at == late_materialize
            # The other run's row is untouched.
            other = await session.get(Run, "run-late")
            assert other is not None
            assert other.status == "success"

    async def test_scoped_rebuild_merges_shared_edges(
        self, client: AsyncClient, session_factory: Any
    ) -> None:
        """Two runs producing the same edge keep both source event ids."""
        shared = "analytics.edge_shared"
        events = wl.dagster_run("run-e1", wl.T0, assets=[shared], steps=1)
        events += wl.dagster_run("run-e2", wl.T0 + dt.timedelta(hours=1), assets=[shared], steps=1)
        counts = await wl.post_all(client, events)
        assert counts["rejected"] == 0

        async with session_factory() as session:
            before = {
                (r.from_entity, r.to_entity, r.relationship_type): sorted(r.source_event_ids or [])
                for r in (await session.execute(select(Relationship))).scalars()
            }
        async with session_factory() as session, session.begin():
            await rebuild_projections(session, run_id="run-e1")
        async with session_factory() as session:
            after = {
                (r.from_entity, r.to_entity, r.relationship_type): sorted(r.source_event_ids or [])
                for r in (await session.execute(select(Relationship))).scalars()
            }
        # No edge lost its provenance: every key survives with at least the
        # source ids it had before the scoped rebuild.
        for key, sources in before.items():
            assert key in after, f"edge {key} lost by scoped rebuild"
            assert set(sources) <= set(after[key]), f"edge {key} lost sources"
