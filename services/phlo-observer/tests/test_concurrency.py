"""Concurrency regressions: lost updates, dedupe races, rebuild locking.

Two observer replicas (or two in-flight requests) folding events onto the
same projection row used to read it unlocked, merge in Python, and write
back — the second commit silently discarded the first. These tests drive
``persist_events`` on independent sessions with a deterministic interleave:
one transaction completes its projection pass uncommitted (holding its row
locks), the second runs concurrently, and the first commits while the
second waits. Under ``SELECT .. FOR UPDATE`` both writers' contributions
survive; under the old unlocked reads the second writer's view predated the
first commit and its write clobbered it.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
from typing import Any

import pytest
from httpx import AsyncClient
from phlo_observer import projections
from phlo_observer.models import Baseline, Entity, Event, Insight, Relationship, Run
from phlo_observer.projections import _PROJECTION_LOCK_KEY, rebuild_projections
from phlo_observer.store import persist_events
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from test_equivalence import diff_snapshots, snapshot_state


async def _persist(factory: Any, events: list[dict[str, Any]]) -> Any:
    async with factory() as session, session.begin():
        return await persist_events(session, events)


def _branch_event(make_event: Any, name: str, branch: str, run_id: str, **kw: Any) -> dict:
    """A WAP lifecycle event on one branch — folds attributes onto the
    ``branch://`` entity and a ``writes_to`` edge onto (run, branch)."""
    return make_event(
        event=name,
        outcome="success",
        service={"name": "wap-svc", "environment": "test"},
        correlation={"run_id": run_id, "branch": branch},
        **kw,
    )


def _metric_event(make_event: Any, asset: str, value: float) -> dict:
    return make_event(
        event="metric.recorded",
        service={"name": "metrics-svc", "environment": "test"},
        entities={"asset": asset},
        attributes={"metric": "row_count", "value": value},
    )


async def _interleaved_persist(
    session_factory: Any, first: dict[str, Any], second: dict[str, Any]
) -> None:
    """Persist ``first`` uncommitted, run ``second`` to its lock point, then
    commit in order.

    With row locking in place ``second`` parks on the rows ``first`` folded
    (still uncommitted); a bounded shielded wait proves it cannot complete
    before the commit releases it. Without locking, ``second`` finishes its
    reads against pre-commit state and its later commit clobbers ``first``'s
    merge — which is exactly the regression these tests guard.
    """
    async with session_factory() as s_first, s_first.begin():
        await persist_events(s_first, [first])
        async with session_factory() as s_second, s_second.begin():
            task = asyncio.create_task(persist_events(s_second, [second]))
            try:
                for _ in range(100):
                    await asyncio.sleep(0)
                    if task.done():
                        break
                if not task.done():
                    try:
                        await asyncio.wait_for(asyncio.shield(task), 0.5)
                    except TimeoutError:
                        pass
                    finally:
                        # Commit the first writer even when the second
                        # failed early — a propagating projection error is a
                        # fail-open bug this suite wants surfaced as itself,
                        # not masked behind an empty-table assertion.
                        if s_first.in_transaction():
                            await s_first.commit()
                else:
                    await s_first.commit()
                await task
                await s_second.commit()
            except BaseException:
                task.cancel()
                raise


@pytest.mark.asyncio
class TestLostUpdates:
    async def test_concurrent_entity_merges_keep_both_writers(
        self, session_factory: Any, make_event: Any
    ) -> None:
        # Seed the branch entity so both racers update the same committed row.
        # Distinct run_ids matter: a shared run row is already FOR-UPDATE
        # locked upstream, which would serialize the racers before the entity
        # fold and leave the entity lock itself unproven. Distinct
        # observed_at keeps the keyed per-field merge deterministic: the
        # rejection's `state` is the newest observed write of that field.
        await _persist(
            session_factory,
            [
                _branch_event(
                    make_event,
                    "wap.branch.create",
                    "main",
                    "r-seed",
                    observed_at="2025-01-01T00:00:00Z",
                )
            ],
        )
        validate = _branch_event(
            make_event, "wap.validate", "main", "r-a", observed_at="2025-01-02T00:00:00Z"
        )
        reject = _branch_event(
            make_event, "wap.reject", "main", "r-b", observed_at="2025-01-03T00:00:00Z"
        )
        await _interleaved_persist(session_factory, validate, reject)

        async with session_factory() as session:
            row = (
                await session.execute(
                    select(Entity).where(Entity.entity_id == "branch://wap-svc/main")
                )
            ).scalar_one()
            attrs = row.attributes or {}
            # Both writers' keyed attributes survived: validation verdict and
            # the rejection, plus the seed's creation fields.
            assert attrs.get("validation") == "success"
            assert attrs.get("state") == "rejected"
            assert attrs.get("rejected_at")
            assert attrs.get("created_at")
            derived = set((row.provenance or {}).get("derived_from") or [])
            assert {str(validate["event_id"]), str(reject["event_id"])} <= derived

    async def test_concurrent_edge_sources_merge(
        self, session_factory: Any, make_event: Any
    ) -> None:
        """Two runs' events extending the same ``produces`` edge: both
        ``source_event_ids`` survive. (Same-run edges are already serialized
        by the run-row lock; this exercises the edge lock on its own.)"""

        def _producer(run_id: str, observed: str) -> dict:
            return make_event(
                event="ingestion.load",
                outcome="success",
                observed_at=observed,
                service={"name": "ingest-svc", "environment": "test"},
                correlation={"run_id": run_id},
                entities={
                    "source": "source://kafka/orders",
                    "asset": "asset://shared.orders",
                },
            )

        await _persist(session_factory, [_producer("r-seed", "2025-01-01T00:00:00Z")])
        e_a = _producer("r-ea", "2025-01-02T00:00:00Z")
        e_b = _producer("r-eb", "2025-01-03T00:00:00Z")
        await _interleaved_persist(session_factory, e_a, e_b)

        async with session_factory() as session:
            row = (
                await session.execute(
                    select(Relationship).where(
                        Relationship.from_entity == "source://kafka/orders",
                        Relationship.to_entity == "asset://shared.orders",
                        Relationship.relationship_type == "produces",
                    )
                )
            ).scalar_one()
            sources = set(row.source_event_ids or [])
            assert {str(e_a["event_id"]), str(e_b["event_id"])} <= sources

    async def test_concurrent_baseline_samples_merge(
        self, session_factory: Any, make_event: Any
    ) -> None:
        asset = "asset://sales.orders"
        await _persist(session_factory, [_metric_event(make_event, asset, 100.0)])
        e_a = _metric_event(make_event, asset, 110.0)
        e_b = _metric_event(make_event, asset, 120.0)
        await _interleaved_persist(session_factory, e_a, e_b)

        async with session_factory() as session:
            row = (
                await session.execute(
                    select(Baseline).where(
                        Baseline.entity_id == asset, Baseline.metric == "row_count"
                    )
                )
            ).scalar_one()
            sample_eids = {s[1] for s in row.samples or []}
            assert {str(e_a["event_id"]), str(e_b["event_id"])} <= sample_eids
            assert row.count == 3

    async def test_concurrent_incremental_state_converges_with_rebuild(
        self, session_factory: Any, make_event: Any
    ) -> None:
        """The merge isn't just complete — it must equal a canonical rebuild."""
        asset = "asset://conv.orders"
        events = [
            _branch_event(
                make_event, "wap.branch.create", "b1", "r-c1", observed_at="2025-01-01T00:00:00Z"
            ),
            _metric_event(make_event, asset, 50.0),
        ]
        await _persist(session_factory, events)
        await _interleaved_persist(
            session_factory,
            _metric_event(make_event, asset, 60.0),
            _branch_event(
                make_event, "wap.validate", "b1", "r-c1", observed_at="2025-01-02T00:00:00Z"
            ),
        )
        async with session_factory() as session:
            before = await snapshot_state(session)
        async with session_factory() as session, session.begin():
            await rebuild_projections(session)
        async with session_factory() as session:
            after = await snapshot_state(session)
        diff = diff_snapshots(before, after)
        assert before == after, f"concurrent ingest diverged from rebuild:\n{diff}"

    async def test_concurrent_open_insight_dedupes_in_db(
        self, session_factory: Any, make_event: Any
    ) -> None:
        """Two replicas racing the same dedupe key: exactly one open row.

        The loser's insert hits the partial unique index; its projection
        savepoint fails open so its event stays durable, and a rebuild folds
        it into the surviving insight's evidence.
        """
        asset = "asset://quality.orders"
        e_a = make_event(
            event="quality.check",
            outcome="failure",
            service={"name": "quality-svc", "environment": "test"},
            entities={"asset": asset},
            error={"message": "nulls in order_id"},
        )
        e_b = make_event(
            event="quality.check",
            outcome="failure",
            service={"name": "quality-svc", "environment": "test"},
            entities={"asset": asset},
            error={"message": "nulls in customer_id"},
        )
        await _interleaved_persist(session_factory, e_a, e_b)

        async with session_factory() as session:
            # Both events are durable regardless of who won the insight race.
            stored = await session.scalar(select(func.count()).select_from(Event))
            assert stored == 2
            open_rows = (
                (await session.execute(select(Insight).where(Insight.state == "open")))
                .scalars()
                .all()
            )
            assert len(open_rows) == 1

        # A rebuild folds the loser's event into the surviving insight.
        async with session_factory() as session, session.begin():
            await rebuild_projections(session)
        async with session_factory() as session:
            open_rows = (
                (await session.execute(select(Insight).where(Insight.state == "open")))
                .scalars()
                .all()
            )
            assert len(open_rows) == 1
            evidence = set(open_rows[0].evidence_event_ids or [])
            assert {str(e_a["event_id"]), str(e_b["event_id"])} <= evidence

    async def test_second_open_row_same_dedupe_key_rejected(
        self, session_factory: Any, make_event: Any
    ) -> None:
        """The partial unique index itself: a second open row cannot commit."""
        await _persist(
            session_factory,
            [
                make_event(
                    event="quality.check",
                    outcome="failure",
                    service={"name": "quality-svc", "environment": "test"},
                    entities={"asset": "asset://dup.key"},
                )
            ],
        )
        async with session_factory() as session:
            existing = (
                await session.execute(select(Insight).where(Insight.state == "open"))
            ).scalar_one()
            dup = Insight(
                rule_id=existing.rule_id,
                rule_version=existing.rule_version,
                title="duplicate open row",
                severity="warning",
                state="open",
                entity_id=existing.entity_id,
                evidence_event_ids=[],
                evidence_metric_ids=[],
                dedupe_key=existing.dedupe_key,
                attributes={},
                created_at=existing.created_at,
                updated_at=existing.updated_at,
            )
            session.add(dup)
            with pytest.raises(IntegrityError):
                await session.commit()

    async def test_reopen_when_another_open_row_exists_is_409(
        self, client: AsyncClient, session_factory: Any, make_event: Any
    ) -> None:
        """A manual resolved->open transition can't bypass the dedupe index."""
        asset = "asset://lifecycle.key"
        await _persist(
            session_factory,
            [
                make_event(
                    event="quality.check",
                    outcome="failure",
                    service={"name": "quality-svc", "environment": "test"},
                    entities={"asset": asset},
                )
            ],
        )
        async with session_factory() as session, session.begin():
            open_row = (
                await session.execute(select(Insight).where(Insight.state == "open"))
            ).scalar_one()
            # A resolved sibling on the same key, as a prior resolve cycle
            # would leave behind.
            resolved = Insight(
                rule_id=open_row.rule_id,
                rule_version=open_row.rule_version,
                title="earlier resolved row",
                severity="warning",
                state="resolved",
                entity_id=open_row.entity_id,
                evidence_event_ids=[],
                evidence_metric_ids=[],
                dedupe_key=open_row.dedupe_key,
                attributes={"resolved_key": ["2000-01-01T00:00:00Z", "resolver"]},
                created_at=open_row.created_at,
                updated_at=open_row.updated_at,
            )
            session.add(resolved)
        rid = str(resolved.insight_id)
        resp = await client.post(f"/v2/insights/{rid}/transition", json={"state": "open"})
        assert resp.status_code == 409


_T0 = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)


async def _blocked_flush(factory: Any, flush_a: Any, flush_b: Any) -> None:
    """Run ``flush_a`` on one open transaction (taking its row locks), then
    ``flush_b`` on another while the first is still uncommitted.

    Under ``FOR UPDATE`` the second flush cannot complete until the first
    commits, so it re-reads post-commit state and merges a union. Without
    the lock it reads the same pre-commit state as the first and commits a
    wholesale overwrite — the lost update.
    """
    s_a = factory()
    s_b = factory()
    await s_a.begin()
    await s_b.begin()
    try:
        await flush_a(s_a)
        task = asyncio.create_task(flush_b(s_b))
        try:
            for _ in range(100):
                await asyncio.sleep(0)
                if task.done():
                    break
            if not task.done():
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(asyncio.shield(task), 0.5)
            await s_a.commit()
            await task
            await s_b.commit()
        except BaseException:
            task.cancel()
            raise
    finally:
        await s_a.close()
        await s_b.close()


def _entity_state(eid: str, attrs: dict, key: tuple) -> dict:
    return {
        eid: {
            "entity_id": eid,
            "kind": "thing",
            "display_name": "shared",
            "first_seen_at": _T0,
            "last_seen_at": key[0],
            "attributes": attrs,
            "attr_at": dict.fromkeys(attrs, key),
            "derived_from": [],
            "derived_keys": [key],
        }
    }


@pytest.mark.asyncio
class TestRowLocks:
    """Isolate the ``FOR UPDATE`` on the projection flushes themselves.

    ``persist_events``'s ``ON CONFLICT DO NOTHING`` precreate already waits
    on another transaction's *emitted* row writes, which masks an unlocked
    select at the persist granularity. Driving ``_flush_entities`` /
    ``_flush_edges`` directly reproduces the original lost-update window:
    both transactions select while the first's merge is still ORM-pending.
    """

    async def test_flush_entities_select_locks_rows(self, session_factory: Any) -> None:
        eid = "thing://shared"
        seed = _entity_state(eid, {"base": True}, (_T0, "seed"))
        async with session_factory() as s, s.begin():
            await projections._flush_entities(s, seed, _T0)

        t_a = _T0 + dt.timedelta(hours=1)
        t_b = _T0 + dt.timedelta(hours=2)
        await _blocked_flush(
            session_factory,
            lambda s: projections._flush_entities(
                s, _entity_state(eid, {"a_field": "A"}, (t_a, "ev-a")), t_a
            ),
            lambda s: projections._flush_entities(
                s, _entity_state(eid, {"b_field": "B"}, (t_b, "ev-b")), t_b
            ),
        )

        async with session_factory() as s:
            row = (await s.execute(select(Entity).where(Entity.entity_id == eid))).scalar_one()
            assert "a_field" in (row.attributes or {})
            assert "b_field" in (row.attributes or {})
            assert "base" in (row.attributes or {})
            derived = set((row.provenance or {}).get("derived_from") or [])
            assert {"seed", "ev-a", "ev-b"} <= derived

    async def test_flush_edges_select_locks_rows(self, session_factory: Any) -> None:
        edge = {
            "from_entity": "source://kafka/orders",
            "to_entity": "asset://shared.orders",
            "relationship_type": "produces",
            "method": "explicit",
            "confidence": 1.0,
        }
        seed_key = (_T0, "seed")
        async with session_factory() as s, s.begin():
            await projections._flush_edges(
                s,
                {
                    (
                        edge["from_entity"],
                        edge["to_entity"],
                        edge["relationship_type"],
                    ): {**edge, "source_keys": [seed_key]}
                },
                _T0,
            )

        t_a = _T0 + dt.timedelta(hours=1)
        t_b = _T0 + dt.timedelta(hours=2)

        def _flush(key: tuple) -> Any:
            return lambda s: projections._flush_edges(
                s,
                {
                    (
                        edge["from_entity"],
                        edge["to_entity"],
                        edge["relationship_type"],
                    ): {**edge, "source_keys": [key]}
                },
                key[0],
            )

        await _blocked_flush(session_factory, _flush((t_a, "ev-a")), _flush((t_b, "ev-b")))

        async with session_factory() as s:
            row = (
                await s.execute(
                    select(Relationship).where(
                        Relationship.from_entity == edge["from_entity"],
                        Relationship.to_entity == edge["to_entity"],
                        Relationship.relationship_type == edge["relationship_type"],
                    )
                )
            ).scalar_one()
            assert {"seed", "ev-a", "ev-b"} <= set(row.source_event_ids or [])


@pytest.mark.asyncio
class TestRebuildSerialization:
    async def test_ingest_projection_writes_wait_for_rebuild_lock(
        self, engine: Any, session_factory: Any, make_event: Any
    ) -> None:
        """While the exclusive rebuild lock is held, an ingest's projection
        pass parks on the shared lock instead of writing into the delete
        window; once it commits, the event's projections exist."""
        async with session_factory() as locker, locker.begin():
            await locker.execute(
                text("SELECT pg_advisory_xact_lock(:k)"), {"k": _PROJECTION_LOCK_KEY}
            )
            async with session_factory() as session, session.begin():
                task = asyncio.create_task(
                    persist_events(
                        session,
                        [make_event(event="pipeline.run", correlation={"run_id": "r-wait"})],
                    )
                )
                try:
                    blocked = 0
                    for _ in range(200):
                        if task.done():
                            break
                        async with engine.connect() as conn:
                            blocked = (
                                await conn.execute(
                                    text(
                                        "SELECT count(*) FROM pg_locks "
                                        "WHERE locktype = 'advisory' AND NOT granted"
                                    )
                                )
                            ).scalar_one()
                        if blocked:
                            break
                        await asyncio.sleep(0)
                    # The shared-lock request is visibly queued behind the
                    # held exclusive lock — without it, the projection pass
                    # would write straight into a rebuild's delete window.
                    assert blocked >= 1
                    assert not task.done()
                    await locker.commit()
                    await task
                    await session.commit()
                except BaseException:
                    task.cancel()
                    raise
        async with session_factory() as session:
            run = (
                await session.execute(select(Run).where(Run.run_id == "r-wait"))
            ).scalar_one_or_none()
            assert run is not None

    async def test_rebuild_with_concurrent_ingest_converges(
        self, client: AsyncClient, session_factory: Any, make_event: Any
    ) -> None:
        """A rebuild racing a live ingest leaves state == a clean rebuild.

        Whichever wins the lock, the outcome is identical: ingest first means
        its events are replayed; rebuild first means ingest folds afterward.
        """
        import workloads as wl

        counts = await wl.post_all(client, wl.mixed_history(seed=5, days=1, daily_runs=2))
        assert counts["rejected"] == 0
        event = make_event(
            event="pipeline.run",
            outcome="success",
            duration_ms=1_500,
            correlation={"run_id": "run-during-rebuild", "job_id": "late-job"},
        )

        async def _rebuild() -> None:
            async with session_factory() as session, session.begin():
                await rebuild_projections(session)

        async def _ingest() -> None:
            async with session_factory() as session, session.begin():
                await persist_events(session, [event])

        await asyncio.gather(_rebuild(), _ingest())

        async with session_factory() as session:
            before = await snapshot_state(session)
            run = (
                await session.execute(select(Run).where(Run.run_id == "run-during-rebuild"))
            ).scalar_one_or_none()
            # No interleaving was possible: the event is either replayed or
            # folded, never dropped by the rebuild's delete window.
            assert run is not None
            assert run.event_count == 1
        async with session_factory() as session, session.begin():
            await rebuild_projections(session)
        async with session_factory() as session:
            after = await snapshot_state(session)
        diff = diff_snapshots(before, after)
        assert before == after, f"rebuild-vs-ingest divergence:\n{diff}"
