"""Projection persistence: apply derived state to entity/asset/run tables.

The incremental ingest path and ``rebuild-projections`` share the same pure
reducers in ``state_engine``; this module owns the read-modify-write against
Postgres, including row-lock serialization for concurrent events hitting the
same projection (spec §12.4).
"""

from __future__ import annotations

from typing import Any

from observe_core.timestamps import utcnow
from sqlalchemy import delete, func, select, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer import baselines, incidents, insights
from phlo_observer import state_engine as se
from phlo_observer.models import (
    Asset,
    Baseline,
    Entity,
    Event,
    Incident,
    Insight,
    Relationship,
    Run,
)


def _event_view(row: Event) -> dict[str, Any]:
    """The canonical payload dict a reducer consumes."""
    payload = row.payload if isinstance(row.payload, dict) else {}
    # The stored payload is authoritative; column copies fill gaps for
    # events stored before the V2 columns existed.
    return {
        "event_id": str(row.event_id),
        "event": row.event,
        "category": row.category,
        "outcome": row.outcome,
        "severity": row.severity,
        "started_at": row.started_at,
        "ended_at": row.ended_at,
        "duration_ms": row.duration_ms,
        "observed_at": row.observed_at,
        "received_at": row.received_at,
        "service": {
            "name": row.service_name,
            "version": row.service_version,
            "environment": row.environment,
        },
        "correlation": {
            "trace_id": row.trace_id,
            "run_id": row.run_id,
            "job_id": row.job_id,
            "invocation_id": row.invocation_id,
            "asset_key": row.asset_key,
            "partition_key": row.partition_key,
            "branch": row.branch,
            "table": row.table_name,
            "snapshot_id": row.snapshot_id,
            "pipeline": row.pipeline,
        },
        "attributes": row.attributes or {},
        "error": row.error,
        "source": row.source or {},
        "entities": row.entities or payload.get("entities") or {},
        "tags": row.tags or payload.get("tags") or {},
    }


async def _apply_asset(
    session: AsyncSession, event: dict[str, Any], entity_id: str, now: Any
) -> None:
    corr = event.get("correlation") or {}
    asset_key = corr.get("asset_key") or entity_id.split("://", 1)[-1]
    row = await session.get(Asset, entity_id, with_for_update=True)
    if row is None:
        try:
            async with session.begin_nested():
                row = Asset(
                    entity_id=entity_id,
                    asset_key=asset_key,
                    status="unknown",
                    updated_at=now,
                    attributes={},
                    provenance={},
                )
                session.add(row)
        except IntegrityError:
            row = await session.get(Asset, entity_id, with_for_update=True)
            if row is None:  # pragma: no cover - defensive
                return
    state = {
        "entity_id": row.entity_id,
        "asset_key": row.asset_key,
        "status": row.status,
        "last_materialized_at": row.last_materialized_at,
        "last_event_at": row.last_event_at,
        "freshness_sla_seconds": row.freshness_sla_seconds,
        "attributes": row.attributes or {},
        "derived_from": list((row.provenance or {}).get("derived_from") or []),
    }
    se.apply_asset_event(state, event)
    row.status = state["status"]
    row.last_materialized_at = state["last_materialized_at"]
    row.last_event_at = state["last_event_at"]
    row.freshness_sla_seconds = state["freshness_sla_seconds"]
    row.updated_at = now
    row.provenance = se.provenance(state, se.ASSET_RULE, se.ASSET_RULE_VERSION, now)


def _accumulate(
    entity_states: dict[str, dict[str, Any]],
    edge_states: dict[tuple[str, str, str], dict[str, Any]],
    batch: se.DerivedBatch,
    event: dict[str, Any],
    event_id: str,
) -> None:
    """Fold one derived batch into per-entity/per-edge accumulator state.

    Shared by the incremental (batched) path and rebuild, so both produce
    identical projections from the same events.
    """
    observed = event.get("observed_at")
    for eid, est in batch.entities.items():
        target = entity_states.setdefault(
            eid,
            {
                **est,
                "first_seen_at": None,
                "last_seen_at": None,
            },
        )
        se._record(target, event)
        if est.get("attributes"):
            target["attributes"] = {
                **(target.get("attributes") or {}),
                **est["attributes"],
            }
        if observed:
            if target["first_seen_at"] is None or observed < target["first_seen_at"]:
                target["first_seen_at"] = observed
            if target["last_seen_at"] is None or observed > target["last_seen_at"]:
                target["last_seen_at"] = observed
    for edge in batch.edges:
        key = (edge.from_entity, edge.to_entity, edge.relationship_type)
        estate = edge_states.setdefault(
            key,
            {
                "from_entity": edge.from_entity,
                "to_entity": edge.to_entity,
                "relationship_type": edge.relationship_type,
                "method": edge.method,
                "confidence": edge.confidence,
                "source_event_ids": [],
            },
        )
        estate["confidence"] = max(estate["confidence"], edge.confidence)
        estate["source_event_ids"] = se.merge_edge_sources(estate["source_event_ids"], event_id)


async def _flush_entities(
    session: AsyncSession,
    entity_states: dict[str, dict[str, Any]],
    now: Any,
) -> None:
    """Bulk-write accumulated entity state: 2 queries regardless of size."""
    if not entity_states:
        return
    eids = list(entity_states)
    await session.execute(
        pg_insert(Entity)
        .values(
            [
                {
                    "entity_id": eid,
                    "kind": st["kind"],
                    "display_name": st["display_name"],
                    "first_seen_at": st["first_seen_at"] or now,
                    "last_seen_at": st["last_seen_at"] or now,
                    "attributes": st.get("attributes") or {},
                    "provenance": {
                        "derived_from": [st["derived_from"][0]] if st.get("derived_from") else [],
                        "rule": se.ENTITY_RULE,
                        "rule_version": se.ENTITY_RULE_VERSION,
                        "derived_at": now.isoformat(),
                    },
                }
                for eid, st in entity_states.items()
            ]
        )
        .on_conflict_do_nothing(index_elements=["entity_id"])
    )
    existing = {
        r.entity_id: r
        for r in (await session.execute(select(Entity).where(Entity.entity_id.in_(eids)))).scalars()
    }
    for eid, state in entity_states.items():
        row = existing.get(eid)
        if row is None:
            continue
        first = state["first_seen_at"]
        last = state["last_seen_at"]
        if first and (row.first_seen_at is None or first < row.first_seen_at):
            row.first_seen_at = first
        if last and (row.last_seen_at is None or last > row.last_seen_at):
            row.last_seen_at = last
        if state.get("attributes"):
            row.attributes = {**(row.attributes or {}), **state["attributes"]}
        refs = (row.provenance or {}).get("derived_from") or []
        merged_refs = list(refs)
        for eid_ref in state.get("derived_from") or []:
            merged_refs = se.merge_sources(merged_refs, eid_ref, se._MAX_DERIVED_FROM)
        if merged_refs != refs:
            row.provenance = {
                **(row.provenance or {}),
                "derived_from": merged_refs,
            }


async def _flush_edges(
    session: AsyncSession,
    edge_states: dict[tuple[str, str, str], dict[str, Any]],
    now: Any,
) -> None:
    """Bulk-write accumulated edges: insert-new + merge-existing, 2 queries."""
    if not edge_states:
        return
    keys = list(edge_states)
    await session.execute(
        pg_insert(Relationship)
        .values(
            [
                {
                    "from_entity": st["from_entity"],
                    "to_entity": st["to_entity"],
                    "relationship_type": st["relationship_type"],
                    "method": st["method"],
                    "confidence": st["confidence"],
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "source_event_ids": st["source_event_ids"],
                    "provenance": {
                        "rule": se.EDGE_RULE,
                        "rule_version": se.EDGE_RULE_VERSION,
                        "derived_at": now.isoformat(),
                    },
                }
                for st in edge_states.values()
            ]
        )
        .on_conflict_do_nothing(index_elements=["from_entity", "to_entity", "relationship_type"])
    )
    existing = (
        await session.execute(
            select(Relationship).where(
                tuple_(
                    Relationship.from_entity,
                    Relationship.to_entity,
                    Relationship.relationship_type,
                ).in_(keys)
            )
        )
    ).scalars()
    for row in existing:
        key = (row.from_entity, row.to_entity, row.relationship_type)
        state = edge_states.get(key)
        if state is None:
            continue
        if now > (row.last_seen_at or now):
            row.last_seen_at = now
        if state["confidence"] > (row.confidence or 0):
            row.confidence = state["confidence"]
        merged = list(row.source_event_ids or [])
        for eid in state["source_event_ids"]:
            merged = se.merge_edge_sources(merged, eid)
        if merged != row.source_event_ids:
            row.source_event_ids = merged


async def apply_events_batch(session: AsyncSession, rows: list[Event]) -> None:
    """Update all projections for a batch of accepted event rows.

    One accumulation pass + bulk flushes per table, so an N-event ingest
    batch costs O(1) round trips for entities/edges instead of O(entities).
    Runs inside the caller's transaction so a projection failure cannot
    reject durable events (spec §12.4).
    """
    entity_states: dict[str, dict[str, Any]] = {}
    edge_states: dict[tuple[str, str, str], dict[str, Any]] = {}
    pending_assets: list[tuple[dict[str, Any], str, Any]] = []
    for row in rows:
        event = _event_view(row)
        batch = se.derive(event)
        now = row.received_at or utcnow()
        _accumulate(entity_states, edge_states, batch, event, str(row.event_id))
        if batch.asset_entity_id:
            pending_assets.append((event, batch.asset_entity_id, now))
    await _flush_entities(session, entity_states, utcnow())
    await _flush_edges(session, edge_states, utcnow())
    for event, eid, now in pending_assets:
        await _apply_asset(session, event, eid, now)


async def apply_event(session: AsyncSession, row: Event) -> None:
    """Update all projections for one accepted event row.

    Thin wrapper over :func:`apply_events_batch` for single-event callers.
    """
    await apply_events_batch(session, [row])


async def update_run_projections(session: AsyncSession, rows: list[Event]) -> None:
    """Fold a batch of correlated events into ``runs`` rows, one get per run.

    Equivalent to calling the per-event fold for every row, but each run is
    fetched/locked once and its events folded in order — a 1,000-event batch
    for one run is one SELECT FOR UPDATE, not a thousand.
    """
    by_run: dict[str, list[Event]] = {}
    for row in rows:
        if row.run_id:
            by_run.setdefault(row.run_id, []).append(row)
    if not by_run:
        return
    existing = {
        r.run_id: r
        for r in (
            await session.execute(select(Run).where(Run.run_id.in_(by_run)).with_for_update())
        ).scalars()
    }
    for run_id, events in by_run.items():
        run = existing.get(run_id)
        if run is None:  # pragma: no cover - bulk insert above guarantees it
            continue
        state = run_state_from_row(run)
        for row in events:
            se.apply_run_event(state, _event_view(row))
        last = max((e.received_at for e in events if e.received_at), default=None)
        apply_run_state(run, state, last or utcnow())


def run_state_from_row(run: Run) -> dict[str, Any]:
    """Reducer state seeded from an existing ``runs`` row."""
    return {
        "run_id": run.run_id,
        "job_name": run.job_name,
        "service_name": run.service_name,
        "environment": run.environment,
        "status": run.status,
        "started_at": run.started_at,
        "ended_at": run.ended_at,
        "duration_ms": run.duration_ms,
        "trigger": run.trigger,
        "event_count": run.event_count or 0,
        "error_count": run.error_count or 0,
        "warning_count": run.warning_count or 0,
        "branch": run.branch,
        "asset_keys": set((run.summary or {}).get("asset_keys") or []),
        "derived_from": list((run.provenance or {}).get("derived_from") or []),
    }


def apply_run_state(run: Run, state: dict[str, Any], now: Any) -> None:
    """Write reducer state back onto a ``runs`` row."""
    run.job_name = state["job_name"]
    run.service_name = state["service_name"]
    run.environment = state["environment"]
    run.status = state["status"]
    run.started_at = state["started_at"]
    run.ended_at = state["ended_at"]
    run.duration_ms = state["duration_ms"]
    run.trigger = state["trigger"]
    run.event_count = state["event_count"]
    run.error_count = state["error_count"]
    run.warning_count = state["warning_count"]
    run.branch = state["branch"]
    assets = sorted(state["asset_keys"])
    run.asset_count = len(assets)
    run.summary = {**(run.summary or {}), "asset_keys": assets}
    run.updated_at = now
    run.provenance = se.provenance(state, se.RUN_RULE, se.RUN_RULE_VERSION, now)


async def rebuild_projections(
    session: AsyncSession,
    *,
    run_id: str | None = None,
    batch_size: int = 1000,
) -> dict[str, int]:
    """Recompute projections from canonical events (spec §12.4).

    With no scope, every projection table is rebuilt from the full event
    history in ``observed_at`` order. With ``--run <id>``, only that run's
    events are replayed — the run row is replaced and the entities/edges/
    assets it touched are re-derived and merged, leaving unrelated
    projections untouched.
    """
    counts = {
        "events": 0,
        "runs": 0,
        "entities": 0,
        "edges": 0,
        "assets": 0,
        "baselines": 0,
        "insights": 0,
        "incidents": 0,
    }
    stmt = select(Event).order_by(Event.observed_at.asc(), Event.event_id.asc())
    if run_id:
        stmt = stmt.where(Event.run_id == run_id)
    else:
        # Full rebuild: clear derived tables first so stale rows disappear.
        # Insights, incidents and baselines are also derived state: replaying
        # the canonical history in observed order reproduces them exactly.
        for table in (Relationship, Asset, Entity, Run, Baseline, Insight, Incident):
            await session.execute(delete(table))

    run_states: dict[str, dict[str, Any]] = {}
    entity_states: dict[str, dict[str, Any]] = {}
    asset_states: dict[str, dict[str, Any]] = {}
    edge_states: dict[tuple[str, str, str], dict[str, Any]] = {}

    rows = (await session.execute(stmt)).scalars()
    for row in rows:
        event = _event_view(row)
        counts["events"] += 1
        batch = se.derive(event)
        rid = batch.run_id
        if rid:
            state = run_states.setdefault(rid, se.new_run_state(rid))
            se.apply_run_event(state, event)
        for eid, est in batch.entities.items():
            target = entity_states.setdefault(
                eid, {**est, "first_seen_at": None, "last_seen_at": None}
            )
            se._record(target, event)
            if est.get("attributes"):
                target["attributes"] = {**(target.get("attributes") or {}), **est["attributes"]}
            observed = event.get("observed_at")
            if observed:
                if target["first_seen_at"] is None or observed < target["first_seen_at"]:
                    target["first_seen_at"] = observed
                if target["last_seen_at"] is None or observed > target["last_seen_at"]:
                    target["last_seen_at"] = observed
        # Insight pass runs before this event's own asset/baseline fold —
        # the same ordering the incremental path uses, so both converge.
        if not run_id:
            findings = await insights.evaluate(session, event, asset_states=asset_states)
            if findings:
                for insight in await insights.record_findings(session, event, findings):
                    await incidents.group_insight(session, insight)
            await insights.resolve_for_event(session, event)
            counts["baselines"] += await baselines.update_baselines(session, event)
        if batch.asset_entity_id:
            corr = event.get("correlation") or {}
            astate = asset_states.setdefault(
                batch.asset_entity_id,
                se.new_asset_state(
                    batch.asset_entity_id,
                    corr.get("asset_key") or batch.asset_entity_id.split("://", 1)[-1],
                ),
            )
            se.apply_asset_event(astate, event)
        for edge in batch.edges:
            key = (edge.from_entity, edge.to_entity, edge.relationship_type)
            estate = edge_states.setdefault(
                key,
                {
                    "from_entity": edge.from_entity,
                    "to_entity": edge.to_entity,
                    "relationship_type": edge.relationship_type,
                    "method": edge.method,
                    "confidence": edge.confidence,
                    "source_event_ids": [],
                },
            )
            estate["source_event_ids"] = se.merge_edge_sources(
                estate["source_event_ids"], str(row.event_id)
            )

    now = utcnow()
    for eid, state in entity_states.items():
        first = state.get("first_seen_at") or now
        last = state.get("last_seen_at") or now
        merged_attrs = dict(state.get("attributes") or {})
        if run_id:
            # Scoped rebuild: keep history outside this run's window and
            # merge attribute updates rather than replacing them.
            existing = await session.get(Entity, eid)
            if existing is not None:
                first = min(x for x in (existing.first_seen_at, first) if x)
                last = max(x for x in (existing.last_seen_at, last) if x)
                merged_attrs = {**(existing.attributes or {}), **merged_attrs}
        await session.merge(
            Entity(
                entity_id=eid,
                kind=state["kind"],
                display_name=state["display_name"],
                first_seen_at=first,
                last_seen_at=last,
                attributes=merged_attrs,
                provenance=se.provenance(state, se.ENTITY_RULE, se.ENTITY_RULE_VERSION, now),
            )
        )
        counts["entities"] += 1
    for key, state in edge_states.items():
        if run_id:
            # Scoped rebuild: replace only edges this run's events re-derive.
            await session.execute(
                delete(Relationship).where(
                    Relationship.from_entity == key[0],
                    Relationship.to_entity == key[1],
                    Relationship.relationship_type == key[2],
                )
            )
        session.add(
            Relationship(
                from_entity=state["from_entity"],
                to_entity=state["to_entity"],
                relationship_type=state["relationship_type"],
                method=state["method"],
                confidence=state["confidence"],
                first_seen_at=now,
                last_seen_at=now,
                source_event_ids=state["source_event_ids"],
                provenance={
                    "rule": se.EDGE_RULE,
                    "rule_version": se.EDGE_RULE_VERSION,
                    "derived_at": now.isoformat(),
                },
            )
        )
        counts["edges"] += 1
    for eid, state in asset_states.items():
        if run_id:
            await session.execute(delete(Asset).where(Asset.entity_id == eid))
        session.add(
            Asset(
                entity_id=eid,
                asset_key=state["asset_key"],
                status=state["status"],
                last_materialized_at=state["last_materialized_at"],
                last_event_at=state["last_event_at"],
                freshness_sla_seconds=state["freshness_sla_seconds"],
                attributes=state["attributes"],
                provenance=se.provenance(state, se.ASSET_RULE, se.ASSET_RULE_VERSION, now),
                updated_at=now,
            )
        )
        counts["assets"] += 1
    for rid, state in run_states.items():
        if run_id:
            await session.execute(delete(Run).where(Run.run_id == rid))
        session.add(
            Run(
                run_id=rid,
                job_name=state["job_name"],
                service_name=state["service_name"],
                environment=state["environment"],
                status=state["status"],
                started_at=state["started_at"],
                ended_at=state["ended_at"],
                duration_ms=state["duration_ms"],
                trigger=state["trigger"],
                event_count=state["event_count"],
                error_count=state["error_count"],
                warning_count=state["warning_count"],
                asset_count=len(state["asset_keys"]),
                branch=state["branch"],
                updated_at=now,
                summary={"asset_keys": sorted(state["asset_keys"])},
                provenance=se.provenance(state, se.RUN_RULE, se.RUN_RULE_VERSION, now),
            )
        )
        counts["runs"] += 1
    if not run_id:
        counts["insights"] = await session.scalar(select(func.count()).select_from(Insight)) or 0
        counts["incidents"] = await session.scalar(select(func.count()).select_from(Incident)) or 0
    return counts
