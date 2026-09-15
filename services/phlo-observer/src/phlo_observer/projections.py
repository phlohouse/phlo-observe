"""Projection persistence: apply derived state to entity/asset/run tables.

The incremental ingest path and ``rebuild-projections`` share the same pure
reducers in ``state_engine``; this module owns the read-modify-write against
Postgres, including row-lock serialization for concurrent events hitting the
same projection (spec §12.4).
"""

from __future__ import annotations

from typing import Any

from observe_core.timestamps import utcnow
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer import state_engine as se
from phlo_observer.models import Asset, Entity, Event, Relationship, Run


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


async def _upsert_entities(
    session: AsyncSession, batch: se.DerivedBatch, now: Any, event_id: str
) -> None:
    """Insert new entities, refresh ``last_seen_at`` on known ones."""
    for eid, state in batch.entities.items():
        try:
            async with session.begin_nested():
                await session.execute(
                    pg_insert(Entity)
                    .values(
                        entity_id=eid,
                        kind=state["kind"],
                        display_name=state["display_name"],
                        first_seen_at=now,
                        last_seen_at=now,
                        attributes={},
                        provenance={
                            "derived_from": [event_id],
                            "rule": se.ENTITY_RULE,
                            "rule_version": se.ENTITY_RULE_VERSION,
                            "derived_at": now.isoformat() if hasattr(now, "isoformat") else now,
                        },
                    )
                    .on_conflict_do_nothing(index_elements=["entity_id"])
                )
        except IntegrityError:  # pragma: no cover - defensive
            pass
        existing = await session.get(Entity, eid)
        if existing is not None:
            if now > (existing.last_seen_at or now):
                existing.last_seen_at = now
            refs = (existing.provenance or {}).get("derived_from") or []
            if event_id not in refs and len(refs) < se._MAX_DERIVED_FROM:
                existing.provenance = {
                    **(existing.provenance or {}),
                    "derived_from": [*refs, event_id],
                }


async def _upsert_edges(
    session: AsyncSession, batch: se.DerivedBatch, now: Any, event_id: str
) -> None:
    for edge in batch.edges:
        stmt = (
            pg_insert(Relationship)
            .values(
                from_entity=edge.from_entity,
                to_entity=edge.to_entity,
                relationship_type=edge.relationship_type,
                method=edge.method,
                confidence=edge.confidence,
                first_seen_at=now,
                last_seen_at=now,
                source_event_ids=[event_id],
                provenance={
                    "rule": se.EDGE_RULE,
                    "rule_version": se.EDGE_RULE_VERSION,
                    "derived_at": now.isoformat() if hasattr(now, "isoformat") else now,
                },
            )
            .on_conflict_do_nothing(
                index_elements=["from_entity", "to_entity", "relationship_type"]
            )
        )
        async with session.begin_nested():
            await session.execute(stmt)
        row = (
            await session.execute(
                select(Relationship).where(
                    Relationship.from_entity == edge.from_entity,
                    Relationship.to_entity == edge.to_entity,
                    Relationship.relationship_type == edge.relationship_type,
                )
            )
        ).scalar_one_or_none()
        if row is not None:
            if now > (row.last_seen_at or now):
                row.last_seen_at = now
            merged = se.merge_edge_sources(list(row.source_event_ids or []), event_id)
            if merged != row.source_event_ids:
                row.source_event_ids = merged


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


async def apply_event(session: AsyncSession, row: Event) -> None:
    """Update all projections for one accepted event row.

    Runs inside the caller's savepoint so a projection failure cannot reject
    a durable event (spec §12.4: projections are derived, never the only
    copy).
    """
    event = _event_view(row)
    batch = se.derive(event)
    now = row.received_at or utcnow()
    event_id = str(row.event_id)
    await _upsert_entities(session, batch, now, event_id)
    await _upsert_edges(session, batch, now, event_id)
    if batch.asset_entity_id:
        await _apply_asset(session, event, batch.asset_entity_id, now)


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
    counts = {"events": 0, "runs": 0, "entities": 0, "edges": 0, "assets": 0}
    stmt = select(Event).order_by(Event.observed_at.asc(), Event.event_id.asc())
    if run_id:
        stmt = stmt.where(Event.run_id == run_id)
    else:
        # Full rebuild: clear derived tables first so stale rows disappear.
        for table in (Relationship, Asset, Entity, Run):
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
            observed = event.get("observed_at")
            if observed:
                if target["first_seen_at"] is None or observed < target["first_seen_at"]:
                    target["first_seen_at"] = observed
                if target["last_seen_at"] is None or observed > target["last_seen_at"]:
                    target["last_seen_at"] = observed
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
        if run_id:
            # Scoped rebuild: keep history outside this run's window.
            existing = await session.get(Entity, eid)
            if existing is not None:
                first = min(x for x in (existing.first_seen_at, first) if x)
                last = max(x for x in (existing.last_seen_at, last) if x)
        await session.merge(
            Entity(
                entity_id=eid,
                kind=state["kind"],
                display_name=state["display_name"],
                first_seen_at=first,
                last_seen_at=last,
                attributes={},
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
    return counts
