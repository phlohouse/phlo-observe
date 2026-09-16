"""Projection persistence: apply derived state to entity/asset/run tables.

The incremental ingest path and ``rebuild-projections`` share the same pure
reducers in ``state_engine``; this module owns the read-modify-write against
Postgres, including row-lock serialization for concurrent events hitting the
same projection (spec §12.4). Every projection row folded from canonical
events is read under ``SELECT .. FOR UPDATE`` before merge: an unlocked
read-modify-write would let a concurrent ingest overwrite this transaction's
merge wholesale and silently lose it.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from observe_core.timestamps import utcnow
from sqlalchemy import delete, func, or_, select, text, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer import insights
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

_PROJECTION_LOCK_KEY = 0x70686C70
"""Advisory lock serializing derived-state writes against full/scoped rebuilds.

Ingest holds it shared for the projection pass (many producers fold
concurrently); ``rebuild_projections`` holds it exclusively, so a rebuild
waits out in-flight ingests and new ingests queue behind it rather than
write rows the rebuild's delete+replay would silently drop. Chosen
arbitrarily; distinct from retention's lock key.
"""


async def lock_projection_writes(session: AsyncSession) -> None:
    """Shared lock for transactions that write derived state (ingest)."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock_shared(:key)"), {"key": _PROJECTION_LOCK_KEY}
    )


async def lock_rebuild(session: AsyncSession) -> None:
    """Exclusive lock for delete+replay rebuilds; blocks ingest writes."""
    await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": _PROJECTION_LOCK_KEY})


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


async def _flush_assets(
    session: AsyncSession,
    pending: list[tuple[dict[str, Any], str, Any]],
    now: Any,
) -> None:
    """Fold and persist asset state for a whole batch: 2 queries + K updates.

    One ``INSERT .. ON CONFLICT DO NOTHING`` pre-creates missing rows, one
    ``SELECT .. FOR UPDATE`` locks every touched asset, then events fold per
    asset in arrival order. K = distinct assets in the batch, not events.
    """
    if not pending:
        return
    first_key: dict[str, str] = {}
    for event, eid, _ in pending:
        if eid not in first_key:
            corr = event.get("correlation") or {}
            first_key[eid] = corr.get("asset_key") or eid.split("://", 1)[-1]
    eids = list(first_key)
    await session.execute(
        pg_insert(Asset)
        .values(
            [
                {
                    "entity_id": eid,
                    "asset_key": first_key[eid],
                    "status": "unknown",
                    "updated_at": now,
                    "attributes": {},
                    "provenance": {},
                }
                for eid in eids
            ]
        )
        .on_conflict_do_nothing(index_elements=["entity_id"])
    )
    rows = {
        r.entity_id: r
        for r in (
            await session.execute(select(Asset).where(Asset.entity_id.in_(eids)).with_for_update())
        ).scalars()
    }
    states: dict[str, dict[str, Any]] = {}
    for eid, row in rows.items():
        states[eid] = se.asset_state_from_row(row)
    for event, eid, _ in pending:
        st = states.get(eid)
        if st is not None:  # concurrent delete between insert and select
            se.apply_asset_event(st, event)
    for eid, st in states.items():
        row = rows[eid]
        row.status = st["status"]
        row.last_materialized_at = st["last_materialized_at"]
        row.last_event_at = st["last_event_at"]
        row.freshness_sla_seconds = st["freshness_sla_seconds"]
        row.attributes = st["attributes"]
        row.fold_state = se.asset_fold_state(st)
        row.updated_at = now
        row.provenance = se.provenance(st, se.ASSET_RULE, se.ASSET_RULE_VERSION, now)


def _accumulate(
    entity_states: dict[str, dict[str, Any]],
    edge_states: dict[tuple[str, str, str], dict[str, Any]],
    batch: se.DerivedBatch,
    event: dict[str, Any],
    event_id: str,
) -> None:
    """Fold one derived batch into per-entity/per-edge accumulator state.

    Shared by the incremental (batched) path and rebuild, so both produce
    identical projections from the same events. Every order-sensitive field
    is keyed on the event's ``(observed_at, event_id)`` position rather than
    fold position.
    """
    observed = se._as_dt(event.get("observed_at"))
    key = se.event_key(event)
    for eid, est in batch.entities.items():
        target = entity_states.setdefault(
            eid,
            {
                **est,
                "first_seen_at": None,
                "last_seen_at": None,
                "attr_at": {},
            },
        )
        se._record(target, event)
        if est.get("attributes"):
            se.merge_attributes(target, est["attributes"], key)
        if observed:
            if target["first_seen_at"] is None or observed < target["first_seen_at"]:
                target["first_seen_at"] = observed
            if target["last_seen_at"] is None or observed > target["last_seen_at"]:
                target["last_seen_at"] = observed
    for edge in batch.edges:
        edge_key = (edge.from_entity, edge.to_entity, edge.relationship_type)
        estate = edge_states.setdefault(
            edge_key,
            {
                "from_entity": edge.from_entity,
                "to_entity": edge.to_entity,
                "relationship_type": edge.relationship_type,
                "method": edge.method,
                "confidence": edge.confidence,
                "source_keys": [],
            },
        )
        estate["confidence"] = max(estate["confidence"], edge.confidence)
        se._insert_sorted(estate["source_keys"], key, se._MAX_EDGE_SOURCES, keep="earliest")


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
                    "fold_state": {
                        "attr_at": se._dump_field_at(st.get("attr_at") or {}),
                        "derived_keys": [se._dump_key(k) for k in st.get("derived_keys") or []],
                    },
                }
                for eid, st in entity_states.items()
            ]
        )
        .on_conflict_do_nothing(index_elements=["entity_id"])
    )
    existing = {
        r.entity_id: r
        for r in (
            await session.execute(
                select(Entity).where(Entity.entity_id.in_(eids)).with_for_update()
            )
        ).scalars()
    }
    for eid, state in entity_states.items():
        row = existing.get(eid)
        if row is None:
            continue
        # The legacy gate must read the row's last-seen BEFORE this batch
        # moves it forward — a field with no recorded write key is treated
        # as last written at the row's previous last-seen, so a late event
        # cannot regress it but a newer one still can.
        prior_last_seen = row.last_seen_at
        first = state["first_seen_at"]
        last = state["last_seen_at"]
        if first and (row.first_seen_at is None or first < row.first_seen_at):
            row.first_seen_at = first
        if last and (row.last_seen_at is None or last > row.last_seen_at):
            row.last_seen_at = last
        fs = dict(row.fold_state or {})
        if state.get("attributes"):
            attrs = dict(row.attributes or {})
            attr_at = se._parse_field_at(fs.get("attr_at"))
            se.gated_attr_merge(
                attrs,
                attr_at,
                state["attributes"],
                state.get("attr_at") or {},
                legacy_gate=(prior_last_seen or se._EPOCH, ""),
            )
            row.attributes = attrs
            fs["attr_at"] = se._dump_field_at(attr_at)
            row.fold_state = fs
        # Provenance keeps the earliest-observed sources, not the
        # earliest-arrived: merge keyed positions, then derive the id list.
        refs = (row.provenance or {}).get("derived_from") or []
        merged_keys = se._derived_keys_from(fs, refs)
        for key in state.get("derived_keys") or []:
            se._insert_sorted(merged_keys, key, se._MAX_DERIVED_FROM, keep="earliest")
        merged_refs = [k[1] for k in merged_keys]
        if merged_refs != refs or fs.get("derived_keys") is None:
            row.provenance = {
                **(row.provenance or {}),
                "derived_from": merged_refs,
            }
            fs["derived_keys"] = [se._dump_key(k) for k in merged_keys]
            row.fold_state = fs


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
                    "source_event_ids": [k[1] for k in st["source_keys"]],
                    "provenance": {
                        "rule": se.EDGE_RULE,
                        "rule_version": se.EDGE_RULE_VERSION,
                        "derived_at": now.isoformat(),
                    },
                    "fold_state": {
                        "source_keys": [se._dump_key(k) for k in st["source_keys"]],
                    },
                }
                for st in edge_states.values()
            ]
        )
        .on_conflict_do_nothing(index_elements=["from_entity", "to_entity", "relationship_type"])
    )
    existing = (
        await session.execute(
            select(Relationship)
            .where(
                tuple_(
                    Relationship.from_entity,
                    Relationship.to_entity,
                    Relationship.relationship_type,
                ).in_(keys)
            )
            .with_for_update()
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
        merged_keys = se.parse_keys((row.fold_state or {}).get("source_keys"))
        if not merged_keys:
            merged_keys = [(se._EPOCH, str(e)) for e in row.source_event_ids or []]
        for src_key in state["source_keys"]:
            se._insert_sorted(merged_keys, src_key, se._MAX_EDGE_SOURCES, keep="earliest")
        merged = [k[1] for k in merged_keys]
        if merged != (row.source_event_ids or []):
            row.source_event_ids = merged
        row.fold_state = {"source_keys": [se._dump_key(k) for k in merged_keys]}


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
    # Fold in observed order: rebuild replays the same ordering, so
    # incremental and rebuilt projections converge event for event.
    ordered = sorted(rows, key=lambda r: (r.observed_at or r.received_at, r.event_id))
    now = utcnow()
    for row in ordered:
        event = _event_view(row)
        batch = se.derive(event)
        _accumulate(entity_states, edge_states, batch, event, str(row.event_id))
        if batch.asset_entity_id:
            pending_assets.append((event, batch.asset_entity_id, row.received_at or now))
    await _flush_entities(session, entity_states, now)
    await _flush_edges(session, edge_states, now)
    await _flush_assets(session, pending_assets, now)


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


_RUN_VALUE_FIELDS = ("job_name", "service_name", "environment", "branch", "duration_ms", "trigger")


def run_state_from_row(run: Run) -> dict[str, Any]:
    """Reducer state seeded from an existing ``runs`` row.

    Rows written before fold bookkeeping gate each populated value field at
    the run's latest known event time — a late event can still fill empty
    fields but cannot regress a recorded one.
    """
    fs = run.fold_state or {}
    field_at = se._parse_field_at(fs.get("field_at"))
    legacy_gate = run.ended_at or run.started_at or run.updated_at or se._EPOCH
    for field_name in _RUN_VALUE_FIELDS:
        if getattr(run, field_name) is not None and field_name not in field_at:
            field_at[field_name] = (legacy_gate, "")
    # started_at bookkeeping: rows written before it bootstrap so the stored
    # value can only move via a real ``started`` timestamp — a late event
    # cannot claim "earliest observed" for a history we cannot verify. Rows
    # with no folded events yet (pre-created shells) start unanchored. When
    # ``run_start`` exists it is authoritative: ``started_at`` may be a
    # fallback ``observed_at``, never evidence of a real ``started``.
    start_state = fs.get("run_start") or {}
    has_history = bool(start_state) or (run.event_count or 0) > 0
    if start_state:
        min_started = se._as_dt(start_state.get("min_started"))
        first_key = se._parse_key(start_state.get("first"))
        first_fallback = se._as_dt(start_state.get("first_fallback"))
    else:
        min_started = run.started_at if has_history else None
        first_key = (se._EPOCH, "") if has_history else None
        first_fallback = None
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
        "derived_keys": se._derived_keys_from(fs, (run.provenance or {}).get("derived_from") or []),
        "field_at": field_at,
        "min_started": min_started,
        "first_key": first_key,
        "first_fallback": first_fallback,
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
    run.fold_state = se.run_fold_state(state)
    run.updated_at = now
    run.provenance = se.provenance(state, se.RUN_RULE, se.RUN_RULE_VERSION, now)


async def _paged_events(
    session: AsyncSession,
    stmt: Any,
    batch_size: int,
) -> AsyncIterator[list[Event]]:
    """Keyset-paginate a rebuild scan on (observed_at, event_id).

    The fold order must match the incremental path exactly (observed_at,
    then event_id as the tie-break), so pagination follows the same key.
    Yields one page at a time — the full history is never materialized in
    memory at production volume.
    """
    last_ts = None
    last_id = None
    while True:
        page = stmt.order_by(Event.observed_at.asc(), Event.event_id.asc()).limit(batch_size)
        if last_id is not None:
            page = page.where(
                (Event.observed_at > last_ts)
                | ((Event.observed_at == last_ts) & (Event.event_id > last_id))
            )
        batch = (await session.execute(page)).scalars().all()
        if not batch:
            break
        yield list(batch)
        last_ts, last_id = batch[-1].observed_at, batch[-1].event_id
        if len(batch) < batch_size:
            break


async def rebuild_projections(
    session: AsyncSession,
    *,
    run_id: str | None = None,
    batch_size: int = 1000,
) -> dict[str, int]:
    """Recompute projections from canonical events (spec §12.4).

    With no scope, every projection table is rebuilt from the full event
    history in ``observed_at`` order. With ``--run <id>``, that run's row is
    replaced; the entities/edges it touched are merged into existing state,
    and assets it touched are re-derived from *all* events referencing the
    asset — a shared asset's history is never regressed by another run's
    contribution. Insights/incidents/baselines are only rebuilt in the
    unscoped path (their state depends on cross-run history).

    The whole rebuild runs under the exclusive projection advisory lock:
    ingest transactions hold it shared, so a rebuild first waits out any
    in-flight ingest and then blocks new ingests' projection writes for its
    duration — events stay durable (canonical inserts are not gated), and
    no fold can be dropped by the delete+replay window.
    """
    await lock_rebuild(session)
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
    stmt = select(Event)
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
    # Asset entity ids / keys this run touched — the scoped pass replays
    # every event touching them, not just this run's.
    asset_eids: set[str] = set()
    asset_keys: set[str] = set()

    # Insight state shares the projection's asset_states dict: BatchState.step
    # folds each event after evaluating it, so both consume the same fold.
    insight_state = insights.BatchState.empty(asset_states)
    async for page_rows in _paged_events(session, stmt, batch_size):
        for row in page_rows:
            event = _event_view(row)
            counts["events"] += 1
            batch = se.derive(event)
            rid = batch.run_id
            if rid:
                state = run_states.setdefault(rid, se.new_run_state(rid))
                se.apply_run_event(state, event)
            _accumulate(entity_states, edge_states, batch, event, str(row.event_id))
            if not run_id:
                # The shared insight pass: evaluate -> record -> group -> resolve
                # -> baselines -> asset fold, identical to incremental ingest.
                await insight_state.step(session, event)
            elif batch.asset_entity_id:
                asset_eids.add(batch.asset_entity_id)
                corr = event.get("correlation") or {}
                if corr.get("asset_key"):
                    asset_keys.add(corr["asset_key"])

    if run_id and (asset_eids or asset_keys):
        # Scoped rebuild: re-derive each touched asset from its full event
        # history so contributions from other runs survive the rebuild.
        asset_predicates = []
        if asset_keys:
            asset_predicates.append(Event.asset_key.in_(asset_keys))
        if asset_eids:
            asset_predicates.append(Event.entities["asset"].as_string().in_(asset_eids))
        asset_stmt = select(Event).where(or_(*asset_predicates))
        async for page_rows in _paged_events(session, asset_stmt, batch_size):
            for row in page_rows:
                event = _event_view(row)
                eid = se.event_entities(event).get("asset")
                if not eid:
                    continue
                corr = event.get("correlation") or {}
                astate = asset_states.setdefault(
                    eid,
                    se.new_asset_state(eid, corr.get("asset_key") or eid.split("://", 1)[-1]),
                )
                se.apply_asset_event(astate, event)

    now = utcnow()
    for eid, state in entity_states.items():
        first = state.get("first_seen_at") or now
        last = state.get("last_seen_at") or now
        merged_attrs = dict(state.get("attributes") or {})
        fold: dict[str, Any] = {
            "attr_at": se._dump_field_at(state.get("attr_at") or {}),
            "derived_keys": [se._dump_key(k) for k in state.get("derived_keys") or []],
        }
        provenance = se.provenance(state, se.ENTITY_RULE, se.ENTITY_RULE_VERSION, now)
        if run_id:
            # Scoped rebuild: keep history outside this run's window and
            # merge attribute updates per-field by write key — a field the
            # run's events did not touch survives untouched, and a field
            # they did touch only moves forward when the rebuild's writer
            # is the newest observation.
            existing = await session.get(Entity, eid, with_for_update=True)
            if existing is not None:
                first = min(x for x in (existing.first_seen_at, first) if x)
                last = max(x for x in (existing.last_seen_at, last) if x)
                merged_attrs = dict(existing.attributes or {})
                attr_at = se._parse_field_at((existing.fold_state or {}).get("attr_at"))
                se.gated_attr_merge(
                    merged_attrs,
                    attr_at,
                    state.get("attributes") or {},
                    state.get("attr_at") or {},
                    legacy_gate=(existing.last_seen_at or se._EPOCH, ""),
                )
                fold["attr_at"] = se._dump_field_at(attr_at)
                merged_keys = se._derived_keys_from(
                    existing.fold_state or {},
                    (existing.provenance or {}).get("derived_from") or [],
                )
                for k in state.get("derived_keys") or []:
                    se._insert_sorted(merged_keys, k, se._MAX_DERIVED_FROM, keep="earliest")
                fold["derived_keys"] = [se._dump_key(k) for k in merged_keys]
                provenance = {
                    **provenance,
                    "derived_from": [k[1] for k in merged_keys],
                }
        await session.merge(
            Entity(
                entity_id=eid,
                kind=state["kind"],
                display_name=state["display_name"],
                first_seen_at=first,
                last_seen_at=last,
                attributes=merged_attrs,
                provenance=provenance,
                fold_state=fold,
            )
        )
        counts["entities"] += 1
    existing_edges: dict[tuple[str, str, str], Relationship] = {}
    if run_id and edge_states:
        # Scoped rebuild merges into shared edges: other runs' events may
        # have contributed the same (from, to, type) tuple.
        existing_edges = {
            (r.from_entity, r.to_entity, r.relationship_type): r
            for r in (
                await session.execute(
                    select(Relationship)
                    .where(
                        tuple_(
                            Relationship.from_entity,
                            Relationship.to_entity,
                            Relationship.relationship_type,
                        ).in_(list(edge_states))
                    )
                    .with_for_update()
                )
            ).scalars()
        }
    for key, state in edge_states.items():
        row = existing_edges.get(key)
        if row is not None:
            merged_keys = se.parse_keys((row.fold_state or {}).get("source_keys"))
            if not merged_keys:
                merged_keys = [(se._EPOCH, str(e)) for e in row.source_event_ids or []]
            for src_key in state["source_keys"]:
                se._insert_sorted(merged_keys, src_key, se._MAX_EDGE_SOURCES, keep="earliest")
            row.source_event_ids = [k[1] for k in merged_keys]
            row.fold_state = {"source_keys": [se._dump_key(k) for k in merged_keys]}
            if state["confidence"] > (row.confidence or 0):
                row.confidence = state["confidence"]
            if now > (row.last_seen_at or now):
                row.last_seen_at = now
        else:
            session.add(
                Relationship(
                    from_entity=state["from_entity"],
                    to_entity=state["to_entity"],
                    relationship_type=state["relationship_type"],
                    method=state["method"],
                    confidence=state["confidence"],
                    first_seen_at=now,
                    last_seen_at=now,
                    source_event_ids=[k[1] for k in state["source_keys"]],
                    provenance={
                        "rule": se.EDGE_RULE,
                        "rule_version": se.EDGE_RULE_VERSION,
                        "derived_at": now.isoformat(),
                    },
                    fold_state={"source_keys": [se._dump_key(k) for k in state["source_keys"]]},
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
                fold_state=se.asset_fold_state(state),
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
                fold_state=se.run_fold_state(state),
            )
        )
        counts["runs"] += 1
    if not run_id:
        counts["baselines"] = await session.scalar(select(func.count()).select_from(Baseline)) or 0
        counts["insights"] = await session.scalar(select(func.count()).select_from(Insight)) or 0
        counts["incidents"] = await session.scalar(select(func.count()).select_from(Incident)) or 0
    return counts
