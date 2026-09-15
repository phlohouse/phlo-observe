"""Persistence: event insert with idempotency, queries, cursor pagination."""

from __future__ import annotations

import base64
import binascii
import datetime as dt
import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

from observe_core.models import EventEnvelope
from observe_core.timestamps import parse_rfc3339, utcnow
from sqlalchemy import asc, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DataError, IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer import alerts, baselines, incidents, insights, metrics, projections
from phlo_observer.correlate import correlation_method, link_trace_to_run
from phlo_observer.models import Event, RawEvent, Run, SchemaRecord

logger = logging.getLogger("phlo_observer.store")

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
DEFAULT_MAX_RAW_PAYLOAD_BYTES = 256 * 1024


class InvalidQuery(ValueError):
    """A query parameter (cursor, timestamp, filter) cannot be interpreted.

    The HTTP layer maps this to a ``400`` response; it must never surface as
    an unhandled internal error.
    """


@dataclass
class IngestResult:
    """Outcome of persisting a batch of canonical events."""

    accepted: int = 0
    rejected: int = 0
    duplicates: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)
    event_ids: list[str] = field(default_factory=list)


async def store_raw(
    session: AsyncSession,
    *,
    producer: str,
    source_kind: str,
    body: bytes,
    content_type: str = "application/json",
    adapter: str | None = None,
    source_version: str | None = None,
    keep_payload: bool = True,
    max_payload_bytes: int = DEFAULT_MAX_RAW_PAYLOAD_BYTES,
    retention_days: int = 14,
) -> RawEvent:
    """Preserve the incoming payload for debugging normalization.

    ``keep_payload=False`` (per-adapter opt-out, spec §36) retains only the
    SHA-256 digest; payloads over ``max_payload_bytes`` are likewise reduced
    to a digest rather than duplicating very large artifacts (spec §34.3).
    """
    now = utcnow()
    raw = RawEvent(
        received_at=now,
        producer=producer,
        source_kind=source_kind,
        source_version=source_version,
        content_type=content_type,
        payload=(
            _stored_payload(body, max_bytes=max_payload_bytes)
            if keep_payload
            else _payload_digest(body)
        ),
        payload_sha256=hashlib.sha256(body).hexdigest(),
        adapter=adapter,
        expires_at=now + dt.timedelta(days=retention_days),
    )
    session.add(raw)
    await session.flush()
    return raw


def _payload_digest(body: bytes) -> dict[str, Any]:
    return {"_encoding": "sha256", "sha256": hashlib.sha256(body).hexdigest()}


def _stored_payload(
    body: bytes, *, max_bytes: int = DEFAULT_MAX_RAW_PAYLOAD_BYTES
) -> dict[str, Any] | list[Any]:
    """Best-effort payload retention: JSON if parseable, else bounded text."""
    if len(body) > max_bytes:
        return _payload_digest(body)
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"_encoding": "utf8", "text": body[:65536].decode("utf-8", "replace")}
    return parsed


def _event_row(data: dict[str, Any], received_at: Any, raw_event_id: uuid.UUID | None) -> Event:
    corr = data.get("correlation") or {}
    service = data.get("service") or {}
    return Event(
        event_id=uuid.UUID(str(data["event_id"])),
        schema_version=data["schema_version"],
        event=data["event"],
        category=data["category"],
        outcome=data["outcome"],
        severity=data["severity"],
        delivery=data["delivery"],
        started_at=data.get("started_at"),
        ended_at=data.get("ended_at"),
        duration_ms=data.get("duration_ms"),
        observed_at=data["observed_at"],
        received_at=received_at,
        service_name=service.get("name"),
        service_version=service.get("version"),
        environment=service.get("environment"),
        trace_id=corr.get("trace_id"),
        span_id=corr.get("span_id"),
        run_id=corr.get("run_id"),
        job_id=corr.get("job_id"),
        invocation_id=corr.get("invocation_id"),
        asset_key=corr.get("asset_key"),
        partition_key=corr.get("partition_key"),
        branch=corr.get("branch"),
        table_name=corr.get("table"),
        snapshot_id=corr.get("snapshot_id"),
        pipeline=corr.get("pipeline"),
        correlation_method=correlation_method(data),
        attributes=data.get("attributes") or {},
        error=data.get("error"),
        source=data.get("source") or {},
        payload=data,
        raw_event_id=raw_event_id,
        entities=data.get("entities") or {},
        tags=data.get("tags") or {},
        contract_id=(data.get("contract") or {}).get("schema_id"),
    )


def _parse_dt(value: Any) -> Any:
    if isinstance(value, str):
        return parse_rfc3339(value)
    return value


async def persist_events(
    session: AsyncSession,
    event_dicts: list[dict[str, Any]],
    *,
    raw_event_id: uuid.UUID | None = None,
    source_indices: list[int] | None = None,
    stream: Any = None,
    alert_urls: list[str] | None = None,
    alert_tasks: set[Any] | None = None,
) -> IngestResult:
    """Insert canonical events idempotently and update run projections.

    Duplicate ``event_id`` with identical content: counted and skipped.
    Duplicate ``event_id`` with different content: reported as a conflict;
    the original row is never overwritten.

    ``source_indices`` maps each event back to its index in the source
    payload (adapters drop invalid items, compacting the list), so reported
    error indices refer to the caller's payload positions.

    The common case is a bulk insert inside a single savepoint (one
    ``INSERT`` round-trip via executemany). Any batch-level failure falls
    back to per-item savepoints so one bad row rejects only itself.
    """
    result = IngestResult()
    received_at = utcnow()
    if source_indices is not None and len(source_indices) != len(event_dicts):
        # An adapter bug must not break ingest; fall back to list positions.
        logger.warning("source_indices length mismatch; using event list positions")
        source_indices = None
    staged: list[tuple[int, Event, dict[str, Any]]] = []
    for index, data in enumerate(event_dicts):
        if source_indices is not None:
            index = source_indices[index]
        try:
            row = _event_row(data, received_at, raw_event_id)
            # ORM columns need real datetimes; canonical dicts carry ISO strings.
            row.observed_at = _parse_dt(row.observed_at)
            row.started_at = _parse_dt(row.started_at)
            row.ended_at = _parse_dt(row.ended_at)
        except (KeyError, TypeError, ValueError) as exc:
            result.rejected += 1
            result.errors.append(
                {
                    "index": index,
                    "code": "SCHEMA_INVALID",
                    "message": f"malformed canonical event: {exc}",
                }
            )
            continue
        staged.append((index, row, data))
    accepted_rows = await _insert_rows(session, staged, result)
    correlated = [row for row in accepted_rows if row.run_id or row.trace_id]
    if accepted_rows:
        # One savepoint for the whole projection pass: event rows are already
        # durable, so a projection failure is logged rather than rejecting
        # anything. Projections are derived state and can be rebuilt.
        try:
            async with session.begin_nested():
                for row in correlated:
                    await link_trace_to_run(session, row)
                # Pre-create missing run projections in one statement so the
                # per-event update below is a single locked read, not a
                # savepoint-guarded insert per run_id.
                run_ids = {row.run_id for row in correlated if row.run_id}
                if run_ids:
                    await session.execute(
                        pg_insert(Run)
                        .values(
                            [
                                {
                                    "run_id": run_id,
                                    "status": "unknown",
                                    "updated_at": received_at,
                                    "summary": {},
                                    "provenance": {},
                                }
                                for run_id in run_ids
                            ]
                        )
                        .on_conflict_do_nothing(index_elements=["run_id"])
                    )
                await projections.update_run_projections(session, correlated)
                await projections.apply_events_batch(session, accepted_rows)
                for row in accepted_rows:
                    event = projections._event_view(row)
                    # Insights evaluate against baselines BEFORE this event's
                    # sample joins them — an observation must not judge itself.
                    findings = await insights.evaluate(session, event)
                    if findings:
                        new_insights = await insights.record_findings(session, event, findings)
                        for insight in new_insights:
                            incident = await incidents.group_insight(session, insight)
                            if stream is not None:
                                stream.publish(
                                    "insight.opened",
                                    {
                                        "insight_id": str(insight.insight_id),
                                        "rule": insight.rule_id,
                                        "entity": insight.entity_id,
                                        "severity": insight.severity,
                                    },
                                )
                                if incident is not None:
                                    stream.publish(
                                        "incident.updated",
                                        {
                                            "incident_id": str(incident.incident_id),
                                            "state": incident.state,
                                        },
                                    )
                            if alert_urls and alert_tasks is not None:
                                await alerts.notify(
                                    alert_urls,
                                    "insight",
                                    {
                                        "insight_id": str(insight.insight_id),
                                        "rule": insight.rule_id,
                                        "title": insight.title,
                                        "severity": insight.severity,
                                        "entity": insight.entity_id,
                                    },
                                    tasks=alert_tasks,
                                    cooldown_key=insight.dedupe_key or str(insight.insight_id),
                                )
                    await insights.resolve_for_event(session, event)
                    await baselines.update_baselines(session, event)
                if stream is not None:
                    for row in correlated:
                        if row.run_id:
                            stream.publish(
                                "run.changed",
                                {"run_id": row.run_id, "event": row.event},
                            )
                # Register contract schemas seen on envelopes (spec §8.3):
                # an event carrying contract.schema_id upserts the registry
                # row so schemas stay queryable without a separate publish.
                contract_refs = {
                    (row.payload.get("contract") or {}).get("schema_id"): row.payload.get(
                        "contract"
                    )
                    for row in accepted_rows
                    if row.contract_id
                }
                for schema_id, ref in contract_refs.items():
                    await session.execute(
                        pg_insert(SchemaRecord)
                        .values(
                            schema_id=schema_id,
                            version=str((ref or {}).get("version", "1")),
                            schema_hash=(ref or {}).get("schema_hash") or "",
                            schema_json={},
                            registered_at=received_at,
                        )
                        .on_conflict_do_nothing(index_elements=["schema_id"])
                    )
                await session.flush()
        except SQLAlchemyError:
            logger.warning(
                "correlation/projection update failed for %d events",
                len(accepted_rows),
                exc_info=True,
            )
    return result


async def _insert_rows(
    session: AsyncSession,
    staged: list[tuple[int, Event, dict[str, Any]]],
    result: IngestResult,
) -> list[Event]:
    """Persist staged rows: bulk fast path, per-item fallback on failure."""
    if not staged:
        return []
    rows = [row for _, row, _ in staged]
    try:
        async with session.begin_nested():
            session.add_all(rows)
    except (IntegrityError, DataError):
        pass  # isolate the bad rows one by one
    else:
        for row in rows:
            result.accepted += 1
            result.event_ids.append(str(row.event_id))
            metrics.EVENTS_STORED.inc()
        return rows
    accepted: list[Event] = []
    for index, row, data in staged:
        try:
            async with session.begin_nested():
                session.add(row)
        except IntegrityError:
            existing = await session.get(Event, row.event_id)
            if existing is not None and _rows_match(existing, data):
                result.duplicates += 1
                metrics.DUPLICATE_EVENTS.inc()
            else:
                result.rejected += 1
                result.errors.append(
                    {
                        "index": index,
                        "code": "INTEGRITY_CONFLICT",
                        "message": f"event_id {row.event_id} already exists with different content",
                    }
                )
            continue
        except DataError as exc:
            result.rejected += 1
            result.errors.append(
                {
                    "index": index,
                    "code": "SCHEMA_INVALID",
                    "message": f"event cannot be stored: {exc.orig}",
                }
            )
            continue
        accepted.append(row)
        result.accepted += 1
        result.event_ids.append(str(row.event_id))
        metrics.EVENTS_STORED.inc()
    return accepted


def _rows_match(existing: Event, incoming: dict[str, Any]) -> bool:
    """Compare a stored row's canonical payload to the incoming duplicate.

    Both sides are normalized through ``EventEnvelope`` so key order, missing
    vs None fields, and datetime representations cannot produce false
    conflicts.
    """
    try:
        stored = EventEnvelope.model_validate(existing.payload).to_canonical_dict()
        new = EventEnvelope.model_validate(incoming).to_canonical_dict()
    except Exception:
        return False
    return stored == new


def _fmt(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        if isinstance(value, dt.datetime) and value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return value.isoformat()
    return value


# -- queries ----------------------------------------------------------------


def _cursor_encode(observed_at: Any, event_id: uuid.UUID) -> str:
    raw = f"{_fmt(observed_at)}|{event_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def _cursor_decode(cursor: str) -> tuple[Any, uuid.UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        ts, eid = raw.rsplit("|", 1)
        return parse_rfc3339(ts), uuid.UUID(eid)
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise InvalidQuery(f"invalid cursor: {exc}") from exc


def _parse_dt_filter(value: Any, name: str) -> Any:
    try:
        return _parse_dt(value)
    except (ValueError, TypeError) as exc:
        raise InvalidQuery(f"invalid {name} timestamp {value!r}: {exc}") from exc


@dataclass
class EventPage:
    """One page of query results."""

    items: list[Event]
    next_cursor: str | None


async def query_events(
    session: AsyncSession,
    *,
    filters: dict[str, Any],
    cursor: str | None = None,
    limit: int = DEFAULT_PAGE_SIZE,
) -> EventPage:
    """Filter + cursor-paginate events ordered by (observed_at, event_id)."""
    limit = min(max(1, limit), MAX_PAGE_SIZE)
    stmt = select(Event).order_by(asc(Event.observed_at), asc(Event.event_id))
    for column, key in (
        (Event.event, "event"),
        (Event.category, "category"),
        (Event.outcome, "outcome"),
        (Event.severity, "severity"),
        (Event.service_name, "service"),
        (Event.environment, "environment"),
        (Event.run_id, "run_id"),
        (Event.asset_key, "asset_key"),
        (Event.partition_key, "partition_key"),
        (Event.branch, "branch"),
        (Event.table_name, "table"),
        (Event.trace_id, "trace_id"),
    ):
        if filters.get(key):
            stmt = stmt.where(column == filters[key])
    if filters.get("since"):
        stmt = stmt.where(Event.observed_at >= _parse_dt_filter(filters["since"], "since"))
    if filters.get("until"):
        stmt = stmt.where(Event.observed_at <= _parse_dt_filter(filters["until"], "until"))
    if filters.get("q"):
        # Postgres-first text search (spec §23): substring match over the
        # highest-signal text columns rather than a full tsvector index —
        # adequate until the §24.4 volume thresholds force a re-evaluation.
        from sqlalchemy import String, cast, or_  # noqa: PLC0415

        term = f"%{filters['q']}%"
        stmt = stmt.where(
            or_(
                Event.event.ilike(term),
                Event.table_name.ilike(term),
                Event.asset_key.ilike(term),
                Event.service_name.ilike(term),
                cast(Event.error["message"], String).ilike(term),
            )
        )
    if cursor:
        cur_ts, cur_id = _cursor_decode(cursor)
        stmt = stmt.where(
            (Event.observed_at > cur_ts)
            | ((Event.observed_at == cur_ts) & (Event.event_id > cur_id))
        )
    stmt = stmt.limit(limit + 1)
    rows = list((await session.execute(stmt)).scalars())
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_cursor = _cursor_encode(last.observed_at, last.event_id)
    return EventPage(items=rows, next_cursor=next_cursor)


async def query_runs(
    session: AsyncSession,
    *,
    status: str | None = None,
    job_name: str | None = None,
    cursor: str | None = None,
    limit: int = DEFAULT_PAGE_SIZE,
) -> tuple[list[Run], str | None]:
    """Paginate run projections; cursor is the run_id (stable, unique)."""
    limit = min(max(1, limit), MAX_PAGE_SIZE)
    stmt = select(Run).order_by(Run.run_id.asc())
    if status:
        stmt = stmt.where(Run.status == status)
    if job_name:
        stmt = stmt.where(Run.job_name == job_name)
    if cursor:
        try:
            marker = base64.urlsafe_b64decode(cursor.encode()).decode()
        except (binascii.Error, UnicodeDecodeError) as exc:
            raise InvalidQuery(f"invalid cursor: {exc}") from exc
        stmt = stmt.where(Run.run_id > marker)
    rows = list((await session.execute(stmt.limit(limit + 1))).scalars())
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = base64.urlsafe_b64encode(rows[-1].run_id.encode()).decode()
    return rows, next_cursor


async def probe_events_table(session: AsyncSession) -> None:
    """Cheap schema-compat probe: raises if the ``events`` table is missing."""
    await session.execute(select(Event.event_id).limit(1))
