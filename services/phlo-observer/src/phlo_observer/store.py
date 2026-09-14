"""Persistence: event insert with idempotency, queries, cursor pagination."""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from observe_core.models import EventEnvelope
from observe_core.timestamps import parse_rfc3339, utcnow
from sqlalchemy import asc, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer import metrics
from phlo_observer.correlate import correlation_method, link_trace_to_run, update_run_projection
from phlo_observer.models import Event, RawEvent, Run

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000


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
    retention_days: int = 14,
) -> RawEvent:
    """Preserve the incoming payload for debugging normalization."""
    now = utcnow()
    raw = RawEvent(
        received_at=now,
        producer=producer,
        source_kind=source_kind,
        source_version=source_version,
        content_type=content_type,
        payload=_stored_payload(body),
        payload_sha256=hashlib.sha256(body).hexdigest(),
        adapter=adapter,
        expires_at=now + dt.timedelta(days=retention_days),
    )
    session.add(raw)
    await session.flush()
    return raw


def _stored_payload(body: bytes) -> dict[str, Any] | list[Any]:
    """Best-effort payload retention: JSON if parseable, else bounded text."""
    if len(body) > 64 * 1024:
        return {"_encoding": "sha256", "sha256": hashlib.sha256(body).hexdigest()}
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
) -> IngestResult:
    """Insert canonical events idempotently and update run projections.

    Duplicate ``event_id`` with identical content: counted and skipped.
    Duplicate ``event_id`` with different content: reported as a conflict;
    the original row is never overwritten.
    """
    result = IngestResult()
    received_at = utcnow()
    for index, data in enumerate(event_dicts):
        row = _event_row(data, received_at, raw_event_id)
        # ORM columns need real datetimes; canonical dicts carry ISO strings.
        row.observed_at = _parse_dt(row.observed_at)
        row.started_at = _parse_dt(row.started_at)
        row.ended_at = _parse_dt(row.ended_at)
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
        await link_trace_to_run(session, row)
        await update_run_projection(session, row)
        result.accepted += 1
        result.event_ids.append(str(row.event_id))
        metrics.EVENTS_STORED.inc()
    return result


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
    raw = base64.urlsafe_b64decode(cursor.encode()).decode()
    ts, eid = raw.rsplit("|", 1)
    return parse_rfc3339(ts), uuid.UUID(eid)


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
        stmt = stmt.where(Event.observed_at >= _parse_dt(filters["since"]))
    if filters.get("until"):
        stmt = stmt.where(Event.observed_at <= _parse_dt(filters["until"]))
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
        marker = base64.urlsafe_b64decode(cursor.encode()).decode()
        stmt = stmt.where(Run.run_id > marker)
    rows = list((await session.execute(stmt.limit(limit + 1))).scalars())
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = base64.urlsafe_b64encode(rows[-1].run_id.encode()).decode()
    return rows, next_cursor


async def count_events(session: AsyncSession) -> int:
    """Total normalized events stored."""
    return int((await session.execute(select(func.count(Event.event_id)))).scalar_one())
