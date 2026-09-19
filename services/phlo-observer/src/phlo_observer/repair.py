"""Durable projection failure reporting and explicit maintenance repair."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer.models import Event, ProjectionFailure
from phlo_observer.projections import lock_rebuild, rebuild_projections


async def projection_status(session: AsyncSession) -> dict[str, Any]:
    """Report committed projection gaps without changing ingest readiness."""
    row = (
        await session.execute(
            select(
                func.count(ProjectionFailure.failure_id),
                func.coalesce(func.sum(ProjectionFailure.event_count), 0),
                func.min(ProjectionFailure.occurred_at),
            )
        )
    ).one()
    return {
        "status": "degraded" if row[0] else "current",
        "pending_batches": row[0],
        "pending_events": row[1],
        "oldest_failure_at": row[2],
    }


async def repair_projections(session: AsyncSession) -> dict[str, Any]:
    """Rebuild projections and clear only the gaps observed under the lock.

    The caller owns the transaction. Any rebuild/identity error leaves both
    the old projections and repair records intact. Failures committed after
    this snapshot remain pending, even if their events enter the replay.
    """
    await lock_rebuild(session)
    failures = list((await session.execute(select(ProjectionFailure).with_for_update())).scalars())
    if not failures:
        return {"repaired_batches": 0, "repaired_events": 0}
    for failure in failures:
        ids = [uuid.UUID(value) for value in failure.event_ids]
        retained = await session.scalar(
            select(func.count()).select_from(Event).where(Event.event_id.in_(ids))
        )
        if retained != len(ids):
            raise ValueError(
                f"projection failure {failure.failure_id} references expired events; "
                "restore the canonical archive before repair"
            )
    counts = await rebuild_projections(session)
    # A failed projection savepoint releases its shared advisory lock. Its
    # durable failure marker can therefore commit while this rebuild runs;
    # deleting by the captured IDs avoids acknowledging unseen work.
    for failure in failures:
        await session.execute(
            delete(ProjectionFailure).where(ProjectionFailure.failure_id == failure.failure_id)
        )
    return {
        "repaired_batches": len(failures),
        "repaired_events": sum(failure.event_count for failure in failures),
        "rebuild": counts,
    }
