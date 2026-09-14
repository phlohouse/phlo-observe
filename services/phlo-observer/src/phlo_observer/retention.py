"""Retention cleanup: delete expired raw payloads, old events, old runs."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any

from observe_core.timestamps import utcnow
from sqlalchemy import delete
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from phlo_observer.models import Event, RawEvent, Run
from phlo_observer.settings import ObserverSettings


@dataclass
class RetentionReport:
    """Rows deleted per table by one cleanup pass."""

    raw_events: int = 0
    events: int = 0
    runs: int = 0


async def run_retention_once(
    factory: async_sessionmaker[AsyncSession], settings: ObserverSettings
) -> RetentionReport:
    """Delete rows past their configured retention. Returns counts."""
    report = RetentionReport()
    now = utcnow()
    async with factory() as session, session.begin():
        raw_cutoff = now  # expires_at was stamped at insert
        result = await session.execute(delete(RawEvent).where(RawEvent.expires_at <= raw_cutoff))
        report.raw_events = _rowcount(result)
        event_cutoff = now - dt.timedelta(days=settings.event_retention_days)
        result = await session.execute(delete(Event).where(Event.observed_at <= event_cutoff))
        report.events = _rowcount(result)
        run_cutoff = now - dt.timedelta(days=settings.run_retention_days)
        result = await session.execute(delete(Run).where(Run.updated_at <= run_cutoff))
        report.runs = _rowcount(result)
    return report


def _rowcount(result: Any) -> int:
    """Rowcount for a DML result (typed Result, runtime CursorResult)."""
    return result.rowcount if isinstance(result, CursorResult) else 0


async def retention_loop(
    factory: async_sessionmaker[AsyncSession],
    settings: ObserverSettings,
    *,
    interval_seconds: float = 3600.0,
    stop: asyncio.Event | None = None,
) -> None:
    """Hourly-ish background retention; stopped by ``stop`` or app shutdown."""
    while stop is None or not stop.is_set():
        try:
            await run_retention_once(factory, settings)
        except Exception:
            logging.getLogger("phlo_observer.retention").exception("retention pass failed")
        try:
            if stop is not None:
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            else:
                await asyncio.sleep(interval_seconds)
        except TimeoutError:
            continue
