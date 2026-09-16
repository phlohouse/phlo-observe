"""Retention cleanup: delete expired raw payloads, old events, old runs."""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any

from observe_core.timestamps import utcnow
from sqlalchemy import delete, text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from phlo_observer.models import (
    AgentAnalysis,
    Event,
    Incident,
    IngestFailure,
    Insight,
    RawEvent,
    Run,
)
from phlo_observer.settings import ObserverSettings

_TERMINAL_INSIGHT_STATES = ("resolved", "suppressed", "expired")
_TERMINAL_INCIDENT_STATES = ("resolved", "suppressed")

# Session-level advisory lock so only one observer instance runs a retention
# pass at a time (spec §38). Chosen arbitrarily; must match across instances.
_RETENTION_LOCK_KEY = 0x70686C6F


@dataclass
class RetentionReport:
    """Rows deleted per table by one cleanup pass."""

    raw_events: int = 0
    events: int = 0
    runs: int = 0
    insights: int = 0
    incidents: int = 0
    ingest_failures: int = 0
    analyses: int = 0
    skipped: bool = False


async def run_retention_once(
    factory: async_sessionmaker[AsyncSession], settings: ObserverSettings
) -> RetentionReport:
    """Delete rows past their configured retention. Returns counts."""
    report = RetentionReport()
    now = utcnow()
    async with factory() as session, session.begin():
        held = await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(:key)"),
            {"key": _RETENTION_LOCK_KEY},
        )
        if not held:
            # Another instance holds the retention lock; it releases on commit.
            report.skipped = True
            return report
        raw_cutoff = now  # expires_at was stamped at insert
        result = await session.execute(delete(RawEvent).where(RawEvent.expires_at <= raw_cutoff))
        report.raw_events = _rowcount(result)
        # Events expire on ``received_at``, the observer-side clock —
        # producer-controlled ``observed_at`` is untrusted: a skewed producer
        # clock would otherwise expire fresh events instantly or pin stale
        # ones forever.
        event_cutoff = now - dt.timedelta(days=settings.event_retention_days)
        result = await session.execute(delete(Event).where(Event.received_at <= event_cutoff))
        report.events = _rowcount(result)
        run_cutoff = now - dt.timedelta(days=settings.run_retention_days)
        result = await session.execute(delete(Run).where(Run.updated_at <= run_cutoff))
        report.runs = _rowcount(result)
        # Terminal-state insights/incidents age out on the run window; open
        # items are live signal and never deleted by retention. Quarantined
        # payloads and recorded analyses expire on the same window so failed
        # payloads cannot be replayed forever.
        result = await session.execute(
            delete(Insight).where(
                Insight.state.in_(_TERMINAL_INSIGHT_STATES),
                Insight.updated_at <= run_cutoff,
            )
        )
        report.insights = _rowcount(result)
        result = await session.execute(
            delete(Incident).where(
                Incident.state.in_(_TERMINAL_INCIDENT_STATES),
                Incident.updated_at <= run_cutoff,
            )
        )
        report.incidents = _rowcount(result)
        result = await session.execute(
            delete(IngestFailure).where(IngestFailure.received_at <= run_cutoff)
        )
        report.ingest_failures = _rowcount(result)
        result = await session.execute(
            delete(AgentAnalysis).where(AgentAnalysis.created_at <= run_cutoff)
        )
        report.analyses = _rowcount(result)
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
    self_observe: bool = False,
) -> None:
    """Hourly-ish background retention; stopped by ``stop`` or app shutdown."""
    while stop is None or not stop.is_set():
        try:
            report = await run_retention_once(factory, settings)
            if self_observe:
                import observe_core  # noqa: PLC0415 - optional internal telemetry

                observe_core.event(
                    "observer.retention",
                    category="observer",
                    attributes={
                        "raw_events_deleted": report.raw_events,
                        "events_deleted": report.events,
                        "runs_deleted": report.runs,
                        "insights_deleted": report.insights,
                        "incidents_deleted": report.incidents,
                        "ingest_failures_deleted": report.ingest_failures,
                        "analyses_deleted": report.analyses,
                    },
                )
        except Exception:
            logging.getLogger("phlo_observer.retention").exception("retention pass failed")
        try:
            if stop is not None:
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            else:
                await asyncio.sleep(interval_seconds)
        except TimeoutError:
            continue
