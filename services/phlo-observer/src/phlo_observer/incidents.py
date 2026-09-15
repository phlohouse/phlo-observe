"""Incident grouping — spec §17.

Rule-created incidents group insights that share a signal: same entity,
same run, tight time window (§17.2). A critical run-failure insight opens
(or joins) an incident; subsequent insights on the same entity inside the
window attach to it rather than spawning noise.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from observe_core.timestamps import parse_rfc3339, utcnow
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer.models import Incident, Insight

GROUPING_WINDOW = dt.timedelta(minutes=30)
"""Insights on the same entity inside this window join the same incident."""

_SEVERITY_RANK = {"info": 30, "warn": 40, "warning": 40, "error": 50, "critical": 60}


def _signal_time(insight: Insight) -> dt.datetime:
    """Event-time anchor: the newest producing event's observed_at.

    ``last_observed_at`` advances on each repeat finding; ``observed_at``
    stays at first detection. Grouping windows on event time (not the wall
    clock) so a projection rebuild replaying old history groups identically
    to live ingest.
    """
    attrs = insight.attributes or {}
    raw = attrs.get("last_observed_at") or attrs.get("observed_at")
    if raw:
        try:
            parsed = parse_rfc3339(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.UTC)
            return parsed
        except (ValueError, TypeError):
            pass
    return insight.created_at or utcnow()


def _last_signal(incident: Incident) -> dt.datetime:
    raw = (incident.attributes or {}).get("last_signal_at")
    if raw:
        try:
            parsed = parse_rfc3339(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.UTC)
            return parsed
        except (ValueError, TypeError):
            pass
    return incident.updated_at or utcnow()


async def group_insight(
    session: AsyncSession,
    insight: Insight,
    *,
    open_incidents: list[Incident] | None = None,
) -> Incident | None:
    """Attach an insight to an open incident, or create one if warranted.

    Only error/critical insights drive grouping (§17.1); informational
    findings stay as insights without incident overhead. ``None`` means no
    incident was touched. ``open_incidents`` optionally supplies the
    batch's preloaded open rows instead of a per-insight query.
    """
    # Only error/critical insights open or join incidents; informational
    # findings stay as insights without incident overhead.
    if insight.severity not in ("error", "critical"):
        return None
    now = utcnow()
    signal = _signal_time(insight)
    if open_incidents is None:
        candidates = list(
            (await session.execute(select(Incident).where(Incident.state == "open")))
            .scalars()
            .all()
        )
    else:
        candidates = [c for c in open_incidents if c.state == "open"]
    for incident in candidates:
        # Symmetric window: a signal far OLDER than the incident's last
        # signal is just as unrelated as one far newer — late or replayed
        # findings must not attach to a current incident.
        if abs(signal - _last_signal(incident)) > GROUPING_WINDOW:
            continue
        if insight.entity_id and insight.entity_id in (incident.entities or []):
            _attach(incident, insight, now, signal)
            return incident
        # Same-run grouping: evidence event ids of insight vs incident's
        # recorded run entities.
        if _shares_run(insight, incident):
            _attach(incident, insight, now, signal)
            return incident
    # No candidate matched: auto-create only for critical insights (§17.1).
    if insight.severity != "critical":
        return None
    incident = Incident(
        incident_id=uuid.uuid4(),
        title=insight.title,
        state="open",
        severity=insight.severity,
        started_at=insight.created_at,
        entities=[insight.entity_id] if insight.entity_id else [],
        insight_ids=[str(insight.insight_id)],
        timeline={"opened_at": now.isoformat()},
        impact={},
        attributes={"created_by": "rule", "last_signal_at": signal.isoformat()},
        updated_at=now,
    )
    session.add(incident)
    if open_incidents is not None:
        open_incidents.append(incident)
    return incident


def _shares_run(insight: Insight, incident: Incident) -> bool:
    return any(
        isinstance(ent, str) and ent.startswith("run://") and ent == insight.entity_id
        for ent in incident.entities or []
    )


def _attach(incident: Incident, insight: Insight, now: Any, signal: dt.datetime) -> None:
    ids = list(incident.insight_ids or [])
    sid = str(insight.insight_id)
    if sid not in ids:
        ids.append(sid)
        incident.insight_ids = ids
    entities = list(incident.entities or [])
    if insight.entity_id and insight.entity_id not in entities:
        entities.append(insight.entity_id)
        incident.entities = entities
    if _SEVERITY_RANK.get(insight.severity, 0) > _SEVERITY_RANK.get(incident.severity, 0):
        incident.severity = insight.severity
    # The freshness anchor only ever advances: an attached insight whose
    # signal predates the last one must not rewind the incident's window.
    last_signal = max(signal, _last_signal(incident))
    incident.attributes = {
        **(incident.attributes or {}),
        "last_signal_at": last_signal.isoformat(),
    }
    incident.updated_at = now
