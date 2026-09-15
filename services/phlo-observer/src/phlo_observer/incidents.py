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

from observe_core.timestamps import utcnow
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer.models import Incident, Insight

GROUPING_WINDOW = dt.timedelta(minutes=30)
"""Insights on the same entity inside this window join the same incident."""

_SEVERITY_RANK = {"info": 30, "warn": 40, "warning": 40, "error": 50, "critical": 60}


async def group_insight(session: AsyncSession, insight: Insight) -> Incident | None:
    """Attach an insight to an open incident, or create one if warranted.

    Only error/critical insights drive grouping (§17.1); informational
    findings stay as insights without incident overhead. ``None`` means no
    incident was touched.
    """
    # Only error/critical insights open or join incidents; informational
    # findings stay as insights without incident overhead.
    if insight.severity not in ("error", "critical"):
        return None
    now = utcnow()
    candidates = (
        (await session.execute(select(Incident).where(Incident.state == "open"))).scalars().all()
    )
    window_start = now - GROUPING_WINDOW
    for incident in candidates:
        if incident.updated_at and incident.updated_at < window_start:
            continue
        if insight.entity_id and insight.entity_id in (incident.entities or []):
            _attach(incident, insight, now)
            return incident
        # Same-run grouping: evidence event ids of insight vs incident's
        # recorded run entities.
        if _shares_run(insight, incident):
            _attach(incident, insight, now)
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
        attributes={"created_by": "rule"},
        updated_at=now,
    )
    session.add(incident)
    return incident


def _shares_run(insight: Insight, incident: Incident) -> bool:
    return any(
        isinstance(ent, str) and ent.startswith("run://") and ent == insight.entity_id
        for ent in incident.entities or []
    )


def _attach(incident: Incident, insight: Insight, now: Any) -> None:
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
    incident.updated_at = now
