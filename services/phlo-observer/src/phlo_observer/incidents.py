"""Incident grouping — spec §17.

Rule-created incidents group insights that share a signal: same entity,
same run, tight time window (§17.2). A critical run-failure insight opens
(or joins) an incident; subsequent insights on the same entity inside the
window attach to it rather than spawning noise.

Grouping decisions key on event time, not arrival order: every attach
records the producing event's ``(observed_at, event_id)`` position in
``attributes["members"]``, and an insight evaluated at position ``k`` only
sees the incident as it stood at ``k``. Incremental ingest and a rebuild
therefore group identically even when events arrive out of order.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

from observe_core.timestamps import parse_rfc3339, utcnow
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer.models import Incident, Insight
from phlo_observer.state_engine import _dump_key, _parse_key

GROUPING_WINDOW = dt.timedelta(minutes=30)
"""Insights on the same entity inside this window join the same incident."""

_SEVERITY_RANK = {"info": 30, "warn": 40, "warning": 40, "error": 50, "critical": 60}

_MAX_MEMBERS = 64
"""Bounded membership records per incident: the earliest 64 attachments."""


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


def _member_key(member: dict[str, Any]) -> tuple[Any, str]:
    parsed = _parse_key(member.get("key"))
    return parsed if parsed is not None else (dt.datetime.min.replace(tzinfo=dt.UTC), "")


def _members(incident: Incident) -> list[dict[str, Any]]:
    """Attachment records: ``{iid, entity, key: [iso, eid]}`` per attach.

    Rows written before member bookkeeping bootstrap their members at the
    incident's start time — the earliest position the incident is known to
    have existed — so events after creation still group against it.
    """
    raw = (incident.attributes or {}).get("members")
    if isinstance(raw, list):
        return [m for m in raw if isinstance(m, dict)]
    at = (incident.started_at or incident.updated_at or _last_signal(incident)).isoformat()
    boot = [
        {"iid": str(iid), "entity": None, "key": [at, ""]} for iid in incident.insight_ids or []
    ]
    boot += [{"iid": "", "entity": ent, "key": [at, ""]} for ent in incident.entities or []]
    return boot


def _members_before(incident: Incident, key: tuple[Any, str]) -> list[dict[str, Any]]:
    """Members attached at or before ``key`` — the incident as-of that position."""
    return [m for m in _members(incident) if _member_key(m) <= key]


def _first_member_key(incident: Incident) -> tuple[Any, str]:
    """The incident's creation position — deterministic candidate order."""
    members = _members(incident)
    return min(
        (_member_key(m) for m in members),
        default=(dt.datetime.max.replace(tzinfo=dt.UTC), ""),
    )


def _rewrite_membership(incident: Incident, members: list[dict[str, Any]]) -> None:
    """Persist members and rederive the row's entity/insight lists."""
    members = sorted(members, key=_member_key)[:_MAX_MEMBERS]
    incident.attributes = {**(incident.attributes or {}), "members": members}
    incident.entities = sorted({m["entity"] for m in members if m.get("entity")})
    incident.insight_ids = sorted({m["iid"] for m in members if m.get("iid")})


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
    signal_key: tuple[Any, str] | None = None,
) -> Incident | None:
    """Attach an insight to an open incident, or create one if warranted.

    Only error/critical insights drive grouping (§17.1); informational
    findings stay as insights without incident overhead. ``None`` means no
    incident was touched. ``open_incidents`` optionally supplies the
    batch's preloaded open rows instead of a per-insight query.

    ``signal_key`` is the producing event's ``(observed_at, event_id)``
    position: the insight only sees incidents as they stood at that
    position — attachments recorded by later-observed events are invisible
    to it — so late and out-of-order arrivals group exactly as a replay
    would.
    """
    # Only error/critical insights open or join incidents; informational
    # findings stay as insights without incident overhead.
    if insight.severity not in ("error", "critical"):
        return None
    now = utcnow()
    if signal_key is None:
        signal = _signal_time(insight)
        signal_key = (signal, str(insight.insight_id))
    if open_incidents is None:
        candidates = list(
            (
                await session.execute(
                    select(Incident).where(Incident.state == "open").with_for_update()
                )
            )
            .scalars()
            .all()
        )
    else:
        candidates = [c for c in open_incidents if c.state == "open"]
    # Earliest-created candidate first: replay builds incidents in event
    # order, and creation position is the order-stable proxy for it.
    candidates.sort(key=_first_member_key)
    for incident in candidates:
        prior = _members_before(incident, signal_key)
        if not prior:
            # The incident's known members all postdate this position: in
            # replay order the insight's finding here would have *created*
            # the incident (critical only) and absorbed those members. Join
            # it when the earliest member sits inside the window — farther
            # back, replay keeps a separate incident.
            if insight.severity != "critical":
                continue
            members = _members(incident)
            if not members:
                continue
            first = min(_member_key(m) for m in members)
            if first[0] - signal_key[0] > GROUPING_WINDOW:
                continue
            if not insight.entity_id or not any(
                m.get("entity") == insight.entity_id for m in members
            ):
                continue
            _attach(incident, insight, now, signal_key)
            return incident
        if isinstance((incident.attributes or {}).get("members"), list):
            last = max(_member_key(m)[0] for m in prior)
        else:
            # Legacy row: per-member positions are unknown; the recorded
            # high-water mark is the only anchor. The distance is symmetric
            # — a signal far on either side of it is unrelated to the
            # incident as replay would see it.
            last = _last_signal(incident)
        # Window on the incident's last signal *as of this position*: a
        # signal far newer than everything the incident then held is
        # unrelated to it, and signals attached later must not reach back.
        if abs(signal_key[0] - last) > GROUPING_WINDOW:
            continue
        if insight.entity_id and any(m.get("entity") == insight.entity_id for m in prior):
            _attach(incident, insight, now, signal_key)
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
        attributes={
            "created_by": "rule",
            "last_signal_at": signal_key[0].isoformat(),
            "members": [
                {
                    "iid": str(insight.insight_id),
                    "entity": insight.entity_id,
                    "key": _dump_key(signal_key),
                }
            ],
        },
        updated_at=now,
    )
    session.add(incident)
    if open_incidents is not None:
        open_incidents.append(incident)
    return incident


def detach_member(open_incidents: list[Incident], insight: Insight, key: tuple[Any, str]) -> None:
    """Drop the single membership an insight recorded at ``key``.

    Used when one producer migrates to a different timeline row: its
    attachment follows it — the new row re-groups at that position.
    """
    sid = str(insight.insight_id)
    for incident in open_incidents:
        members = _members(incident)
        kept = [m for m in members if not (m.get("iid") == sid and _member_key(m) == key)]
        if len(kept) != len(members):
            _rewrite_membership(incident, kept)
            incident.updated_at = utcnow()


def _attach(incident: Incident, insight: Insight, now: Any, key: tuple[Any, str]) -> None:
    members = _members(incident)
    entry = {
        "iid": str(insight.insight_id),
        "entity": insight.entity_id,
        "key": _dump_key(key),
    }
    if not any(m.get("iid") == entry["iid"] and _member_key(m) == key for m in members):
        members.append(entry)
    if key < min((_member_key(m) for m in members), default=key):
        # The replay creator is the earliest member's insight: when a late
        # finding lands before the members that built this row, the
        # incident takes its title/start, matching the rebuild.
        incident.title = insight.title
        incident.started_at = insight.created_at
    _rewrite_membership(incident, members)
    if _SEVERITY_RANK.get(insight.severity, 0) > _SEVERITY_RANK.get(incident.severity, 0):
        incident.severity = insight.severity
    # The freshness anchor only ever advances: an attached insight whose
    # signal predates the last one must not rewind the incident's window.
    last_signal = max(key[0], _last_signal(incident))
    incident.attributes = {
        **(incident.attributes or {}),
        "last_signal_at": last_signal.isoformat(),
    }
    incident.updated_at = now
