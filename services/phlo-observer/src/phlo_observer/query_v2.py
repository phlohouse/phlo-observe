"""V2 operational query layer — spec §20, §22.

Answers are shaped around operational questions (what failed, what changed,
what's impacted) rather than storage tables. Every response carries evidence
IDs so agent consumers can ground conclusions (spec §19.3).
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import String, asc, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer.models import (
    Asset,
    Entity,
    Event,
    Incident,
    Insight,
    Relationship,
    Run,
)
from phlo_observer.store import _fmt

_CHANGE_CATEGORIES = {"infrastructure", "storage"}
_CHANGE_VERBS = {"deploy", "commit", "create", "migrate", "release", "upgrade"}


def _event_json(row: Event) -> dict[str, Any]:
    return {
        "event_id": str(row.event_id),
        "event": row.event,
        "category": row.category,
        "outcome": row.outcome,
        "severity": row.severity,
        "observed_at": _fmt(row.observed_at),
        "started_at": _fmt(row.started_at),
        "ended_at": _fmt(row.ended_at),
        "duration_ms": row.duration_ms,
        "correlation": {
            "run_id": row.run_id,
            "asset_key": row.asset_key,
            "table": row.table_name,
            "branch": row.branch,
            "trace_id": row.trace_id,
        },
        "entities": row.entities or {},
        "tags": row.tags or {},
        "attributes": row.attributes or {},
        "error": row.error,
        "source": row.source or {},
    }


def _run_json(run: Run) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "status": run.status,
        "job_name": run.job_name,
        "service_name": run.service_name,
        "environment": run.environment,
        "branch": run.branch,
        "trigger": run.trigger,
        "started_at": _fmt(run.started_at),
        "ended_at": _fmt(run.ended_at),
        "duration_ms": run.duration_ms,
        "event_count": run.event_count,
        "error_count": run.error_count,
        "warning_count": run.warning_count,
        "asset_count": run.asset_count,
        "summary": run.summary,
        "provenance": run.provenance or {},
        "updated_at": _fmt(run.updated_at),
    }


def _insight_json(row: Insight) -> dict[str, Any]:
    return {
        "insight_id": str(row.insight_id),
        "kind": (row.attributes or {}).get("kind"),
        "severity": row.severity,
        "state": row.state,
        "entity": row.entity_id,
        "title": row.title,
        "summary": (row.attributes or {}).get("summary"),
        "evidence": row.evidence_event_ids or [],
        "rule": row.rule_id,
        "rule_version": row.rule_version,
        "detected_at": _fmt(row.created_at),
        "updated_at": _fmt(row.updated_at),
        "recommended_action": row.recommended_action,
    }


def _incident_json(row: Incident) -> dict[str, Any]:
    return {
        "incident_id": str(row.incident_id),
        "title": row.title,
        "state": row.state,
        "severity": row.severity,
        "started_at": _fmt(row.started_at),
        "resolved_at": _fmt(row.resolved_at),
        "entities": row.entities or [],
        "insight_ids": row.insight_ids or [],
        "impact": row.impact or {},
        "updated_at": _fmt(row.updated_at),
    }


# -- run queries ------------------------------------------------------------


_MAX_RUN_EVENTS = 10_000


async def run_events(
    session: AsyncSession, run_id: str, *, limit: int = _MAX_RUN_EVENTS
) -> list[Event]:
    """Events correlated to one run, ordered and bounded."""
    rows = (
        await session.execute(
            select(Event)
            .where(Event.run_id == run_id)
            .order_by(asc(Event.observed_at), asc(Event.event_id))
            .limit(limit)
        )
    ).scalars()
    return list(rows)


async def get_run_v2(session: AsyncSession, run_id: str) -> dict[str, Any] | None:
    """Run projection plus the entities its edges touch."""
    run = await session.get(Run, run_id)
    if run is None:
        return None
    entity_rows = (
        await session.execute(
            select(Relationship).where(
                Relationship.from_entity.like("run://%"),
                Relationship.to_entity.like("%/%"),
            )
        )
    ).scalars()
    # Edges anchored on any run entity for this run_id.
    run_entities = {e.to_entity for e in entity_rows if e.from_entity.endswith(f"/{run_id}")} | {
        e.from_entity for e in entity_rows if e.to_entity.endswith(f"/{run_id}")
    }
    return {**_run_json(run), "entities": sorted(run_entities)}


async def run_failures(session: AsyncSession, run_id: str) -> dict[str, Any] | None:
    """Failure/error events for one run, with evidence IDs."""
    run = await session.get(Run, run_id)
    if run is None:
        return None
    rows = list(
        (
            await session.execute(
                select(Event)
                .where(
                    Event.run_id == run_id,
                    or_(
                        Event.outcome == "failure",
                        # JSON null is stored, not SQL NULL — check both.
                        Event.error.is_not(None),
                    ),
                    func.coalesce(cast(Event.error, String), "null") != "null",
                )
                .order_by(asc(Event.observed_at), asc(Event.event_id))
                .limit(_MAX_RUN_EVENTS)
            )
        ).scalars()
    )
    return {
        "run_id": run_id,
        "failures": [_event_json(e) for e in rows],
        "evidence": [str(e.event_id) for e in rows],
    }


def _is_change_event(row: Event) -> bool:
    verb = (row.event or "").rsplit(".", 1)[-1]
    return row.category in _CHANGE_CATEGORIES or verb in _CHANGE_VERBS


async def run_changes(
    session: AsyncSession, run_id: str, *, window: dt.timedelta | None = None
) -> dict[str, Any] | None:
    """Change events in the window before/around a run (spec §18)."""
    run = await session.get(Run, run_id)
    if run is None:
        return None
    window = window or dt.timedelta(hours=24)
    start = run.started_at or run.updated_at
    if start is None:
        return {"run_id": run_id, "changes": [], "evidence": []}
    since = start - window
    rows = (
        await session.execute(
            select(Event)
            .where(Event.observed_at >= since, Event.observed_at <= start)
            .order_by(asc(Event.observed_at))
        )
    ).scalars()
    changes = [e for e in rows if _is_change_event(e)]
    return {
        "run_id": run_id,
        "window_hours": window.total_seconds() / 3600,
        "changes": [_event_json(e) for e in changes],
        "evidence": [str(e.event_id) for e in changes],
    }


async def run_impact(session: AsyncSession, run_id: str) -> dict[str, Any] | None:
    """Downstream entities touched via the run's relationship edges."""
    run = await session.get(Run, run_id)
    if run is None:
        return None
    suffix = f"/{run_id}"
    edges = (
        await session.execute(
            select(Relationship).where(Relationship.from_entity.like(f"%{suffix}"))
        )
    ).scalars()
    impacted: dict[str, list[str]] = {}
    for edge in edges:
        impacted.setdefault(edge.relationship_type, []).append(edge.to_entity)
    evidence = sorted({eid for edge in edges for eid in (edge.source_event_ids or [])})
    return {"run_id": run_id, "impact": impacted, "evidence": evidence}


async def compare_runs(session: AsyncSession, run_a: str, run_b: str) -> dict[str, Any] | None:
    """Side-by-side comparison of two run projections (spec §22)."""
    a = await session.get(Run, run_a)
    b = await session.get(Run, run_b)
    if a is None or b is None:
        return None
    delta_ms = None
    if a.duration_ms is not None and b.duration_ms is not None:
        delta_ms = b.duration_ms - a.duration_ms
    return {
        "a": _run_json(a),
        "b": _run_json(b),
        "duration_delta_ms": delta_ms,
        "status_changed": a.status != b.status,
        "error_delta": (b.error_count or 0) - (a.error_count or 0),
    }


# -- asset queries ------------------------------------------------------------


async def get_asset_v2(session: AsyncSession, entity_id: str) -> dict[str, Any] | None:
    """Asset projection row for one canonical entity id."""
    asset = await session.get(Asset, entity_id)
    if asset is None:
        return None
    entity = await session.get(Entity, entity_id)
    return {
        "entity_id": asset.entity_id,
        "asset_key": asset.asset_key,
        "status": asset.status,
        "last_materialized_at": _fmt(asset.last_materialized_at),
        "last_event_at": _fmt(asset.last_event_at),
        "freshness_sla_seconds": asset.freshness_sla_seconds,
        "first_seen_at": _fmt(entity.first_seen_at) if entity else None,
        "provenance": asset.provenance or {},
        "updated_at": _fmt(asset.updated_at),
    }


async def asset_health(session: AsyncSession, entity_id: str) -> dict[str, Any] | None:
    """Asset health: status, freshness, open insights."""
    asset = await session.get(Asset, entity_id)
    if asset is None:
        return None
    open_insights = (
        (
            await session.execute(
                select(Insight).where(
                    Insight.entity_id == entity_id,
                    Insight.state.in_(["open", "acknowledged"]),
                )
            )
        )
        .scalars()
        .all()
    )
    freshness: dict[str, Any] = {"sla_seconds": asset.freshness_sla_seconds}
    if asset.last_materialized_at is not None and asset.freshness_sla_seconds:
        last = asset.last_materialized_at
        if last.tzinfo is None:
            last = last.replace(tzinfo=dt.UTC)
        age = (dt.datetime.now(dt.UTC) - last).total_seconds()
        freshness["age_seconds"] = age
        freshness["breached"] = age > asset.freshness_sla_seconds
    return {
        "entity_id": entity_id,
        "status": asset.status,
        "freshness": freshness,
        "open_insights": [_insight_json(i) for i in open_insights],
    }


async def asset_history(
    session: AsyncSession, entity_id: str, *, limit: int = 200
) -> dict[str, Any] | None:
    """Ordered event history for one asset."""
    asset = await session.get(Asset, entity_id)
    if asset is None:
        return None
    asset_key = asset.asset_key
    rows = (
        await session.execute(
            select(Event)
            .where(Event.asset_key == asset_key)
            .order_by(asc(Event.observed_at))
            .limit(limit)
        )
    ).scalars()
    return {
        "entity_id": entity_id,
        "asset_key": asset_key,
        "history": [_event_json(e) for e in rows],
    }


async def asset_lineage(session: AsyncSession, entity_id: str) -> dict[str, Any] | None:
    """Upstream and downstream edges for one entity (spec §14)."""
    asset = await session.get(Asset, entity_id)
    if asset is None:
        return None
    upstream = (
        await session.execute(select(Relationship).where(Relationship.to_entity == entity_id))
    ).scalars()
    downstream = (
        await session.execute(select(Relationship).where(Relationship.from_entity == entity_id))
    ).scalars()

    def _edge(e: Relationship) -> dict[str, Any]:
        return {
            "from": e.from_entity,
            "to": e.to_entity,
            "type": e.relationship_type,
            "kind": "runtime" if e.method == "explicit" else e.method,
            "confidence": e.confidence,
            "evidence": e.source_event_ids or [],
        }

    return {
        "entity_id": entity_id,
        "upstream": [_edge(e) for e in upstream],
        "downstream": [_edge(e) for e in downstream],
    }


# -- insights / incidents -----------------------------------------------------


async def list_insights(
    session: AsyncSession,
    *,
    state: str | None = None,
    entity: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Insights, newest first, with optional filters."""
    stmt = select(Insight).order_by(Insight.created_at.desc()).limit(min(limit, 1000))
    if state:
        stmt = stmt.where(Insight.state == state)
    if entity:
        stmt = stmt.where(Insight.entity_id == entity)
    rows = (await session.execute(stmt)).scalars()
    return [_insight_json(r) for r in rows]


async def list_incidents(
    session: AsyncSession, *, state: str | None = None, limit: int = 100
) -> list[dict[str, Any]]:
    """Incidents, most recently updated first."""
    stmt = select(Incident).order_by(Incident.updated_at.desc()).limit(min(limit, 1000))
    if state:
        stmt = stmt.where(Incident.state == state)
    rows = (await session.execute(stmt)).scalars()
    return [_incident_json(r) for r in rows]


async def get_incident(session: AsyncSession, incident_id: str) -> dict[str, Any] | None:
    """One incident with its grouped insights."""
    row = await session.get(Incident, incident_id)
    if row is None:
        return None
    insights = []
    for iid in row.insight_ids or []:
        insight = (
            await session.execute(select(Insight).where(Insight.insight_id == iid))
        ).scalar_one_or_none()
        if insight is not None:
            insights.append(_insight_json(insight))
    return {**_incident_json(row), "insights": insights}


# -- investigation bundle (spec §20) -------------------------------------------


async def investigation_bundle(session: AsyncSession, run_id: str) -> dict[str, Any] | None:
    """Deterministic evidence bundle for a failed run (spec §20).

    Gathers run metadata, failures, changes, baseline context and impact —
    the structured input an LLM summary is allowed to see (§21.3).
    """
    run = await session.get(Run, run_id)
    if run is None:
        return None
    events = await run_events(session, run_id)
    failures = [e for e in events if e.outcome == "failure" or e.error is not None]
    warnings = [e for e in events if e.severity in ("warn", "error")]
    changes = await run_changes(session, run_id)
    impact = await run_impact(session, run_id)

    # Recent comparable runs: same job, other run_ids, latest first.
    comparable = []
    if run.job_name:
        rows = (
            await session.execute(
                select(Run)
                .where(Run.job_name == run.job_name, Run.run_id != run_id)
                .order_by(Run.updated_at.desc())
                .limit(5)
            )
        ).scalars()
        comparable = [
            {
                "run_id": r.run_id,
                "status": r.status,
                "duration_ms": r.duration_ms,
                "ended_at": _fmt(r.ended_at),
            }
            for r in rows
        ]

    return {
        "run_id": run_id,
        "run": _run_json(run),
        "failure": {
            "failed_stage": next(
                (
                    {
                        "event": e.event,
                        "error": e.error,
                        "observed_at": _fmt(e.observed_at),
                    }
                    for e in reversed(failures)
                ),
                None,
            ),
            "count": len(failures),
        },
        "timeline": [_event_json(e) for e in events],
        "preceding_warnings": [_event_json(e) for e in warnings],
        "related_changes": (changes or {}).get("changes", []),
        "comparisons": comparable,
        "impact": (impact or {}).get("impact", {}),
        "candidate_causes": _candidate_causes(failures, changes or {}),
        "evidence": [str(e.event_id) for e in events],
        "truncated": len(events) >= _MAX_RUN_EVENTS,
    }


def _candidate_causes(failures: list[Event], changes: dict[str, Any]) -> list[dict[str, Any]]:
    """Ranked hypotheses with evidence refs — never invented causal links."""
    causes: list[dict[str, Any]] = []
    if failures:
        first = failures[0]
        causes.append(
            {
                "hypothesis": "direct_failure",
                "detail": f"{first.event}: {(first.error or {}).get('message', 'unknown error')}",
                "evidence": [str(first.event_id)],
            }
        )
    causes.extend(
        {
            "hypothesis": "preceding_change",
            "detail": f"{change['event']} observed before the run window",
            "evidence": [change["event_id"]],
        }
        for change in (changes.get("changes") or [])[:3]
    )
    return causes
