"""Lifecycle decisions and projection identity survive full rebuilds."""

from __future__ import annotations

import asyncio
import copy
import uuid
from typing import Any

import pytest
from phlo_observer.models import Event, Incident, Insight
from phlo_observer.projections import LifecycleRebuildError, lock_rebuild, rebuild_projections
from sqlalchemy import delete, select

pytestmark = pytest.mark.asyncio


def _failure(event_id: str | None = None, run_id: str = "stable-run") -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "event_id": event_id or str(uuid.uuid4()),
        "event": "pipeline.run",
        "category": "pipeline",
        "outcome": "failure",
        "severity": "critical",
        "delivery": "telemetry",
        "observed_at": "2025-01-01T00:00:00Z",
        "service": {"name": "lifecycle-test"},
        "correlation": {"run_id": run_id},
        "attributes": {},
    }


async def _rebuild_twice(session_factory: Any) -> None:
    async with session_factory() as session, session.begin():
        await rebuild_projections(session)
    async with session_factory() as session, session.begin():
        await rebuild_projections(session)


@pytest.mark.parametrize(
    "insight_steps,incident_steps",
    [
        (("acknowledged",), ("acknowledged",)),
        (("acknowledged", "suppressed"), ("acknowledged", "suppressed")),
        (("resolved",), ("resolved",)),
        (("resolved", "open"), ("resolved", "open")),
    ],
)
async def test_manual_lifecycle_and_ids_survive_repeated_rebuild(
    client: Any,
    session_factory: Any,
    insight_steps: tuple[str, ...],
    incident_steps: tuple[str, ...],
) -> None:
    response = await client.post("/v1/events", json=_failure(run_id=f"stable-{uuid.uuid4()}"))
    assert response.status_code == 202
    async with session_factory() as session, session.begin():
        insight = await session.scalar(select(Insight))
        incident = await session.scalar(select(Incident))
        assert insight is not None and incident is not None
        insight_id, incident_id = str(insight.insight_id), str(incident.incident_id)

    for target in insight_steps:
        response = await client.post(
            f"/v2/insights/{insight_id}/transition", json={"state": target}
        )
        assert response.status_code == 200, response.text
    for target in incident_steps:
        response = await client.post(
            f"/v2/incidents/{incident_id}/transition", json={"state": target}
        )
        assert response.status_code == 200, response.text

    async with session_factory() as session, session.begin():
        before_insight = await session.get(Insight, uuid.UUID(insight_id))
        before_incident = await session.get(Incident, uuid.UUID(incident_id))
        assert before_insight is not None and before_incident is not None
        before = (
            before_insight.state,
            before_insight.updated_at,
            before_incident.state,
            before_incident.updated_at,
        )
    await _rebuild_twice(session_factory)
    async with session_factory() as session, session.begin():
        after_insight = await session.get(Insight, uuid.UUID(insight_id))
        after_incident = await session.get(Incident, uuid.UUID(incident_id))
        assert after_insight is not None and after_incident is not None
        assert (
            after_insight.state,
            after_insight.updated_at,
            after_incident.state,
            after_incident.updated_at,
        ) == before


async def test_missing_canonical_evidence_preserves_full_manual_rows(
    client: Any, session_factory: Any
) -> None:
    event_id = str(uuid.uuid4())
    assert (await client.post("/v1/events", json=_failure(event_id))).status_code == 202
    async with session_factory() as session, session.begin():
        insight = await session.scalar(select(Insight))
        incident = await session.scalar(select(Incident))
        assert insight is not None and incident is not None
        iid, cid = insight.insight_id, incident.incident_id
        assert (
            await client.post(f"/v2/incidents/{cid}/transition", json={"state": "acknowledged"})
        ).status_code == 200
        incident = await session.get(Incident, cid)
        assert incident is not None
        attrs, members = copy.deepcopy(insight.attributes), copy.deepcopy(incident.attributes)
        await session.delete(insight)
        await session.flush()
        # Reinsert a manual row with evidence that is no longer canonical.
        session.add(
            Insight(
                insight_id=iid,
                rule_id="run-failure",
                rule_version=1,
                title="operator title",
                severity="critical",
                state="acknowledged",
                entity_id="run://stable-run",
                evidence_event_ids=[event_id],
                evidence_metric_ids=[],
                recommended_action="operator action",
                recommended_action_verified=1,
                created_at=insight.created_at,
                updated_at=insight.updated_at,
                dedupe_key=insight.dedupe_key,
                attributes={**attrs, "operator": "keep"},
            )
        )
        await session.execute(delete(Event).where(Event.event_id == uuid.UUID(event_id)))
        incident.insight_ids = [str(iid)]
        incident.attributes = {**members, "operator": "keep"}
    await _rebuild_twice(session_factory)
    async with session_factory() as session:
        restored = await session.get(Insight, iid)
        restored_incident = await session.get(Incident, cid)
        assert restored is not None and restored_incident is not None
        assert restored.title == "operator title"
        assert restored.recommended_action == "operator action"
        assert restored.attributes.get("operator") == "keep"
        assert restored_incident.attributes.get("operator") == "keep"
        assert restored_incident.insight_ids == [str(iid)]


async def test_ambiguous_manual_episode_rolls_back_without_loss(
    client: Any, session_factory: Any
) -> None:
    first_id, second_id = str(uuid.uuid4()), str(uuid.uuid4())
    run_id = f"ambiguous-{uuid.uuid4()}"
    assert (await client.post("/v1/events", json=_failure(first_id, run_id))).status_code == 202
    async with session_factory() as session, session.begin():
        row = await session.scalar(select(Insight))
        assert row is not None
        first_insight_id = str(row.insight_id)
    assert (
        await client.post(
            f"/v2/insights/{first_insight_id}/transition", json={"state": "acknowledged"}
        )
    ).status_code == 200
    assert (await client.post("/v1/events", json=_failure(second_id, run_id))).status_code == 202
    async with session_factory() as session:
        rows = list((await session.execute(select(Insight))).scalars())
        assert len(rows) == 2
        before = {str(row.insight_id): row.state for row in rows}
    with pytest.raises(LifecycleRebuildError):
        async with session_factory() as session, session.begin():
            await rebuild_projections(session)
    async with session_factory() as session:
        after = {
            str(row.insight_id): row.state
            for row in (await session.execute(select(Insight))).scalars()
        }
        assert after == before


async def test_transition_waits_for_rebuild_lock_and_keeps_identity(
    client: Any, session_factory: Any
) -> None:
    assert (await client.post("/v1/events", json=_failure())).status_code == 202
    async with session_factory() as session:
        row = await session.scalar(select(Insight))
        assert row is not None
        insight_id = str(row.insight_id)
        await session.rollback()
    async with session_factory() as lock_session, lock_session.begin():
        await lock_rebuild(lock_session)
        pending = asyncio.create_task(
            client.post(f"/v2/insights/{insight_id}/transition", json={"state": "acknowledged"})
        )
        await asyncio.sleep(0.1)
        assert not pending.done()
    response = await pending
    assert response.status_code == 200
    async with session_factory() as session:
        row = await session.get(Insight, uuid.UUID(insight_id))
        assert row is not None and row.state == "acknowledged"
