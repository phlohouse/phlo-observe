"""V2 insight layer: baselines, deterministic rules, incidents (§15-17)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from phlo_observer.baselines import compute, observations_of
from phlo_observer.models import Baseline, Incident, Insight
from sqlalchemy import select


def _event(
    *,
    event: str = "pipeline.run",
    outcome: str = "success",
    run_id: str | None = "run-1",
    asset_key: str | None = None,
    duration_ms: float | None = None,
    attributes: dict[str, Any] | None = None,
    entities: dict[str, str] | None = None,
    error: dict[str, Any] | None = None,
    observed_at: str = "2025-01-01T00:00:00Z",
) -> dict[str, Any]:
    corr: dict[str, Any] = {}
    if run_id:
        corr["run_id"] = run_id
    if asset_key:
        corr["asset_key"] = asset_key
    body: dict[str, Any] = {
        "schema_version": "2.0",
        "event_id": str(uuid.uuid4()),
        "event": event,
        "category": "pipeline",
        "outcome": outcome,
        "severity": "info",
        "delivery": "telemetry",
        "observed_at": observed_at,
        "service": {"name": "svc"},
        "correlation": corr,
        "attributes": attributes or {},
        "source": {"producer": "dagster"},
    }
    if duration_ms is not None:
        body["duration_ms"] = duration_ms
    if entities:
        body["entities"] = entities
    if error:
        body["error"] = error
    return body


async def _post(client: Any, events: list[dict[str, Any]]) -> dict[str, Any]:
    resp = await client.post("/v1/events", json=events)
    assert resp.status_code == 202, resp.text
    return resp.json()


class TestBaselineStats:
    def test_compute_empty(self) -> None:
        assert compute([]) == {
            "median": None,
            "mad": None,
            "mean": None,
            "p10": None,
            "p90": None,
        }

    def test_compute_median_mad(self) -> None:
        stats = compute([1.0, 2.0, 3.0, 4.0, 100.0])
        assert stats["median"] == 3.0
        assert stats["mean"] == pytest.approx(22.0)

    def test_observations_from_metric_event(self) -> None:
        obs = observations_of(
            _event(
                event="metric.recorded",
                run_id=None,
                attributes={"metric": "rows_processed", "value": 500},
                entities={"asset": "asset://a/b"},
            )
        )
        assert obs == [("asset://a/b", "rows_processed", 500.0)]

    def test_observations_from_run_duration(self) -> None:
        obs = observations_of(_event(duration_ms=1200.0, run_id="r1"))
        assert ("run://dagster/r1", "run.duration_ms", 1200.0) in obs

    def test_partition_aware_key(self) -> None:
        obs = observations_of(
            _event(
                event="metric.recorded",
                run_id=None,
                attributes={"metric": "rows", "value": 10},
                entities={"asset": "asset://a"},
            )
            | {"correlation": {"partition_key": "2025-01"}}
        )
        assert obs[0][1] == "rows|2025-01"


@pytest.mark.asyncio
class TestInsightRules:
    async def test_run_failure_creates_critical_insight(
        self, client: Any, session_factory: Any
    ) -> None:
        await _post(
            client,
            [
                _event(
                    outcome="failure",
                    error={"message": "OOM"},
                    run_id="run-fail",
                )
            ],
        )
        async with session_factory() as session:
            rows = (await session.execute(select(Insight))).scalars().all()
        assert len(rows) == 1
        assert rows[0].rule_id == "run-failure"
        assert rows[0].severity == "critical"
        assert rows[0].state == "open"
        assert rows[0].evidence_event_ids

    async def test_critical_insight_opens_incident(self, client: Any, session_factory: Any) -> None:
        await _post(client, [_event(outcome="failure", run_id="run-inc")])
        async with session_factory() as session:
            incidents = (await session.execute(select(Incident))).scalars().all()
        assert len(incidents) == 1
        assert incidents[0].state == "open"
        assert incidents[0].severity == "critical"
        assert incidents[0].insight_ids

    async def test_quality_failure_dedupes(self, client: Any, session_factory: Any) -> None:
        event = _event(
            event="quality.check",
            outcome="failure",
            asset_key="a/b",
            error={"message": "nulls found"},
        )
        await _post(client, [event])
        await _post(
            client,
            [
                _event(
                    event="quality.check",
                    outcome="failure",
                    asset_key="a/b",
                    error={"message": "nulls found"},
                )
            ],
        )
        async with session_factory() as session:
            rows = (
                (await session.execute(select(Insight).where(Insight.rule_id == "quality-failure")))
                .scalars()
                .all()
            )
        assert len(rows) == 1
        assert len(rows[0].evidence_event_ids) == 2

    async def test_success_resolves_quality_insight(
        self, client: Any, session_factory: Any
    ) -> None:
        await _post(
            client,
            [_event(event="quality.check", outcome="failure", asset_key="a/b")],
        )
        await _post(
            client,
            [_event(event="quality.check", outcome="success", asset_key="a/b")],
        )
        async with session_factory() as session:
            rows = (
                (await session.execute(select(Insight).where(Insight.rule_id == "quality-failure")))
                .scalars()
                .all()
            )
        assert rows[0].state == "resolved"

    async def test_duration_regression_after_baseline(
        self, client: Any, session_factory: Any
    ) -> None:
        # Seed 6 normal-duration runs to build a baseline.
        for i in range(6):
            await _post(
                client,
                [
                    _event(
                        run_id=f"seed-{i}",
                        duration_ms=1000.0,
                        outcome="success",
                        observed_at=f"2025-01-0{i + 1}T00:00:00Z",
                    )
                ],
            )
        # A 3x-duration run on the same service entity should flag.
        # Baseline keys on the run entity, so we need per-run entities —
        # use a shared asset entity instead so samples aggregate.
        for i in range(6):
            await _post(
                client,
                [
                    _event(
                        event="metric.recorded",
                        run_id=None,
                        attributes={"metric": "duration_ms", "value": 1000.0},
                        entities={"asset": "asset://slow/one"},
                        observed_at=f"2025-01-0{i + 1}T01:00:00Z",
                    )
                ],
            )
        result = await _post(
            client,
            [
                _event(
                    event="metric.recorded",
                    run_id=None,
                    attributes={"metric": "duration_ms", "value": 5000.0},
                    entities={"asset": "asset://slow/one"},
                    observed_at="2025-01-10T01:00:00Z",
                )
            ],
        )
        assert result["accepted"] == 1
        async with session_factory() as session:
            rows = (
                (
                    await session.execute(
                        select(Insight).where(Insight.rule_id == "duration-regression")
                    )
                )
                .scalars()
                .all()
            )
            baseline = (
                await session.execute(
                    select(Baseline).where(Baseline.entity_id == "asset://slow/one")
                )
            ).scalar_one_or_none()
        assert len(rows) == 1
        assert "5.0x" in rows[0].attributes["summary"]
        assert baseline is not None
        assert baseline.count == 7

    async def test_baseline_updated_on_ingest(self, client: Any, session_factory: Any) -> None:
        await _post(client, [_event(run_id="rb", duration_ms=2000.0)])
        async with session_factory() as session:
            row = (
                await session.execute(select(Baseline).where(Baseline.metric == "run.duration_ms"))
            ).scalar_one_or_none()
        assert row is not None
        assert row.count == 1
        assert row.median == 2000.0

    async def test_batch_state_load_scopes_to_batch_entities(
        self, client: Any, session_factory: Any
    ) -> None:
        """Regression: preload must not pull every open row in the deployment."""
        from observe_core.timestamps import utcnow
        from phlo_observer.insights import BatchState

        touched = "asset://scoped/yes"
        other = "asset://scoped/no"
        async with session_factory() as session, session.begin():
            session.add(
                Insight(
                    insight_id=uuid.uuid4(),
                    rule_id="quality-failure",
                    rule_version=1,
                    title="t",
                    severity="warn",
                    state="open",
                    entity_id=other,
                    dedupe_key=f"dedupe-{uuid.uuid4().hex[:8]}",
                    evidence_event_ids=[],
                    evidence_metric_ids=[],
                    attributes={},
                    created_at=utcnow(),
                    updated_at=utcnow(),
                )
            )
            session.add(
                Incident(
                    incident_id=uuid.uuid4(),
                    title="i",
                    state="open",
                    severity="warn",
                    entities=[other],
                    insight_ids=[],
                    timeline={},
                    impact={},
                    attributes={},
                    updated_at=utcnow(),
                )
            )
        batch_event = _event(
            event="quality.check",
            outcome="failure",
            entities={"asset": touched},
            run_id=None,
        )
        async with session_factory() as session:
            state = await BatchState.load(session, [batch_event])
        # The unrelated open insight/incident is out of scope.
        assert all(i.entity_id != other for i in state.open_insights)
        assert all(other not in (c.entities or []) for c in state.open_incidents)

    async def test_batch_state_load_includes_touched_open_rows(
        self, client: Any, session_factory: Any
    ) -> None:
        """Dedupe still sees open insights on entities this batch touches."""
        from observe_core.timestamps import utcnow
        from phlo_observer.insights import BatchState

        touched = "asset://scoped/hit"
        dedupe = f"dedupe-{uuid.uuid4().hex[:8]}"
        async with session_factory() as session, session.begin():
            session.add(
                Insight(
                    insight_id=uuid.uuid4(),
                    rule_id="quality-failure",
                    rule_version=1,
                    title="t",
                    severity="warn",
                    state="open",
                    entity_id=touched,
                    dedupe_key=dedupe,
                    evidence_event_ids=[],
                    evidence_metric_ids=[],
                    attributes={},
                    created_at=utcnow(),
                    updated_at=utcnow(),
                )
            )
            session.add(
                Incident(
                    incident_id=uuid.uuid4(),
                    title="i",
                    state="open",
                    severity="warn",
                    entities=[touched],
                    insight_ids=[],
                    timeline={},
                    impact={},
                    attributes={},
                    updated_at=utcnow(),
                )
            )
        batch_event = _event(
            event="quality.check",
            outcome="failure",
            entities={"asset": touched},
            run_id=None,
        )
        async with session_factory() as session:
            state = await BatchState.load(session, [batch_event])
        assert dedupe in state.open_by_dedupe
        assert any(touched in (c.entities or []) for c in state.open_incidents)
