"""V2 state engine: projections, provenance, rebuild equivalence (spec §12)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from phlo_observer.models import Asset, Entity, Relationship, Run
from phlo_observer.projections import rebuild_projections
from phlo_observer.state_engine import (
    apply_run_event,
    edges_of,
    event_entities,
    new_run_state,
)
from sqlalchemy import select


def _event(
    *,
    event: str = "pipeline.run",
    outcome: str = "success",
    severity: str = "info",
    run_id: str | None = "run-1",
    asset_key: str | None = None,
    branch: str | None = None,
    table: str | None = None,
    observed_at: str = "2025-01-01T00:00:00Z",
    started_at: str | None = None,
    ended_at: str | None = None,
    duration_ms: float | None = None,
    attributes: dict[str, Any] | None = None,
    entities: dict[str, str] | None = None,
    error: dict[str, Any] | None = None,
    source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    corr: dict[str, Any] = {}
    if run_id:
        corr["run_id"] = run_id
    if asset_key:
        corr["asset_key"] = asset_key
    if branch:
        corr["branch"] = branch
    if table:
        corr["table"] = table
    body: dict[str, Any] = {
        "schema_version": "2.0",
        "event_id": str(uuid.uuid4()),
        "event": event,
        "category": "pipeline",
        "outcome": outcome,
        "severity": severity,
        "delivery": "telemetry",
        "observed_at": observed_at,
        "service": {"name": "test-service", "environment": "test"},
        "correlation": corr,
        "attributes": attributes or {},
        "source": source or {"producer": "dagster"},
    }
    if started_at:
        body["started_at"] = started_at
    if ended_at:
        body["ended_at"] = ended_at
    if duration_ms is not None:
        body["duration_ms"] = duration_ms
    if entities:
        body["entities"] = entities
    if error:
        body["error"] = error
    return body


async def _post(client: Any, events: list[dict[str, Any]]) -> None:
    resp = await client.post("/v1/events", json=events)
    assert resp.status_code == 202, resp.text
    assert resp.json()["rejected"] == 0, resp.json()


class TestEntityDerivation:
    def test_v1_correlation_fields_derive_entities(self) -> None:
        entities = event_entities(_event(asset_key="silver/samples", table="silver.samples"))
        assert entities["run"] == "run://dagster/run-1"
        assert entities["asset"] == "asset://silver/samples"
        assert entities["table"] == "table://silver.samples"
        assert entities["service"] == "service://test-service"

    def test_v2_entities_take_precedence(self) -> None:
        entities = event_entities(
            _event(
                run_id="run-1",
                entities={"run": "run://custom/abc", "asset": "asset://x/y"},
            )
        )
        assert entities["run"] == "run://custom/abc"
        assert entities["asset"] == "asset://x/y"

    def test_edges_from_verb_table(self) -> None:
        edges = edges_of(_event(event="asset.materialize", asset_key="a/b"))
        pairs = {(e.from_entity, e.to_entity, e.relationship_type) for e in edges}
        assert ("service://test-service", "run://dagster/run-1", "executes") in pairs
        assert ("run://dagster/run-1", "asset://a/b", "produces") in pairs

    def test_read_and_validate_verbs(self) -> None:
        read = edges_of(_event(event="table.read", run_id="r", table="t1"))
        assert any(e.relationship_type == "reads_from" for e in read)
        check = edges_of(_event(event="quality.check", asset_key="a"))
        assert any(e.relationship_type == "validates" for e in check)


class TestRunReducer:
    def test_terminal_event_sets_status(self) -> None:
        state = new_run_state("r1")
        apply_run_event(state, _event(outcome="success"))
        assert state["status"] == "success"

    def test_out_of_order_events_idempotent(self) -> None:
        # Terminal success arrives before the "started" event.
        state = new_run_state("r1")
        apply_run_event(state, _event(outcome="success", observed_at="2025-01-01T00:01:00Z"))
        apply_run_event(
            state,
            _event(
                event="pipeline.step",
                outcome="unknown",
                observed_at="2025-01-01T00:00:00Z",
            ),
        )
        assert state["status"] == "success"
        assert state["event_count"] == 2

    def test_failure_beats_success(self) -> None:
        state = new_run_state("r1")
        apply_run_event(state, _event(outcome="success"))
        apply_run_event(state, _event(outcome="failure"))
        assert state["status"] == "failure"


@pytest.mark.asyncio
class TestIncrementalAndRebuild:
    async def test_entities_registered_on_ingest(self, client: Any, session_factory: Any) -> None:
        await _post(client, [_event(asset_key="silver/samples", table="silver.samples")])
        async with session_factory() as session:
            rows = (await session.execute(select(Entity))).scalars().all()
        ids = {r.entity_id for r in rows}
        assert "asset://silver/samples" in ids
        assert "table://silver.samples" in ids
        assert "run://dagster/run-1" in ids
        assert "service://test-service" in ids
        by_id = {r.entity_id: r for r in rows}
        assert by_id["asset://silver/samples"].kind == "asset"
        assert by_id["run://dagster/run-1"].provenance["rule"] == "entity-registry-v2"

    async def test_edges_recorded_on_ingest(self, client: Any, session_factory: Any) -> None:
        await _post(client, [_event(event="asset.materialize", asset_key="a/b")])
        async with session_factory() as session:
            rows = (await session.execute(select(Relationship))).scalars().all()
        triples = {(r.from_entity, r.to_entity, r.relationship_type) for r in rows}
        assert ("run://dagster/run-1", "asset://a/b", "produces") in triples
        assert (
            "service://test-service",
            "run://dagster/run-1",
            "executes",
        ) in triples

    async def test_asset_projection_updated(self, client: Any, session_factory: Any) -> None:
        await _post(
            client,
            [
                _event(
                    event="asset.materialized",
                    outcome="success",
                    asset_key="silver/samples",
                    observed_at="2025-01-01T00:05:00Z",
                )
            ],
        )
        async with session_factory() as session:
            asset = await session.get(Asset, "asset://silver/samples")
        assert asset is not None
        assert asset.status == "healthy"
        assert asset.last_materialized_at is not None
        assert asset.provenance["rule"] == "asset-state-v2"

    async def test_run_provenance_recorded(self, client: Any, session_factory: Any) -> None:
        await _post(client, [_event(outcome="success")])
        async with session_factory() as session:
            run = await session.get(Run, "run-1")
        assert run is not None
        assert run.provenance["rule"] == "run-state-v2"
        assert len(run.provenance["derived_from"]) == 1

    async def test_rebuild_matches_incremental(self, client: Any, session_factory: Any) -> None:
        events = [
            _event(
                event="pipeline.run",
                outcome="unknown",
                run_id="run-a",
                observed_at="2025-01-01T00:00:00Z",
            ),
            _event(
                event="asset.materialize",
                outcome="success",
                run_id="run-a",
                asset_key="a/one",
                observed_at="2025-01-01T00:01:00Z",
            ),
            _event(
                event="quality.check",
                outcome="failure",
                severity="error",
                run_id="run-a",
                asset_key="a/one",
                error={"message": "check failed"},
                observed_at="2025-01-01T00:02:00Z",
            ),
            _event(
                event="pipeline.run",
                outcome="failure",
                run_id="run-a",
                observed_at="2025-01-01T00:03:00Z",
                ended_at="2025-01-01T00:03:00Z",
            ),
            # A second, unrelated run to prove scoping works.
            _event(
                event="pipeline.run",
                outcome="success",
                run_id="run-b",
                observed_at="2025-01-01T00:04:00Z",
            ),
        ]
        await _post(client, events)

        async with session_factory() as session:
            before_run = await session.get(Run, "run-a")
            before = (
                before_run.status,
                before_run.event_count,
                before_run.error_count,
                before_run.asset_count,
            )
            entities_before = {
                r.entity_id for r in (await session.execute(select(Entity))).scalars().all()
            }
            edges_before = {
                (r.from_entity, r.to_entity, r.relationship_type)
                for r in (await session.execute(select(Relationship))).scalars().all()
            }

        # Full rebuild from canonical events.
        async with session_factory() as session, session.begin():
            counts = await rebuild_projections(session)

        assert counts["events"] == 5
        assert counts["runs"] == 2
        async with session_factory() as session:
            run = await session.get(Run, "run-a")
            assert (run.status, run.event_count, run.error_count, run.asset_count) == before
            entities_after = {
                r.entity_id for r in (await session.execute(select(Entity))).scalars().all()
            }
            edges_after = {
                (r.from_entity, r.to_entity, r.relationship_type)
                for r in (await session.execute(select(Relationship))).scalars().all()
            }
        assert entities_after == entities_before
        assert edges_after == edges_before

    async def test_scoped_rebuild_only_touches_run(self, client: Any, session_factory: Any) -> None:
        await _post(
            client,
            [
                _event(run_id="run-a", outcome="success", asset_key="a/x"),
                _event(
                    run_id="run-b",
                    outcome="success",
                    observed_at="2025-01-01T00:05:00Z",
                ),
            ],
        )
        # Corrupt run-a's projection to prove the rebuild repairs it.
        async with session_factory() as session, session.begin():
            run = await session.get(Run, "run-a")
            run.status = "unknown"
            run.event_count = 0
            run_b_before = await session.get(Run, "run-b")
            b_state = (run_b_before.status, run_b_before.event_count)

        async with session_factory() as session, session.begin():
            counts = await rebuild_projections(session, run_id="run-a")
        assert counts["runs"] == 1

        async with session_factory() as session:
            run_a = await session.get(Run, "run-a")
            run_b = await session.get(Run, "run-b")
        assert run_a.status == "success"
        assert run_a.event_count == 1
        assert (run_b.status, run_b.event_count) == b_state

    async def test_rebuild_after_incremental_duplicates(
        self, client: Any, session_factory: Any
    ) -> None:
        # Re-posting an identical event is a duplicate, not a second fold.
        event = _event(run_id="run-dup", outcome="success")
        await _post(client, [event])
        await _post(client, [event])
        async with session_factory() as session:
            run = await session.get(Run, "run-dup")
        assert run.event_count == 1


class TestRebuildCLI:
    def test_rebuild_projections_command(self) -> None:
        from phlo_observer.cli import app
        from typer.testing import CliRunner

        result = CliRunner().invoke(app, ["rebuild-projections", "--help"])
        assert result.exit_code == 0
        # Rich wraps options on narrow CI terminals; strip newlines first.
        assert "--run" in result.output.replace("\n", "")


@pytest.mark.asyncio
class TestBranchProjection:
    async def test_branch_lifecycle_attributes(self, client: Any, session_factory: Any) -> None:
        await _post(
            client,
            [
                _event(
                    event="wap.branch.create",
                    run_id=None,
                    branch="run-abc",
                    attributes={"base_branch": "main"},
                ),
                _event(
                    event="wap.promote",
                    run_id=None,
                    branch="run-abc",
                    outcome="success",
                    attributes={"target": "main"},
                    observed_at="2025-01-01T01:00:00Z",
                ),
            ],
        )
        async with session_factory() as session:
            ent = await session.get(Entity, "branch://dagster/run-abc")
        assert ent is not None
        assert ent.attributes["state"] == "promoted"
        assert ent.attributes["base_branch"] == "main"
        assert ent.attributes["target"] == "main"

    async def test_branch_rejected_state(self, client: Any, session_factory: Any) -> None:
        await _post(
            client,
            [
                _event(event="wap.branch.create", run_id=None, branch="r2"),
                _event(
                    event="wap.reject",
                    run_id=None,
                    branch="r2",
                    observed_at="2025-01-01T02:00:00Z",
                ),
            ],
        )
        async with session_factory() as session:
            ent = await session.get(Entity, "branch://dagster/r2")
        assert ent.attributes["state"] == "rejected"

    async def test_branch_state_survives_rebuild(self, client: Any, session_factory: Any) -> None:
        await _post(
            client,
            [
                _event(event="wap.branch.create", run_id=None, branch="r3"),
                _event(
                    event="wap.cleanup",
                    run_id=None,
                    branch="r3",
                    observed_at="2025-01-01T03:00:00Z",
                ),
            ],
        )
        async with session_factory() as session, session.begin():
            await rebuild_projections(session)
        async with session_factory() as session:
            ent = await session.get(Entity, "branch://dagster/r3")
        assert ent.attributes["state"] == "cleaned"
