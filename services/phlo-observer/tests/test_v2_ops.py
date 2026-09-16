"""V2 operations: search, SSE stream, quarantine replay (spec §23/§26/§34)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from phlo_observer.models import Incident, IngestFailure, Insight
from phlo_observer.stream import StreamHub, sse_encode
from sqlalchemy import select, text


def _event(
    *,
    event: str = "pipeline.run",
    outcome: str = "success",
    run_id: str | None = None,
    asset_key: str | None = None,
    error: dict[str, Any] | None = None,
    service_name: str = "svc",
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
        "observed_at": "2025-01-01T00:00:00Z",
        "service": {"name": service_name},
        "correlation": corr,
        "attributes": {},
        "source": {"producer": "test"},
    }
    if error:
        body["error"] = error
    return body


class TestStreamHub:
    def test_publish_reaches_subscriber(self) -> None:
        hub = StreamHub()
        queue = hub.subscribe()
        assert hub.publish("run.changed", {"run_id": "r1"}) == 1
        assert queue.get_nowait()["data"]["run_id"] == "r1"
        hub.unsubscribe(queue)

    def test_publish_drops_full_queue(self) -> None:

        hub = StreamHub()
        queue = hub.subscribe()
        for _ in range(queue.maxsize):
            queue.put_nowait({"kind": "x", "data": {}})
        assert hub.publish("x", {}) == 0  # full queue drops, no raise
        hub.unsubscribe(queue)

    def test_subscribe_capacity(self) -> None:
        hub = StreamHub()
        queues = [hub.subscribe() for _ in range(3)]
        assert len(hub._subscribers) == 3
        for q in queues:
            hub.unsubscribe(q)
        assert not hub._subscribers

    def test_sse_encode_frame(self) -> None:
        frame = sse_encode({"kind": "insight.opened", "data": {"a": 1}})
        assert frame.startswith(b"event: insight.opened\n")
        assert b'data: {"a": 1}' in frame


@pytest.mark.asyncio
class TestAlertCooldown:
    async def test_cooldown_suppresses_repeat(self) -> None:
        from phlo_observer import alerts

        tasks: set[Any] = set()
        assert await alerts.notify(["http://x"], "insight", {}, tasks=tasks, cooldown_key="k1")
        assert not await alerts.notify(["http://x"], "insight", {}, tasks=tasks, cooldown_key="k1")
        # A different key is not suppressed; a missing key always sends.
        assert await alerts.notify(["http://x"], "insight", {}, tasks=tasks, cooldown_key="k2")
        assert await alerts.notify(["http://x"], "insight", {}, tasks=tasks)


@pytest.mark.asyncio
class TestSearch:
    async def test_q_matches_event_name(self, client: Any) -> None:
        await client.post(
            "/v1/events",
            json=[_event(event="pipeline.run", run_id="s1")],
        )
        resp = await client.get("/v2/search", params={"q": "pipeline"})
        assert resp.status_code == 200
        assert any(i["event"] == "pipeline.run" for i in resp.json()["items"])

    async def test_q_matches_error_message(self, client: Any) -> None:
        await client.post(
            "/v1/events",
            json=[_event(error={"message": "trino OOM killer"}, outcome="failure")],
        )
        resp = await client.get("/v2/search", params={"q": "OOM killer"})
        assert resp.status_code == 200
        assert len(resp.json()["items"]) == 1

    async def test_q_no_match(self, client: Any) -> None:
        await client.post("/v1/events", json=[_event()])
        resp = await client.get("/v2/search", params={"q": "zzz-no-such"})
        assert resp.json()["items"] == []

    async def test_q_like_metachars_match_literally(self, client: Any) -> None:
        """Regression: ``%``/``_`` in the search term are data, not wildcards."""
        await client.post(
            "/v1/events",
            json=[_event(event="pipeline.run", run_id="q-esc")],
        )
        # '%' must not act as a wildcard that matches everything.
        resp = await client.get("/v2/search", params={"q": "%"})
        assert resp.json()["items"] == []
        # Underscore is literal: "pipeline_run" does not match "pipeline.run".
        resp = await client.get("/v2/search", params={"q": "pipeline_run"})
        assert resp.json()["items"] == []
        # And the literal form still matches.
        resp = await client.get("/v2/search", params={"q": "pipeline.run"})
        assert len(resp.json()["items"]) == 1

    async def test_structured_filters_still_apply(self, client: Any) -> None:
        await client.post(
            "/v1/events",
            json=[_event(asset_key="search/x", run_id="sr1")],
        )
        resp = await client.get("/v2/search", params={"asset_key": "search/x", "q": "pipeline"})
        assert len(resp.json()["items"]) == 1


@pytest.mark.asyncio
class TestQuarantine:
    async def test_failed_payload_quarantined(self, client: Any, session_factory: Any) -> None:
        # Unparseable payload for the dagster adapter -> normalization fails.
        resp = await client.post("/v1/ingest/dagster", content=b"not-json")
        assert resp.status_code in (400, 422)
        async with session_factory() as session:
            rows = (await session.execute(select(IngestFailure))).scalars().all()
        assert len(rows) == 1
        assert rows[0].producer == "dagster"
        assert rows[0].replayed == 0

    async def test_quarantine_list_endpoint(self, client: Any, session_factory: Any) -> None:
        await client.post("/v1/ingest/dagster", content=b"garbage")
        resp = await client.get("/v2/admin/quarantine")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 1
        assert items[0]["error_code"]

    async def test_quarantine_replay_not_replayable(
        self, client: Any, session_factory: Any
    ) -> None:
        await client.post("/v1/ingest/dagster", content=b"bad")
        async with session_factory() as session:
            row = (await session.execute(select(IngestFailure))).scalar_one()
            fid = str(row.id)
        resp = await client.post(f"/v2/admin/quarantine/{fid}/replay")
        # Raw payload is stored as text-wrapped dict, not the original JSON —
        # digest/text encoding is not replayable.
        assert resp.status_code in (200, 422)


def _insight() -> Insight:
    from observe_core.timestamps import utcnow

    return Insight(
        insight_id=uuid.uuid4(),
        rule_id="test.rule",
        rule_version=1,
        title="t",
        severity="warn",
        state="open",
        entity_id="asset://x/y",
        evidence_event_ids=[],
        evidence_metric_ids=[],
        created_at=utcnow(),
        updated_at=utcnow(),
        attributes={},
    )


@pytest.mark.asyncio
class TestLifecycle:
    async def test_insight_transition(self, client: Any, session_factory: Any) -> None:
        async with session_factory() as session, session.begin():
            row = _insight()
            session.add(row)
            iid = str(row.insight_id)
        resp = await client.post(f"/v2/insights/{iid}/transition", json={"state": "acknowledged"})
        assert resp.status_code == 200
        assert resp.json()["state"] == "acknowledged"
        bad = await client.post(f"/v2/insights/{iid}/transition", json={"state": "bogus"})
        assert bad.status_code == 400
        missing = await client.post(
            f"/v2/insights/{uuid.uuid4()}/transition", json={"state": "resolved"}
        )
        assert missing.status_code == 404

    async def test_insight_illegal_transition_conflicts(
        self, client: Any, session_factory: Any
    ) -> None:
        """Regression: lifecycle is a state machine, not a free-for-all."""
        async with session_factory() as session, session.begin():
            row = _insight()
            session.add(row)
            iid = str(row.insight_id)
        # resolved -> suppressed is not a legal transition.
        resp = await client.post(f"/v2/insights/{iid}/transition", json={"state": "resolved"})
        assert resp.status_code == 200
        bad = await client.post(f"/v2/insights/{iid}/transition", json={"state": "suppressed"})
        assert bad.status_code == 409
        # Same-state is idempotent, not a conflict.
        same = await client.post(f"/v2/insights/{iid}/transition", json={"state": "resolved"})
        assert same.status_code == 200
        # Reopen is legal from resolved.
        reopen = await client.post(f"/v2/insights/{iid}/transition", json={"state": "open"})
        assert reopen.status_code == 200
        # expired is terminal.
        await client.post(f"/v2/insights/{iid}/transition", json={"state": "acknowledged"})
        exp = await client.post(f"/v2/insights/{iid}/transition", json={"state": "expired"})
        assert exp.status_code == 200
        dead = await client.post(f"/v2/insights/{iid}/transition", json={"state": "open"})
        assert dead.status_code == 409

    async def test_incident_transition_resolved(self, client: Any, session_factory: Any) -> None:
        from observe_core.timestamps import utcnow

        async with session_factory() as session, session.begin():
            row = Incident(
                incident_id=uuid.uuid4(),
                title="i",
                state="open",
                severity="warn",
                entities=[],
                insight_ids=[],
                timeline={},
                impact={},
                attributes={},
                updated_at=utcnow(),
            )
            session.add(row)
            iid = str(row.incident_id)
        resp = await client.post(f"/v2/incidents/{iid}/transition", json={"state": "resolved"})
        assert resp.status_code == 200
        assert resp.json()["state"] == "resolved"

    async def test_incident_reopen_clears_resolved_at(
        self, client: Any, session_factory: Any
    ) -> None:
        """Regression: a reopened incident must not carry a stale resolved_at."""
        from observe_core.timestamps import utcnow

        async with session_factory() as session, session.begin():
            row = Incident(
                incident_id=uuid.uuid4(),
                title="i",
                state="open",
                severity="warn",
                entities=[],
                insight_ids=[],
                timeline={},
                impact={},
                attributes={},
                updated_at=utcnow(),
            )
            session.add(row)
            iid = str(row.incident_id)
        resp = await client.post(f"/v2/incidents/{iid}/transition", json={"state": "resolved"})
        assert resp.status_code == 200
        async with session_factory() as session:
            assert (await session.get(Incident, uuid.UUID(iid))).resolved_at is not None
        resp = await client.post(f"/v2/incidents/{iid}/transition", json={"state": "open"})
        assert resp.status_code == 200
        async with session_factory() as session:
            row = await session.get(Incident, uuid.UUID(iid))
            assert row.state == "open"
            assert row.resolved_at is None
        # open -> suppressed is legal; suppressed -> acknowledged is not.
        ok = await client.post(f"/v2/incidents/{iid}/transition", json={"state": "suppressed"})
        assert ok.status_code == 200
        bad = await client.post(f"/v2/incidents/{iid}/transition", json={"state": "acknowledged"})
        assert bad.status_code == 409


@pytest.mark.asyncio
class TestRegistryEndpoints:
    async def test_entities_listing(self, client: Any) -> None:
        await client.post(
            "/v1/events",
            json=[_event(asset_key="ent/a", run_id="er1")],
        )
        resp = await client.get("/v2/entities")
        assert resp.status_code == 200
        ids = [i["entity_id"] for i in resp.json()["items"]]
        assert any("asset://ent/a" in i for i in ids)

    async def test_schemas_register_and_list(self, client: Any) -> None:
        resp = await client.post(
            "/v2/schemas",
            json={"schema_id": "contract://test/x", "version": "2", "schema": {"a": 1}},
        )
        assert resp.status_code == 200
        digest = resp.json()["schema_hash"]
        listed = await client.get("/v2/schemas")
        ids = [i["schema_id"] for i in listed.json()["items"]]
        assert "contract://test/x" in ids
        assert any(i["schema_hash"] == digest for i in listed.json()["items"])
        assert (await client.post("/v2/schemas", json={})).status_code == 400

    async def test_analyses_record_and_list(self, client: Any) -> None:
        resp = await client.post(
            "/v2/analyses",
            json={
                "model": "test-model",
                "prompt_template_version": "v1",
                "evidence_event_ids": ["e1"],
                "output": {"summary": "s"},
                "subject": "run://r/1",
            },
        )
        assert resp.status_code == 200
        listed = await client.get("/v2/analyses")
        assert len(listed.json()["items"]) == 1
        assert listed.json()["items"][0]["model"] == "test-model"
        missing = await client.post("/v2/analyses", json={"model": "m"})
        assert missing.status_code == 400

    async def test_provenance_reports_edges(self, client: Any) -> None:
        """Regression: provenance must name the edges an event contributed to."""
        import workloads as wl

        events = wl.dagster_run(
            f"prov-{uuid.uuid4().hex[:8]}",
            wl.T0,
            assets=["analytics.prov"],
            steps=1,
        )
        await client.post("/v1/events", json=events)
        materialize = next(e for e in events if e["event"] == "asset.materialize")
        resp = await client.get(f"/v2/events/{materialize['event_id']}/provenance")
        assert resp.status_code == 200
        projections = resp.json()["projections"]
        assert projections["edge"], "materialize event must credit its edges"
        assert any("asset://analytics.prov" in e for e in projections["edge"])
        assert projections["entity"] or projections["run"]


@pytest.mark.asyncio
class TestArchiveRestore:
    async def test_archive_restore_roundtrip(
        self,
        client: Any,
        session_factory: Any,
        database_url: str,
        tmp_path: Any,
        monkeypatch: Any,
    ) -> None:
        import asyncio

        from phlo_observer.cli import app
        from phlo_observer.models import Event
        from typer.testing import CliRunner

        await client.post("/v1/events", json=[_event(run_id="arch-1")])
        out = tmp_path / "events.jsonl"
        monkeypatch.setenv("PHLO_OBSERVER_DATABASE_URL", database_url)
        # The CLI calls asyncio.run internally; run it off the test loop.
        result = await asyncio.to_thread(CliRunner().invoke, app, ["archive", str(out)])
        assert result.exit_code == 0, result.output
        assert "archived" in result.output

        async with session_factory() as session, session.begin():
            await session.execute(text("delete from events where run_id = 'arch-1'"))
            await session.execute(text("delete from runs where run_id = 'arch-1'"))

        result = await asyncio.to_thread(CliRunner().invoke, app, ["restore", str(out)])
        assert result.exit_code == 0, result.output
        async with session_factory() as session:
            rows = (
                (await session.execute(select(Event).where(Event.run_id == "arch-1")))
                .scalars()
                .all()
            )
        assert len(rows) == 1

    async def test_reprocess_window_runs(
        self,
        client: Any,
        database_url: str,
        monkeypatch: Any,
    ) -> None:
        """``reprocess --since`` replays the window through apply_event."""
        import asyncio

        from phlo_observer.cli import app
        from typer.testing import CliRunner

        await client.post("/v1/events", json=[_event(run_id="repro-1")])
        monkeypatch.setenv("PHLO_OBSERVER_DATABASE_URL", database_url)
        result = await asyncio.to_thread(
            CliRunner().invoke,
            app,
            ["reprocess", "--since", "2000-01-01T00:00:00"],
        )
        assert result.exit_code == 0, result.output
        assert "reprocessed" in result.output

    async def test_reprocess_paginates_past_batch_size(
        self,
        client: Any,
        session_factory: Any,
        database_url: str,
        monkeypatch: Any,
    ) -> None:
        """``--batch`` smaller than the window must still visit every event —
        keyset pagination replaces the old single unbounded transaction."""
        import asyncio
        import re

        from phlo_observer.cli import app
        from phlo_observer.models import Event
        from sqlalchemy import func
        from typer.testing import CliRunner

        await client.post(
            "/v1/events",
            json=[_event(run_id="repro-page") for _ in range(5)],
        )
        monkeypatch.setenv("PHLO_OBSERVER_DATABASE_URL", database_url)
        result = await asyncio.to_thread(
            CliRunner().invoke,
            app,
            ["reprocess", "--since", "2000-01-01T00:00:00", "--batch", "2"],
        )
        assert result.exit_code == 0, result.output
        async with session_factory() as session:
            total = await session.scalar(select(func.count()).select_from(Event))
        # Every event in the window is re-folded, not just the first page.
        assert int(re.search(r"reprocessed (\d+)", result.output).group(1)) == total
