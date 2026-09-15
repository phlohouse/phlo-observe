"""Observatory readiness (hardening item 10).

Each test answers one operational question through the public query API
against a realistic ingested history — no private session shortcuts.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import workloads as wl

pytestmark = pytest.mark.asyncio


async def _seed(client: Any) -> list[dict[str, Any]]:
    """One day's mixed history: healthy runs, a failed run, a WAP lifecycle."""
    events = wl.mixed_history(days=2, seed=22)
    response = await client.post("/v1/events", json=events)
    assert response.status_code == 202, response.text
    assert response.json()["accepted"] > 0
    return events


async def test_what_failed(client: Any) -> None:
    """'What failed?' -> run list filtered to failed status."""
    events = await _seed(client)
    failed_ids = {
        e["correlation"]["run_id"]
        for e in events
        if e["event"] == "pipeline.run" and e.get("outcome") == "failure"
    }
    response = await client.get("/v1/runs", params={"status": "failure"})
    assert response.status_code == 200
    found = {r["run_id"] for r in response.json()["items"]}
    assert failed_ids <= found


async def test_why_did_this_run_fail(client: Any) -> None:
    """'Why did this run fail?' -> failures endpoint surfaces the error."""
    events = await _seed(client)
    failed = next(
        e["correlation"]["run_id"]
        for e in events
        if e["event"] == "pipeline.run" and e.get("outcome") == "failure"
    )
    response = await client.get(f"/v2/runs/{failed}/failures")
    assert response.status_code == 200
    failures = response.json()["failures"]
    assert failures, "failed run has no recorded failures"
    assert any(f.get("error") for f in failures)


async def test_what_is_affected_and_related(client: Any) -> None:
    """'What is affected?'/'what is related?' -> impact + lineage."""
    events = await _seed(client)
    run_id = events[0]["correlation"]["run_id"]
    impact = await client.get(f"/v2/runs/{run_id}/impact")
    assert impact.status_code == 200
    # Lineage for a materialized asset.
    asset_entity = next(
        e["entities"]["asset"] for e in events if e.get("entities", {}).get("asset")
    )
    lineage = await client.get(f"/v2/assets/{asset_entity}/lineage")
    assert lineage.status_code == 200


async def test_asset_history_and_wap_branch(client: Any) -> None:
    """'What happened to this asset?'/'this WAP branch?' -> history endpoints."""
    events = await _seed(client)
    asset_entity = next(
        e["entities"]["asset"] for e in events if e.get("entities", {}).get("asset")
    )
    history = await client.get(f"/v2/assets/{asset_entity}/history")
    assert history.status_code == 200
    assert history.json()["history"], "asset history is empty"

    health = await client.get(f"/v2/assets/{asset_entity}/health")
    assert health.status_code == 200


async def test_is_this_unusual_and_happened_before(client: Any) -> None:
    """'Is this unusual?'/'happened before?' -> insights carry evidence."""
    await _seed(client)
    response = await client.get("/v2/insights")
    assert response.status_code == 200
    for insight in response.json()["items"]:
        assert insight["rule"]
        assert "evidence" in insight


async def test_evidence_provenance(client: Any, make_event: Any) -> None:
    """'What evidence supports this?' -> event provenance endpoint."""
    event = make_event()
    await client.post("/v1/events", json=[event])
    response = await client.get(f"/v2/events/{event['event_id']}/provenance")
    assert response.status_code == 200


async def test_search_finds_telemetry(client: Any) -> None:
    """Free-text search over canonical events."""
    run_id = f"search-{uuid.uuid4().hex[:8]}"
    events = wl.dagster_run(run_id, wl.T0)
    response = await client.post("/v1/events", json=events)
    assert response.status_code == 202
    found = await client.get("/v2/search", params={"run_id": run_id})
    assert found.status_code == 200
    assert len(found.json()["items"]) == len(events)
