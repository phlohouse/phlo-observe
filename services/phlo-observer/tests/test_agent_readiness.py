"""Agent/query readiness (hardening item 11).

observe-query consumers must get bounded, structured, evidence-bearing
data — never unbounded histories or write access.
"""

from __future__ import annotations

import inspect
from typing import Any

import observe_query
import pytest
import workloads as wl

asyncio_only = pytest.mark.asyncio


def test_client_surface_is_read_only() -> None:
    """The public client exposes no mutating HTTP verbs beyond the
    read-style compare-runs query."""
    mutating = [
        name
        for name, _ in inspect.getmembers(observe_query.ObserverClient, inspect.isfunction)
        if not name.startswith("_") and name in {"put", "delete", "patch", "create", "update"}
    ]
    assert not mutating
    for name, fn in inspect.getmembers(observe_query.ObserverClient, inspect.isfunction):
        if name.startswith("_") or name in {"close"}:
            continue
        # Every public method hits a GET or the compare-runs query POST.
        src = inspect.getsource(fn)
        assert "._get(" in src or "compare-runs" in src or "._post(" not in src, name


@asyncio_only
async def test_insight_and_incident_lists_are_bounded(client: Any) -> None:
    """List endpoints enforce their server-side caps."""
    await client.post("/v1/events", json=wl.mixed_history(days=2, seed=22))
    for path in ("/v2/insights", "/v2/incidents", "/v2/schemas"):
        too_large = await client.get(path, params={"limit": 100_000})
        assert too_large.status_code == 422, path
        capped = await client.get(path, params={"limit": 5})
        assert capped.status_code == 200
        assert len(capped.json()["items"]) <= 5


@asyncio_only
async def test_asset_history_is_bounded(client: Any) -> None:
    """Asset history honors the limit parameter even with long histories."""
    events = wl.mixed_history(days=2, seed=22)
    await client.post("/v1/events", json=events)
    asset = next(e["entities"]["asset"] for e in events if e.get("entities", {}).get("asset"))
    response = await client.get(f"/v2/assets/{asset}/history", params={"limit": 2})
    assert response.status_code == 200
    assert len(response.json()["history"]) <= 2


@asyncio_only
async def test_investigation_bundle_shape(client: Any) -> None:
    """Bundles carry evidence IDs, separate facts from candidate causes, and
    flag truncation."""
    events = wl.mixed_history(days=2, seed=22)
    await client.post("/v1/events", json=events)
    run_id = events[0]["correlation"]["run_id"]
    bundle = await client.get(f"/v2/runs/{run_id}/investigate")
    assert bundle.status_code == 200
    body = bundle.json()
    assert isinstance(body["evidence"], list)
    assert all(isinstance(eid, str) for eid in body["evidence"])
    assert "candidate_causes" in body  # derived, labeled separately
    assert "truncated" in body
    assert "run" in body and "timeline" in body


@asyncio_only
async def test_event_provenance_round_trips(client: Any, make_event: Any) -> None:
    """Every canonical event id is resolvable to its provenance record."""
    event = make_event()
    await client.post("/v1/events", json=[event])
    response = await client.get(f"/v2/events/{event['event_id']}/provenance")
    assert response.status_code == 200
    body = response.json()
    assert body["event_id"] == event["event_id"]
