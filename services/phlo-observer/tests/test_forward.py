"""OTLP forwarding: payload shape, fire-and-forget scheduling, failure isolation."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
import respx
from httpx import AsyncClient
from phlo_observer.forward import _logs_url, forward_events, otlp_logs_payload


def test_logs_url_appends_default_path() -> None:
    assert _logs_url("http://collector:4318") == "http://collector:4318/v1/logs"
    assert _logs_url("http://collector:4318/") == "http://collector:4318/v1/logs"
    assert _logs_url("http://collector:4318/custom") == "http://collector:4318/custom"


def test_otlp_payload_shape(make_event: Any) -> None:
    event = make_event(
        severity="error",
        correlation={"run_id": "r-1", "extra": {"custom_key": "v"}},
        attributes={"rows_out": 12},
    )
    payload = otlp_logs_payload([event], "1700000000000000000")
    resource_logs = payload["resourceLogs"]
    assert len(resource_logs) == 1
    resource_attrs = {a["key"]: a["value"] for a in resource_logs[0]["resource"]["attributes"]}
    assert resource_attrs["service.name"] == {"stringValue": "test-service"}
    records = resource_logs[0]["scopeLogs"][0]["logRecords"]
    assert len(records) == 1
    record = records[0]
    assert record["severityNumber"] == 17
    assert record["severityText"] == "ERROR"
    assert record["body"] == {"stringValue": "pipeline.run"}
    attrs = {a["key"]: a["value"] for a in record["attributes"]}
    # Forwarded records use the observe.* encoding shared with the OTLP drain,
    # so a re-ingesting observer restores correlation and structured sections.
    assert attrs["observe.correlation.run_id"] == {"stringValue": "r-1"}
    assert json.loads(attrs["observe.correlation.extra"]["stringValue"]) == {"custom_key": "v"}
    assert json.loads(attrs["observe.attributes"]["stringValue"]) == {"rows_out": 12}
    assert attrs["observe.outcome"] == {"stringValue": "success"}
    assert attrs["observe.event_id"] == {"stringValue": event["event_id"]}


def test_otlp_payload_groups_by_service(make_event: Any) -> None:
    a = make_event(service={"name": "svc-a"})
    b = make_event(service={"name": "svc-b"})
    payload = otlp_logs_payload([a, b], "1")
    assert len(payload["resourceLogs"]) == 2


@pytest.mark.asyncio
@respx.mock
async def test_forward_posts_otlp_json(make_event: Any) -> None:
    route = respx.post("http://collector:4318/v1/logs").respond(200)
    await forward_events([make_event()], "http://collector:4318")
    assert route.called
    body = json.loads(route.calls[0].request.content)
    assert "resourceLogs" in body


@pytest.mark.asyncio
@respx.mock
async def test_forward_failure_does_not_raise(make_event: Any) -> None:
    respx.post("http://collector:4318/v1/logs").respond(500)
    await forward_events([make_event()], "http://collector:4318")
    respx.post("http://down:4318/v1/logs").mock(side_effect=httpx.ConnectError("down"))
    await forward_events([make_event()], "http://down:4318")


@pytest.mark.asyncio
async def test_ingest_forward_is_fire_and_forget(
    client: AsyncClient, app: Any, settings: Any, make_event: Any, monkeypatch: Any
) -> None:
    """A hung forward endpoint must not delay the ingest response."""
    started = asyncio.Event()
    release = asyncio.Event()
    captured: list[dict[str, Any]] = []

    async def fake_forward(events: list[dict[str, Any]], endpoint: str) -> None:
        started.set()
        captured.extend(events)
        await release.wait()

    monkeypatch.setattr("phlo_observer.app.forward_events", fake_forward)
    settings.otlp_endpoint = "http://collector:4318"

    good = make_event(correlation={"run_id": "r-forward"})
    bad = make_event(event_id="not-a-uuid")
    resp = await client.post("/v1/events", json=[good, bad])
    assert resp.status_code == 202
    body = resp.json()
    assert body["accepted"] == 1
    # The response returned even though the forward task is still blocked.
    await asyncio.wait_for(started.wait(), timeout=2.0)
    # Only the persisted event is forwarded.
    assert [e["event_id"] for e in captured] == [good["event_id"]]
    release.set()
    pending = [t for t in app.state.forward_tasks if not t.done()]
    if pending:
        await asyncio.wait_for(asyncio.gather(*pending), timeout=2.0)
