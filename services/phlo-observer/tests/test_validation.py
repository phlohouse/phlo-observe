"""Malformed-input hardening: every failure mode must be a controlled 4xx.

Covers the review findings: non-UUID event_ids, malformed adapter payloads,
bad cursors/dates, gzip bombs, oversized bodies, and the run-upsert race.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import uuid
from typing import Any

import pytest
from httpx import AsyncClient

# -- canonical /v1/events ---------------------------------------------------


@pytest.mark.asyncio
async def test_non_uuid_event_id_rejected_per_item(client: AsyncClient) -> None:
    """A schema-valid but non-UUID event_id is an item error, never a 500."""
    bad = {
        "schema_version": "1.0",
        "event_id": "not-a-uuid",
        "event": "pipeline.step",
        "category": "pipeline",
        "outcome": "success",
        "severity": "info",
        "delivery": "telemetry",
        "observed_at": "2025-01-01T00:00:00Z",
        "service": {"name": "x"},
        "correlation": {},
        "attributes": {},
    }
    resp = await client.post("/v1/events", json=bad)
    assert resp.status_code == 422
    body = resp.json()
    assert body["rejected"] == 1
    assert body["errors"][0]["code"] == "SCHEMA_INVALID"


@pytest.mark.asyncio
async def test_mixed_batch_bad_uuid_keeps_good_items(client: AsyncClient, make_event: Any) -> None:
    good = make_event()
    bad = dict(make_event())
    bad["event_id"] = "01JNOTAUUID000000000000000"
    resp = await client.post("/v1/events", json=[good, bad])
    body = resp.json()
    assert resp.status_code == 202
    assert body["accepted"] == 1
    assert body["rejected"] == 1
    assert body["errors"][0]["index"] == 1


@pytest.mark.asyncio
async def test_bad_observed_at_rejected_per_item(client: AsyncClient, make_event: Any) -> None:
    bad = make_event(observed_at="not-a-date")
    resp = await client.post("/v1/events", json=bad)
    body = resp.json()
    assert body["rejected"] == 1
    assert body["errors"][0]["code"] == "SCHEMA_INVALID"


# -- source adapters ---------------------------------------------------------


@pytest.mark.asyncio
async def test_dagster_malformed_json_is_422(client: AsyncClient) -> None:
    resp = await client.post("/v1/ingest/dagster", content=b"{not valid json")
    assert resp.status_code == 422
    assert resp.json()["errors"][0]["code"] == "NORMALIZATION_FAILED"


@pytest.mark.asyncio
async def test_dagster_scalar_payload_is_422(client: AsyncClient) -> None:
    for payload in (b"42", b'"just a string"', b"null", b"[1, 2, 3]"):
        resp = await client.post("/v1/ingest/dagster", content=payload)
        assert resp.status_code == 422, payload


@pytest.mark.asyncio
async def test_dbt_non_object_payload_is_422(client: AsyncClient) -> None:
    for payload in (b"[1,2,3]", b'"text"', b"42", b"null"):
        resp = await client.post("/v1/ingest/dbt", content=payload)
        assert resp.status_code == 422, payload
        assert resp.json()["errors"]


@pytest.mark.asyncio
async def test_dbt_bad_json_is_422(client: AsyncClient) -> None:
    resp = await client.post("/v1/ingest/dbt", content=b"{{{{")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_generic_bad_json_is_422(client: AsyncClient) -> None:
    resp = await client.post("/v1/ingest/generic", content=b"}{")
    assert resp.status_code == 422


# -- query parameter validation ---------------------------------------------


@pytest.mark.asyncio
async def test_invalid_cursor_events_is_400(client: AsyncClient) -> None:
    resp = await client.get("/v1/events", params={"cursor": "!!!not-base64!!!"})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_invalid_cursor_shape_is_400(client: AsyncClient) -> None:
    import base64

    # valid base64 but not a "timestamp|uuid" cursor
    resp = await client.get(
        "/v1/events", params={"cursor": base64.urlsafe_b64encode(b"nope").decode()}
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_invalid_cursor_runs_is_400(client: AsyncClient) -> None:
    resp = await client.get("/v1/runs", params={"cursor": "!!!not-base64!!!"})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_invalid_since_until_is_400(client: AsyncClient) -> None:
    resp = await client.get("/v1/events", params={"since": "not-a-date"})
    assert resp.status_code == 400
    resp = await client.get("/v1/events", params={"until": "yesterday"})
    assert resp.status_code == 400


# -- body limits --------------------------------------------------------------


@pytest.mark.asyncio
async def test_gzip_bomb_rejected_413(
    client: AsyncClient, database_url: str, session_factory: Any, make_event: Any
) -> None:
    """A small gzip body decompressing past the limit is rejected, not OOMed."""
    from httpx import ASGITransport
    from phlo_observer.app import create_app
    from phlo_observer.settings import ObserverSettings

    settings = ObserverSettings(database_url=database_url, max_body_bytes=64 * 1024)
    app = create_app(settings)
    app.state.session_factory = session_factory
    big = json.dumps([make_event() for _ in range(2000)]).encode()  # ~1MB plain
    compressed = gzip.compress(big)
    assert len(compressed) < 64 * 1024  # wire size under the limit
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        resp = await c.post(
            "/v1/events",
            content=compressed,
            headers={"Content-Encoding": "gzip"},
        )
        assert resp.status_code == 413


@pytest.mark.asyncio
async def test_chunked_body_still_bounded(
    client: AsyncClient, database_url: str, session_factory: Any, make_event: Any
) -> None:
    """No content-length header: the stream read itself enforces the cap."""
    from httpx import ASGITransport
    from phlo_observer.app import create_app
    from phlo_observer.settings import ObserverSettings

    settings = ObserverSettings(database_url=database_url, max_body_bytes=256)
    app = create_app(settings)
    app.state.session_factory = session_factory
    payload = json.dumps([make_event() for _ in range(10)]).encode()

    async def stream():
        for i in range(0, len(payload), 64):
            yield payload[i : i + 64]

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        resp = await c.post("/v1/events", content=stream())
        assert resp.status_code == 413


# -- concurrent ingestion -----------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_first_events_same_run(client: AsyncClient, make_event: Any) -> None:
    """Different events racing to create one run_id must all succeed."""
    run_id = f"race-{uuid.uuid4().hex[:8]}"
    events = [make_event(event="pipeline.step", correlation={"run_id": run_id}) for _ in range(8)]
    statuses = await asyncio.gather(*[client.post("/v1/events", json=e) for e in events])
    assert all(r.status_code == 202 for r in statuses), [r.status_code for r in statuses]
    resp = await client.get("/v1/events", params={"run_id": run_id})
    assert len(resp.json()["items"]) == len(events)
    run = (await client.get(f"/v1/runs/{run_id}")).json()
    assert run["event_count"] == len(events)


# -- run projection semantics -------------------------------------------------


@pytest.mark.asyncio
async def test_running_run_has_no_ended_at(client: AsyncClient, make_event: Any) -> None:
    """A start signal (outcome=unknown) must not write ended_at."""
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    await client.post(
        "/v1/events",
        json=make_event(
            event="pipeline.run",
            outcome="unknown",
            correlation={"run_id": run_id},
        ),
    )
    run = (await client.get(f"/v1/runs/{run_id}")).json()
    assert run["status"] == "running"
    assert run["ended_at"] is None


@pytest.mark.asyncio
async def test_late_start_signal_does_not_downgrade_terminal(
    client: AsyncClient, make_event: Any
) -> None:
    """A STARTED arriving after SUCCESS must not flip status back to running."""
    run_id = f"run-{uuid.uuid4().hex[:8]}"
    await client.post(
        "/v1/events",
        json=make_event(
            event="pipeline.run",
            outcome="success",
            correlation={"run_id": run_id},
        ),
    )
    await client.post(
        "/v1/events",
        json=make_event(
            event="pipeline.run",
            outcome="unknown",  # late duplicate/ordering anomaly
            correlation={"run_id": run_id},
        ),
    )
    run = (await client.get(f"/v1/runs/{run_id}")).json()
    assert run["status"] == "success"


# -- scoped query routes --------------------------------------------------------


@pytest.mark.asyncio
async def test_asset_events_route(client: AsyncClient, make_event: Any) -> None:
    await client.post(
        "/v1/events",
        json=make_event(
            event="asset.materialize",
            correlation={"asset_key": "mart/fct_orders"},
        ),
    )
    resp = await client.get("/v1/assets/mart/fct_orders/events")
    assert resp.status_code == 200
    assert len(resp.json()["items"]) == 1


@pytest.mark.asyncio
async def test_branch_events_route_with_slashes(client: AsyncClient, make_event: Any) -> None:
    branch = "run/R1/attempt-2"
    await client.post(
        "/v1/events",
        json=make_event(event="wap.promote", correlation={"branch": branch}),
    )
    resp = await client.get(f"/v1/branches/{branch}/events")
    assert resp.status_code == 200
    assert len(resp.json()["items"]) == 1


@pytest.mark.asyncio
async def test_table_events_route(client: AsyncClient, make_event: Any) -> None:
    await client.post(
        "/v1/events",
        json=make_event(event="table.commit", correlation={"table": "iceberg.mart.t"}),
    )
    resp = await client.get("/v1/tables/iceberg.mart.t/events")
    assert resp.status_code == 200
    assert len(resp.json()["items"]) == 1


@pytest.mark.asyncio
async def test_scoped_routes_respect_time_filters(client: AsyncClient, make_event: Any) -> None:
    await client.post(
        "/v1/events",
        json=make_event(
            observed_at="2025-01-01T00:00:00Z",
            correlation={"asset_key": "a.b"},
        ),
    )
    resp = await client.get("/v1/assets/a.b/events", params={"since": "2025-06-01T00:00:00Z"})
    assert resp.json()["items"] == []
    resp = await client.get("/v1/assets/a.b/events", params={"since": "bogus"})
    assert resp.status_code == 400


# -- health ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_spec_health_routes(client: AsyncClient) -> None:
    assert (await client.get("/health/live")).status_code == 200
    ready = await client.get("/health/ready")
    assert ready.status_code == 200
    assert ready.json()["database"] == "ok"


@pytest.mark.asyncio
async def test_health_aliases_still_work(client: AsyncClient) -> None:
    assert (await client.get("/healthz")).status_code == 200
    assert (await client.get("/readyz")).status_code == 200


# -- startup enforcement --------------------------------------------------------


def test_require_tokens_hard_fails_app_creation(database_url: str) -> None:
    from phlo_observer.app import create_app
    from phlo_observer.settings import ObserverSettings

    with pytest.raises(ValueError, match="ingest tokens"):
        create_app(ObserverSettings(database_url=database_url, auth_optional_dev=False))


def test_self_observe_drains_reject_http(database_url: str) -> None:
    from phlo_observer.settings import ObserverSettings

    settings = ObserverSettings(database_url=database_url, self_observe_drains="console,http")
    with pytest.raises(ValueError, match="may not include 'http'"):
        settings.self_observe_drain_configs()


def test_self_observe_drains_parse(database_url: str) -> None:
    from phlo_observer.settings import ObserverSettings

    settings = ObserverSettings(
        database_url=database_url, self_observe_drains="console,otlp", otlp_endpoint="http://x"
    )
    configs = settings.self_observe_drain_configs()
    assert configs[0] == {"type": "console"}
    assert configs[1] == {"type": "otlp", "endpoint": "http://x"}


def test_load_settings_resolves_token_file(
    database_url: str, tmp_path: Any, monkeypatch: Any
) -> None:
    from phlo_observer.settings import load_settings

    secret = tmp_path / "tokens"
    secret.write_text("file-token-1,file-token-2\n")
    monkeypatch.setenv("PHLO_OBSERVER_INGEST_TOKENS_FILE", str(secret))
    monkeypatch.setenv("PHLO_OBSERVER_DATABASE_URL", database_url)
    settings = load_settings()
    assert set(settings.ingest_tokens) == {"file-token-1", "file-token-2"}


@pytest.mark.asyncio
async def test_docs_disabled_setting(database_url: str, session_factory: Any) -> None:
    from httpx import ASGITransport
    from phlo_observer.app import create_app
    from phlo_observer.settings import ObserverSettings

    app = create_app(ObserverSettings(database_url=database_url, docs_enabled=False))
    app.state.session_factory = session_factory
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        assert (await c.get("/docs")).status_code == 404
        assert (await c.get("/openapi.json")).status_code == 404


@pytest.mark.asyncio
async def test_unmatched_route_metric_label(client: AsyncClient) -> None:
    """404s must not leak raw paths into the route label (cardinality)."""
    await client.get("/no/such/path/exists")
    body = (await client.get("/metrics")).text
    assert 'route="unmatched"' in body
    assert "/no/such/path" not in body
