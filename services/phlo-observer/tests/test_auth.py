"""Authentication: ingest/read token enforcement and dev mode."""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from phlo_observer.app import create_app
from phlo_observer.settings import ObserverSettings


@pytest_asyncio.fixture
async def authed_client(database_url: str, session_factory: Any) -> AsyncClient:
    """Client for an app configured with both token sets."""
    settings = ObserverSettings(
        database_url=database_url,
        ingest_tokens="ingest-one,ingest-two",
        read_tokens="read-one",
    )
    app = create_app(settings)
    app.state.session_factory = session_factory
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_ingest_requires_token(authed_client: AsyncClient, make_event: Any) -> None:
    resp = await authed_client.post("/v1/events", json=make_event())
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_ingest_accepts_bearer(authed_client: AsyncClient, make_event: Any) -> None:
    resp = await authed_client.post(
        "/v1/events",
        json=make_event(),
        headers={"Authorization": "Bearer ingest-two"},
    )
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 1


@pytest.mark.asyncio
async def test_ingest_accepts_api_key_header(authed_client: AsyncClient, make_event: Any) -> None:
    resp = await authed_client.post(
        "/v1/events",
        json=make_event(),
        headers={"X-API-Key": "ingest-one"},
    )
    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_read_rejects_ingest_token(authed_client: AsyncClient) -> None:
    resp = await authed_client.get("/v1/events", headers={"Authorization": "Bearer ingest-one"})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_read_accepts_read_token(authed_client: AsyncClient) -> None:
    resp = await authed_client.get("/v1/events", headers={"Authorization": "Bearer read-one"})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_health_endpoints_unauthenticated(authed_client: AsyncClient) -> None:
    assert (await authed_client.get("/health/live")).status_code == 200
    assert (await authed_client.get("/health/ready")).status_code == 200
    # metrics stays private unless metrics_public=true
    assert (await authed_client.get("/metrics")).status_code == 401
    resp = await authed_client.get("/metrics", headers={"Authorization": "Bearer read-one"})
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dev_mode_accepts_without_tokens(client: AsyncClient, make_event: Any) -> None:
    """No configured tokens = documented dev mode: everything allowed."""
    resp = await client.post("/v1/events", json=make_event())
    assert resp.status_code == 202
