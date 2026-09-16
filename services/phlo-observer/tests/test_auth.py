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


@pytest.mark.asyncio
async def test_admin_requires_admin_token_not_read(authed_client: AsyncClient) -> None:
    """Regression: a read token must not inherit admin powers.

    ``authed_client`` configures ingest+read tokens but no admin tokens —
    the admin surface must fail closed, not silently fall back to readers.
    """
    resp = await authed_client.get(
        "/v2/admin/quarantine", headers={"Authorization": "Bearer read-one"}
    )
    assert resp.status_code == 401
    resp = await authed_client.get("/v2/admin/quarantine")
    assert resp.status_code == 401
    resp = await authed_client.get(
        "/v2/admin/quarantine", headers={"Authorization": "Bearer ingest-one"}
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_admin_token_unlocks_admin_surface(database_url: str, session_factory: Any) -> None:
    """An explicitly configured admin token reaches admin endpoints."""
    settings = ObserverSettings(
        database_url=database_url,
        ingest_tokens="ingest-one",
        read_tokens="read-one",
        admin_tokens="admin-one",
    )
    app = create_app(settings)
    app.state.session_factory = session_factory
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/v2/admin/quarantine", headers={"Authorization": "Bearer admin-one"})
        assert resp.status_code == 200
        # Admin tokens are not read credentials either.
        resp = await c.get("/v1/events", headers={"Authorization": "Bearer admin-one"})
        assert resp.status_code == 401


def test_strict_mode_requires_admin_tokens(database_url: str) -> None:
    """auth_optional_dev=false must fail fast when admin tokens are missing."""
    settings = ObserverSettings(
        database_url=database_url,
        auth_optional_dev=False,
        ingest_tokens="i",
        read_tokens="r",
    )
    with pytest.raises(ValueError, match="admin tokens"):
        settings.require_tokens()
    # Fully configured passes.
    settings = ObserverSettings(
        database_url=database_url,
        auth_optional_dev=False,
        ingest_tokens="i",
        read_tokens="r",
        admin_tokens="a",
    )
    settings.require_tokens()


@pytest.fixture
def partial_client(database_url: str, session_factory: Any) -> Any:
    """Client factory: builds an app with whatever token set is passed."""

    def _make(**tokens: str) -> AsyncClient:
        settings = ObserverSettings(database_url=database_url, **tokens)
        app = create_app(settings)
        app.state.session_factory = session_factory
        return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")

    return _make


@pytest.mark.asyncio
async def test_ingest_only_tokens_fail_reads_closed(partial_client: Any, make_event: Any) -> None:
    """Regression: configuring only ingest tokens must not leave reads open.

    Previously an unset read-token list meant "dev mode" on the read
    surface even when credentials existed elsewhere — anonymous disclosure
    of asset keys, table names and error payloads.
    """
    client = partial_client(ingest_tokens="ingest-one")
    async with client:
        assert (await client.get("/v1/events")).status_code == 401
        assert (await client.get("/v2/insights")).status_code == 401
        resp = await client.post(
            "/v1/events",
            json=make_event(),
            headers={"Authorization": "Bearer ingest-one"},
        )
        assert resp.status_code == 202


@pytest.mark.asyncio
async def test_read_only_tokens_fail_ingest_closed(partial_client: Any, make_event: Any) -> None:
    """Regression: configuring only read tokens must not leave ingest open —
    anonymous writes would inject canonical events into the system of record."""
    client = partial_client(read_tokens="read-one")
    async with client:
        assert (await client.post("/v1/events", json=make_event())).status_code == 401
        resp = await client.get("/v1/events", headers={"Authorization": "Bearer read-one"})
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_admin_only_tokens_fail_other_surfaces_closed(
    partial_client: Any, make_event: Any
) -> None:
    """Admin tokens alone still fail ingest/read closed."""
    client = partial_client(admin_tokens="admin-one")
    async with client:
        assert (await client.post("/v1/events", json=make_event())).status_code == 401
        assert (await client.get("/v1/events")).status_code == 401
        resp = await client.get(
            "/v2/admin/quarantine", headers={"Authorization": "Bearer admin-one"}
        )
        assert resp.status_code == 200
