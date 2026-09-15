"""Shared fixtures for repo-level contract/integration/performance tests.

Observer-backed fixtures reuse the same test Postgres as the service suite
(PHLO_OBSERVER_TEST_DATABASE_URL, default localhost:5432/phlo_observer_test —
created by `docker compose up -d postgres`) and skip cleanly when it is
unreachable.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

TEST_DATABASE_URL = os.environ.get(
    "PHLO_OBSERVER_TEST_DATABASE_URL",
    "postgresql+asyncpg://phlo:phlo@localhost:5432/phlo_observer_test",
)


async def _reachable(url: str) -> bool:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            await conn.exec_driver_sql("select 1")
        return True
    except Exception:
        return False
    finally:
        await engine.dispose()


def canonical_event(**overrides: Any) -> dict[str, Any]:
    """A minimal valid canonical event dict."""
    event = {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "event": "pipeline.run",
        "category": "pipeline",
        "outcome": "success",
        "severity": "info",
        "delivery": "telemetry",
        "observed_at": "2025-01-01T00:00:00Z",
        "service": {"name": "test-service", "version": "1.0", "environment": "test"},
        "correlation": {},
        "attributes": {},
    }
    event.update(overrides)
    return event


@pytest.fixture
def make_event() -> Any:
    """Factory fixture producing canonical event dicts."""
    return canonical_event


@pytest_asyncio.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[Any]]:
    """Session factory on a freshly created observer schema."""
    if not await _reachable(TEST_DATABASE_URL):
        pytest.skip("PostgreSQL not reachable; set PHLO_OBSERVER_TEST_DATABASE_URL")
    from phlo_observer.models import Base

    engine = create_async_engine(TEST_DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest_asyncio.fixture
async def client(session_factory: Any) -> AsyncIterator[AsyncClient]:
    """HTTP client against a fresh-schema observer app."""
    from phlo_observer.app import create_app
    from phlo_observer.settings import ObserverSettings

    app = create_app(ObserverSettings(database_url=TEST_DATABASE_URL))
    app.state.session_factory = session_factory
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
