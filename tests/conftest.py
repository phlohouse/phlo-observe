"""Shared fixtures for repo-level contract/integration/performance tests.

Observer-backed fixtures reuse the same test Postgres as the service suite
(PHLO_OBSERVER_TEST_DATABASE_URL, default localhost:5433) and skip cleanly
when it is unreachable.
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
    "postgresql+asyncpg://phlo:phlo@localhost:5433/phlo_test",
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
async def client() -> AsyncIterator[AsyncClient]:
    """HTTP client against a fresh-schema observer app."""
    if not await _reachable(TEST_DATABASE_URL):
        pytest.skip("PostgreSQL not reachable; set PHLO_OBSERVER_TEST_DATABASE_URL")
    from phlo_observer.app import create_app
    from phlo_observer.models import Base
    from phlo_observer.settings import ObserverSettings

    engine = create_async_engine(TEST_DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    app = create_app(ObserverSettings(database_url=TEST_DATABASE_URL))
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    await engine.dispose()
