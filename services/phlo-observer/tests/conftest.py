"""Observer test fixtures: real PostgreSQL via PHLO_OBSERVER_TEST_DATABASE_URL.

Tests skip when Postgres is unreachable — CI provides it as a service. SQLite
is never substituted: JSONB semantics and the migration suite need Postgres.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from phlo_observer.app import create_app
from phlo_observer.models import Base
from phlo_observer.settings import ObserverSettings
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

TEST_DATABASE_URL = os.environ.get(
    "PHLO_OBSERVER_TEST_DATABASE_URL",
    "postgresql+asyncpg://phlo:phlo@localhost:5432/phlo_observer_test",
)


async def _reachable(url: str) -> bool:
    engine = create_async_engine(url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            await conn.exec_driver_sql("select 1")
        return True
    except Exception:
        return False
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def database_url() -> str:
    """The Postgres URL tests run against."""
    return TEST_DATABASE_URL


@pytest_asyncio.fixture
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    """Per-test engine with a freshly created schema."""
    if not await _reachable(database_url):
        pytest.skip("PostgreSQL not reachable; set PHLO_OBSERVER_TEST_DATABASE_URL")
    engine = create_async_engine(database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
def settings(database_url: str) -> ObserverSettings:
    """Observer settings bound to the test database, dev-mode auth."""
    return ObserverSettings(
        database_url=database_url,
        ingest_tokens="",
        read_tokens="",
        metrics_enabled=True,
    )


@pytest_asyncio.fixture
async def session_factory(
    engine: AsyncEngine,
) -> AsyncIterator[async_sessionmaker[Any]]:
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory


@pytest_asyncio.fixture
async def app(settings: ObserverSettings, session_factory: Any):
    """The ASGI app wired to the test session factory (lifespan bypassed)."""
    application = create_app(settings)
    application.state.session_factory = session_factory
    application.state.engine = None
    return application


@pytest_asyncio.fixture
async def client(app: Any) -> AsyncIterator[AsyncClient]:
    """HTTP client against the ASGI app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


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
