"""Async engine and session plumbing."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from phlo_observer.settings import ObserverSettings


def make_engine(settings: ObserverSettings) -> AsyncEngine:
    """Create the async SQLAlchemy engine for the configured database."""
    kwargs: dict = {}
    if settings.database_url.startswith("sqlite"):
        kwargs["connect_args"] = {"timeout": 30}
    else:
        kwargs["pool_size"] = settings.db_pool_size
        kwargs["max_overflow"] = settings.db_pool_max_overflow
        kwargs["pool_pre_ping"] = True
    return create_async_engine(settings.database_url, **kwargs)


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Session factory bound to ``engine``."""
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def check_database(factory: async_sessionmaker[AsyncSession]) -> bool:
    """Return True when a session can execute ``SELECT 1``."""
    try:
        async with factory() as session:
            await session.execute(text("select 1"))
        return True
    except Exception:
        return False


@asynccontextmanager
async def session_scope(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """Async context manager yielding a session, committing on success."""
    async with factory() as session, session.begin():
        yield session
