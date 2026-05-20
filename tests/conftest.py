"""Shared fixtures for dewey tests — uses real Postgres."""

import os

import pytest
import pytest_asyncio
from sqlalchemy import create_engine, delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from dewey.sqlalchemy.models import Base, TaskEntryModel
from dewey.sqlalchemy.notification_models import (  # noqa: F401 — ensure models registered
    NotificationAttemptModel,
    NotificationEntryModel,
)

# Default: local Postgres via Docker (port 5432)
# Override with DEWEY_TEST_DATABASE_URL env var
DEFAULT_TEST_DB = "postgresql://postgres:postgres@localhost:5432/dewey_test"
DEFAULT_TEST_DB_ASYNC = "postgresql+asyncpg://postgres:postgres@localhost:5432/dewey_test"


@pytest.fixture(scope="session")
def engine():
    """Postgres engine for testing — real JSONB, real partial indexes."""
    url = os.environ.get("DEWEY_TEST_DATABASE_URL", DEFAULT_TEST_DB)
    engine = create_engine(url)

    # Fresh schema each test run
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    yield engine

    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture(autouse=True)
def cleanup(engine):
    """Delete all task entries after each test — handles committed data."""
    yield
    with Session(engine) as session:
        session.execute(delete(NotificationAttemptModel))
        session.execute(delete(NotificationEntryModel))
        session.execute(delete(TaskEntryModel))
        session.commit()


@pytest.fixture
def session(engine):
    """Session for test use. process_task commits internally; cleanup handles teardown."""
    with Session(engine) as session:
        yield session


# --- Async fixtures (asyncpg) ---


@pytest_asyncio.fixture
async def async_engine():
    """Async Postgres engine — fresh per test to avoid event loop mismatch."""
    url = os.environ.get("DEWEY_TEST_DATABASE_URL_ASYNC", DEFAULT_TEST_DB_ASYNC)
    engine = create_async_engine(url, pool_size=2, max_overflow=0)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def async_session(async_engine):
    """Fresh session per test. Cleanup runs in a separate session."""
    factory = async_sessionmaker(async_engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        yield session

    # Cleanup in a separate session
    async with factory() as cleanup_session:
        await cleanup_session.execute(NotificationAttemptModel.__table__.delete())
        await cleanup_session.execute(NotificationEntryModel.__table__.delete())
        await cleanup_session.execute(TaskEntryModel.__table__.delete())
        await cleanup_session.commit()
