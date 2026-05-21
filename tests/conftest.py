"""Shared fixtures for dewey tests — uses real Postgres."""

import multiprocessing as mp
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import create_engine, delete, text
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


@dataclass(frozen=True)
class ClaimRunResult:
    """Result from a multi-process SKIP LOCKED claim harness run."""

    claimed_ids: list[int]
    by_worker: dict[str, list[int]]


@dataclass(frozen=True)
class PostgresConcurrencyHarness:
    """Reusable real-Postgres harness for multi-process claim tests."""

    database_url: str
    table_name: str = "dewey_skip_locked_smoke"

    def reset_table(self, item_count: int) -> None:
        engine = create_engine(self.database_url)
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {self.table_name}"))
                conn.execute(
                    text(
                        f"""
                        CREATE TABLE {self.table_name} (
                            id integer PRIMARY KEY,
                            claimed_by text,
                            claimed_at timestamptz
                        )
                        """
                    )
                )
                conn.execute(
                    text(f"INSERT INTO {self.table_name} (id) SELECT generate_series(1, :count)"),
                    {"count": item_count},
                )
        finally:
            engine.dispose()

    def run_claimers(self, *, worker_count: int, batch_size: int = 1) -> ClaimRunResult:
        ctx = mp.get_context("spawn")
        result_queue: mp.Queue = ctx.Queue()
        workers = [
            ctx.Process(
                target=_skip_locked_claim_worker,
                args=(self.database_url, self.table_name, f"worker-{idx}", batch_size, result_queue),
            )
            for idx in range(worker_count)
        ]

        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=30)

        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
                raise RuntimeError(f"{worker.name} did not finish")
            if worker.exitcode != 0:
                raise RuntimeError(f"{worker.name} exited with {worker.exitcode}")

        by_worker: dict[str, list[int]] = {}
        for _ in workers:
            worker_id, claimed = result_queue.get(timeout=5)
            by_worker[worker_id] = claimed

        claimed_ids = [claim for claims in by_worker.values() for claim in claims]
        return ClaimRunResult(claimed_ids=claimed_ids, by_worker=by_worker)


# The worker must be module-level so multiprocessing spawn can import it.
def _skip_locked_claim_worker(
    database_url: str,
    table_name: str,
    worker_id: str,
    batch_size: int,
    result_queue: Any,
) -> None:
    engine = create_engine(database_url)
    claimed: list[int] = []
    try:
        while True:
            with engine.begin() as conn:
                rows = conn.execute(
                    text(
                        f"""
                        SELECT id
                        FROM {table_name}
                        WHERE claimed_by IS NULL
                        ORDER BY id
                        LIMIT :batch_size
                        FOR UPDATE SKIP LOCKED
                        """
                    ),
                    {"batch_size": batch_size},
                ).scalars().all()
                if not rows:
                    break
                conn.execute(
                    text(
                        f"""
                        UPDATE {table_name}
                        SET claimed_by = :worker_id, claimed_at = now()
                        WHERE id = ANY(:ids)
                        """
                    ),
                    {"worker_id": worker_id, "ids": rows},
                )
                claimed.extend(rows)
    finally:
        engine.dispose()

    result_queue.put((worker_id, claimed))


@pytest.fixture
def postgres_concurrency_harness() -> Callable[[int], PostgresConcurrencyHarness]:
    """Build a reusable multi-process real-Postgres concurrency harness."""

    def build(item_count: int) -> PostgresConcurrencyHarness:
        url = os.environ.get("DEWEY_TEST_DATABASE_URL", DEFAULT_TEST_DB)
        harness = PostgresConcurrencyHarness(database_url=url)
        harness.reset_table(item_count)
        return harness

    return build


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
