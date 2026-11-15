"""Real Postgres + spawned processes: old outcomes cannot overwrite a replacement."""

from __future__ import annotations

import asyncio
import copy
import multiprocessing
from contextlib import contextmanager

import pytest
from sqlalchemy.orm import Session

from dewey.core.states import TaskStatus
from tests._helpers.execution_fencing import execute_probe

BACKENDS = [
    "sqlalchemy",
    "sqlalchemy-async",
    pytest.param("django", marks=pytest.mark.django_db(transaction=True)),
]
FIELDS = (
    "status",
    "attempts",
    "error",
    "scheduled_for",
    "started_at",
    "completed_at",
    "updated_at",
)


def seed(backend, engine):
    if backend == "django":
        from django.db import connection

        from dewey.django.executor import create_task

        task = create_task(task_type="synthetic.fencing", max_attempts=3)
        return task.id, copy.deepcopy(connection.settings_dict)
    from dewey.sqlalchemy.executor import create_task

    with Session(engine) as session:
        task = create_task(session, task_type="synthetic.fencing", max_attempts=3)
        task_id = task.id
        session.commit()
    return task_id, engine.url.render_as_string(hide_password=False)


def snapshot(backend, engine, task_id):
    if backend == "django":
        from dewey.django.models import TaskEntry

        row = TaskEntry.objects.get(pk=task_id)
        values = {field: getattr(row, field) for field in FIELDS}
        values["execution_token"] = getattr(row, "execution_token", None)
        return values
    from dewey.sqlalchemy.models import TaskEntryModel

    with Session(engine) as session:
        row = session.get(TaskEntryModel, task_id)
        assert row is not None
        values = {field: getattr(row, field) for field in FIELDS}
        values["execution_token"] = getattr(row, "execution_token", None)
        return values


def operator_action(backend, engine, task_id, action):
    if backend == "django":
        from dewey.django.queries import bulk_retry, kill_task, retry_task

        if action in ("kill", "kill-retry"):
            kill_task(task_id)
        if action in ("retry", "kill-retry"):
            retry_task(task_id)
        if action == "bulk-retry":
            assert bulk_retry(status=TaskStatus.PROCESSING) == 1
        return
    if backend == "sqlalchemy":
        from dewey.sqlalchemy.queries import bulk_retry, kill_task, retry_task

        with Session(engine) as session:
            if action in ("kill", "kill-retry"):
                kill_task(session, task_id)
            if action in ("retry", "kill-retry"):
                retry_task(session, task_id)
            if action == "bulk-retry":
                assert bulk_retry(session, status=TaskStatus.PROCESSING) == 1
            session.commit()
        return
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    from dewey.sqlalchemy.async_queries import bulk_retry_async, kill_task_async, retry_task_async

    async def run():
        async_engine = create_async_engine(engine.url.set(drivername="postgresql+asyncpg"))
        try:
            async with AsyncSession(async_engine) as session:
                if action in ("kill", "kill-retry"):
                    await kill_task_async(session, task_id)
                if action in ("retry", "kill-retry"):
                    await retry_task_async(session, task_id)
                if action == "bulk-retry":
                    assert await bulk_retry_async(session, status=TaskStatus.PROCESSING) == 1
                await session.commit()
        finally:
            await async_engine.dispose()

    asyncio.run(run())


@contextmanager
def worker(backend, database, task_id, outcome="success"):
    context = multiprocessing.get_context("spawn")
    started, release = context.Event(), context.Event()
    results = context.Queue()
    child = context.Process(
        target=execute_probe,
        args=(backend, database, task_id, started, release, results, outcome),
    )
    child.start()
    try:
        assert started.wait(8), "Spawned worker never entered the handler"
        yield child, release, results
    finally:
        # A killed Event waiter cannot acknowledge multiprocessing.Condition.notify_all.
        # Never signal its barrier after death; that can deadlock the test parent.
        if child.is_alive():
            release.set()
        child.join(2)
        if child.is_alive():
            child.terminate()
            child.join(2)
        if child.is_alive():
            child.kill()
            child.join(2)
        assert not child.is_alive(), "Owned probe child was not reaped"
        child.close()
        results.close()
        results.join_thread()


def finish(child, release, results):
    release.set()
    result = results.get(timeout=8)
    child.join(3)
    assert child.exitcode == 0
    return result


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    "outcome,status",
    [
        ("success", TaskStatus.COMPLETED),
        ("retryable", TaskStatus.FAILED),
        ("nonretryable", TaskStatus.DEAD),
    ],
)
def test_current_execution_clears_token_when_recording_its_outcome(
    engine, backend, outcome, status
):
    task_id, database = seed(backend, engine)
    with worker(backend, database, task_id, outcome) as current:
        assert snapshot(backend, engine, task_id)["execution_token"]
        assert finish(*current) is (outcome == "success")
    after = snapshot(backend, engine, task_id)
    assert after["attempts"] == 1
    assert after["status"] == status.value
    assert after["execution_token"] is None


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("action", ["retry", "bulk-retry", "kill", "kill-retry"])
def test_operator_action_invalidates_old_execution_without_stopping_external_effects(
    engine, backend, action
):
    """Reset/kill invalidates the writer, not the already running OS process."""
    task_id, database = seed(backend, engine)
    with worker(backend, database, task_id) as old:
        original = snapshot(backend, engine, task_id)
        assert original["execution_token"]
        operator_action(backend, engine, task_id, action)
        invalidated = snapshot(backend, engine, task_id)
        assert invalidated["execution_token"] is None
        if action == "kill":
            assert invalidated["status"] == TaskStatus.DEAD.value
            assert invalidated["attempts"] == 1
            assert finish(*old) is False
            assert snapshot(backend, engine, task_id) == invalidated
        else:
            assert invalidated["status"] == TaskStatus.PENDING.value
            # Preserve existing explicit operator reset semantics.
            assert invalidated["attempts"] == 0
            with worker(backend, database, task_id) as replacement:
                before = snapshot(backend, engine, task_id)
                assert before["attempts"] == 1
                assert before["execution_token"] != original["execution_token"]
                assert finish(*old) is False
                assert snapshot(backend, engine, task_id) == before
                assert finish(*replacement) is True


@pytest.mark.parametrize("backend", ["sqlalchemy", "sqlalchemy-async"])
@pytest.mark.parametrize("action", ["retry", "kill"])
def test_operator_locked_read_refreshes_cached_terminal_state(engine, backend, action):
    """A cached PROCESSING object must not resurrect a newer COMPLETED row."""
    from datetime import UTC, datetime

    from dewey.sqlalchemy.models import TaskEntryModel

    task_id, _database = seed(backend, engine)
    with Session(engine) as session:
        row = session.get(TaskEntryModel, task_id)
        row.status = TaskStatus.PROCESSING.value
        row.execution_token = "synthetic-old-token"
        session.commit()

    def complete(row):
        row.status = TaskStatus.COMPLETED.value
        row.execution_token = None
        row.completed_at = datetime.now(UTC)

    if backend == "sqlalchemy":
        from dewey.sqlalchemy.queries import kill_task, retry_task

        with Session(engine, expire_on_commit=False) as stale:
            cached = stale.get(TaskEntryModel, task_id)
            with Session(engine) as newer:
                complete(newer.get(TaskEntryModel, task_id))
                newer.commit()
            before = snapshot(backend, engine, task_id)
            assert cached.status == TaskStatus.PROCESSING.value
            result = (retry_task if action == "retry" else kill_task)(stale, task_id)
            stale.commit()
            assert result.status == TaskStatus.COMPLETED
            assert snapshot(backend, engine, task_id) == before
        return
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    from dewey.sqlalchemy.async_queries import kill_task_async, retry_task_async

    async def run():
        async_engine = create_async_engine(engine.url.set(drivername="postgresql+asyncpg"))
        try:
            async with AsyncSession(async_engine, expire_on_commit=False) as stale:
                cached = await stale.get(TaskEntryModel, task_id)
                async with AsyncSession(async_engine) as newer:
                    complete(await newer.get(TaskEntryModel, task_id))
                    await newer.commit()
                before = snapshot(backend, engine, task_id)
                assert cached.status == TaskStatus.PROCESSING.value
                operation = retry_task_async if action == "retry" else kill_task_async
                result = await operation(stale, task_id)
                await stale.commit()
                assert result.status == TaskStatus.COMPLETED
                assert snapshot(backend, engine, task_id) == before
        finally:
            await async_engine.dispose()

    asyncio.run(run())
