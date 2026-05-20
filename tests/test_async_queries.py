"""Tests for async queries — mirrors test_queries.py."""

from datetime import UTC, datetime, timedelta

import pytest

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.async_executor import create_task_async
from dewey.sqlalchemy.async_queries import (
    bulk_retry_async,
    get_dead_async,
    get_failed_async,
    get_pending_async,
    get_processing_async,
    get_recent_async,
    get_stats_async,
    get_stuck_async,
    get_task_async,
    kill_task_async,
    purge_completed_async,
    retry_task_async,
)
from dewey.sqlalchemy.models import TaskEntryModel


@pytest.mark.asyncio
async def test_get_stats_empty(async_session):
    stats = await get_stats_async(async_session)
    assert stats == {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "dead": 0}


@pytest.mark.asyncio
async def test_get_stats_with_tasks(async_session):
    await create_task_async(async_session, task_type="a", payload={})
    await create_task_async(async_session, task_type="b", payload={})
    await async_session.commit()

    stats = await get_stats_async(async_session)
    assert stats["pending"] == 2


@pytest.mark.asyncio
async def test_get_pending(async_session):
    await create_task_async(async_session, task_type="scan", payload={"n": 1})
    await create_task_async(async_session, task_type="scan", payload={"n": 2})
    await create_task_async(async_session, task_type="report", payload={"n": 3})
    await async_session.commit()

    all_pending = await get_pending_async(async_session)
    assert len(all_pending) == 3

    scan_pending = await get_pending_async(async_session, task_type="scan")
    assert len(scan_pending) == 2


@pytest.mark.asyncio
async def test_get_processing(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.PROCESSING.value
    row.started_at = datetime.now(UTC)
    await async_session.commit()

    processing = await get_processing_async(async_session)
    assert len(processing) == 1
    assert processing[0].id == task.id


@pytest.mark.asyncio
async def test_get_stuck(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.PROCESSING.value
    row.started_at = datetime.now(UTC) - timedelta(minutes=30)
    await async_session.commit()

    stuck = await get_stuck_async(async_session, older_than_minutes=10)
    assert len(stuck) == 1
    assert stuck[0].id == task.id


@pytest.mark.asyncio
async def test_get_failed(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.FAILED.value
    row.error = "timeout"
    await async_session.commit()

    failed = await get_failed_async(async_session)
    assert len(failed) == 1
    assert failed[0].error == "timeout"


@pytest.mark.asyncio
async def test_get_dead(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.DEAD.value
    await async_session.commit()

    dead = await get_dead_async(async_session)
    assert len(dead) == 1


@pytest.mark.asyncio
async def test_get_task(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={"x": 1})
    await async_session.commit()

    entry = await get_task_async(async_session, task.id)
    assert entry is not None
    assert entry.task_type == "scan"
    assert entry.payload == {"x": 1}


@pytest.mark.asyncio
async def test_get_task_not_found(async_session):
    entry = await get_task_async(async_session, "nope")
    assert entry is None


@pytest.mark.asyncio
async def test_get_recent(async_session):
    await create_task_async(async_session, task_type="scan", payload={})
    await create_task_async(async_session, task_type="report", payload={})
    await async_session.commit()

    recent = await get_recent_async(async_session, limit=10)
    assert len(recent) == 2

    scan_recent = await get_recent_async(async_session, task_type="scan")
    assert len(scan_recent) == 1


@pytest.mark.asyncio
async def test_retry_failed_task(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.FAILED.value
    row.error = "timeout"
    row.attempts = 2
    await async_session.commit()

    entry = await retry_task_async(async_session, task.id)
    assert entry is not None
    assert entry.status == TaskStatus.PENDING
    assert entry.attempts == 0
    assert entry.error == ""


@pytest.mark.asyncio
async def test_retry_completed_task_is_noop(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.COMPLETED.value
    await async_session.commit()

    entry = await retry_task_async(async_session, task.id)
    assert entry is not None
    assert entry.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_bulk_retry(async_session):
    for _ in range(3):
        t = await create_task_async(async_session, task_type="scan", payload={})
        await async_session.flush()
        row = await async_session.get(TaskEntryModel, t.id)
        row.status = TaskStatus.FAILED.value
    await async_session.commit()

    count = await bulk_retry_async(async_session, task_type="scan")
    assert count == 3


@pytest.mark.asyncio
async def test_bulk_retry_invalid_status_raises(async_session):
    with pytest.raises(ValueError):
        await bulk_retry_async(async_session, status=TaskStatus.COMPLETED)


@pytest.mark.asyncio
async def test_kill_task(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    entry = await kill_task_async(async_session, task.id)
    assert entry is not None
    assert entry.status == TaskStatus.DEAD


@pytest.mark.asyncio
async def test_purge_completed(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.COMPLETED.value
    row.completed_at = datetime.now(UTC) - timedelta(days=60)
    await async_session.commit()

    count = await purge_completed_async(async_session, older_than_days=30)
    assert count == 1

    entry = await get_task_async(async_session, task.id)
    assert entry is None
