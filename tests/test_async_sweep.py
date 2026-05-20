"""Tests for async sweep — sweep_async, sweep_failed_async, sweep_stuck_async."""

from datetime import UTC, datetime, timedelta

import pytest

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.async_executor import create_task_async
from dewey.sqlalchemy.async_sweep import sweep_async, sweep_failed_async, sweep_stuck_async
from dewey.sqlalchemy.models import TaskEntryModel


@pytest.mark.asyncio
async def test_sweep_failed_re_enqueues(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.FAILED.value
    row.process_after = datetime.now(UTC) - timedelta(minutes=1)
    await async_session.commit()

    ids = await sweep_failed_async(async_session)
    assert task.id in ids

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.PENDING.value


@pytest.mark.asyncio
async def test_sweep_failed_skips_future_process_after(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.FAILED.value
    row.process_after = datetime.now(UTC) + timedelta(hours=1)
    await async_session.commit()

    ids = await sweep_failed_async(async_session)
    assert task.id not in ids


@pytest.mark.asyncio
async def test_sweep_failed_dead_letters_exhausted_tasks(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={}, max_attempts=1)
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.FAILED.value
    row.attempts = 1
    row.process_after = datetime.now(UTC) - timedelta(minutes=1)
    await async_session.commit()

    ids = await sweep_failed_async(async_session)
    assert task.id not in ids

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.DEAD.value


@pytest.mark.asyncio
async def test_sweep_stuck_resets_abandoned_tasks(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.PROCESSING.value
    row.started_at = datetime.now(UTC) - timedelta(minutes=20)
    await async_session.commit()

    ids = await sweep_stuck_async(async_session, stuck_threshold_minutes=10)
    assert task.id in ids

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.PENDING.value


@pytest.mark.asyncio
async def test_sweep_stuck_leaves_recent_processing(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.PROCESSING.value
    row.started_at = datetime.now(UTC) - timedelta(minutes=2)
    await async_session.commit()

    ids = await sweep_stuck_async(async_session, stuck_threshold_minutes=10)
    assert task.id not in ids


@pytest.mark.asyncio
async def test_sweep_stuck_dead_letters_exhausted_tasks(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={}, max_attempts=1)
    await async_session.commit()

    row = await async_session.get(TaskEntryModel, task.id)
    row.status = TaskStatus.PROCESSING.value
    row.attempts = 1
    row.started_at = datetime.now(UTC) - timedelta(minutes=20)
    await async_session.commit()

    ids = await sweep_stuck_async(async_session, stuck_threshold_minutes=10)
    assert task.id not in ids

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.DEAD.value


@pytest.mark.asyncio
async def test_sweep_combined(async_session):
    t1 = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()
    row1 = await async_session.get(TaskEntryModel, t1.id)
    row1.status = TaskStatus.FAILED.value
    row1.process_after = datetime.now(UTC) - timedelta(minutes=1)
    await async_session.commit()

    t2 = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()
    row2 = await async_session.get(TaskEntryModel, t2.id)
    row2.status = TaskStatus.PROCESSING.value
    row2.started_at = datetime.now(UTC) - timedelta(minutes=20)
    await async_session.commit()

    result = await sweep_async(async_session, stuck_threshold_minutes=10)
    assert t1.id in result["failed"]
    assert t2.id in result["stuck"]


@pytest.mark.asyncio
async def test_sweep_empty(async_session):
    result = await sweep_async(async_session)
    assert result == {"failed": [], "stuck": []}
