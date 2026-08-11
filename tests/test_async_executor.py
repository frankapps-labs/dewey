"""Tests for async executor — create_task_async + process_task_async."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.async_executor import create_task_async, process_task_async
from dewey.sqlalchemy.models import TaskEntryModel

# --- create_task_async ---


@pytest.mark.asyncio
async def test_create_task(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        kwargs={"url": "https://example.com"},
    )
    assert task.id is not None
    assert task.task_type == "scan"
    assert task.status == TaskStatus.PENDING.value
    assert task.kwargs == {"url": "https://example.com"}
    assert task.queue == "default"
    assert task.attempts == 0


@pytest.mark.asyncio
async def test_create_task_custom_queue_and_priority(async_session):
    task = await create_task_async(
        async_session,
        task_type="report",
        kwargs={"id": "r1"},
        queue="bulk",
        priority=10,
        max_attempts=3,
    )
    assert task.queue == "bulk"
    assert task.priority == 10
    assert task.max_attempts == 3


@pytest.mark.asyncio
async def test_create_task_with_idempotency_key(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        kwargs={},
        idempotency_key="scan-abc",
    )
    assert task.idempotency_key == "scan-abc"


@pytest.mark.asyncio
async def test_create_task_with_metadata(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        kwargs={},
        metadata={"customer_id": "cust_123"},
    )
    assert task.task_metadata == {"customer_id": "cust_123"}


# --- process_task_async ---


@pytest.mark.asyncio
async def test_process_task_success(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        kwargs={"url": "https://example.com"},
    )
    await async_session.commit()

    calls = []

    async def handler(**kwargs):
        calls.append(kwargs)

    result = await process_task_async(async_session, task.id, handler)

    assert result is True
    assert len(calls) == 1
    assert calls[0] == {"url": "https://example.com"}

    # Verify DB state
    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.COMPLETED.value
    assert row.attempts == 1
    assert row.completed_at is not None
    assert row.error == ""


@pytest.mark.asyncio
async def test_process_task_failure(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        kwargs={},
    )
    await async_session.commit()

    async def failing_handler(**kwargs):
        raise RuntimeError("browser crashed")

    result = await process_task_async(async_session, task.id, failing_handler)

    assert result is False

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.FAILED.value
    assert row.attempts == 1
    assert "browser crashed" in row.error


@pytest.mark.asyncio
async def test_process_task_failure_backoff_starts_at_failure_time(async_session):
    task = await create_task_async(async_session, task_type="scan", kwargs={})
    await async_session.commit()
    failure_time = None

    async def failing_handler(**kwargs):
        nonlocal failure_time
        await asyncio.sleep(0.01)
        failure_time = datetime.now(UTC)
        raise RuntimeError("late failure")

    result = await process_task_async(
        async_session,
        task.id,
        failing_handler,
        backoff=lambda attempts: timedelta(0),
    )

    assert result is False

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.scheduled_for >= failure_time


@pytest.mark.asyncio
async def test_process_task_dead_letter_after_max_attempts(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        kwargs={},
        max_attempts=1,
    )
    await async_session.commit()

    async def failing_handler(**kwargs):
        raise RuntimeError("always fails")

    result = await process_task_async(async_session, task.id, failing_handler)

    assert result is False

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.DEAD.value
    assert row.attempts == 1


@pytest.mark.asyncio
async def test_process_task_not_found(async_session):
    async def handler(**kwargs):
        pass

    result = await process_task_async(async_session, "nonexistent-id", handler)
    assert result is False


@pytest.mark.asyncio
async def test_process_task_already_completed(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        kwargs={},
    )
    await async_session.commit()

    async def handler(**kwargs):
        pass

    # Process once
    await process_task_async(async_session, task.id, handler)

    # Process again — should skip
    result = await process_task_async(async_session, task.id, handler)
    assert result is False


@pytest.mark.asyncio
async def test_process_task_respects_scheduled_for(async_session):
    from datetime import datetime, timedelta

    future = datetime.now(UTC) + timedelta(hours=1)
    task = await create_task_async(
        async_session,
        task_type="scan",
        kwargs={},
        scheduled_for=future,
    )
    await async_session.commit()

    async def handler(**kwargs):
        pass

    result = await process_task_async(async_session, task.id, handler)
    assert result is False

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.PENDING.value


@pytest.mark.asyncio
async def test_expiry_creation_and_delivery(async_session):
    scheduled = datetime.now(UTC) + timedelta(minutes=5)
    expires = scheduled + timedelta(minutes=10)
    task = await create_task_async(
        async_session,
        task_type="deadline",
        scheduled_for=scheduled,
        expires_at=expires,
    )
    assert task.initial_scheduled_for == scheduled
    assert task.expires_at == expires
    task.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    await async_session.commit()
    runs: list[int] = []

    async def handler():
        runs.append(1)

    assert await process_task_async(async_session, task.id, handler) is False
    row = await async_session.get(TaskEntryModel, task.id)
    assert row is not None
    await async_session.refresh(row)
    assert runs == []
    assert row.status == TaskStatus.EXPIRED.value
    assert row.expired_at is not None
    assert row.attempts == 0


@pytest.mark.asyncio
async def test_naive_expiry_is_rejected(async_session):
    with pytest.raises(ValueError, match="timezone-aware"):
        await create_task_async(async_session, task_type="deadline", expires_at=datetime.now())
