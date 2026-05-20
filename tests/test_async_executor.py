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
        payload={"url": "https://example.com"},
    )
    assert task.id is not None
    assert task.task_type == "scan"
    assert task.status == TaskStatus.PENDING.value
    assert task.payload == {"url": "https://example.com"}
    assert task.queue == "default"
    assert task.attempts == 0


@pytest.mark.asyncio
async def test_create_task_custom_queue_and_priority(async_session):
    task = await create_task_async(
        async_session,
        task_type="report",
        payload={"id": "r1"},
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
        payload={},
        idempotency_key="scan-abc",
    )
    assert task.idempotency_key == "scan-abc"


@pytest.mark.asyncio
async def test_create_task_with_metadata(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        payload={},
        metadata={"customer_id": "cust_123"},
    )
    assert task.task_metadata == {"customer_id": "cust_123"}


# --- process_task_async ---


@pytest.mark.asyncio
async def test_process_task_success(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        payload={"url": "https://example.com"},
    )
    await async_session.commit()

    calls = []

    async def handler(task_type, payload):
        calls.append((task_type, payload))

    result = await process_task_async(async_session, task.id, handler)

    assert result is True
    assert len(calls) == 1
    assert calls[0] == ("scan", {"url": "https://example.com"})

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
        payload={},
    )
    await async_session.commit()

    async def failing_handler(task_type, payload):
        raise RuntimeError("browser crashed")

    result = await process_task_async(async_session, task.id, failing_handler)

    assert result is False

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.FAILED.value
    assert row.attempts == 1
    assert "browser crashed" in row.error


@pytest.mark.asyncio
async def test_process_task_failure_backoff_starts_at_failure_time(async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    await async_session.commit()
    failure_time = None

    async def failing_handler(task_type, payload):
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
    assert row.process_after >= failure_time


@pytest.mark.asyncio
async def test_process_task_dead_letter_after_max_attempts(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        payload={},
        max_attempts=1,
    )
    await async_session.commit()

    async def failing_handler(task_type, payload):
        raise RuntimeError("always fails")

    result = await process_task_async(async_session, task.id, failing_handler)

    assert result is False

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.DEAD.value
    assert row.attempts == 1


@pytest.mark.asyncio
async def test_process_task_not_found(async_session):
    async def handler(t, p):
        pass

    result = await process_task_async(async_session, "nonexistent-id", handler)
    assert result is False


@pytest.mark.asyncio
async def test_process_task_already_completed(async_session):
    task = await create_task_async(
        async_session,
        task_type="scan",
        payload={},
    )
    await async_session.commit()

    async def handler(t, p):
        pass

    # Process once
    await process_task_async(async_session, task.id, handler)

    # Process again — should skip
    result = await process_task_async(async_session, task.id, handler)
    assert result is False


@pytest.mark.asyncio
async def test_process_task_respects_process_after(async_session):
    from datetime import datetime, timedelta

    future = datetime.now(UTC) + timedelta(hours=1)
    task = await create_task_async(
        async_session,
        task_type="scan",
        payload={},
        process_after=future,
    )
    await async_session.commit()

    async def handler(t, p):
        pass

    result = await process_task_async(async_session, task.id, handler)
    assert result is False

    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.PENDING.value
