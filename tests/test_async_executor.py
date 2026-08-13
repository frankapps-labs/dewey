"""Tests for async executor — create_task_async + process_task_async."""

import asyncio
from datetime import UTC, datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

import dewey.sqlalchemy.async_executor as executor_module
from dewey.core.states import TaskStatus
from dewey.sqlalchemy.async_executor import (
    create_or_get_task_async,
    create_task_async,
    process_task_async,
)
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


# --- scheduled_for validation ---
# Naive scheduled_for fails fast — exact error, no row written, no NOTIFY sent.

_AWARE_OR_NONE_SCHEDULES = pytest.mark.parametrize(
    "scheduled_for",
    [
        None,
        datetime(2026, 12, 1, tzinfo=UTC),
        datetime(2026, 12, 1, tzinfo=timezone(timedelta(hours=5, minutes=30))),
    ],
    ids=["none", "utc", "offset"],
)


def _spy_notifications(monkeypatch):
    notifications = []

    async def spy(*args, **kwargs):
        notifications.append((args, kwargs))

    monkeypatch.setattr(executor_module, "notify_work_available_async", spy)
    return notifications


async def _count_tasks(session):
    return await session.scalar(select(func.count()).select_from(TaskEntryModel))


@pytest.mark.asyncio
async def test_create_task_rejects_naive_scheduled_for(async_session, monkeypatch):
    notifications = _spy_notifications(monkeypatch)
    with pytest.raises(ValueError) as excinfo:
        await create_task_async(async_session, task_type="scan", scheduled_for=datetime.now())
    assert str(excinfo.value) == "scheduled_for must be a timezone-aware datetime"
    assert notifications == []
    assert await _count_tasks(async_session) == 0


@pytest.mark.asyncio
async def test_create_or_get_task_rejects_naive_scheduled_for(async_session, monkeypatch):
    notifications = _spy_notifications(monkeypatch)
    with pytest.raises(ValueError) as excinfo:
        await create_or_get_task_async(
            async_session,
            task_type="scan",
            idempotency_key="scan-naive",
            scheduled_for=datetime.now(),
        )
    assert str(excinfo.value) == "scheduled_for must be a timezone-aware datetime"
    assert notifications == []
    assert await _count_tasks(async_session) == 0


@pytest.mark.asyncio
@_AWARE_OR_NONE_SCHEDULES
async def test_create_task_accepts_aware_and_none_scheduled_for(async_session, scheduled_for):
    task = await create_task_async(async_session, task_type="scan", scheduled_for=scheduled_for)
    await async_session.refresh(task)
    assert task.scheduled_for == scheduled_for
    assert await _count_tasks(async_session) == 1


@pytest.mark.asyncio
@_AWARE_OR_NONE_SCHEDULES
async def test_create_or_get_task_accepts_aware_and_none_scheduled_for(
    async_session, scheduled_for
):
    task = await create_or_get_task_async(
        async_session,
        task_type="scan",
        idempotency_key=f"scan-aware-{scheduled_for!s}",
        scheduled_for=scheduled_for,
    )
    await async_session.refresh(task)
    assert task.scheduled_for == scheduled_for
    assert await _count_tasks(async_session) == 1


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
async def test_deadline_is_checked_after_the_task_lock_is_acquired(async_session, monkeypatch):
    import dewey.sqlalchemy.async_executor as executor_module

    before = datetime.now(UTC)
    deadline = before + timedelta(minutes=1)
    after = deadline + timedelta(seconds=1)
    task = await create_task_async(async_session, task_type="deadline", expires_at=deadline)
    await async_session.commit()

    observed = [before]

    class LockAwareClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return observed[0]

    real_execute = async_session.execute

    async def execute_after_wait(*args, **kwargs):
        observed[0] = after
        return await real_execute(*args, **kwargs)

    monkeypatch.setattr(executor_module, "datetime", LockAwareClock)
    monkeypatch.setattr(async_session, "execute", execute_after_wait)
    runs = []

    async def handler():
        runs.append(1)

    assert await process_task_async(async_session, task.id, handler) is False
    assert runs == []
    row = await async_session.get(TaskEntryModel, task.id)
    assert row.status == TaskStatus.EXPIRED.value


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
