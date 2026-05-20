"""Tests for Postgres LISTEN/NOTIFY wake-on-insert helpers."""

from datetime import UTC, datetime, timedelta

import pytest

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.async_executor import create_task_async
from dewey.sqlalchemy.async_notifications import create_notification_async
from dewey.sqlalchemy.async_queries import retry_task_async
from dewey.sqlalchemy.async_sweep import sweep_failed_async
from dewey.sqlalchemy.listen import AsyncPostgresWorkListener, notify_work_available_async


@pytest.mark.asyncio
async def test_create_task_async_notifies_on_commit(async_engine, async_session):
    async with AsyncPostgresWorkListener(async_engine) as listener:
        task = await create_task_async(
            async_session,
            task_type="scan",
            payload={"url": "https://example.com"},
            queue="critical",
        )

        # pg_notify is transactional: nothing is delivered until commit.
        assert await listener.wait(timeout=0.05) == []

        await async_session.commit()
        notifications = await listener.wait(timeout=1.0)

    assert notifications
    assert notifications[0].kind == "task"
    assert notifications[0].id == task.id
    assert notifications[0].queue == "critical"


@pytest.mark.asyncio
async def test_create_notification_async_notifies_on_commit(async_engine, async_session):
    async with AsyncPostgresWorkListener(async_engine) as listener:
        notification = await create_notification_async(
            async_session,
            event_type="order.confirmed",
            channel="email",
            recipient="user@example.com",
        )
        await async_session.commit()
        notifications = await listener.wait(timeout=1.0)

    assert notifications
    assert notifications[0].kind == "notification"
    assert notifications[0].id == notification.id
    assert notifications[0].queue == "email"


@pytest.mark.asyncio
async def test_retry_task_async_notifies_on_commit(async_engine, async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    task.status = TaskStatus.FAILED.value
    task.process_after = datetime.now(UTC) - timedelta(seconds=1)
    await async_session.commit()

    async with AsyncPostgresWorkListener(async_engine) as listener:
        retried = await retry_task_async(async_session, task.id)
        assert retried is not None
        await async_session.commit()
        notifications = await listener.wait(timeout=1.0)

    assert notifications
    assert notifications[0].kind == "task"
    assert notifications[0].id == task.id


@pytest.mark.asyncio
async def test_sweep_failed_async_notifies_on_commit(async_engine, async_session):
    task = await create_task_async(async_session, task_type="scan", payload={})
    task.status = TaskStatus.FAILED.value
    task.process_after = datetime.now(UTC) - timedelta(seconds=1)
    await async_session.commit()

    async with AsyncPostgresWorkListener(async_engine) as listener:
        swept = await sweep_failed_async(async_session)
        await async_session.commit()
        notifications = await listener.wait(timeout=1.0)

    assert swept == [task.id]
    assert notifications
    assert notifications[0].kind == "task"
    assert notifications[0].id == task.id


@pytest.mark.asyncio
async def test_manual_notify_work_available_async(async_engine, async_session):
    async with AsyncPostgresWorkListener(async_engine) as listener:
        queued = await notify_work_available_async(
            async_session,
            kind="task",
            entry_id="task-123",
            queue="default",
        )
        await async_session.commit()
        notifications = await listener.wait(timeout=1.0)

    assert queued is True
    assert notifications == [notifications[0]]
    assert notifications[0].kind == "task"
    assert notifications[0].id == "task-123"
    assert notifications[0].queue == "default"
