"""Tests for the optional ``backoff`` parameter on process_task / send_notification.

Covers:
- Default behavior unchanged when ``backoff`` not passed.
- Custom backoff is honored on failure.
- Forwarded correctly through process_notification.
- Async parity.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dewey.core.notifications import ChannelRegistry, ChannelResult
from dewey.core.states import TaskStatus
from dewey.sqlalchemy.async_executor import create_task_async, process_task_async
from dewey.sqlalchemy.async_notifications import (
    create_notification_async,
    process_notification_async,
    send_notification_async,
)
from dewey.sqlalchemy.executor import create_task, process_task
from dewey.sqlalchemy.models import TaskEntryModel
from dewey.sqlalchemy.notification_models import NotificationEntryModel
from dewey.sqlalchemy.notifications import (
    create_notification,
    process_notification,
    send_notification,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FailingChannel:
    name = "test"

    def send(self, recipient, subject, body, payload):
        return ChannelResult(success=False, error="boom")


def always_fail(task_type, payload):
    raise RuntimeError("boom")


async def always_fail_async(task_type, payload):
    raise RuntimeError("boom")


# ---------------------------------------------------------------------------
# Sync task
# ---------------------------------------------------------------------------


class TestSyncTaskBackoff:
    def test_custom_backoff_used_on_failure(self, session):
        task = create_task(session, task_type="t", max_attempts=5)
        session.commit()
        task_id = task.id

        before = datetime.now(UTC)
        process_task(session, task_id, always_fail, backoff=lambda a: timedelta(seconds=7))

        row = session.execute(
            TaskEntryModel.__table__.select().where(TaskEntryModel.id == task_id)
        ).one()
        assert row.status == TaskStatus.FAILED.value
        # process_after should be ~7s in the future (within a generous window).
        delta = (row.process_after - before).total_seconds()
        assert 6 <= delta <= 12, f"expected ~7s, got {delta}s"

    def test_default_backoff_when_omitted(self, session):
        task = create_task(session, task_type="t", max_attempts=5)
        session.commit()
        task_id = task.id

        before = datetime.now(UTC)
        process_task(session, task_id, always_fail)

        row = session.execute(
            TaskEntryModel.__table__.select().where(TaskEntryModel.id == task_id)
        ).one()
        # Default task backoff is ~120s base (±25% jitter): expect well over 7s.
        delta = (row.process_after - before).total_seconds()
        assert delta > 30, f"expected default backoff > 30s, got {delta}s"


# ---------------------------------------------------------------------------
# Async task
# ---------------------------------------------------------------------------


class TestAsyncTaskBackoff:
    @pytest.mark.asyncio
    async def test_custom_backoff_used_on_failure(self, async_session):
        task = await create_task_async(async_session, task_type="t", max_attempts=5)
        await async_session.commit()
        task_id = task.id

        before = datetime.now(UTC)
        await process_task_async(
            async_session, task_id, always_fail_async, backoff=lambda a: timedelta(seconds=3)
        )

        result = await async_session.execute(
            TaskEntryModel.__table__.select().where(TaskEntryModel.id == task_id)
        )
        row = result.one()
        delta = (row.process_after - before).total_seconds()
        assert 2 <= delta <= 8, f"expected ~3s, got {delta}s"


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------


class TestNotificationBackoff:
    def test_send_custom_backoff(self, session):
        notif = create_notification(
            session,
            event_type="evt",
            channel="test",
            recipient="x@y.z",
            max_attempts=5,
        )
        session.commit()
        nid = notif.id

        before = datetime.now(UTC)
        send_notification(session, nid, FailingChannel(), backoff=lambda a: timedelta(seconds=4))

        row = session.execute(
            NotificationEntryModel.__table__.select().where(NotificationEntryModel.id == nid)
        ).one()
        delta = (row.process_after - before).total_seconds()
        assert 3 <= delta <= 9, f"expected ~4s, got {delta}s"

    def test_process_forwards_backoff(self, session):
        registry = ChannelRegistry()
        registry.register_channel(FailingChannel())

        notif = create_notification(
            session, event_type="evt", channel="test", recipient="x@y.z", max_attempts=5
        )
        session.commit()
        nid = notif.id

        before = datetime.now(UTC)
        process_notification(session, nid, registry, backoff=lambda a: timedelta(seconds=5))

        row = session.execute(
            NotificationEntryModel.__table__.select().where(NotificationEntryModel.id == nid)
        ).one()
        delta = (row.process_after - before).total_seconds()
        assert 4 <= delta <= 10, f"expected ~5s, got {delta}s"

    @pytest.mark.asyncio
    async def test_send_async_custom_backoff(self, async_session):
        notif = await create_notification_async(
            async_session,
            event_type="evt",
            channel="test",
            recipient="x@y.z",
            max_attempts=5,
        )
        await async_session.commit()
        nid = notif.id

        before = datetime.now(UTC)
        await send_notification_async(
            async_session, nid, FailingChannel(), backoff=lambda a: timedelta(seconds=3)
        )

        result = await async_session.execute(
            NotificationEntryModel.__table__.select().where(NotificationEntryModel.id == nid)
        )
        row = result.one()
        delta = (row.process_after - before).total_seconds()
        assert 2 <= delta <= 8, f"expected ~3s, got {delta}s"

    @pytest.mark.asyncio
    async def test_process_async_forwards_backoff(self, async_session):
        registry = ChannelRegistry()
        registry.register_channel(FailingChannel())

        notif = await create_notification_async(
            async_session,
            event_type="evt",
            channel="test",
            recipient="x@y.z",
            max_attempts=5,
        )
        await async_session.commit()
        nid = notif.id

        before = datetime.now(UTC)
        await process_notification_async(
            async_session, nid, registry, backoff=lambda a: timedelta(seconds=5)
        )

        result = await async_session.execute(
            NotificationEntryModel.__table__.select().where(NotificationEntryModel.id == nid)
        )
        row = result.one()
        delta = (row.process_after - before).total_seconds()
        assert 4 <= delta <= 10, f"expected ~5s, got {delta}s"
