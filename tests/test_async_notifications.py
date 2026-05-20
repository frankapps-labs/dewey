"""Tests for async notifications — create, send, sweep, queries."""

from datetime import UTC, datetime, timedelta

import pytest

from dewey.core.notifications import ChannelRegistry, NotificationStatus
from dewey.sqlalchemy.async_executor import create_task_async
from dewey.sqlalchemy.async_notifications import (
    create_notification_async,
    create_notifications_for_event_async,
    get_dead_notifications_async,
    get_failed_notifications_async,
    get_notification_async,
    get_notification_attempts_async,
    get_notification_stats_async,
    get_notifications_for_task_async,
    get_pending_notifications_async,
    kill_notification_async,
    process_notification_async,
    purge_sent_notifications_async,
    retry_notification_async,
    send_notification_async,
    sweep_failed_notifications_async,
    sweep_notifications_async,
    sweep_stuck_notifications_async,
)
from dewey.sqlalchemy.notification_models import NotificationEntryModel
from tests.helpers import FakeChannel, RaisingChannel


def _make_registry(channel=None):
    ch = channel or FakeChannel("email")
    registry = ChannelRegistry()
    registry.register_channel(ch)
    registry.on(
        "order.confirmed",
        channel="email",
        recipient=lambda p: p["email"],
        render=lambda evt, p: ("Order confirmed", f"Order {p['order_id']}"),
    )
    return registry, ch


# --- create ---


@pytest.mark.asyncio
async def test_create_notification(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="order.confirmed",
        channel="email",
        recipient="user@example.com",
        subject="Hello",
        body="World",
    )
    assert notif.id is not None
    assert notif.status == NotificationStatus.PENDING
    assert notif.channel == "email"


@pytest.mark.asyncio
async def test_create_notification_linked_to_task(async_session):
    task = await create_task_async(async_session, task_type="order", payload={})
    await async_session.flush()

    notif = await create_notification_async(
        async_session,
        event_type="order.confirmed",
        channel="email",
        recipient="user@example.com",
        task_id=task.id,
    )
    assert notif.task_id == task.id


@pytest.mark.asyncio
async def test_create_notifications_for_event(async_session):
    registry, _ = _make_registry()
    notifs = await create_notifications_for_event_async(
        async_session,
        registry=registry,
        event_type="order.confirmed",
        payload={"email": "buyer@test.com", "order_id": "ORD-1"},
    )
    assert len(notifs) == 1
    assert notifs[0].recipient == "buyer@test.com"
    assert "ORD-1" in notifs[0].body


@pytest.mark.asyncio
async def test_create_notifications_unknown_event(async_session):
    registry, _ = _make_registry()
    notifs = await create_notifications_for_event_async(
        async_session,
        registry=registry,
        event_type="unknown.event",
        payload={},
    )
    assert notifs == []


# --- send ---


@pytest.mark.asyncio
async def test_send_notification_success(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="order.confirmed",
        channel="email",
        recipient="user@example.com",
        subject="Hi",
        body="Done",
    )
    await async_session.commit()

    ch = FakeChannel("email", succeed=True)
    result = await send_notification_async(async_session, notif.id, ch)

    assert result is True
    assert len(ch.calls) == 1

    entry = await get_notification_async(async_session, notif.id)
    assert entry.status == NotificationStatus.SENT


@pytest.mark.asyncio
async def test_send_notification_failure(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="order.confirmed",
        channel="email",
        recipient="user@example.com",
        max_attempts=3,
    )
    await async_session.commit()

    ch = FakeChannel("email", succeed=False, error="SMTP timeout")
    result = await send_notification_async(async_session, notif.id, ch)

    assert result is False

    entry = await get_notification_async(async_session, notif.id)
    assert entry.status == NotificationStatus.FAILED
    assert "SMTP timeout" in entry.error


@pytest.mark.asyncio
async def test_send_notification_dead_letter(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="order.confirmed",
        channel="email",
        recipient="user@example.com",
        max_attempts=1,
    )
    await async_session.commit()

    ch = FakeChannel("email", succeed=False)
    result = await send_notification_async(async_session, notif.id, ch)

    assert result is False

    entry = await get_notification_async(async_session, notif.id)
    assert entry.status == NotificationStatus.DEAD


@pytest.mark.asyncio
async def test_send_notification_channel_exception(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="order.confirmed",
        channel="broken",
        recipient="user@example.com",
        max_attempts=3,
    )
    await async_session.commit()

    ch = RaisingChannel()
    result = await send_notification_async(async_session, notif.id, ch)

    assert result is False

    entry = await get_notification_async(async_session, notif.id)
    assert entry.status == NotificationStatus.FAILED
    assert "channel exploded" in entry.error


@pytest.mark.asyncio
async def test_send_notification_records_attempt(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="user@example.com",
    )
    await async_session.commit()

    ch = FakeChannel("email", succeed=True)
    await send_notification_async(async_session, notif.id, ch)

    attempts = await get_notification_attempts_async(async_session, notif.id)
    assert len(attempts) == 1
    assert attempts[0].status == "sent"
    assert attempts[0].attempt_number == 1


@pytest.mark.asyncio
async def test_process_notification_via_registry(async_session):
    registry, ch = _make_registry()
    notif = await create_notification_async(
        async_session,
        event_type="order.confirmed",
        channel="email",
        recipient="user@example.com",
        subject="Hi",
        body="Done",
    )
    await async_session.commit()

    result = await process_notification_async(async_session, notif.id, registry)
    assert result is True
    assert len(ch.calls) == 1


# --- sweep ---


@pytest.mark.asyncio
async def test_sweep_failed_notifications(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="user@example.com",
    )
    await async_session.commit()

    row = await async_session.get(NotificationEntryModel, notif.id)
    row.status = NotificationStatus.FAILED.value
    row.process_after = datetime.now(UTC) - timedelta(minutes=1)
    await async_session.commit()

    ids = await sweep_failed_notifications_async(async_session)
    assert notif.id in ids

    entry = await get_notification_async(async_session, notif.id)
    assert entry.status == NotificationStatus.PENDING


@pytest.mark.asyncio
async def test_sweep_stuck_notifications(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="user@example.com",
    )
    await async_session.commit()

    row = await async_session.get(NotificationEntryModel, notif.id)
    row.status = NotificationStatus.SENDING.value
    row.updated_at = datetime.now(UTC) - timedelta(minutes=10)
    await async_session.commit()

    ids = await sweep_stuck_notifications_async(async_session, stuck_threshold_minutes=5)
    assert notif.id in ids


@pytest.mark.asyncio
async def test_sweep_notifications_combined(async_session):
    result = await sweep_notifications_async(async_session)
    assert "failed" in result
    assert "stuck" in result


# --- queries ---


@pytest.mark.asyncio
async def test_get_notification_stats(async_session):
    stats = await get_notification_stats_async(async_session)
    assert "pending" in stats
    assert "sent" in stats


@pytest.mark.asyncio
async def test_get_pending_notifications(async_session):
    await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="a@b.com",
    )
    await async_session.commit()

    pending = await get_pending_notifications_async(async_session)
    assert len(pending) >= 1


@pytest.mark.asyncio
async def test_get_pending_notifications_respects_process_after(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="a@b.com",
        process_after=datetime.now(UTC) + timedelta(hours=1),
    )
    await async_session.commit()

    pending = await get_pending_notifications_async(async_session)
    ids = [n.id for n in pending]
    assert notif.id not in ids


@pytest.mark.asyncio
async def test_get_notifications_for_task(async_session):
    task = await create_task_async(async_session, task_type="order", payload={})
    await async_session.flush()

    await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="a@b.com",
        task_id=task.id,
    )
    await async_session.commit()

    notifs = await get_notifications_for_task_async(async_session, task.id)
    assert len(notifs) == 1


# --- retry / kill / purge ---


@pytest.mark.asyncio
async def test_retry_notification(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="a@b.com",
    )
    await async_session.commit()

    row = await async_session.get(NotificationEntryModel, notif.id)
    row.status = NotificationStatus.FAILED.value
    row.attempts = 2
    row.error = "timeout"
    await async_session.commit()

    entry = await retry_notification_async(async_session, notif.id)
    assert entry.status == NotificationStatus.PENDING
    assert entry.attempts == 0
    assert entry.error == ""


@pytest.mark.asyncio
async def test_kill_notification(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="a@b.com",
    )
    await async_session.commit()

    entry = await kill_notification_async(async_session, notif.id)
    assert entry.status == NotificationStatus.DEAD


@pytest.mark.asyncio
async def test_purge_sent_notifications(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="a@b.com",
    )
    await async_session.commit()

    row = await async_session.get(NotificationEntryModel, notif.id)
    row.status = NotificationStatus.SENT.value
    row.sent_at = datetime.now(UTC) - timedelta(days=60)
    await async_session.commit()

    count = await purge_sent_notifications_async(async_session, older_than_days=30)
    assert count == 1


@pytest.mark.asyncio
async def test_get_failed_notifications(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="a@b.com",
    )
    await async_session.commit()

    row = await async_session.get(NotificationEntryModel, notif.id)
    row.status = NotificationStatus.FAILED.value
    await async_session.commit()

    failed = await get_failed_notifications_async(async_session)
    assert len(failed) >= 1


@pytest.mark.asyncio
async def test_get_dead_notifications(async_session):
    notif = await create_notification_async(
        async_session,
        event_type="test",
        channel="email",
        recipient="a@b.com",
    )
    await async_session.commit()

    row = await async_session.get(NotificationEntryModel, notif.id)
    row.status = NotificationStatus.DEAD.value
    await async_session.commit()

    dead = await get_dead_notifications_async(async_session)
    assert len(dead) >= 1
