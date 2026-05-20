"""Tests for Django notification layer — create, send, sweep, queries."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dewey.core.notifications import (
    ChannelRegistry,
    NotificationStatus,
)
from dewey.django.notification_models import NotificationEntry
from dewey.django.notifications import (
    create_notification,
    create_notifications_for_event,
    get_dead_notifications,
    get_failed_notifications,
    get_notification,
    get_notification_attempts,
    get_notification_stats,
    get_notifications_for_task,
    get_pending_notifications,
    kill_notification,
    process_notification,
    purge_sent_notifications,
    retry_notification,
    send_notification,
    sweep_failed_notifications,
    sweep_notifications,
    sweep_stuck_notifications,
)
from tests.helpers import FakeChannel, RaisingChannel

pytestmark = pytest.mark.django_db(transaction=True)


class TestCreateNotification:
    def test_create_basic(self):
        notif = create_notification(
            event_type="order.confirmed",
            channel="email",
            recipient="user@test.com",
            subject="Confirmation",
            body="Your order is confirmed.",
            payload={"order_id": "ORD-1"},
        )
        assert notif.id
        assert notif.event_type == "order.confirmed"
        assert notif.channel == "email"
        assert notif.status == NotificationStatus.PENDING

    def test_create_with_task_id(self):
        from dewey.django.executor import create_task

        task = create_task(task_type="test.task")
        notif = create_notification(
            event_type="task.completed",
            channel="webhook",
            recipient="https://example.com/hook",
            task_id=task.id,
        )
        assert notif.task_id == task.id


class TestCreateNotificationsForEvent:
    def test_creates_from_registry(self):
        registry = ChannelRegistry()
        registry.register_channel(FakeChannel("email"))
        registry.register_channel(FakeChannel("slack"))

        registry.on(
            "order.confirmed",
            channel="email",
            recipient=lambda p: p["email"],
            render=lambda e, p: ("Confirmed", f"Order {p['id']}"),
        )
        registry.on(
            "order.confirmed",
            channel="slack",
            recipient=lambda p: "#orders",
            render=lambda e, p: ("", f"New order {p['id']}"),
        )

        notifications = create_notifications_for_event(
            registry=registry,
            event_type="order.confirmed",
            payload={"email": "buyer@test.com", "id": "ORD-1"},
        )
        assert len(notifications) == 2

    def test_no_bindings_returns_empty(self):
        registry = ChannelRegistry()
        result = create_notifications_for_event(
            registry=registry,
            event_type="nothing.bound",
            payload={},
        )
        assert result == []


class TestSendNotification:
    def test_successful_send(self):
        notif = create_notification(
            event_type="test",
            channel="email",
            recipient="user@test.com",
            subject="Hello",
            body="World",
            payload={"key": "val"},
        )
        channel = FakeChannel("email")
        result = send_notification(notif.id, channel)

        assert result is True
        assert len(channel.calls) == 1

        updated = get_notification(notif.id)
        assert updated.status == NotificationStatus.SENT
        assert updated.attempts == 1

    def test_failed_send(self):
        notif = create_notification(
            event_type="test",
            channel="email",
            recipient="user@test.com",
        )
        channel = FakeChannel("email", succeed=False, error="SMTP timeout")
        result = send_notification(notif.id, channel)

        assert result is False
        updated = get_notification(notif.id)
        assert updated.status == NotificationStatus.FAILED
        assert updated.error == "SMTP timeout"

    def test_dead_letters_at_max(self):
        notif = create_notification(
            event_type="test",
            channel="email",
            recipient="user@test.com",
            max_attempts=1,
        )
        channel = FakeChannel("email", succeed=False)
        send_notification(notif.id, channel)

        updated = get_notification(notif.id)
        assert updated.status == NotificationStatus.DEAD

    def test_channel_exception_handled(self):
        notif = create_notification(
            event_type="test",
            channel="broken",
            recipient="user@test.com",
        )
        channel = RaisingChannel()
        result = send_notification(notif.id, channel)

        assert result is False
        updated = get_notification(notif.id)
        assert "channel exploded" in updated.error

    def test_send_nonexistent_returns_false(self):
        channel = FakeChannel("email")
        assert send_notification("nonexistent", channel) is False


class TestAttemptTracking:
    def test_attempt_logged(self):
        notif = create_notification(
            event_type="test",
            channel="email",
            recipient="a@b.com",
        )
        channel = FakeChannel("email")
        send_notification(notif.id, channel)

        attempts = get_notification_attempts(notif.id)
        assert len(attempts) == 1
        assert attempts[0].status == "sent"
        assert attempts[0].response_data == {"message_id": "msg-1"}

    def test_failed_attempt_logged(self):
        notif = create_notification(
            event_type="test",
            channel="email",
            recipient="a@b.com",
        )
        channel = FakeChannel("email", succeed=False, error="timeout")
        send_notification(notif.id, channel)

        attempts = get_notification_attempts(notif.id)
        assert len(attempts) == 1
        assert attempts[0].status == "failed"
        assert attempts[0].error == "timeout"


class TestProcessNotification:
    def test_process_via_registry(self):
        registry = ChannelRegistry()
        channel = FakeChannel("email")
        registry.register_channel(channel)

        notif = create_notification(
            event_type="test",
            channel="email",
            recipient="a@b.com",
        )
        result = process_notification(notif.id, registry)
        assert result is True
        assert len(channel.calls) == 1

    def test_process_unknown_channel(self):
        registry = ChannelRegistry()
        notif = create_notification(
            event_type="test",
            channel="email",
            recipient="a@b.com",
        )
        result = process_notification(notif.id, registry)
        assert result is False


class TestSweep:
    def test_sweep_failed(self):
        notif = create_notification(
            event_type="test",
            channel="email",
            recipient="a@b.com",
        )
        NotificationEntry.objects.filter(id=notif.id).update(
            status=NotificationStatus.FAILED.value,
            process_after=datetime.now(UTC) - timedelta(minutes=5),
            attempts=1,
        )
        swept = sweep_failed_notifications()
        assert notif.id in swept

        updated = get_notification(notif.id)
        assert updated.status == NotificationStatus.PENDING

    def test_sweep_stuck(self):
        notif = create_notification(
            event_type="test",
            channel="email",
            recipient="a@b.com",
        )
        NotificationEntry.objects.filter(id=notif.id).update(
            status=NotificationStatus.SENDING.value,
            updated_at=datetime.now(UTC) - timedelta(minutes=10),
        )
        swept = sweep_stuck_notifications(stuck_threshold_minutes=5)
        assert notif.id in swept

    def test_sweep_combined(self):
        result = sweep_notifications()
        assert "failed" in result
        assert "stuck" in result


class TestQueries:
    def test_get_stats(self):
        create_notification(event_type="a", channel="email", recipient="a@b.com")
        create_notification(event_type="b", channel="email", recipient="a@b.com")

        stats = get_notification_stats()
        assert stats["pending"] == 2

    def test_get_notifications_for_task(self):
        from dewey.django.executor import create_task

        task = create_task(task_type="test")
        create_notification(event_type="a", channel="email", recipient="a@b.com", task_id=task.id)
        create_notification(
            event_type="b",
            channel="email",
            recipient="x@y.com",  # no task_id
        )
        results = get_notifications_for_task(task.id)
        assert len(results) == 1

    def test_get_pending(self):
        create_notification(event_type="a", channel="email", recipient="a@b.com")
        create_notification(event_type="b", channel="slack", recipient="#ch")

        assert len(get_pending_notifications()) == 2
        assert len(get_pending_notifications(channel="email")) == 1

    def test_get_failed(self):
        notif = create_notification(event_type="test", channel="email", recipient="a@b.com")
        NotificationEntry.objects.filter(id=notif.id).update(
            status=NotificationStatus.FAILED.value,
        )
        assert len(get_failed_notifications()) == 1

    def test_get_dead(self):
        notif = create_notification(event_type="test", channel="email", recipient="a@b.com")
        NotificationEntry.objects.filter(id=notif.id).update(
            status=NotificationStatus.DEAD.value,
        )
        assert len(get_dead_notifications()) == 1


class TestActions:
    def test_retry_notification(self):
        notif = create_notification(event_type="test", channel="email", recipient="a@b.com")
        NotificationEntry.objects.filter(id=notif.id).update(
            status=NotificationStatus.FAILED.value,
            error="timeout",
            attempts=2,
        )
        result = retry_notification(notif.id)
        assert result.status == NotificationStatus.PENDING
        assert result.attempts == 0

    def test_kill_notification(self):
        notif = create_notification(event_type="test", channel="email", recipient="a@b.com")
        result = kill_notification(notif.id)
        assert result.status == NotificationStatus.DEAD

    def test_purge_sent(self):
        notif = create_notification(event_type="test", channel="email", recipient="a@b.com")
        NotificationEntry.objects.filter(id=notif.id).update(
            status=NotificationStatus.SENT.value,
            sent_at=datetime.now(UTC) - timedelta(days=60),
        )
        count = purge_sent_notifications(older_than_days=30)
        assert count == 1
        assert get_notification(notif.id) is None
