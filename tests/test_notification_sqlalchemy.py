"""Tests for SQLAlchemy notification layer — create, send, sweep, queries."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import update

from dewey.core.notifications import (
    ChannelRegistry,
    NotificationStatus,
)
from dewey.sqlalchemy.notification_models import (
    NotificationEntryModel,
)
from dewey.sqlalchemy.notifications import (
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
    purge_sent_notifications,
    retry_notification,
    send_notification,
    sweep_failed_notifications,
    sweep_notifications,
    sweep_stuck_notifications,
)
from tests.helpers import FakeChannel, RaisingChannel


class TestCreateNotification:
    def test_create_basic(self, session):
        notif = create_notification(
            session,
            event_type="order.confirmed",
            channel="email",
            recipient="user@test.com",
            subject="Confirmation",
            body="Your order is confirmed.",
            payload={"order_id": "ORD-1"},
        )
        session.commit()

        assert notif.id
        assert notif.event_type == "order.confirmed"
        assert notif.channel == "email"
        assert notif.recipient == "user@test.com"
        assert notif.status == NotificationStatus.PENDING
        assert notif.attempts == 0
        assert notif.max_attempts == 3

    def test_create_with_task_id(self, session):
        from dewey.sqlalchemy.executor import create_task

        task = create_task(session, task_type="test.task")
        session.commit()

        notif = create_notification(
            session,
            event_type="task.completed",
            channel="webhook",
            recipient="https://example.com/hook",
            task_id=task.id,
        )
        session.commit()

        assert notif.task_id == task.id

    def test_create_with_metadata(self, session):
        notif = create_notification(
            session,
            event_type="test",
            channel="email",
            recipient="a@b.com",
            metadata={"source": "test"},
        )
        session.commit()
        assert notif.metadata == {"source": "test"}


class TestCreateNotificationsForEvent:
    def test_creates_from_registry(self, session):
        registry = ChannelRegistry()
        email_ch = FakeChannel("email")
        slack_ch = FakeChannel("slack")
        registry.register_channel(email_ch)
        registry.register_channel(slack_ch)

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
            session,
            registry=registry,
            event_type="order.confirmed",
            payload={"email": "buyer@test.com", "id": "ORD-1"},
        )
        session.commit()

        assert len(notifications) == 2
        channels = {n.channel for n in notifications}
        assert channels == {"email", "slack"}

        email_notif = next(n for n in notifications if n.channel == "email")
        assert email_notif.recipient == "buyer@test.com"
        assert email_notif.subject == "Confirmed"
        assert email_notif.body == "Order ORD-1"

    def test_no_bindings_returns_empty(self, session):
        registry = ChannelRegistry()
        result = create_notifications_for_event(
            session,
            registry=registry,
            event_type="nothing.bound",
            payload={},
        )
        assert result == []


class TestSendNotification:
    def test_successful_send(self, session):
        notif = create_notification(
            session,
            event_type="test",
            channel="email",
            recipient="user@test.com",
            subject="Hello",
            body="World",
            payload={"key": "val"},
        )
        session.commit()

        channel = FakeChannel("email")
        result = send_notification(session, notif.id, channel)

        assert result is True
        assert len(channel.calls) == 1
        assert channel.calls[0]["recipient"] == "user@test.com"

        # Check DB state
        updated = get_notification(session, notif.id)
        assert updated.status == NotificationStatus.SENT
        assert updated.attempts == 1
        assert updated.sent_at is not None

    def test_failed_send_marks_failed(self, session):
        notif = create_notification(
            session,
            event_type="test",
            channel="email",
            recipient="user@test.com",
        )
        session.commit()

        channel = FakeChannel("email", succeed=False, error="SMTP timeout")
        result = send_notification(session, notif.id, channel)

        assert result is False

        updated = get_notification(session, notif.id)
        assert updated.status == NotificationStatus.FAILED
        assert updated.error == "SMTP timeout"
        assert updated.attempts == 1
        assert updated.process_after is not None  # backoff set

    def test_failed_send_dead_letters_at_max(self, session):
        notif = create_notification(
            session,
            event_type="test",
            channel="email",
            recipient="user@test.com",
            max_attempts=1,
        )
        session.commit()

        channel = FakeChannel("email", succeed=False)
        result = send_notification(session, notif.id, channel)

        assert result is False
        updated = get_notification(session, notif.id)
        assert updated.status == NotificationStatus.DEAD

    def test_channel_exception_handled(self, session):
        notif = create_notification(
            session,
            event_type="test",
            channel="broken",
            recipient="user@test.com",
        )
        session.commit()

        channel = RaisingChannel()
        result = send_notification(session, notif.id, channel)

        assert result is False
        updated = get_notification(session, notif.id)
        assert updated.status == NotificationStatus.FAILED
        assert "channel exploded" in updated.error

    def test_send_skips_terminal(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        # Manually mark as sent
        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(status=NotificationStatus.SENT.value)
        )
        session.commit()

        channel = FakeChannel("email")
        result = send_notification(session, notif.id, channel)
        assert result is False
        assert len(channel.calls) == 0

    def test_send_skips_not_pending(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(status=NotificationStatus.FAILED.value)
        )
        session.commit()

        channel = FakeChannel("email")
        result = send_notification(session, notif.id, channel)
        assert result is False

    def test_send_respects_process_after(self, session):
        future = datetime.now(UTC) + timedelta(hours=1)
        notif = create_notification(
            session,
            event_type="test",
            channel="email",
            recipient="a@b.com",
            process_after=future,
        )
        session.commit()

        channel = FakeChannel("email")
        result = send_notification(session, notif.id, channel)
        assert result is False
        assert len(channel.calls) == 0

    def test_send_nonexistent_returns_false(self, session):
        channel = FakeChannel("email")
        result = send_notification(session, "nonexistent-id", channel)
        assert result is False


class TestAttemptTracking:
    def test_successful_attempt_logged(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        channel = FakeChannel("email")
        send_notification(session, notif.id, channel)

        attempts = get_notification_attempts(session, notif.id)
        assert len(attempts) == 1
        assert attempts[0].attempt_number == 1
        assert attempts[0].status == "sent"
        assert attempts[0].response_data == {"message_id": "msg-1"}

    def test_failed_attempt_logged(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        channel = FakeChannel("email", succeed=False, error="timeout")
        send_notification(session, notif.id, channel)

        attempts = get_notification_attempts(session, notif.id)
        assert len(attempts) == 1
        assert attempts[0].status == "failed"
        assert attempts[0].error == "timeout"

    def test_multiple_attempts_after_retry(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        # First attempt: fail
        channel = FakeChannel("email", succeed=False)
        send_notification(session, notif.id, channel)

        # Reset to pending (simulate sweep)
        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(status=NotificationStatus.PENDING.value, process_after=None)
        )
        session.commit()

        # Second attempt: succeed
        channel2 = FakeChannel("email", succeed=True)
        send_notification(session, notif.id, channel2)

        attempts = get_notification_attempts(session, notif.id)
        assert len(attempts) == 2
        assert attempts[0].status == "failed"
        assert attempts[1].status == "sent"


class TestProcessNotification:
    def test_process_via_registry(self, session):
        from dewey.sqlalchemy.notifications import process_notification

        registry = ChannelRegistry()
        channel = FakeChannel("email")
        registry.register_channel(channel)

        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        result = process_notification(session, notif.id, registry)
        assert result is True
        assert len(channel.calls) == 1

    def test_process_unknown_channel(self, session):
        from dewey.sqlalchemy.notifications import process_notification

        registry = ChannelRegistry()  # No channels registered

        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        result = process_notification(session, notif.id, registry)
        assert result is False


class TestSweep:
    def test_sweep_failed_notifications(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        # Mark as failed with past process_after
        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(
                status=NotificationStatus.FAILED.value,
                process_after=datetime.now(UTC) - timedelta(minutes=5),
                attempts=1,
            )
        )
        session.commit()

        swept = sweep_failed_notifications(session)
        session.commit()

        assert notif.id in swept

        updated = get_notification(session, notif.id)
        assert updated.status == NotificationStatus.PENDING

    def test_sweep_stuck_notifications(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        # Mark as sending with old updated_at
        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(
                status=NotificationStatus.SENDING.value,
                updated_at=datetime.now(UTC) - timedelta(minutes=10),
            )
        )
        session.commit()

        swept = sweep_stuck_notifications(session, stuck_threshold_minutes=5)
        session.commit()

        assert notif.id in swept
        updated = get_notification(session, notif.id)
        assert updated.status == NotificationStatus.PENDING

    def test_sweep_combined(self, session):
        result = sweep_notifications(session)
        assert "failed" in result
        assert "stuck" in result

    def test_sweep_skips_not_ready(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        # Failed but process_after is in the future
        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(
                status=NotificationStatus.FAILED.value,
                process_after=datetime.now(UTC) + timedelta(hours=1),
            )
        )
        session.commit()

        swept = sweep_failed_notifications(session)
        assert notif.id not in swept


class TestQueries:
    def test_get_stats(self, session):
        create_notification(session, event_type="a", channel="email", recipient="a@b.com")
        create_notification(session, event_type="b", channel="email", recipient="a@b.com")
        session.commit()

        stats = get_notification_stats(session)
        assert stats["pending"] == 2
        assert stats["sent"] == 0

    def test_get_notification(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        result = get_notification(session, notif.id)
        assert result is not None
        assert result.event_type == "test"

    def test_get_notification_not_found(self, session):
        assert get_notification(session, "nonexistent") is None

    def test_get_notifications_for_task(self, session):
        from dewey.sqlalchemy.executor import create_task

        task = create_task(session, task_type="test")
        session.commit()

        create_notification(
            session, event_type="a", channel="email", recipient="a@b.com", task_id=task.id
        )
        create_notification(
            session, event_type="b", channel="slack", recipient="#ch", task_id=task.id
        )
        create_notification(
            session,
            event_type="c",
            channel="email",
            recipient="x@y.com",  # no task_id
        )
        session.commit()

        results = get_notifications_for_task(session, task.id)
        assert len(results) == 2

    def test_get_pending_notifications(self, session):
        create_notification(session, event_type="a", channel="email", recipient="a@b.com")
        create_notification(session, event_type="b", channel="slack", recipient="#ch")
        session.commit()

        results = get_pending_notifications(session)
        assert len(results) == 2

        results = get_pending_notifications(session, channel="email")
        assert len(results) == 1

    def test_get_failed_notifications(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(status=NotificationStatus.FAILED.value)
        )
        session.commit()

        results = get_failed_notifications(session)
        assert len(results) == 1

    def test_get_dead_notifications(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(status=NotificationStatus.DEAD.value)
        )
        session.commit()

        results = get_dead_notifications(session)
        assert len(results) == 1


class TestActions:
    def test_retry_notification(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(
                status=NotificationStatus.FAILED.value,
                error="timeout",
                attempts=2,
            )
        )
        session.commit()

        result = retry_notification(session, notif.id)
        session.commit()

        assert result.status == NotificationStatus.PENDING
        assert result.attempts == 0
        assert result.error == ""

    def test_retry_dead_notification(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(status=NotificationStatus.DEAD.value, attempts=3)
        )
        session.commit()

        result = retry_notification(session, notif.id)
        session.commit()

        assert result.status == NotificationStatus.PENDING

    def test_kill_notification(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        result = kill_notification(session, notif.id)
        session.commit()

        assert result.status == NotificationStatus.DEAD

    def test_purge_sent(self, session):
        notif = create_notification(
            session, event_type="test", channel="email", recipient="a@b.com"
        )
        session.commit()

        session.execute(
            update(NotificationEntryModel)
            .where(NotificationEntryModel.id == notif.id)
            .values(
                status=NotificationStatus.SENT.value,
                sent_at=datetime.now(UTC) - timedelta(days=60),
            )
        )
        session.commit()

        count = purge_sent_notifications(session, older_than_days=30)
        session.commit()

        assert count == 1
        assert get_notification(session, notif.id) is None
