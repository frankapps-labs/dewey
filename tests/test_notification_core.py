"""Tests for core notification types, state machine, channel protocol, and registry."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from dewey.core.notifications import (
    Channel,
    ChannelRegistry,
    ChannelResult,
    NotificationEntry,
    NotificationStatus,
)
from tests.helpers import FakeChannel


class TestNotificationStatus:
    def test_terminal_states(self):
        assert NotificationStatus.SENT.is_terminal
        assert NotificationStatus.DEAD.is_terminal
        assert not NotificationStatus.PENDING.is_terminal
        assert not NotificationStatus.SENDING.is_terminal
        assert not NotificationStatus.FAILED.is_terminal

    def test_values(self):
        assert NotificationStatus.PENDING.value == "pending"
        assert NotificationStatus.SENDING.value == "sending"
        assert NotificationStatus.SENT.value == "sent"
        assert NotificationStatus.FAILED.value == "failed"
        assert NotificationStatus.DEAD.value == "dead"


class TestTransitions:
    def test_pending_to_sending(self):
        assert NotificationStatus.PENDING.can_transition_to(NotificationStatus.SENDING)

    def test_pending_to_dead(self):
        assert NotificationStatus.PENDING.can_transition_to(NotificationStatus.DEAD)

    def test_sending_to_sent(self):
        assert NotificationStatus.SENDING.can_transition_to(NotificationStatus.SENT)

    def test_sending_to_failed(self):
        assert NotificationStatus.SENDING.can_transition_to(NotificationStatus.FAILED)

    def test_sending_to_dead(self):
        assert NotificationStatus.SENDING.can_transition_to(NotificationStatus.DEAD)

    def test_sending_to_pending_sweep(self):
        assert NotificationStatus.SENDING.can_transition_to(NotificationStatus.PENDING)

    def test_failed_to_pending(self):
        assert NotificationStatus.FAILED.can_transition_to(NotificationStatus.PENDING)

    def test_failed_to_dead(self):
        assert NotificationStatus.FAILED.can_transition_to(NotificationStatus.DEAD)

    def test_dead_to_pending_manual_retry(self):
        assert NotificationStatus.DEAD.can_transition_to(NotificationStatus.PENDING)

    def test_sent_is_fully_terminal(self):
        for status in NotificationStatus:
            if status != NotificationStatus.SENT:
                assert not NotificationStatus.SENT.can_transition_to(status)

    def test_invalid_transition(self):
        assert not NotificationStatus.PENDING.can_transition_to(NotificationStatus.SENT)
        assert not NotificationStatus.FAILED.can_transition_to(NotificationStatus.SENT)

    def test_cross_type_rejected(self):
        """NotificationStatus.can_transition_to rejects TaskStatus values."""
        from dewey.core.states import TaskStatus

        assert not NotificationStatus.PENDING.can_transition_to(TaskStatus.PROCESSING)  # type: ignore[arg-type]


class TestChannelResult:
    def test_success(self):
        r = ChannelResult(success=True, response_data={"id": "123"})
        assert r.success
        assert r.error == ""

    def test_failure(self):
        r = ChannelResult(success=False, error="timeout")
        assert not r.success
        assert r.error == "timeout"


class TestChannelProtocol:
    def test_fake_channel_satisfies_protocol(self):
        ch = FakeChannel()
        assert isinstance(ch, Channel)

    def test_fake_channel_send(self):
        ch = FakeChannel()
        result = ch.send("user@test.com", "Hello", "Body", {"key": "val"})
        assert result.success
        assert len(ch.calls) == 1
        assert ch.calls[0]["recipient"] == "user@test.com"


class TestNotificationEntry:
    def test_is_terminal(self):
        now = datetime.now(UTC)
        entry = NotificationEntry(
            id="1",
            task_id=None,
            event_type="test",
            channel="email",
            recipient="a@b.com",
            subject="",
            body="",
            payload={},
            status=NotificationStatus.SENT,
            attempts=1,
            max_attempts=3,
            error="",
            created_at=now,
            updated_at=now,
        )
        assert entry.is_terminal

    def test_is_retryable(self):
        now = datetime.now(UTC)
        entry = NotificationEntry(
            id="1",
            task_id=None,
            event_type="test",
            channel="email",
            recipient="a@b.com",
            subject="",
            body="",
            payload={},
            status=NotificationStatus.FAILED,
            attempts=1,
            max_attempts=3,
            error="oops",
            created_at=now,
            updated_at=now,
        )
        assert entry.is_retryable

    def test_not_retryable_at_max(self):
        now = datetime.now(UTC)
        entry = NotificationEntry(
            id="1",
            task_id=None,
            event_type="test",
            channel="email",
            recipient="a@b.com",
            subject="",
            body="",
            payload={},
            status=NotificationStatus.FAILED,
            attempts=3,
            max_attempts=3,
            error="oops",
            created_at=now,
            updated_at=now,
        )
        assert not entry.is_retryable


class TestChannelRegistry:
    def test_register_and_get_channel(self):
        registry = ChannelRegistry()
        ch = FakeChannel("email")
        registry.register_channel(ch)
        assert registry.get_channel("email") is ch

    def test_get_unknown_channel(self):
        registry = ChannelRegistry()
        assert registry.get_channel("nonexistent") is None

    def test_on_requires_registered_channel(self):
        registry = ChannelRegistry()
        with pytest.raises(ValueError, match="Channel 'email' not registered"):
            registry.on(
                "order.confirmed",
                channel="email",
                recipient=lambda p: p["email"],
                render=lambda e, p: ("Subject", "Body"),
            )

    def test_on_and_get_bindings(self):
        registry = ChannelRegistry()
        registry.register_channel(FakeChannel("email"))

        registry.on(
            "order.confirmed",
            channel="email",
            recipient=lambda p: p["email"],
            render=lambda e, p: ("Order confirmed", f"Order {p['order_id']}"),
        )

        bindings = registry.get_bindings("order.confirmed")
        assert len(bindings) == 1
        assert bindings[0].channel_name == "email"

    def test_multiple_bindings_per_event(self):
        registry = ChannelRegistry()
        registry.register_channel(FakeChannel("email"))
        registry.register_channel(FakeChannel("slack"))

        registry.on(
            "task.dead",
            channel="email",
            recipient=lambda p: "admin@test.com",
            render=lambda e, p: ("Alert", "Task died"),
        )
        registry.on(
            "task.dead",
            channel="slack",
            recipient=lambda p: "#alerts",
            render=lambda e, p: ("", "Task died"),
        )

        bindings = registry.get_bindings("task.dead")
        assert len(bindings) == 2
        channels = {b.channel_name for b in bindings}
        assert channels == {"email", "slack"}

    def test_no_bindings_for_unknown_event(self):
        registry = ChannelRegistry()
        assert registry.get_bindings("unknown") == []

    def test_binding_recipient_resolver(self):
        registry = ChannelRegistry()
        registry.register_channel(FakeChannel("email"))
        registry.on(
            "order.confirmed",
            channel="email",
            recipient=lambda p: p["customer_email"],
            render=lambda e, p: ("S", "B"),
        )
        binding = registry.get_bindings("order.confirmed")[0]
        assert (
            binding.recipient_resolver({"customer_email": "user@example.com"}) == "user@example.com"
        )

    def test_binding_body_renderer(self):
        registry = ChannelRegistry()
        registry.register_channel(FakeChannel("email"))
        registry.on(
            "order.confirmed",
            channel="email",
            recipient=lambda p: "a@b.com",
            render=lambda e, p: (f"Re: {e}", f"Order {p['id']}"),
        )
        binding = registry.get_bindings("order.confirmed")[0]
        subject, body = binding.body_renderer("order.confirmed", {"id": "123"})
        assert subject == "Re: order.confirmed"
        assert body == "Order 123"

    def test_custom_max_attempts(self):
        registry = ChannelRegistry()
        registry.register_channel(FakeChannel("email"))
        registry.on(
            "critical.alert",
            channel="email",
            recipient=lambda p: "admin@test.com",
            render=lambda e, p: ("Alert", "Critical"),
            max_attempts=10,
        )
        binding = registry.get_bindings("critical.alert")[0]
        assert binding.max_attempts == 10

    def test_channels_property(self):
        registry = ChannelRegistry()
        registry.register_channel(FakeChannel("email"))
        registry.register_channel(FakeChannel("slack"))
        assert set(registry.channels.keys()) == {"email", "slack"}

    def test_event_types_property(self):
        registry = ChannelRegistry()
        registry.register_channel(FakeChannel("email"))
        registry.on("a", channel="email", recipient=lambda p: "", render=lambda e, p: ("", ""))
        registry.on("b", channel="email", recipient=lambda p: "", render=lambda e, p: ("", ""))
        assert set(registry.event_types) == {"a", "b"}
