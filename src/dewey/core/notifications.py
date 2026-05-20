"""Notification primitives — status, types, channel protocol, registry."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class NotificationStatus(str, Enum):  # noqa: UP042 — keeping (str, Enum) intentional; StrEnum changes str() repr
    """Notification delivery lifecycle states."""

    PENDING = "pending"
    SENDING = "sending"
    SENT = "sent"
    FAILED = "failed"
    DEAD = "dead"

    @property
    def is_terminal(self) -> bool:
        """Terminal for auto-processing — sweep and send skip these.

        Note: DEAD is terminal but allows manual retry (DEAD → PENDING).
        SENT is fully terminal — no transitions out.
        """
        return self in _TERMINAL_STATES

    def can_transition_to(self, target: NotificationStatus) -> bool:
        """Check if transitioning to ``target`` is allowed by the state machine."""
        return target in _ALLOWED_TRANSITIONS.get(self, set())


_TERMINAL_STATES = {NotificationStatus.SENT, NotificationStatus.DEAD}

_ALLOWED_TRANSITIONS: dict[NotificationStatus, set[NotificationStatus]] = {
    NotificationStatus.PENDING: {NotificationStatus.SENDING, NotificationStatus.DEAD},
    NotificationStatus.SENDING: {
        NotificationStatus.SENT,
        NotificationStatus.FAILED,
        NotificationStatus.DEAD,
        NotificationStatus.PENDING,  # sweep resets stuck sends
    },
    NotificationStatus.FAILED: {NotificationStatus.PENDING, NotificationStatus.DEAD},
    NotificationStatus.DEAD: {NotificationStatus.PENDING},  # manual retry only
}


# --- Result type ---


@dataclass
class ChannelResult:
    """Result of a channel send attempt."""

    success: bool
    error: str = ""
    response_data: dict[str, Any] = field(default_factory=dict)


# --- Channel protocol ---


@runtime_checkable
class Channel(Protocol):
    """
    Protocol for notification channels.

    Implement this to add a new delivery channel (email, webhook, Slack, etc.).
    The channel's job is simple: take a notification and deliver it.
    """

    @property
    def name(self) -> str:
        """Unique channel identifier (e.g. 'email', 'webhook', 'slack')."""
        ...

    def send(
        self,
        recipient: str,
        subject: str | None,
        body: str,
        payload: dict[str, Any],
    ) -> ChannelResult:
        """
        Deliver a notification.

        Args:
            recipient: Channel-specific destination (email address, URL, channel ID).
            subject: Optional subject line (used by email, ignored by others).
            body: Rendered notification content.
            payload: Raw data — channels can use this for structured delivery
                     (e.g. webhook sends JSON, Slack sends blocks).

        Returns:
            ChannelResult with success/failure info.
        """
        ...


# --- Data types ---


@dataclass
class NotificationAttempt:
    """Record of a single delivery attempt."""

    id: str
    notification_id: str
    attempt_number: int
    status: str  # "sent" or "failed"
    error: str
    response_data: dict[str, Any]
    created_at: datetime


@dataclass
class NotificationEntry:
    """Read-only snapshot of a notification row."""

    id: str
    task_id: str | None
    event_type: str
    channel: str
    recipient: str
    subject: str
    body: str
    payload: dict[str, Any]
    status: NotificationStatus
    attempts: int
    max_attempts: int
    error: str
    created_at: datetime
    updated_at: datetime
    process_after: datetime | None = None
    sent_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.status.is_terminal

    @property
    def is_retryable(self) -> bool:
        return self.status == NotificationStatus.FAILED and self.attempts < self.max_attempts


# Recipient resolver: given event payload, return the recipient string for a channel
RecipientResolver = Callable[[dict[str, Any]], str]

# Body renderer: given event_type + payload, return (subject, body)
BodyRenderer = Callable[[str, dict[str, Any]], tuple[str, str]]


@dataclass
class ChannelBinding:
    """
    Maps an event type to a channel with recipient resolution and body rendering.

    Used by the registry to know: when event X fires, send to channel Y,
    resolve recipient with Z, render body with W.
    """

    channel_name: str
    recipient_resolver: RecipientResolver
    body_renderer: BodyRenderer
    max_attempts: int = 3


class ChannelRegistry:
    """
    Registry that maps event types to notification channels.

    Usage::

        registry = ChannelRegistry()

        # Register channels
        registry.register_channel(email_channel)
        registry.register_channel(slack_channel)

        # Bind events to channels
        registry.on(
            "order.confirmed",
            channel="email",
            recipient=lambda p: p["customer_email"],
            render=lambda evt, p: ("Order confirmed", f"Order {p['order_id']} is confirmed."),
        )
        registry.on(
            "task.dead",
            channel="slack",
            recipient=lambda p: "#alerts",
            render=lambda evt, p: ("", f"Task {p.get('task_id', '?')} is dead-lettered."),
        )

        # Look up what to do when an event fires
        bindings = registry.get_bindings("order.confirmed")
        channel = registry.get_channel("email")
    """

    def __init__(self) -> None:
        self._channels: dict[str, Channel] = {}
        self._bindings: dict[str, list[ChannelBinding]] = {}

    def register_channel(self, channel: Channel) -> None:
        """Register a channel implementation."""
        self._channels[channel.name] = channel

    def get_channel(self, name: str) -> Channel | None:
        """Get a registered channel by name."""
        return self._channels.get(name)

    def on(
        self,
        event_type: str,
        *,
        channel: str,
        recipient: RecipientResolver,
        render: BodyRenderer,
        max_attempts: int = 3,
    ) -> None:
        """
        Bind an event type to a channel.

        When the event fires, the recipient resolver determines who to notify,
        and the body renderer produces the (subject, body) content.

        Multiple bindings per event type are allowed (e.g. email + slack).
        """
        if channel not in self._channels:
            raise ValueError(
                f"Channel {channel!r} not registered. "
                f"Call register_channel() first. Available: {list(self._channels.keys())}"
            )

        binding = ChannelBinding(
            channel_name=channel,
            recipient_resolver=recipient,
            body_renderer=render,
            max_attempts=max_attempts,
        )
        self._bindings.setdefault(event_type, []).append(binding)

    def get_bindings(self, event_type: str) -> list[ChannelBinding]:
        """Get all channel bindings for an event type."""
        return self._bindings.get(event_type, [])

    @property
    def channels(self) -> dict[str, Channel]:
        """All registered channels."""
        return dict(self._channels)

    @property
    def event_types(self) -> list[str]:
        """All event types with bindings."""
        return list(self._bindings.keys())
