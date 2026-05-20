"""Django models for notifications — per-attempt tracking."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from django.db import models

from dewey.core.notifications import NotificationStatus

if TYPE_CHECKING:
    from dewey.core.notifications import NotificationAttempt as NotificationAttemptDC
    from dewey.core.notifications import NotificationEntry as NotificationEntryDC


class NotificationEntry(models.Model):
    """
    Notification delivery record.

    Tracks each notification through its lifecycle: pending → sending → sent/failed/dead.
    Linked optionally to a task for audit trail.
    """

    class Status(models.TextChoices):
        PENDING = NotificationStatus.PENDING.value, "Pending"
        SENDING = NotificationStatus.SENDING.value, "Sending"
        SENT = NotificationStatus.SENT.value, "Sent"
        FAILED = NotificationStatus.FAILED.value, "Failed"
        DEAD = NotificationStatus.DEAD.value, "Dead"

    id = models.CharField(
        max_length=36, primary_key=True, default=lambda: str(uuid.uuid4()), editable=False
    )

    # Optional link to the task that triggered this notification
    task_id = models.CharField(max_length=36, null=True, blank=True, db_index=True)

    # What event triggered this notification
    event_type = models.CharField(max_length=100, db_index=True)

    # Delivery target
    channel = models.CharField(max_length=50, db_index=True)
    recipient = models.CharField(max_length=500)

    # Content
    subject = models.CharField(max_length=500, default="", blank=True)
    body = models.TextField(default="", blank=True)
    payload = models.JSONField(default=dict, blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    # Status tracking
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    attempts = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=3)
    error = models.TextField(default="", blank=True)

    # Timestamps
    created_at = models.DateTimeField(default=lambda: datetime.now(UTC), db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    process_after = models.DateTimeField(null=True, blank=True)
    sent_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "notification_entries"
        indexes = [
            models.Index(
                fields=["process_after"],
                name="ix_notif_pending_pa",
                condition=models.Q(status="pending"),
            ),
            models.Index(
                fields=["updated_at"],
                name="ix_notif_sending_ua",
                condition=models.Q(status="sending"),
            ),
            models.Index(
                fields=["process_after"],
                name="ix_notif_failed_pa",
                condition=models.Q(status="failed"),
            ),
            models.Index(
                fields=["task_id", "created_at"],
                name="ix_notif_task_created_dj",
            ),
        ]

    def __str__(self) -> str:
        return (
            f"<NotificationEntry id={self.id!r} event={self.event_type!r} "
            f"channel={self.channel!r} status={self.status!r}>"
        )

    def to_dataclass(self) -> NotificationEntryDC:
        """Convert to framework-agnostic NotificationEntry dataclass."""
        from dewey.core.notifications import NotificationEntry as NotificationEntryDC

        return NotificationEntryDC(
            id=str(self.id),
            task_id=self.task_id,
            event_type=self.event_type,
            channel=self.channel,
            recipient=self.recipient,
            subject=self.subject,
            body=self.body,
            payload=self.payload,
            status=NotificationStatus(self.status),
            attempts=self.attempts,
            max_attempts=self.max_attempts,
            error=self.error,
            created_at=self.created_at,
            updated_at=self.updated_at,
            process_after=self.process_after,
            sent_at=self.sent_at,
            metadata=self.metadata,
        )


class NotificationAttempt(models.Model):
    """
    Per-attempt delivery log.

    Every send attempt — success or failure — is logged here for audit.
    """

    id = models.CharField(
        max_length=36, primary_key=True, default=lambda: str(uuid.uuid4()), editable=False
    )
    notification = models.ForeignKey(
        NotificationEntry,
        on_delete=models.CASCADE,
        related_name="delivery_attempts",
    )
    attempt_number = models.IntegerField()
    status = models.CharField(max_length=20)  # "sent" or "failed"
    error = models.TextField(default="", blank=True)
    response_data = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=lambda: datetime.now(UTC))

    class Meta:
        db_table = "notification_attempts"
        indexes = [
            models.Index(
                fields=["notification", "attempt_number"],
                name="ix_notif_attempt_num_dj",
            ),
        ]

    def __str__(self) -> str:
        return (
            f"<NotificationAttempt id={self.id!r} notification={self.notification_id!r} "
            f"attempt={self.attempt_number} status={self.status!r}>"
        )

    def to_dataclass(self) -> NotificationAttemptDC:
        """Convert to framework-agnostic NotificationAttempt dataclass."""
        from dewey.core.notifications import NotificationAttempt as NotificationAttemptDC

        return NotificationAttemptDC(
            id=str(self.id),
            notification_id=str(self.notification_id),
            attempt_number=self.attempt_number,
            status=self.status,
            error=self.error,
            response_data=self.response_data,
            created_at=self.created_at,
        )
