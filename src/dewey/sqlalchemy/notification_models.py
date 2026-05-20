"""SQLAlchemy models for notifications — per-attempt tracking."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dewey.core.notifications import NotificationStatus
from dewey.sqlalchemy.models import Base


def utcnow() -> datetime:
    return datetime.now(UTC)


class NotificationEntryModel(Base):
    """
    Notification delivery record.

    Tracks each notification through its lifecycle: pending → sending → sent/failed/dead.
    Linked optionally to a task for audit trail.
    """

    __tablename__ = "notification_entries"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    # Optional link to the task that triggered this notification
    task_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("task_entries.id", ondelete="SET NULL"), nullable=True, index=True
    )

    # What event triggered this notification
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)

    # Delivery target
    channel: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    recipient: Mapped[str] = mapped_column(String(500), nullable=False)

    # Content
    subject: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    body: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # Stored as JSON (works on all databases). For Postgres JSONB operators:
    #   ALTER TABLE notification_entries ALTER COLUMN payload TYPE jsonb USING payload::jsonb;
    #   ALTER TABLE notification_entries ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    notification_metadata: Mapped[dict] = mapped_column(
        "metadata", JSON, nullable=False, default=dict
    )

    # Status tracking
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=NotificationStatus.PENDING.value, index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    error: Mapped[str] = mapped_column(Text, nullable=False, default="")

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )
    process_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Relationship to attempts
    delivery_attempts: Mapped[list[NotificationAttemptModel]] = relationship(
        back_populates="notification",
        cascade="all, delete-orphan",
        order_by="NotificationAttemptModel.attempt_number",
    )

    __table_args__ = (
        # Partial index: pending notifications ready to send
        Index(
            "ix_notif_pending_process_after",
            "process_after",
            postgresql_where=(status == NotificationStatus.PENDING.value),
        ),
        # Partial index: stuck sending notifications
        Index(
            "ix_notif_sending_updated",
            "updated_at",
            postgresql_where=(status == NotificationStatus.SENDING.value),
        ),
        # Partial index: failed notifications eligible for retry
        Index(
            "ix_notif_failed_process_after",
            "process_after",
            postgresql_where=(status == NotificationStatus.FAILED.value),
        ),
        # Composite: notifications by task
        Index("ix_notif_task_created", "task_id", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<NotificationEntry id={self.id!r} event={self.event_type!r} "
            f"channel={self.channel!r} status={self.status!r}>"
        )


class NotificationAttemptModel(Base):
    """
    Per-attempt delivery log.

    Every send attempt — success or failure — is logged here for audit.
    """

    __tablename__ = "notification_attempts"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    notification_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("notification_entries.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # "sent" or "failed"
    error: Mapped[str] = mapped_column(Text, nullable=False, default="")
    response_data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    notification: Mapped[NotificationEntryModel] = relationship(back_populates="delivery_attempts")

    __table_args__ = (Index("ix_notif_attempt_notif_num", "notification_id", "attempt_number"),)

    def __repr__(self) -> str:
        return (
            f"<NotificationAttempt id={self.id!r} notification={self.notification_id!r} "
            f"attempt={self.attempt_number} status={self.status!r}>"
        )
