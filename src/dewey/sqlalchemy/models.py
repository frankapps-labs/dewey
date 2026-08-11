"""SQLAlchemy models for the task ledger — optimised for Postgres."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from dewey.core.states import TaskStatus


class Base(DeclarativeBase):
    """Base class — users can use this or bring their own."""

    pass


def utcnow() -> datetime:
    return datetime.now(UTC)


class TaskEntryModel(Base):
    """
    Postgres-backed task ledger row.

    Every task gets written here before being enqueued to the broker.
    Postgres is the source of truth; the broker is the fast path.
    """

    __tablename__ = "task_entries"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    task_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=TaskStatus.PENDING.value, index=True
    )

    # Handler arguments — stored as JSON (works on all databases including SQLite).
    # Dewey never queries inside these columns — they're decoded and splatted into
    # the registered handler as ``handler(*args, **kwargs)``.
    #
    # To enable Postgres JSONB operators (->>, @>, GIN indexes):
    #   ALTER TABLE task_entries ALTER COLUMN args TYPE jsonb USING args::jsonb;
    #   ALTER TABLE task_entries ALTER COLUMN kwargs TYPE jsonb USING kwargs::jsonb;
    #   ALTER TABLE task_entries ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;
    args: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    kwargs: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    task_metadata: Mapped[dict] = mapped_column("metadata", JSON, nullable=False, default=dict)

    # Queue routing
    queue: Mapped[str] = mapped_column(String(50), nullable=False, default="default")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Retry tracking
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    error: Mapped[str] = mapped_column(Text, nullable=False, default="")

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )
    scheduled_for: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dispatching_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Idempotency
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)

    __table_args__ = (
        # Idempotency is only enforced when a key is set: Postgres treats NULLs as
        # distinct, so rows without a key never collide with each other.
        UniqueConstraint(
            "task_type",
            "idempotency_key",
            name="uq_task_type_idempotency_key",
        ),
        # Partial index: sweep picks up PENDING tasks ready to process
        # Only indexes rows where status='pending' — tiny index, fast scan
        Index(
            "ix_task_entries_pending_scheduled_for",
            "scheduled_for",
            postgresql_where=(status == TaskStatus.PENDING.value),
        ),
        # Partial index: sweep finds rows a dispatcher claimed but no worker took
        Index(
            "ix_task_entries_dispatching_at",
            "dispatching_at",
            postgresql_where=(status == TaskStatus.DISPATCHING.value),
        ),
        # Partial index: sweep finds stuck PROCESSING tasks
        # Only indexes rows where status='processing' — at most a handful at any time
        Index(
            "ix_task_entries_processing_started",
            "started_at",
            postgresql_where=(status == TaskStatus.PROCESSING.value),
        ),
        # Partial index: failed tasks eligible for retry
        Index(
            "ix_task_entries_failed_scheduled_for",
            "scheduled_for",
            postgresql_where=(status == TaskStatus.FAILED.value),
        ),
        # Composite: recent tasks by type (dashboard queries)
        Index("ix_task_entries_type_created", "task_type", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<TaskEntry id={self.id!r} type={self.task_type!r} status={self.status!r}>"
