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
    and_,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from dewey.core.states import TaskStatus


class Base(DeclarativeBase):
    """Base class — users can use this or bring their own."""

    pass


def utcnow() -> datetime:
    return datetime.now(UTC)


# Statuses that can still transition to EXPIRED — the deadline scan only ever
# needs to look at these rows.
_EXPIRY_CANDIDATE_STATUSES = (
    TaskStatus.PENDING.value,
    TaskStatus.DISPATCHING.value,
    TaskStatus.PROCESSING.value,
    TaskStatus.FAILED.value,
)


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
    # Immutable snapshot of the creation-time schedule. scheduled_for mutates on
    # retry; idempotent creation matches against this internal field instead.
    initial_scheduled_for: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    dispatching_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Deadline and expiry: now == expires_at is already expired; expired_at is the
    # audit timestamp for when Dewey observed the deadline had passed.
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

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
        # Partial index: nonterminal rows with a deadline — expiry enforcement
        # scans only tasks that can still transition to EXPIRED
        Index(
            "ix_task_entries_expires_at",
            "expires_at",
            postgresql_where=and_(
                status.in_(_EXPIRY_CANDIDATE_STATUSES),
                expires_at.is_not(None),
            ),
        ),
        # Composite: recent tasks by type (dashboard queries)
        Index("ix_task_entries_type_created", "task_type", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<TaskEntry id={self.id!r} type={self.task_type!r} status={self.status!r}>"


class DispatcherHeartbeatModel(Base):
    """
    Dispatcher liveness row — one per running dispatcher instance.

    Mirrors :class:`dewey.core.heartbeat.DispatcherHeartbeat`. Stores only a
    random instance identifier, Dewey version, backend kind, non-secret database
    identifier, queues, and timestamps — never DSNs, credentials, hostnames, or
    process environment. Staleness is decided by readers comparing last_seen_at;
    there is no database-level TTL.
    """

    __tablename__ = "dewey_dispatcher_heartbeats"

    #: Random identifier minted by the dispatcher at startup — not a hostname or PID.
    instance_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    dewey_version: Mapped[str] = mapped_column(String(50), nullable=False)
    #: Backend kind, e.g. "django" or "sqlalchemy".
    backend: Mapped[str] = mapped_column(String(50), nullable=False)
    #: Non-secret database alias/identifier — never a DSN.
    database: Mapped[str] = mapped_column(String(200), nullable=False)
    #: Queues this dispatcher serves; NULL means all queues.
    queues: Mapped[list | None] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        # Freshness checks and stale-row cleanup scan by last seen time
        Index("ix_dewey_heartbeats_last_seen", "last_seen_at"),
        # Readiness matches a dispatcher by backend kind and database identifier
        Index("ix_dewey_heartbeats_backend_database", "backend", "database"),
    )

    def __repr__(self) -> str:
        return (
            f"<DispatcherHeartbeat instance_id={self.instance_id!r} "
            f"backend={self.backend!r} database={self.database!r}>"
        )
