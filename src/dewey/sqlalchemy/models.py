"""SQLAlchemy models for the task ledger — optimised for Postgres."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    and_,
    event,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from dewey.core.states import TaskStatus
from dewey.dashboard.history import install_task_history


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
    TaskStatus.FAILED.value,
)


class TaskEntryModel(Base):
    """
    Postgres-backed task ledger row.

    A task exists when its row exists: workers claim it from this table, and
    nothing is enqueued anywhere else.
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
    # Internal execution ownership; null for legacy/unclaimed rows, never a lease.
    execution_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    # Native assignment ownership. Nullable additive fields preserve legacy rows.
    dispatch_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    worker_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    execution_deadline_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    latest_outcome: Mapped[str | None] = mapped_column(String(32), nullable=True)
    execution_timeout_s: Mapped[float | None] = mapped_column(Float, nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )

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
        CheckConstraint(
            "execution_timeout_s > 0 AND execution_timeout_s < 'Infinity'::float8",
            name="ck_task_execution_timeout",
        ),
        # What a worker claims, in the order it claims it: the head of a queue is
        # read straight off this index however long the backlog is.
        Index(
            "ix_task_claim",
            "queue",
            priority.desc(),
            func.coalesce(scheduled_for, created_at),
            "created_at",
            "id",
            postgresql_where=and_(
                status.in_((TaskStatus.PENDING.value, TaskStatus.FAILED.value)),
                dispatch_token.is_(None),
            ),
        ),
        Index(
            "ix_task_native_lease",
            "lease_expires_at",
            postgresql_where=lease_expires_at.is_not(None),
        ),
        # Idempotency is only enforced when a key is set: Postgres treats NULLs as
        # distinct, so rows without a key never collide with each other.
        UniqueConstraint(
            "task_type",
            "idempotency_key",
            name="uq_task_type_idempotency_key",
        ),
        # Partial index: PENDING tasks by when they are due
        # Only indexes rows where status='pending' — tiny index, fast scan
        Index(
            "ix_task_entries_pending_scheduled_for",
            "scheduled_for",
            postgresql_where=(status == TaskStatus.PENDING.value),
        ),
        # Partial index over assigned-but-not-started rows
        Index(
            "ix_task_entries_dispatching_at",
            "dispatching_at",
            postgresql_where=(status == TaskStatus.DISPATCHING.value),
        ),
        # Partial index: PROCESSING tasks by start time (operator queries)
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


class TaskEventModel(Base):
    """Durable status transitions and operator audits; no payload/error content."""

    __tablename__ = "dewey_task_events"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    task_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("task_entries.id", ondelete="CASCADE"), nullable=False
    )
    queue: Mapped[str] = mapped_column(String(50), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    diagnostics: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), nullable=False, default=dict
    )
    audit: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), nullable=False, default=dict
    )
    __table_args__ = (
        Index("ix_dewey_event_time", "occurred_at"),
        Index("ix_dewey_event_queue_time", "queue", "occurred_at"),
        Index("ix_dewey_event_task_time", "task_id", "occurred_at"),
    )


class HistoryStateModel(Base):
    __tablename__ = "dewey_history_state"
    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


def _install_created_history(metadata, connection, **kwargs):
    # Only when the event table was actually created, not on every create_all()
    # probe. Existing deployments must use an explicit migration.
    if connection.dialect.name == "postgresql" and TaskEventModel.__table__ in kwargs.get(
        "tables", ()
    ):
        install_task_history(connection)


event.listen(Base.metadata, "after_create", _install_created_history)


class WorkerHeartbeatModel(Base):
    """
    Worker liveness row — one per running worker instance.

    Mirrors :class:`dewey.core.heartbeat.WorkerHeartbeat`. Stores only a
    random instance identifier, Dewey version, backend kind, non-secret database
    identifier, queues, and timestamps — never DSNs, credentials, hostnames, or
    process environment. Staleness is decided by readers comparing last_seen_at;
    there is no database-level TTL.
    """

    __tablename__ = "dewey_dispatcher_heartbeats"

    #: Random identifier minted by the worker at startup — not a hostname or PID.
    instance_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    dewey_version: Mapped[str] = mapped_column(String(50), nullable=False)
    #: Backend kind, e.g. "django" or "sqlalchemy".
    backend: Mapped[str] = mapped_column(String(50), nullable=False)
    #: Non-secret database alias/identifier — never a DSN.
    database: Mapped[str] = mapped_column(String(200), nullable=False)
    #: Queues this worker serves; NULL means all queues.
    queues: Mapped[list | None] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        # Freshness checks and stale-row cleanup scan by last seen time
        Index("ix_dewey_heartbeats_last_seen", "last_seen_at"),
        # Readiness matches a worker by backend kind and database identifier
        Index("ix_dewey_heartbeats_backend_database", "backend", "database"),
    )

    def __repr__(self) -> str:
        return (
            f"<WorkerHeartbeat instance_id={self.instance_id!r} "
            f"backend={self.backend!r} database={self.database!r}>"
        )


class ScheduleEntryModel(Base):
    """
    One applied schedule: its definition, and the cursor that makes ticks happen once.

    Written only by ``apply`` (definition) and tick creation (cursor, counters).
    ``paused`` is operator state: applying a definition never changes it.
    """

    __tablename__ = "dewey_schedules"

    name: Mapped[str] = mapped_column(String(100), primary_key=True)
    #: Everything a tick needs, resolved at apply time — see Schedule.definition().
    definition: Mapped[dict] = mapped_column(
        JSON().with_variant(JSONB, "postgresql"), nullable=False
    )
    definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    #: The last tick accounted for (created, missed or skipped), or the apply time.
    cursor_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    next_due_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    paused: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    #: A timed pause ends by itself at this instant; NULL pauses until resumed.
    paused_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    #: The most recent tick's task, for the overlap check.
    last_task_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    missed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    skipped_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )

    __table_args__ = (Index("ix_dewey_schedules_next_due", "next_due_at"),)

    def __repr__(self) -> str:
        return f"<ScheduleEntry name={self.name!r} next_due_at={self.next_due_at!r}>"
