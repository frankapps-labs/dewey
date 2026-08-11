"""Django model for the task ledger — mirrors the SQLAlchemy TaskEntryModel."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from django.db import models, router

from dewey.core.states import TaskStatus

if TYPE_CHECKING:
    from dewey.core.types import TaskEntry as TaskEntryDC


def new_uuid() -> str:
    """Default row ID. A module-level function, because Django migrations have to be
    able to serialise it — a lambda cannot be written into a migration file."""
    return str(uuid.uuid4())


def utcnow() -> datetime:
    """Default creation timestamp, serialisable for the same reason."""
    return datetime.now(UTC)


class TaskEntry(models.Model):
    """
    Postgres-backed task ledger row.

    Every task gets written here before being enqueued to the broker.
    Postgres is the source of truth; the broker is the fast path.
    """

    class Status(models.TextChoices):
        PENDING = TaskStatus.PENDING.value, "Pending"
        DISPATCHING = TaskStatus.DISPATCHING.value, "Dispatching"
        PROCESSING = TaskStatus.PROCESSING.value, "Processing"
        COMPLETED = TaskStatus.COMPLETED.value, "Completed"
        FAILED = TaskStatus.FAILED.value, "Failed"
        DEAD = TaskStatus.DEAD.value, "Dead"
        EXPIRED = TaskStatus.EXPIRED.value, "Expired"

    id = models.CharField(max_length=36, primary_key=True, default=new_uuid, editable=False)
    task_type = models.CharField(max_length=100, db_index=True)
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )

    # Handler arguments — JSONField uses Postgres JSONB
    args = models.JSONField(default=list, blank=True)
    kwargs = models.JSONField(default=dict, blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    # Queue routing
    queue = models.CharField(max_length=50, default="default")
    priority = models.IntegerField(default=0)

    # Retry tracking
    attempts = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=5)
    error = models.TextField(default="", blank=True)

    # Timestamps
    created_at = models.DateTimeField(default=utcnow, db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    scheduled_for = models.DateTimeField(null=True, blank=True)
    # Immutable snapshot of the creation-time schedule. scheduled_for mutates on
    # retry; idempotent creation matches against this internal field instead.
    initial_scheduled_for = models.DateTimeField(null=True, editable=False)
    dispatching_at = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    # Deadline and expiry: now == expires_at is already expired; expired_at is the
    # audit timestamp for when Dewey observed the deadline had passed.
    expires_at = models.DateTimeField(null=True, blank=True)
    expired_at = models.DateTimeField(null=True, blank=True)

    # Idempotency
    idempotency_key = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        db_table = "task_entries"
        constraints = [
            models.UniqueConstraint(
                fields=["task_type", "idempotency_key"],
                name="uq_task_type_idempotency_key",
            ),
        ]
        indexes = [
            # Partial index: pending tasks ready to process
            models.Index(
                fields=["scheduled_for"],
                name="ix_task_pending_sched",
                condition=models.Q(status="pending"),
            ),
            # Partial index: rows a dispatcher claimed but no worker took
            models.Index(
                fields=["dispatching_at"],
                name="ix_task_dispatching",
                condition=models.Q(status="dispatching"),
            ),
            # Partial index: stuck processing tasks
            models.Index(
                fields=["started_at"],
                name="ix_task_processing_started",
                condition=models.Q(status="processing"),
            ),
            # Partial index: failed tasks eligible for retry
            models.Index(
                fields=["scheduled_for"],
                name="ix_task_failed_sched",
                condition=models.Q(status="failed"),
            ),
            # Partial index: nonterminal rows with a deadline — expiry enforcement
            # scans only tasks that can still transition to EXPIRED
            models.Index(
                fields=["expires_at"],
                name="ix_task_expires",
                condition=models.Q(
                    expires_at__isnull=False,
                    status__in=["pending", "dispatching", "failed"],
                ),
            ),
            # Composite: recent tasks by type
            models.Index(
                fields=["task_type", "created_at"],
                name="ix_task_type_created",
            ),
        ]

    def __str__(self) -> str:
        return f"<TaskEntry id={self.id!r} type={self.task_type!r} status={self.status!r}>"

    def to_dataclass(self) -> TaskEntryDC:
        """Convert to framework-agnostic TaskEntry dataclass."""
        from dewey.core.types import TaskEntry as TaskEntryDC

        return TaskEntryDC(
            id=str(self.id),
            task_type=self.task_type,
            status=TaskStatus(self.status),
            args=self.args,
            kwargs=self.kwargs,
            queue=self.queue,
            priority=self.priority,
            attempts=self.attempts,
            max_attempts=self.max_attempts,
            error=self.error,
            created_at=self.created_at,
            updated_at=self.updated_at,
            scheduled_for=self.scheduled_for,
            dispatching_at=self.dispatching_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            idempotency_key=self.idempotency_key,
            metadata=self.metadata,
            expires_at=self.expires_at,
            initial_scheduled_for=self.initial_scheduled_for,
            expired_at=self.expired_at,
        )


class DispatcherHeartbeat(models.Model):
    """
    Dispatcher liveness row — one per running dispatcher instance.

    Mirrors the SQLAlchemy DispatcherHeartbeatModel and the framework-agnostic
    dewey.core.heartbeat.DispatcherHeartbeat contract. Stores only a random
    instance identifier, Dewey version, backend kind, non-secret database
    identifier, queues, and timestamps — never DSNs, credentials, hostnames, or
    process environment. Staleness is decided by readers comparing last_seen_at;
    there is no database-level TTL.
    """

    # Random identifier minted by the dispatcher at startup — not a hostname or PID.
    instance_id = models.CharField(max_length=36, primary_key=True)
    dewey_version = models.CharField(max_length=50)
    # Backend kind, e.g. "django" or "sqlalchemy".
    backend = models.CharField(max_length=50)
    # Non-secret database alias/identifier — never a DSN.
    database = models.CharField(max_length=200)
    # Queues this dispatcher serves; NULL means all queues.
    queues = models.JSONField(null=True, blank=True)
    started_at = models.DateTimeField()
    last_seen_at = models.DateTimeField()

    class Meta:
        db_table = "dewey_dispatcher_heartbeats"
        indexes = [
            # Freshness checks and stale-row cleanup scan by last seen time
            models.Index(fields=["last_seen_at"], name="ix_heartbeat_last_seen"),
            # Readiness matches a dispatcher by backend kind and database identifier
            models.Index(fields=["backend", "database"], name="ix_heartbeat_backend_db"),
        ]

    def __str__(self) -> str:
        return (
            f"<DispatcherHeartbeat instance_id={self.instance_id!r} "
            f"backend={self.backend!r} database={self.database!r}>"
        )


def resolve_db_alias(using: str | None = None) -> str:
    """The database alias Dewey operates on.

    An explicit ``using`` wins. Otherwise the project's database routers decide,
    exactly as they would for any ORM write, falling back to ``default``. Every
    Dewey transaction, ``SELECT FOR UPDATE`` and NOTIFY must run on this one
    alias — ``transaction.atomic()`` does not consult routers, so resolving the
    alias up front is what keeps the lock, the write and the wake-up in the same
    transaction on multi-database projects.
    """
    return using or router.db_for_write(TaskEntry)
