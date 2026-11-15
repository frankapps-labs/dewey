"""Django model for the task ledger — mirrors the SQLAlchemy TaskEntryModel."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from django.db import models, router
from django.db.models.functions import Coalesce

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

    A task exists when its row exists: workers claim it from this table, and
    nothing is enqueued anywhere else.
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
    # Internal execution ownership; null for legacy/unclaimed rows, never a lease.
    execution_token = models.CharField(max_length=36, null=True, editable=False)
    dispatch_token = models.CharField(max_length=36, null=True, editable=False)
    worker_id = models.CharField(max_length=36, null=True, editable=False)
    lease_expires_at = models.DateTimeField(null=True, editable=False)
    execution_deadline_at = models.DateTimeField(null=True, editable=False)
    latest_outcome = models.CharField(max_length=32, null=True, editable=False)
    execution_timeout_s = models.FloatField(null=True, blank=True)
    cancel_requested = models.BooleanField(default=False, db_default=False, editable=False)

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
            models.CheckConstraint(
                condition=models.Q(execution_timeout_s__gt=0, execution_timeout_s__lt=float("inf")),
                name="ck_task_execution_timeout",
            ),
            models.UniqueConstraint(
                fields=["task_type", "idempotency_key"],
                name="uq_task_type_idempotency_key",
            ),
        ]
        permissions = [
            ("retry_taskentry", "Can request one diagnosed task retry"),
            ("manage_task_retention", "Can configure completed task retention"),
        ]
        indexes = [
            models.Index(
                fields=["completed_at", "id"],
                name="ix_task_completed_retention",
                condition=models.Q(status="completed"),
            ),
            # What a worker claims, in the order it claims it: the head of a queue
            # is read straight off this index however long the backlog is.
            models.Index(
                models.F("queue"),
                models.F("priority").desc(),
                Coalesce("scheduled_for", "created_at"),
                models.F("created_at"),
                models.F("id"),
                name="ix_task_claim",
                condition=models.Q(status__in=["pending", "failed"], dispatch_token__isnull=True),
            ),
            models.Index(
                fields=["lease_expires_at"],
                name="ix_task_native_lease",
                condition=models.Q(lease_expires_at__isnull=False),
            ),
            # Partial index: pending tasks ready to process
            models.Index(
                fields=["scheduled_for"],
                name="ix_task_pending_sched",
                condition=models.Q(status="pending"),
            ),
            # Partial index over assigned-but-not-started rows
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
            execution_timeout_s=self.execution_timeout_s,
        )


class WorkerHeartbeat(models.Model):
    """
    Worker liveness row — one per running worker instance.

    Mirrors the SQLAlchemy WorkerHeartbeatModel and the framework-agnostic
    dewey.core.heartbeat.WorkerHeartbeat contract. Stores only a random
    instance identifier, Dewey version, backend kind, non-secret database
    identifier, queues, and timestamps — never DSNs, credentials, hostnames, or
    process environment. Staleness is decided by readers comparing last_seen_at;
    there is no database-level TTL.
    """

    # Random identifier minted by the worker at startup — not a hostname or PID.
    instance_id = models.CharField(max_length=36, primary_key=True)
    dewey_version = models.CharField(max_length=50)
    # Backend kind, e.g. "django" or "sqlalchemy".
    backend = models.CharField(max_length=50)
    # Non-secret database alias/identifier — never a DSN.
    database = models.CharField(max_length=200)
    # Queues this worker serves; NULL means all queues.
    queues = models.JSONField(null=True, blank=True)
    started_at = models.DateTimeField()
    last_seen_at = models.DateTimeField()

    class Meta:
        db_table = "dewey_dispatcher_heartbeats"
        indexes = [
            # Freshness checks and stale-row cleanup scan by last seen time
            models.Index(fields=["last_seen_at"], name="ix_heartbeat_last_seen"),
            # Readiness matches a worker by backend kind and database identifier
            models.Index(fields=["backend", "database"], name="ix_heartbeat_backend_db"),
        ]

    def __str__(self) -> str:
        return (
            f"<WorkerHeartbeat instance_id={self.instance_id!r} "
            f"backend={self.backend!r} database={self.database!r}>"
        )


class ScheduleEntry(models.Model):
    """
    One applied schedule: its definition, and the cursor that makes ticks happen once.

    Mirrors the SQLAlchemy ScheduleEntryModel. Written only by ``apply``
    (definition) and tick creation (cursor, counters). ``paused`` is operator
    state: applying a definition never changes it.
    """

    name = models.CharField(max_length=100, primary_key=True)
    # Everything a tick needs, resolved at apply time — see Schedule.definition().
    definition = models.JSONField()
    definition_hash = models.CharField(max_length=64)
    # The last tick accounted for (created, missed or skipped), or the apply time.
    cursor_at = models.DateTimeField()
    next_due_at = models.DateTimeField()
    paused = models.BooleanField(default=False, db_default=False)
    # A timed pause ends by itself at this instant; NULL pauses until resumed.
    paused_until = models.DateTimeField(null=True, blank=True)
    last_fired_at = models.DateTimeField(null=True, blank=True)
    # The most recent tick's task, for the overlap check.
    last_task_id = models.CharField(max_length=36, null=True, blank=True)
    missed_count = models.IntegerField(default=0)
    skipped_count = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "dewey_schedules"
        indexes = [models.Index(fields=["next_due_at"], name="ix_dewey_schedules_next_due")]

    def __str__(self) -> str:
        return f"<ScheduleEntry name={self.name!r} next_due_at={self.next_due_at!r}>"


class TaskEvent(models.Model):
    """Append-only transition/audit identity; diagnostics are host-reviewed enrichment."""

    id = models.CharField(max_length=36, primary_key=True, default=new_uuid, editable=False)
    task = models.ForeignKey(TaskEntry, on_delete=models.CASCADE, db_constraint=False)
    queue = models.CharField(max_length=50)
    attempt = models.IntegerField()
    kind = models.CharField(max_length=20)
    occurred_at = models.DateTimeField(default=utcnow)
    diagnostics = models.JSONField(default=dict)
    audit = models.JSONField(default=dict)

    class Meta:
        db_table = "dewey_task_events"
        indexes = [
            models.Index(fields=["occurred_at"], name="ix_dewey_event_time"),
            models.Index(fields=["queue", "occurred_at"], name="ix_dewey_event_queue_time"),
            models.Index(fields=["task", "occurred_at"], name="ix_dewey_event_task_time"),
        ]


class HistoryState(models.Model):
    id = models.CharField(max_length=20, primary_key=True)
    started_at = models.DateTimeField(default=utcnow)

    class Meta:
        db_table = "dewey_history_state"


class TaskRetention(models.Model):
    """Database-scoped policy. Reads default to disabled without creating a row."""

    id = models.CharField(max_length=20, primary_key=True, default="completed")
    days = models.PositiveIntegerField(default=30)
    enabled = models.BooleanField(default=False)
    revision = models.PositiveIntegerField(default=0)
    updated_at = models.DateTimeField(default=utcnow)

    class Meta:
        db_table = "dewey_task_retention"
        constraints = [
            models.CheckConstraint(
                condition=models.Q(id="completed", days__gte=1, days__lte=3650),
                name="ck_dewey_retention_policy",
            ),
        ]


class TaskRetentionAudit(models.Model):
    """Configuration changes only; no task payloads or deletion evidence lost to cascade."""

    id = models.CharField(max_length=36, primary_key=True, default=new_uuid, editable=False)
    occurred_at = models.DateTimeField(default=utcnow, db_index=True)
    revision = models.PositiveIntegerField()
    actor = models.CharField(max_length=200)
    reason = models.CharField(max_length=500)
    old_config = models.JSONField()
    new_config = models.JSONField()

    class Meta:
        db_table = "dewey_task_retention_audit"


class TaskRetentionRun(models.Model):
    """Independent execution receipt; running is not proof of job liveness."""

    id = models.CharField(max_length=36, primary_key=True, default=new_uuid, editable=False)
    started_at = models.DateTimeField(default=utcnow, db_index=True)
    finished_at = models.DateTimeField(null=True)
    revision = models.PositiveIntegerField(default=0)
    days = models.PositiveIntegerField(default=30)
    cutoff_at = models.DateTimeField(null=True)
    status = models.CharField(max_length=20, default="running")
    deleted_count = models.PositiveIntegerField(default=0)
    error_code = models.CharField(max_length=64, default="")

    class Meta:
        db_table = "dewey_task_retention_runs"


def resolve_db_alias(using: str | None = None) -> str:
    """The database alias Dewey operates on.

    An explicit ``using`` wins. Otherwise the project's database routers decide,
    exactly as they would for any ORM write, falling back to ``default``. Every
    Dewey transaction and ``SELECT FOR UPDATE`` must run on this one
    alias — ``transaction.atomic()`` does not consult routers, so resolving the
    alias up front is what keeps the lock and the write in the same
    transaction on multi-database projects.
    """
    return using or router.db_for_write(TaskEntry)
