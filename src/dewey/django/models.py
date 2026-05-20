"""Django model for the task ledger — mirrors the SQLAlchemy TaskEntryModel."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from django.db import models

from dewey.core.states import TaskStatus

if TYPE_CHECKING:
    from dewey.core.types import TaskEntry as TaskEntryDC


class TaskEntry(models.Model):
    """
    Postgres-backed task ledger row.

    Every task gets written here before being enqueued to the broker.
    Postgres is the source of truth; the broker is the fast path.
    """

    class Status(models.TextChoices):
        PENDING = TaskStatus.PENDING.value, "Pending"
        PROCESSING = TaskStatus.PROCESSING.value, "Processing"
        COMPLETED = TaskStatus.COMPLETED.value, "Completed"
        FAILED = TaskStatus.FAILED.value, "Failed"
        DEAD = TaskStatus.DEAD.value, "Dead"

    id = models.CharField(
        max_length=36, primary_key=True, default=lambda: str(uuid.uuid4()), editable=False
    )
    task_type = models.CharField(max_length=100, db_index=True)
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )

    # Payload — JSONField uses Postgres JSONB
    payload = models.JSONField(default=dict, blank=True)
    metadata = models.JSONField(default=dict, blank=True)

    # Queue routing
    queue = models.CharField(max_length=50, default="default")
    priority = models.IntegerField(default=0)

    # Retry tracking
    attempts = models.IntegerField(default=0)
    max_attempts = models.IntegerField(default=5)
    error = models.TextField(default="", blank=True)

    # Timestamps
    created_at = models.DateTimeField(default=lambda: datetime.now(UTC), db_index=True)
    updated_at = models.DateTimeField(auto_now=True)
    process_after = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

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
                fields=["process_after"],
                name="ix_task_entries_pending_pa",
                condition=models.Q(status="pending"),
            ),
            # Partial index: stuck processing tasks
            models.Index(
                fields=["started_at"],
                name="ix_task_entries_processing_sa",
                condition=models.Q(status="processing"),
            ),
            # Partial index: failed tasks eligible for retry
            models.Index(
                fields=["process_after"],
                name="ix_task_entries_failed_pa",
                condition=models.Q(status="failed"),
            ),
            # Composite: recent tasks by type
            models.Index(
                fields=["task_type", "created_at"],
                name="ix_task_entries_type_created",
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
            payload=self.payload,
            queue=self.queue,
            priority=self.priority,
            attempts=self.attempts,
            max_attempts=self.max_attempts,
            error=self.error,
            created_at=self.created_at,
            updated_at=self.updated_at,
            process_after=self.process_after,
            started_at=self.started_at,
            completed_at=self.completed_at,
            idempotency_key=self.idempotency_key,
            metadata=self.metadata,
        )
