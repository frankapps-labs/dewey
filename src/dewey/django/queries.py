"""Query & action API for Django — building blocks for dashboards, CLIs, and API endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from django.db import transaction
from django.db.models import Count

from dewey.core.states import TaskStatus
from dewey.core.types import TaskEntry as TaskEntryDC
from dewey.django.models import TaskEntry


def _to_list(qs) -> list[TaskEntryDC]:
    return [obj.to_dataclass() for obj in qs]


# --- Stats ---


def get_stats() -> dict[str, int]:
    """
    Counts by status — the health overview.

    Returns: {"pending": 12, "processing": 3, "completed": 4891, "failed": 2, "dead": 1}
    """
    rows = TaskEntry.objects.values("status").annotate(count=Count("id")).order_by()
    stats = {s.value: 0 for s in TaskStatus}
    for row in rows:
        stats[row["status"]] = row["count"]
    return stats


# --- List queries ---


def get_pending(
    limit: int = 50,
    task_type: str | None = None,
) -> list[TaskEntryDC]:
    """Tasks waiting to be picked up."""
    qs = TaskEntry.objects.filter(status=TaskStatus.PENDING.value)
    if task_type:
        qs = qs.filter(task_type=task_type)
    return _to_list(qs.order_by("created_at")[:limit])


def get_processing(limit: int = 50) -> list[TaskEntryDC]:
    """Tasks currently being processed."""
    qs = TaskEntry.objects.filter(
        status=TaskStatus.PROCESSING.value,
    ).order_by("started_at")
    return _to_list(qs[:limit])


def get_stuck(older_than_minutes: int = 10) -> list[TaskEntryDC]:
    """Tasks in PROCESSING too long — sweep candidates."""
    threshold = datetime.now(UTC) - timedelta(minutes=older_than_minutes)
    qs = TaskEntry.objects.filter(
        status=TaskStatus.PROCESSING.value,
        started_at__lt=threshold,
    ).order_by("started_at")
    return _to_list(qs)


def get_failed(
    limit: int = 50,
    task_type: str | None = None,
) -> list[TaskEntryDC]:
    """Failed tasks eligible for retry."""
    qs = TaskEntry.objects.filter(status=TaskStatus.FAILED.value)
    if task_type:
        qs = qs.filter(task_type=task_type)
    return _to_list(qs.order_by("-created_at")[:limit])


def get_dead(
    limit: int = 50,
    task_type: str | None = None,
) -> list[TaskEntryDC]:
    """Dead-lettered tasks — terminal, needs human decision."""
    qs = TaskEntry.objects.filter(status=TaskStatus.DEAD.value)
    if task_type:
        qs = qs.filter(task_type=task_type)
    return _to_list(qs.order_by("-created_at")[:limit])


def get_task(task_id: str) -> TaskEntryDC | None:
    """Single task by ID — for detail views."""
    try:
        return TaskEntry.objects.get(id=task_id).to_dataclass()
    except TaskEntry.DoesNotExist:
        return None


def get_recent(
    limit: int = 50,
    task_type: str | None = None,
    status: TaskStatus | None = None,
    since: datetime | None = None,
) -> list[TaskEntryDC]:
    """Recent tasks with optional filters — for list views."""
    qs = TaskEntry.objects.all()
    if task_type:
        qs = qs.filter(task_type=task_type)
    if status:
        qs = qs.filter(status=status.value)
    if since:
        qs = qs.filter(created_at__gte=since)
    return _to_list(qs.order_by("-created_at")[:limit])


# --- Actions ---


@transaction.atomic
def retry_task(task_id: str) -> TaskEntryDC | None:
    """Reset a failed/dead task back to pending for re-processing."""
    try:
        task = TaskEntry.objects.select_for_update().get(id=task_id)
    except TaskEntry.DoesNotExist:
        return None

    if not TaskStatus(task.status).can_transition_to(TaskStatus.PENDING):
        return task.to_dataclass()

    task.status = TaskStatus.PENDING.value
    task.process_after = None
    task.error = ""
    task.attempts = 0
    task.save(update_fields=["status", "process_after", "error", "attempts", "updated_at"])
    return task.to_dataclass()


def bulk_retry(
    task_type: str | None = None,
    status: TaskStatus = TaskStatus.FAILED,
) -> int:
    """
    Retry all failed (or dead) tasks, optionally filtered by type.
    Returns count of tasks re-enqueued.

    Raises ValueError if the source status doesn't allow transition to PENDING.
    """
    if not status.can_transition_to(TaskStatus.PENDING):
        raise ValueError(
            f"Cannot retry tasks in {status.value!r} state — "
            f"no transition {status.value} → pending is allowed."
        )
    qs = TaskEntry.objects.filter(status=status.value)
    if task_type:
        qs = qs.filter(task_type=task_type)
    return qs.update(
        status=TaskStatus.PENDING.value,
        process_after=None,
        error="",
        attempts=0,
    )


@transaction.atomic
def kill_task(task_id: str) -> TaskEntryDC | None:
    """Force a task to DEAD — stop retrying."""
    try:
        task = TaskEntry.objects.select_for_update().get(id=task_id)
    except TaskEntry.DoesNotExist:
        return None

    if not TaskStatus(task.status).can_transition_to(TaskStatus.DEAD):
        return task.to_dataclass()

    task.status = TaskStatus.DEAD.value
    task.save(update_fields=["status", "updated_at"])
    return task.to_dataclass()


def purge_completed(
    older_than_days: int = 30,
    task_type: str | None = None,
) -> int:
    """
    Delete completed tasks older than N days.
    Returns count of rows deleted.
    """
    threshold = datetime.now(UTC) - timedelta(days=older_than_days)
    qs = TaskEntry.objects.filter(
        status=TaskStatus.COMPLETED.value,
        completed_at__lt=threshold,
    )
    if task_type:
        qs = qs.filter(task_type=task_type)
    count, _ = qs.delete()
    return count
