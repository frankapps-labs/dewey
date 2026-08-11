"""Query & action API for Django — building blocks for dashboards, CLIs, and API endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from django.db import transaction
from django.db.models import Count

from dewey.core.heartbeat import DEFAULT_HEARTBEAT_STALE_SECONDS
from dewey.core.heartbeat import DispatcherHeartbeat as HeartbeatDC
from dewey.core.states import TaskStatus
from dewey.core.types import TaskEntry as TaskEntryDC
from dewey.django.models import DispatcherHeartbeat, TaskEntry, resolve_db_alias


def _to_list(qs) -> list[TaskEntryDC]:
    return [obj.to_dataclass() for obj in qs]


def get_dispatchers(
    *,
    using: str = "default",
    database: str | None = None,
    queues: list[str] | tuple[str, ...] | None = None,
    fresh_within_seconds: float = DEFAULT_HEARTBEAT_STALE_SECONDS,
    now: datetime | None = None,
) -> list[HeartbeatDC]:
    """Fresh dispatchers matching the requested database identity and queues."""
    observed_at = now or datetime.now(UTC)
    qs = DispatcherHeartbeat.objects.using(using).filter(
        last_seen_at__gte=observed_at - timedelta(seconds=fresh_within_seconds)
    )
    if database is not None:
        qs = qs.filter(database=database)
    requested = tuple(queues) if queues else None
    result = []
    for row in qs.order_by("-last_seen_at"):
        heartbeat = HeartbeatDC(
            instance_id=row.instance_id,
            dewey_version=row.dewey_version,
            backend=row.backend,
            database=row.database,
            queues=tuple(row.queues) if row.queues is not None else None,
            started_at=row.started_at,
            last_seen_at=row.last_seen_at,
        )
        if heartbeat.serves(requested):
            result.append(heartbeat)
    return result


# --- Stats ---


def get_stats() -> dict[str, int]:
    """
    Counts by status — the health overview.

    Returns one count per status, including zeros:
    ``{"pending": 12, "dispatching": 1, "processing": 3, "completed": 4891,
    "failed": 2, "dead": 1}``
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


def get_dispatching(limit: int = 50) -> list[TaskEntryDC]:
    """Tasks claimed for dispatch but not yet started by a worker."""
    qs = TaskEntry.objects.filter(
        status=TaskStatus.DISPATCHING.value,
    ).order_by("dispatching_at")
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


def get_expired(
    limit: int = 50,
    task_type: str | None = None,
) -> list[TaskEntryDC]:
    """Tasks that reached their start deadline without running."""
    qs = TaskEntry.objects.filter(status=TaskStatus.EXPIRED.value)
    if task_type:
        qs = qs.filter(task_type=task_type)
    return _to_list(qs.order_by("-expired_at")[:limit])


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


def retry_task(task_id: str, using: str | None = None) -> TaskEntryDC | None:
    """Reset a failed/dead task back to pending for re-processing.

    ``using`` pins the action to a database alias; when omitted, the project's
    routers decide.
    """
    alias = resolve_db_alias(using)
    with transaction.atomic(using=alias):
        try:
            task = TaskEntry.objects.using(alias).select_for_update().get(id=task_id)
        except TaskEntry.DoesNotExist:
            return None

        status = TaskStatus(task.status)
        if status == TaskStatus.EXPIRED:
            return None
        if not status.can_transition_to(TaskStatus.PENDING):
            return task.to_dataclass()

        task.status = TaskStatus.PENDING.value
        task.scheduled_for = None
        task.error = ""
        task.attempts = 0
        task.save(update_fields=["status", "scheduled_for", "error", "attempts", "updated_at"])
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
        scheduled_for=None,
        error="",
        attempts=0,
    )


def kill_task(task_id: str, using: str | None = None) -> TaskEntryDC | None:
    """Force a task to DEAD — stop retrying.

    ``using`` pins the action to a database alias; when omitted, the project's
    routers decide.
    """
    alias = resolve_db_alias(using)
    with transaction.atomic(using=alias):
        try:
            task = TaskEntry.objects.using(alias).select_for_update().get(id=task_id)
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
