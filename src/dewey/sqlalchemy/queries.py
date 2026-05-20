"""Query & action API — building blocks for dashboards, CLIs, and API endpoints."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session

from dewey.core.states import TaskStatus
from dewey.core.types import TaskEntry
from dewey.sqlalchemy.listen import notify_work_available
from dewey.sqlalchemy.models import TaskEntryModel


def _to_dataclass(row: TaskEntryModel) -> TaskEntry:
    """Convert ORM model to pure Python dataclass."""
    return TaskEntry(
        id=row.id,
        task_type=row.task_type,
        status=TaskStatus(row.status),
        payload=row.payload,
        queue=row.queue,
        priority=row.priority,
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        error=row.error,
        created_at=row.created_at,
        updated_at=row.updated_at,
        process_after=row.process_after,
        started_at=row.started_at,
        completed_at=row.completed_at,
        idempotency_key=row.idempotency_key,
        metadata=row.task_metadata,
    )


def _to_list(rows: Sequence[TaskEntryModel]) -> list[TaskEntry]:
    return [_to_dataclass(r) for r in rows]


# --- Stats ---


def get_stats(session: Session) -> dict[str, int]:
    """
    Counts by status — the health overview.

    Returns: {"pending": 12, "processing": 3, "completed": 4891, "failed": 2, "dead": 1}
    """
    stmt = select(TaskEntryModel.status, func.count()).group_by(TaskEntryModel.status)
    results = session.execute(stmt).all()
    # Ensure all statuses are represented
    stats = {s.value: 0 for s in TaskStatus}
    for status, count in results:
        stats[status] = count
    return stats


# --- List queries ---


def get_pending(
    session: Session,
    limit: int = 50,
    task_type: str | None = None,
) -> list[TaskEntry]:
    """Tasks waiting to be picked up."""
    stmt = select(TaskEntryModel).where(TaskEntryModel.status == TaskStatus.PENDING.value)
    if task_type:
        stmt = stmt.where(TaskEntryModel.task_type == task_type)
    stmt = stmt.order_by(TaskEntryModel.created_at).limit(limit)
    return _to_list(session.execute(stmt).scalars().all())


def get_processing(session: Session, limit: int = 50) -> list[TaskEntry]:
    """Tasks currently being processed."""
    stmt = (
        select(TaskEntryModel)
        .where(TaskEntryModel.status == TaskStatus.PROCESSING.value)
        .order_by(TaskEntryModel.started_at)
        .limit(limit)
    )
    return _to_list(session.execute(stmt).scalars().all())


def get_stuck(session: Session, older_than_minutes: int = 10) -> list[TaskEntry]:
    """Tasks in PROCESSING too long — sweep candidates."""
    from datetime import timedelta

    threshold = datetime.now(UTC) - timedelta(minutes=older_than_minutes)
    stmt = (
        select(TaskEntryModel)
        .where(
            TaskEntryModel.status == TaskStatus.PROCESSING.value,
            TaskEntryModel.started_at < threshold,
        )
        .order_by(TaskEntryModel.started_at)
    )
    return _to_list(session.execute(stmt).scalars().all())


def get_failed(
    session: Session,
    limit: int = 50,
    task_type: str | None = None,
) -> list[TaskEntry]:
    """Failed tasks eligible for retry."""
    stmt = select(TaskEntryModel).where(TaskEntryModel.status == TaskStatus.FAILED.value)
    if task_type:
        stmt = stmt.where(TaskEntryModel.task_type == task_type)
    stmt = stmt.order_by(TaskEntryModel.created_at.desc()).limit(limit)
    return _to_list(session.execute(stmt).scalars().all())


def get_dead(
    session: Session,
    limit: int = 50,
    task_type: str | None = None,
) -> list[TaskEntry]:
    """Dead-lettered tasks — terminal, needs human decision."""
    stmt = select(TaskEntryModel).where(TaskEntryModel.status == TaskStatus.DEAD.value)
    if task_type:
        stmt = stmt.where(TaskEntryModel.task_type == task_type)
    stmt = stmt.order_by(TaskEntryModel.created_at.desc()).limit(limit)
    return _to_list(session.execute(stmt).scalars().all())


def get_task(session: Session, task_id: str) -> TaskEntry | None:
    """Single task by ID — for detail views."""
    stmt = select(TaskEntryModel).where(TaskEntryModel.id == task_id)
    row = session.execute(stmt).scalar_one_or_none()
    return _to_dataclass(row) if row else None


def get_recent(
    session: Session,
    limit: int = 50,
    task_type: str | None = None,
    status: TaskStatus | None = None,
    since: datetime | None = None,
) -> list[TaskEntry]:
    """Recent tasks with optional filters — for list views."""
    stmt = select(TaskEntryModel)
    if task_type:
        stmt = stmt.where(TaskEntryModel.task_type == task_type)
    if status:
        stmt = stmt.where(TaskEntryModel.status == status.value)
    if since:
        stmt = stmt.where(TaskEntryModel.created_at >= since)
    stmt = stmt.order_by(TaskEntryModel.created_at.desc()).limit(limit)
    return _to_list(session.execute(stmt).scalars().all())


# --- Actions ---


def retry_task(session: Session, task_id: str) -> TaskEntry | None:
    """Reset a failed/dead task back to pending for re-processing."""
    stmt = select(TaskEntryModel).where(TaskEntryModel.id == task_id).with_for_update()
    task = session.execute(stmt).scalar_one_or_none()
    if task is None:
        return None

    status = TaskStatus(task.status)
    if not status.can_transition_to(TaskStatus.PENDING):
        return _to_dataclass(task)

    task.status = TaskStatus.PENDING.value
    task.process_after = None
    task.error = ""
    task.attempts = 0
    session.flush()
    notify_work_available(session, kind="task", entry_id=task.id, queue=task.queue)
    return _to_dataclass(task)


def bulk_retry(
    session: Session,
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
    ids_stmt = select(TaskEntryModel.id, TaskEntryModel.queue).where(
        TaskEntryModel.status == status.value
    )
    if task_type:
        ids_stmt = ids_stmt.where(TaskEntryModel.task_type == task_type)
    rows = list(session.execute(ids_stmt).all())

    stmt = update(TaskEntryModel).where(TaskEntryModel.status == status.value)
    if task_type:
        stmt = stmt.where(TaskEntryModel.task_type == task_type)
    stmt = stmt.values(
        status=TaskStatus.PENDING.value,
        process_after=None,
        error="",
        attempts=0,
    )
    result = session.execute(stmt)
    session.flush()
    for task_id, queue in rows:
        notify_work_available(session, kind="task", entry_id=task_id, queue=queue)
    return result.rowcount  # type: ignore[return-value]


def kill_task(session: Session, task_id: str) -> TaskEntry | None:
    """Force a task to DEAD — stop retrying."""
    stmt = select(TaskEntryModel).where(TaskEntryModel.id == task_id).with_for_update()
    task = session.execute(stmt).scalar_one_or_none()
    if task is None:
        return None

    status = TaskStatus(task.status)
    if not status.can_transition_to(TaskStatus.DEAD):
        return _to_dataclass(task)

    task.status = TaskStatus.DEAD.value
    session.flush()
    return _to_dataclass(task)


def purge_completed(
    session: Session,
    older_than_days: int = 30,
    task_type: str | None = None,
) -> int:
    """
    Delete completed tasks older than N days.
    Returns count of rows deleted.
    """
    from datetime import timedelta

    threshold = datetime.now(UTC) - timedelta(days=older_than_days)
    stmt = delete(TaskEntryModel).where(
        TaskEntryModel.status == TaskStatus.COMPLETED.value,
        TaskEntryModel.completed_at < threshold,
    )
    if task_type:
        stmt = stmt.where(TaskEntryModel.task_type == task_type)
    result = session.execute(stmt)
    session.flush()
    return result.rowcount  # type: ignore[return-value]
