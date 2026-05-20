"""Sweep — catches tasks the broker dropped or workers left stuck."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.listen import notify_work_available
from dewey.sqlalchemy.models import TaskEntryModel

logger = logging.getLogger(__name__)

# Default: tasks stuck in PROCESSING for >10 minutes are considered abandoned
DEFAULT_STUCK_THRESHOLD_MINUTES = 10


def sweep_failed(
    session: Session,
    limit: int = 100,
) -> list[str]:
    """
    Find FAILED tasks ready for retry (process_after has passed).
    Resets them to PENDING so the broker can pick them up.

    Returns list of task IDs that were re-enqueued.
    """
    now = datetime.now(UTC)

    stmt = (
        select(TaskEntryModel.id)
        .where(
            TaskEntryModel.status == TaskStatus.FAILED.value,
            TaskEntryModel.process_after <= now,
        )
        .order_by(TaskEntryModel.process_after)
        .limit(limit)
    )
    task_ids = list(session.execute(stmt).scalars().all())

    if not task_ids:
        return []

    retry_rows = list(
        session.execute(
            update(TaskEntryModel)
            .where(
                TaskEntryModel.id.in_(task_ids),
                TaskEntryModel.status == TaskStatus.FAILED.value,
                TaskEntryModel.attempts < TaskEntryModel.max_attempts,
            )
            .values(status=TaskStatus.PENDING.value)
            .returning(TaskEntryModel.id, TaskEntryModel.queue)
        ).all()
    )
    dead_ids = list(
        session.execute(
            update(TaskEntryModel)
            .where(
                TaskEntryModel.id.in_(task_ids),
                TaskEntryModel.status == TaskStatus.FAILED.value,
                TaskEntryModel.attempts >= TaskEntryModel.max_attempts,
            )
            .values(status=TaskStatus.DEAD.value)
            .returning(TaskEntryModel.id)
        ).scalars()
    )
    session.flush()
    for task_id, queue in retry_rows:
        notify_work_available(session, kind="task", entry_id=task_id, queue=queue)

    if dead_ids:
        logger.warning("Sweep dead-lettered %d exhausted failed tasks", len(dead_ids))
    logger.info("Sweep re-enqueued %d failed tasks", len(retry_rows))
    return [task_id for task_id, _queue in retry_rows]


def sweep_stuck(
    session: Session,
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    limit: int = 100,
) -> list[str]:
    """
    Find tasks stuck in PROCESSING (worker died mid-task).
    Resets them to PENDING for re-processing.

    Returns list of task IDs that were unstuck.
    """
    threshold = datetime.now(UTC) - timedelta(minutes=stuck_threshold_minutes)

    stmt = (
        select(TaskEntryModel.id)
        .where(
            TaskEntryModel.status == TaskStatus.PROCESSING.value,
            TaskEntryModel.started_at < threshold,
        )
        .order_by(TaskEntryModel.started_at)
        .limit(limit)
    )
    task_ids = list(session.execute(stmt).scalars().all())

    if not task_ids:
        return []

    retry_rows = list(
        session.execute(
            update(TaskEntryModel)
            .where(
                TaskEntryModel.id.in_(task_ids),
                TaskEntryModel.status == TaskStatus.PROCESSING.value,
                TaskEntryModel.attempts < TaskEntryModel.max_attempts,
            )
            .values(status=TaskStatus.PENDING.value)
            .returning(TaskEntryModel.id, TaskEntryModel.queue)
        ).all()
    )
    dead_ids = list(
        session.execute(
            update(TaskEntryModel)
            .where(
                TaskEntryModel.id.in_(task_ids),
                TaskEntryModel.status == TaskStatus.PROCESSING.value,
                TaskEntryModel.attempts >= TaskEntryModel.max_attempts,
            )
            .values(status=TaskStatus.DEAD.value)
            .returning(TaskEntryModel.id)
        ).scalars()
    )
    session.flush()
    for task_id, queue in retry_rows:
        notify_work_available(session, kind="task", entry_id=task_id, queue=queue)

    if dead_ids:
        logger.warning("Sweep dead-lettered %d exhausted stuck tasks", len(dead_ids))
    logger.warning(
        "Sweep unstuck %d processing tasks (threshold=%dm)",
        len(retry_rows),
        stuck_threshold_minutes,
    )
    return [task_id for task_id, _queue in retry_rows]


def sweep(
    session: Session,
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    limit: int = 100,
) -> dict[str, list[str]]:
    """
    Run both sweeps. Returns dict with 'failed' and 'stuck' task ID lists.

    Call this from a periodic task (e.g. every 5 minutes).
    """
    return {
        "failed": sweep_failed(session, limit=limit),
        "stuck": sweep_stuck(session, stuck_threshold_minutes=stuck_threshold_minutes, limit=limit),
    }
