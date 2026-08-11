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

# Default: a row claimed for dispatch but not started within 5 minutes is assumed
# lost. Must stay above the worst-case broker backlog wait for your deployment.
DEFAULT_DISPATCH_TIMEOUT_SECONDS = 300


def sweep_failed(
    session: Session,
    limit: int = 100,
) -> list[str]:
    """
    Find FAILED tasks ready for retry (scheduled_for has passed).
    Resets them to PENDING so the broker can pick them up.

    Returns list of task IDs that were re-enqueued.
    """
    now = datetime.now(UTC)

    stmt = (
        select(TaskEntryModel.id)
        .where(
            TaskEntryModel.status == TaskStatus.FAILED.value,
            TaskEntryModel.scheduled_for <= now,
        )
        .order_by(TaskEntryModel.scheduled_for)
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


def sweep_expired(session: Session, limit: int = 100) -> list[str]:
    """Terminalize bounded deadline candidates; never touch active PROCESSING rows."""
    now = datetime.now(UTC)
    stmt = (
        select(TaskEntryModel.id)
        .where(
            TaskEntryModel.status.in_(
                [
                    TaskStatus.PENDING.value,
                    TaskStatus.FAILED.value,
                    TaskStatus.DISPATCHING.value,
                ]
            ),
            TaskEntryModel.expires_at.is_not(None),
            TaskEntryModel.expires_at <= now,
        )
        .order_by(TaskEntryModel.expires_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    task_ids = list(session.execute(stmt).scalars())
    if not task_ids:
        return []
    expired_ids = list(
        session.execute(
            update(TaskEntryModel)
            .where(
                TaskEntryModel.id.in_(task_ids),
                TaskEntryModel.status.in_(
                    [
                        TaskStatus.PENDING.value,
                        TaskStatus.FAILED.value,
                        TaskStatus.DISPATCHING.value,
                    ]
                ),
            )
            .values(
                status=TaskStatus.EXPIRED.value,
                expired_at=now,
                dispatching_at=None,
            )
            .returning(TaskEntryModel.id)
        ).scalars()
    )
    session.flush()
    if expired_ids:
        logger.info("Sweep expired %d task(s)", len(expired_ids))
    return expired_ids


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


def sweep_dispatching(
    session: Session,
    dispatch_timeout_seconds: int = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
    limit: int = 100,
) -> list[str]:
    """
    Find tasks a dispatcher claimed but no worker ever picked up, and return them
    to PENDING so a dispatcher can hand them out again.

    This is the backstop for a dispatcher that died between committing the claim
    and reaching the transport. A synchronous transport failure does not need it:
    the dispatcher resets the row itself, immediately.

    ``dispatch_timeout_seconds`` must exceed the worst-case time a task can wait
    in the broker before a worker starts it, or healthy backlogs get reclaimed
    and dispatched twice.

    Returns list of task IDs that were reclaimed.
    """
    threshold = datetime.now(UTC) - timedelta(seconds=dispatch_timeout_seconds)

    stmt = (
        select(TaskEntryModel.id)
        .where(
            TaskEntryModel.status == TaskStatus.DISPATCHING.value,
            TaskEntryModel.dispatching_at < threshold,
        )
        .order_by(TaskEntryModel.dispatching_at)
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
                TaskEntryModel.status == TaskStatus.DISPATCHING.value,
                TaskEntryModel.attempts < TaskEntryModel.max_attempts,
            )
            .values(status=TaskStatus.PENDING.value, dispatching_at=None)
            .returning(TaskEntryModel.id, TaskEntryModel.queue)
        ).all()
    )
    dead_ids = list(
        session.execute(
            update(TaskEntryModel)
            .where(
                TaskEntryModel.id.in_(task_ids),
                TaskEntryModel.status == TaskStatus.DISPATCHING.value,
                TaskEntryModel.attempts >= TaskEntryModel.max_attempts,
            )
            .values(status=TaskStatus.DEAD.value, dispatching_at=None)
            .returning(TaskEntryModel.id)
        ).scalars()
    )
    session.flush()
    for task_id, queue in retry_rows:
        notify_work_available(session, kind="task", entry_id=task_id, queue=queue)

    if dead_ids:
        logger.warning("Sweep dead-lettered %d exhausted dispatching tasks", len(dead_ids))
    if retry_rows:
        logger.warning(
            "Sweep reclaimed %d dispatching tasks (timeout=%ds)",
            len(retry_rows),
            dispatch_timeout_seconds,
        )
    return [task_id for task_id, _queue in retry_rows]


def sweep(
    session: Session,
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    dispatch_timeout_seconds: int = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
    limit: int = 100,
) -> dict[str, list[str]]:
    """
    Run every recovery pass. Returns dict with 'failed', 'dispatching' and
    'stuck' task ID lists.

    The dispatcher calls this on its own interval. It is also safe to call from
    cron or a management command — and worth knowing that without something
    calling it, failed tasks never become eligible for retry.
    """
    return {
        "expired": sweep_expired(session, limit=limit),
        "failed": sweep_failed(session, limit=limit),
        "dispatching": sweep_dispatching(
            session, dispatch_timeout_seconds=dispatch_timeout_seconds, limit=limit
        ),
        "stuck": sweep_stuck(session, stuck_threshold_minutes=stuck_threshold_minutes, limit=limit),
    }
