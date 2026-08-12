"""Async sweep — catches tasks the broker dropped or workers left stuck."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.listen import notify_work_available_async
from dewey.sqlalchemy.models import TaskEntryModel

logger = logging.getLogger(__name__)

DEFAULT_STUCK_THRESHOLD_MINUTES = 10
DEFAULT_DISPATCH_TIMEOUT_SECONDS = 300


async def sweep_failed_async(
    session: AsyncSession,
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
    result = await session.execute(stmt)
    task_ids = list(result.scalars().all())

    if not task_ids:
        return []

    retry_result = await session.execute(
        update(TaskEntryModel)
        .where(
            TaskEntryModel.id.in_(task_ids),
            TaskEntryModel.status == TaskStatus.FAILED.value,
            TaskEntryModel.attempts < TaskEntryModel.max_attempts,
        )
        .values(status=TaskStatus.PENDING.value)
        .returning(TaskEntryModel.id, TaskEntryModel.queue)
    )
    retry_rows = list(retry_result.all())
    dead_result = await session.execute(
        update(TaskEntryModel)
        .where(
            TaskEntryModel.id.in_(task_ids),
            TaskEntryModel.status == TaskStatus.FAILED.value,
            TaskEntryModel.attempts >= TaskEntryModel.max_attempts,
        )
        .values(status=TaskStatus.DEAD.value)
        .returning(TaskEntryModel.id)
    )
    dead_ids = list(dead_result.scalars())
    await session.flush()
    for task_id, queue in retry_rows:
        await notify_work_available_async(session, kind="task", entry_id=task_id, queue=queue)

    if dead_ids:
        logger.warning("Sweep dead-lettered %d exhausted failed tasks", len(dead_ids))
    logger.info("Sweep re-enqueued %d failed tasks", len(retry_rows))
    return [task_id for task_id, _queue in retry_rows]


async def sweep_expired_async(session: AsyncSession, limit: int = 100) -> list[str]:
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
    result = await session.execute(stmt)
    task_ids = list(result.scalars())
    if not task_ids:
        return []
    result = await session.execute(
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
    )
    expired_ids = list(result.scalars())
    await session.flush()
    if expired_ids:
        logger.info("Sweep expired %d task(s)", len(expired_ids))
    return expired_ids


async def sweep_stuck_async(
    session: AsyncSession,
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
    result = await session.execute(stmt)
    task_ids = list(result.scalars().all())

    if not task_ids:
        return []

    retry_result = await session.execute(
        update(TaskEntryModel)
        .where(
            TaskEntryModel.id.in_(task_ids),
            TaskEntryModel.status == TaskStatus.PROCESSING.value,
            TaskEntryModel.attempts < TaskEntryModel.max_attempts,
        )
        .values(status=TaskStatus.PENDING.value)
        .returning(TaskEntryModel.id, TaskEntryModel.queue)
    )
    retry_rows = list(retry_result.all())
    dead_result = await session.execute(
        update(TaskEntryModel)
        .where(
            TaskEntryModel.id.in_(task_ids),
            TaskEntryModel.status == TaskStatus.PROCESSING.value,
            TaskEntryModel.attempts >= TaskEntryModel.max_attempts,
        )
        .values(status=TaskStatus.DEAD.value)
        .returning(TaskEntryModel.id)
    )
    dead_ids = list(dead_result.scalars())
    await session.flush()
    for task_id, queue in retry_rows:
        await notify_work_available_async(session, kind="task", entry_id=task_id, queue=queue)

    if dead_ids:
        logger.warning("Sweep dead-lettered %d exhausted stuck tasks", len(dead_ids))
    logger.warning(
        "Sweep unstuck %d processing tasks (threshold=%dm)",
        len(retry_rows),
        stuck_threshold_minutes,
    )
    return [task_id for task_id, _queue in retry_rows]


async def sweep_dispatching_async(
    session: AsyncSession,
    dispatch_timeout_seconds: int = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
    limit: int = 100,
) -> list[str]:
    """
    Find tasks a dispatcher claimed but no worker ever picked up, and return them
    to PENDING. Async version of sweep_dispatching().
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
    result = await session.execute(stmt)
    task_ids = list(result.scalars().all())

    if not task_ids:
        return []

    retry_result = await session.execute(
        update(TaskEntryModel)
        .where(
            TaskEntryModel.id.in_(task_ids),
            TaskEntryModel.status == TaskStatus.DISPATCHING.value,
            TaskEntryModel.attempts < TaskEntryModel.max_attempts,
        )
        .values(status=TaskStatus.PENDING.value, dispatching_at=None)
        .returning(TaskEntryModel.id, TaskEntryModel.queue)
    )
    retry_rows = list(retry_result.all())
    dead_result = await session.execute(
        update(TaskEntryModel)
        .where(
            TaskEntryModel.id.in_(task_ids),
            TaskEntryModel.status == TaskStatus.DISPATCHING.value,
            TaskEntryModel.attempts >= TaskEntryModel.max_attempts,
        )
        .values(status=TaskStatus.DEAD.value, dispatching_at=None)
        .returning(TaskEntryModel.id)
    )
    dead_ids = list(dead_result.scalars())
    await session.flush()
    for task_id, queue in retry_rows:
        await notify_work_available_async(session, kind="task", entry_id=task_id, queue=queue)

    if dead_ids:
        logger.warning("Sweep dead-lettered %d exhausted dispatching tasks", len(dead_ids))
    if retry_rows:
        logger.warning(
            "Sweep reclaimed %d dispatching tasks (timeout=%ds)",
            len(retry_rows),
            dispatch_timeout_seconds,
        )
    return [task_id for task_id, _queue in retry_rows]


async def sweep_async(
    session: AsyncSession,
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    dispatch_timeout_seconds: int = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
    limit: int = 100,
) -> dict[str, list[str]]:
    """
    Run every recovery pass. Returns dict with 'failed', 'dispatching' and
    'stuck' task ID lists.
    """
    return {
        "expired": await sweep_expired_async(session, limit=limit),
        "failed": await sweep_failed_async(session, limit=limit),
        "dispatching": await sweep_dispatching_async(
            session,
            dispatch_timeout_seconds=dispatch_timeout_seconds,
            limit=limit,
        ),
        "stuck": await sweep_stuck_async(
            session,
            stuck_threshold_minutes=stuck_threshold_minutes,
            limit=limit,
        ),
    }
