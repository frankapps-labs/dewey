"""Async task executor — mirrors executor.py for AsyncSession (FastAPI, etc.)."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dewey.core.backoff import BackoffFn
from dewey.core.execution import classify_failure, resolve_handler
from dewey.core.logging import (
    extract_trace_context,
    reset_trace_context,
    set_trace_context,
)
from dewey.core.states import TaskStatus
from dewey.policy import resolve_policy
from dewey.sqlalchemy.listen import notify_work_available_async
from dewey.sqlalchemy.models import TaskEntryModel

logger = logging.getLogger(__name__)

# States a worker may claim from. PENDING covers in-process execution with no
# broker in the path; DISPATCHING covers the normal dispatcher-driven flow.
_CLAIMABLE = (TaskStatus.PENDING, TaskStatus.DISPATCHING)

# Async handler: awaited as handler(*args, **kwargs) with the decoded task arguments.
AsyncTaskHandler = Callable[..., Awaitable[Any]]


async def create_task_async(
    session: AsyncSession,
    *,
    task_type: str,
    args: Sequence[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
    queue: str | None = None,
    priority: int | None = None,
    max_attempts: int | None = None,
    scheduled_for: datetime | None = None,
    idempotency_key: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> TaskEntryModel:
    """
    Write a task to the ledger. Async version of create_task().

    The dispatcher picks the row up from Postgres — producers never talk to a
    broker.

    Returns the created TaskEntryModel (with .id).
    """
    policy = resolve_policy(task_type)
    queue = policy.queue if queue is None else queue
    task = TaskEntryModel(
        task_type=task_type,
        args=list(args or []),
        kwargs=dict(kwargs or {}),
        queue=queue,
        priority=policy.priority if priority is None else priority,
        max_attempts=policy.max_attempts if max_attempts is None else max_attempts,
        scheduled_for=scheduled_for,
        idempotency_key=idempotency_key,
        task_metadata=metadata or {},
    )
    session.add(task)
    await session.flush()

    await notify_work_available_async(session, kind="task", entry_id=task.id, queue=queue)

    logger.info("Task created id=%s type=%s queue=%s", task.id, task_type, queue)
    return task


async def process_task_async(
    session: AsyncSession,
    task_id: str,
    handler: AsyncTaskHandler | None = None,
    *,
    backoff: BackoffFn | None = None,
) -> bool:
    """
    Process a single task using two-phase commit. Async version of process_task().

    `backoff` is an optional ``(attempts: int) -> timedelta`` function that
    decides how long to wait before retrying a failed task. Defaults to
    :func:`dewey.core.backoff.default_task_backoff` (2 min base, 1 hr cap).
    Useful for fast-retry queues, custom strategies, or deterministic tests.

    Phase 1: PENDING → PROCESSING (committed — visible to sweep)
    Phase 2: Run async handler
    Phase 3: PROCESSING → COMPLETED/FAILED/DEAD (committed)

    If the process dies during phase 2, the task stays PROCESSING and
    sweep_stuck will reset it to PENDING.

    Uses SELECT FOR UPDATE to prevent concurrent processing.
    Commits on the session at each phase — use a dedicated session.

    Returns True if the task was processed successfully.
    """
    now = datetime.now(UTC)

    # Phase 1: Claim the task
    stmt = select(TaskEntryModel).where(TaskEntryModel.id == task_id).with_for_update()
    result = await session.execute(stmt)
    task = result.scalar_one_or_none()

    if task is None:
        logger.warning("Task not found id=%s", task_id)
        return False

    current_status = TaskStatus(task.status)

    if current_status.is_terminal:
        logger.info("Task already terminal id=%s status=%s", task_id, task.status)
        return False

    # Claimable from PENDING (in-process execution) or DISPATCHING (a dispatcher
    # already handed this ID to the transport). Anything else means another worker
    # got there first, or an operator intervened — a duplicate delivery is a
    # logged no-op, never an error.
    if current_status not in _CLAIMABLE:
        logger.info("Task not claimable id=%s status=%s", task_id, task.status)
        return False

    if task.scheduled_for and task.scheduled_for > now:
        logger.info("Task not ready id=%s scheduled_for=%s", task_id, task.scheduled_for)
        return False

    if not current_status.can_transition_to(TaskStatus.PROCESSING):
        logger.warning("Invalid transition id=%s from=%s to=PROCESSING", task_id, task.status)
        return False

    task.status = TaskStatus.PROCESSING.value
    task.started_at = now
    task.dispatching_at = None
    task.attempts += 1

    # Cache values before commit (objects expire after commit)
    task_type = task.task_type
    task_args = list(task.args or [])
    task_kwargs = dict(task.kwargs or {})
    attempts = task.attempts
    max_attempts = task.max_attempts
    task_metadata = dict(task.task_metadata or {})
    policy = resolve_policy(task_type)

    await session.commit()  # PROCESSING is now visible — sweep can find stuck tasks

    # Restore the trace context captured at task/notification creation
    # time so every log line through Phase 2 and Phase 3 is correlated
    # with the originating request.
    _trace_token = set_trace_context(extract_trace_context(task_metadata))
    try:
        # Phase 2: Execute async handler
        try:
            await resolve_handler(task_type, handler, policy=policy)(*task_args, **task_kwargs)
        except Exception as exc:
            # Phase 3a: Mark failed or dead-lettered
            error_msg = str(exc)

            stmt = select(TaskEntryModel).where(TaskEntryModel.id == task_id).with_for_update()
            result = await session.execute(stmt)
            task = result.scalar_one_or_none()

            if task is None:
                logger.warning("Task disappeared during processing id=%s", task_id)
                return False

            current = TaskStatus(task.status)
            if current != TaskStatus.PROCESSING:
                logger.info(
                    "Task status changed during processing id=%s status=%s, skipping update",
                    task_id,
                    task.status,
                )
                await session.commit()
                return False

            task.error = error_msg
            outcome = classify_failure(
                exc,
                policy=policy,
                attempts=attempts,
                max_attempts=max_attempts,
                now=datetime.now(UTC),
                backoff=backoff,
            )
            task.status = outcome.status.value

            if outcome.is_dead:
                logger.error(
                    "Task dead-lettered id=%s type=%s attempts=%d reason=%s error=%s",
                    task_id,
                    task_type,
                    attempts,
                    outcome.reason,
                    exc,
                )
            else:
                task.scheduled_for = outcome.retry_at
                logger.warning(
                    "Task failed id=%s type=%s attempts=%d/%d retry_at=%s error=%s",
                    task_id,
                    task_type,
                    attempts,
                    max_attempts,
                    outcome.retry_at,
                    exc,
                )

            await session.commit()
            return False

        # Phase 3b: Mark completed
        stmt = select(TaskEntryModel).where(TaskEntryModel.id == task_id).with_for_update()
        result = await session.execute(stmt)
        task = result.scalar_one_or_none()

        if task is None:
            logger.warning("Task disappeared during processing id=%s", task_id)
            return False

        current = TaskStatus(task.status)
        if current != TaskStatus.PROCESSING:
            logger.info(
                "Task status changed during processing id=%s status=%s, skipping update",
                task_id,
                task.status,
            )
            await session.commit()
            return False

        task.status = TaskStatus.COMPLETED.value
        task.completed_at = datetime.now(UTC)
        task.error = ""
        await session.commit()

        logger.info("Task completed id=%s type=%s", task_id, task_type)
        return True
    finally:
        reset_trace_context(_trace_token)
