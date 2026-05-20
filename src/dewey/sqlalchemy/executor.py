"""Task executor — the core processing loop for SQLAlchemy."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dewey.core.backoff import BackoffFn, default_task_backoff
from dewey.core.logging import (
    extract_trace_context,
    reset_trace_context,
    set_trace_context,
)
from dewey.core.states import TaskStatus, should_die
from dewey.sqlalchemy.listen import notify_work_available
from dewey.sqlalchemy.models import TaskEntryModel

logger = logging.getLogger(__name__)

# Type for task handler: receives (task_type, payload) → returns result dict or None
TaskHandler = Callable[[str, dict[str, Any]], Any]


def create_task(
    session: Session,
    *,
    task_type: str,
    payload: dict[str, Any] | None = None,
    queue: str = "default",
    priority: int = 0,
    max_attempts: int = 5,
    process_after: datetime | None = None,
    idempotency_key: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> TaskEntryModel:
    """
    Write a task to the ledger. This is step 1 — Postgres is the source of truth.

    After calling this, enqueue the task ID to your broker (Huey, Celery, etc.)
    using the appropriate adapter.

    Returns the created TaskEntryModel (with .id for enqueue).
    """
    task = TaskEntryModel(
        task_type=task_type,
        payload=payload or {},
        queue=queue,
        priority=priority,
        max_attempts=max_attempts,
        process_after=process_after,
        idempotency_key=idempotency_key,
        task_metadata=metadata or {},
    )
    session.add(task)
    session.flush()  # Get the ID without committing (caller controls transaction)

    notify_work_available(session, kind="task", entry_id=task.id, queue=queue)

    logger.info("Task created id=%s type=%s queue=%s", task.id, task_type, queue)
    return task


def process_task(
    session: Session,
    task_id: str,
    handler: TaskHandler,
    *,
    backoff: BackoffFn | None = None,
) -> bool:
    """
    Process a single task using two-phase commit.

    `backoff` is an optional ``(attempts: int) -> timedelta`` function that
    decides how long to wait before retrying a failed task. Defaults to
    :func:`dewey.core.backoff.default_task_backoff` (2 min base, 1 hr cap).
    Useful for fast-retry queues, custom strategies, or deterministic tests.

    Two-phase commit:

    Phase 1: PENDING → PROCESSING (committed — visible to sweep)
    Phase 2: Run handler
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
    task = session.execute(stmt).scalar_one_or_none()

    if task is None:
        logger.warning("Task not found id=%s", task_id)
        return False

    current_status = TaskStatus(task.status)

    # Already processed or dead — skip
    if current_status.is_terminal:
        logger.info("Task already terminal id=%s status=%s", task_id, task.status)
        return False

    # Only process PENDING tasks
    if current_status != TaskStatus.PENDING:
        logger.info("Task not pending id=%s status=%s", task_id, task.status)
        return False

    # Respect process_after scheduling
    if task.process_after and task.process_after > now:
        logger.info("Task not ready id=%s process_after=%s", task_id, task.process_after)
        return False

    # Validate state machine
    if not current_status.can_transition_to(TaskStatus.PROCESSING):
        logger.warning("Invalid transition id=%s from=%s to=PROCESSING", task_id, task.status)
        return False

    # Transition to PROCESSING
    task.status = TaskStatus.PROCESSING.value
    task.started_at = now
    task.attempts += 1

    # Cache values before commit (objects expire after commit)
    task_type = task.task_type
    payload = dict(task.payload)
    attempts = task.attempts
    max_attempts = task.max_attempts
    task_metadata = dict(task.task_metadata or {})

    session.commit()  # PROCESSING is now visible — sweep can find stuck tasks

    # Restore the trace context captured at task/notification creation
    # time so every log line through Phase 2 and Phase 3 is correlated
    # with the originating request.
    _trace_token = set_trace_context(extract_trace_context(task_metadata))
    try:
        # Phase 2: Execute handler
        try:
            handler(task_type, payload)
        except Exception as exc:
            # Phase 3a: Mark failed or dead-lettered
            error_msg = str(exc)

            stmt = select(TaskEntryModel).where(TaskEntryModel.id == task_id).with_for_update()
            task = session.execute(stmt).scalar_one_or_none()

            if task is None:
                logger.warning("Task disappeared during processing id=%s", task_id)
                return False

            # Task was killed mid-processing — respect the kill
            current = TaskStatus(task.status)
            if current != TaskStatus.PROCESSING:
                logger.info(
                    "Task status changed during processing id=%s status=%s, skipping update",
                    task_id,
                    task.status,
                )
                session.commit()
                return False

            task.error = error_msg
            failure_now = datetime.now(UTC)

            if should_die(attempts, max_attempts):
                task.status = TaskStatus.DEAD.value
                logger.error(
                    "Task dead-lettered id=%s type=%s attempts=%d error=%s",
                    task_id,
                    task_type,
                    attempts,
                    exc,
                )
            else:
                task.status = TaskStatus.FAILED.value
                task.process_after = failure_now + (backoff or default_task_backoff)(attempts)
                logger.warning(
                    "Task failed id=%s type=%s attempts=%d/%d error=%s",
                    task_id,
                    task_type,
                    attempts,
                    max_attempts,
                    exc,
                )

            session.commit()
            return False

        # Phase 3b: Mark completed
        stmt = select(TaskEntryModel).where(TaskEntryModel.id == task_id).with_for_update()
        task = session.execute(stmt).scalar_one_or_none()

        if task is None:
            logger.warning("Task disappeared during processing id=%s", task_id)
            return False

        # Task was killed mid-processing — respect the kill
        current = TaskStatus(task.status)
        if current != TaskStatus.PROCESSING:
            logger.info(
                "Task status changed during processing id=%s status=%s, skipping update",
                task_id,
                task.status,
            )
            session.commit()
            return False

        task.status = TaskStatus.COMPLETED.value
        task.completed_at = datetime.now(UTC)
        task.error = ""
        session.commit()

        logger.info("Task completed id=%s type=%s", task_id, task_type)
        return True
    finally:
        reset_trace_context(_trace_token)
