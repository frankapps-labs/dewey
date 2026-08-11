"""Task executor — the core processing loop for SQLAlchemy."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dewey.core.backoff import BackoffFn
from dewey.core.execution import classify_failure, resolve_handler
from dewey.core.logging import (
    extract_trace_context,
    reset_trace_context,
    set_trace_context,
)
from dewey.core.states import TaskStatus
from dewey.policy import resolve_policy
from dewey.serialization import encode_args, encode_kwargs
from dewey.sqlalchemy.listen import notify_work_available
from dewey.sqlalchemy.models import TaskEntryModel

logger = logging.getLogger(__name__)

# States a worker may claim from. PENDING covers in-process execution with no
# broker in the path; DISPATCHING covers the normal dispatcher-driven flow.
_CLAIMABLE = (TaskStatus.PENDING, TaskStatus.DISPATCHING)

# Task handler: invoked as handler(*args, **kwargs) with the decoded task arguments.
TaskHandler = Callable[..., Any]


def create_task(
    session: Session,
    *,
    task_type: str,
    args: Sequence[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
    queue: str | None = None,
    priority: int | None = None,
    max_attempts: int | None = None,
    scheduled_for: datetime | None = None,
    expires_at: datetime | None = None,
    idempotency_key: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> TaskEntryModel:
    """
    Write a task to the ledger. This is step 1 — Postgres is the source of truth.

    The dispatcher picks the row up from Postgres — producers never talk to a
    broker, and never import the handler.

    ``queue``, ``priority`` and ``max_attempts`` default to the resolved policy
    for ``task_type``. The attempt budget is stamped onto the row, so a task
    keeps the budget it was created with even if the policy changes later.

    Returns the created TaskEntryModel (with .id).
    """
    if expires_at is not None and expires_at.utcoffset() is None:
        raise ValueError("expires_at must be a timezone-aware datetime")
    policy = resolve_policy(task_type)
    queue = policy.queue if queue is None else queue
    task = TaskEntryModel(
        task_type=task_type,
        args=encode_args(args),
        kwargs=encode_kwargs(kwargs),
        queue=queue,
        priority=policy.priority if priority is None else priority,
        max_attempts=policy.max_attempts if max_attempts is None else max_attempts,
        scheduled_for=scheduled_for,
        initial_scheduled_for=scheduled_for,
        expires_at=expires_at,
        idempotency_key=idempotency_key,
        task_metadata=metadata or {},
    )
    session.add(task)
    session.flush()  # Get the ID without committing (caller controls transaction)

    notify_work_available(session, kind="task", entry_id=task.id, queue=queue)

    logger.info("Task created id=%s type=%s queue=%s", task.id, task_type, queue)
    return task


def create_or_get_task(
    session: Session,
    *,
    task_type: str,
    idempotency_key: str,
    args: Sequence[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
    queue: str | None = None,
    priority: int | None = None,
    max_attempts: int | None = None,
    scheduled_for: datetime | None = None,
    expires_at: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> TaskEntryModel:
    """Create once, or return the identical existing task without aborting the caller."""
    from dewey.errors import IdempotencyConflictError

    if not idempotency_key:
        raise ValueError("create_or_get_task requires a non-empty idempotency_key")
    if scheduled_for is not None and scheduled_for.utcoffset() is None:
        raise ValueError("scheduled_for must be a timezone-aware datetime")
    if expires_at is not None and expires_at.utcoffset() is None:
        raise ValueError("expires_at must be a timezone-aware datetime")
    policy = resolve_policy(task_type)
    resolved_queue = policy.queue if queue is None else queue
    resolved_priority = policy.priority if priority is None else priority
    resolved_max_attempts = policy.max_attempts if max_attempts is None else max_attempts
    expected = {
        "task_type": task_type,
        "args": encode_args(args),
        "kwargs": encode_kwargs(kwargs),
        "queue": resolved_queue,
        "priority": resolved_priority,
        "max_attempts": resolved_max_attempts,
        "scheduled_for": scheduled_for,
        "expires_at": expires_at,
    }
    try:
        with session.begin_nested():
            return create_task(
                session,
                task_type=task_type,
                args=args,
                kwargs=kwargs,
                queue=resolved_queue,
                priority=resolved_priority,
                max_attempts=resolved_max_attempts,
                scheduled_for=scheduled_for,
                expires_at=expires_at,
                idempotency_key=idempotency_key,
                metadata=metadata,
            )
    except IntegrityError as exc:
        existing = session.execute(
            select(TaskEntryModel).where(
                TaskEntryModel.task_type == task_type,
                TaskEntryModel.idempotency_key == idempotency_key,
            )
        ).scalar_one_or_none()
        if existing is None:
            raise exc
        actual = {
            "task_type": existing.task_type,
            "args": existing.args,
            "kwargs": existing.kwargs,
            "queue": existing.queue,
            "priority": existing.priority,
            "max_attempts": existing.max_attempts,
            "scheduled_for": existing.initial_scheduled_for,
            "expires_at": existing.expires_at,
        }
        differing = tuple(name for name, value in expected.items() if actual[name] != value)
        if differing:
            raise IdempotencyConflictError(differing) from exc
        return existing


def process_task(
    session: Session,
    task_id: str,
    handler: TaskHandler | None = None,
    *,
    backoff: BackoffFn | None = None,
) -> bool:
    """
    Process a single task using two-phase commit.

    ``handler`` is optional: when omitted, the handler registered for the task
    type with ``@dewey.task`` is used. Passing one explicitly overrides the
    registry — the escape hatch for tests and in-process use.

    Retry, dead-letter and backoff behaviour all come from the resolved policy.
    ``backoff`` overrides the policy's backoff for this call, which is mostly
    useful for deterministic tests.

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
    # Phase 1: Claim the task
    stmt = select(TaskEntryModel).where(TaskEntryModel.id == task_id).with_for_update()
    task = session.execute(stmt).scalar_one_or_none()

    if task is None:
        logger.warning("Task not found id=%s", task_id)
        return False

    # SELECT FOR UPDATE may have waited past the deadline. Observe time only after
    # the row is ours so expiry, scheduling, and started_at share a fresh instant.
    now = datetime.now(UTC)
    current_status = TaskStatus(task.status)

    # Already processed or dead — skip
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

    if task.expires_at is not None and task.expires_at <= now:
        task.status = TaskStatus.EXPIRED.value
        task.expired_at = now
        task.dispatching_at = None
        session.commit()
        logger.info("Task expired before handler invocation id=%s", task_id)
        return False

    # Respect scheduled_for scheduling
    if task.scheduled_for and task.scheduled_for > now:
        logger.info("Task not ready id=%s scheduled_for=%s", task_id, task.scheduled_for)
        return False

    # Validate state machine
    if not current_status.can_transition_to(TaskStatus.PROCESSING):
        logger.warning("Invalid transition id=%s from=%s to=PROCESSING", task_id, task.status)
        return False

    # Transition to PROCESSING
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

    session.commit()  # PROCESSING is now visible — sweep can find stuck tasks

    # Restore the trace context captured at task creation
    # time so every log line through Phase 2 and Phase 3 is correlated
    # with the originating request.
    _trace_token = set_trace_context(extract_trace_context(task_metadata))
    try:
        # Phase 2: Execute handler
        try:
            resolve_handler(task_type, handler, policy=policy)(*task_args, **task_kwargs)
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
