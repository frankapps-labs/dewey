"""Shared native state decisions applied only while a storage row lock is held.

No ORM imports. Adapters own transactions and obtain fresh database time after
locking. These helpers never commit; their dictionaries are atomic write sets.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from dewey._worker.contracts import Reconciliation, Timing
from dewey._worker.pool import Assignment
from dewey.core.execution import classify_failure, log_outcome
from dewey.core.states import TaskStatus
from dewey.core.validation import require_execution_timeout
from dewey.policy import resolve_policy

OWNERSHIP_FIELDS = (
    "dispatch_token",
    "worker_id",
    "lease_expires_at",
    "execution_deadline_at",
)
CLEAR_NATIVE: dict[str, Any] = dict.fromkeys(OWNERSHIP_FIELDS)
QUARANTINE_SCAN_LIMIT = 100
logger = logging.getLogger(__name__)
worker_log = logging.getLogger("dewey.worker")


def tagged(row: Any) -> bool:
    return any(getattr(row, field) is not None for field in OWNERSHIP_FIELDS)


def clear_native(row: Any) -> None:
    for field in OWNERSHIP_FIELDS:
        setattr(row, field, None)


def validate(row: Any) -> None:
    if not tagged(row):
        return
    if any(getattr(row, field) is None for field in OWNERSHIP_FIELDS):
        raise RuntimeError("Incomplete native ownership tuple")
    if row.status not in (TaskStatus.DISPATCHING.value, TaskStatus.PROCESSING.value):
        raise RuntimeError("Native ownership on an inactive task")
    if row.status == TaskStatus.PROCESSING.value and row.execution_token is None:
        raise RuntimeError("Native processing task has no execution token")


def report_invalid(row: Any) -> None:
    """Diagnostic quarantine only: never clear ownership or imply process stop."""
    try:
        validate(row)
    except RuntimeError as exc:
        logger.warning("Native ownership quarantined: task_id=%s reason=%s", row.id, exc)


def outcome_log(row: Any, failure: BaseException | None = None) -> Callable[[], None] | None:
    """The log record for a row a settlement or recovery just left failed or dead.

    Returns a callable for the storage's outbox, to run once the transaction
    has committed: a change the database budget rolls back is logged by the
    retry that commits it, not twice, and the log handler's own time stays
    outside the budget. The values are captured now, since the row may be
    expired after the commit. A task process has the handler's exception; the
    worker parent recovering a lost execution has only the row. An operator's
    kill is their own decision and is not logged as a failure.
    """
    if row.status not in (TaskStatus.FAILED.value, TaskStatus.DEAD.value):
        return None
    reason = row.latest_outcome or ""
    if reason == "cancelled":
        return None
    if reason == "handler_failure":
        reason = row.error or reason
    fields = {
        "task_id": str(row.id),
        "task_type": row.task_type,
        "queue": row.queue,
        "attempt": row.attempts,
        "max_attempts": row.max_attempts,
        "status": row.status,
        "reason": reason,
        "retry_at": row.scheduled_for if row.status == TaskStatus.FAILED.value else None,
    }
    return lambda: log_outcome(worker_log, exc=failure, **fields)


def owns(row: Any, assignment: Assignment, owner: str) -> bool:
    validate(row)
    return row.dispatch_token == assignment.dispatch_token and row.worker_id == owner


def assignment_changes(row: Any, owner: str, now: datetime, timing: Timing) -> dict[str, Any]:
    if tagged(row):
        raise RuntimeError("Cannot assign an already owned task")
    budget = getattr(row, "execution_timeout_s", None)
    require_execution_timeout(budget)
    timeout = (
        min(budget, timing.task_timeout_seconds)
        if budget is not None
        else timing.task_timeout_seconds
    )
    return {
        "status": TaskStatus.DISPATCHING.value,
        "dispatch_token": str(uuid4()),
        "worker_id": owner,
        "lease_expires_at": now + timedelta(seconds=timing.assignment_lease_seconds),
        "execution_deadline_at": now + timedelta(seconds=timeout),
        "dispatching_at": now,
        "execution_token": None,
        "latest_outcome": None,
        "cancel_requested": False,
        "updated_at": now,
    }


def admission_changes(
    row: Any,
    assignment: Assignment,
    owner: str,
    now: datetime,
    timing: Timing,
) -> dict[str, Any] | None:
    if not owns(row, assignment, owner) or row.status != TaskStatus.DISPATCHING.value:
        return None
    if row.lease_expires_at <= now or row.execution_deadline_at <= now:
        return None
    if row.expires_at is not None and row.expires_at <= now:
        return dict(
            CLEAR_NATIVE,
            status=TaskStatus.EXPIRED.value,
            expired_at=now,
            dispatching_at=None,
            execution_token=None,
            updated_at=now,
            latest_outcome="expired",
        )
    if row.attempts >= row.max_attempts:
        return dict(
            CLEAR_NATIVE,
            status=TaskStatus.DEAD.value,
            execution_token=None,
            dispatching_at=None,
            latest_outcome="exhausted",
            updated_at=now,
        )
    if row.scheduled_for is not None and row.scheduled_for > now:
        return None
    return {
        "status": TaskStatus.PROCESSING.value,
        "started_at": now,
        "dispatching_at": None,
        "attempts": row.attempts + 1,
        "execution_token": str(uuid4()),
        "updated_at": now,
        "lease_expires_at": min(
            row.execution_deadline_at, now + timedelta(seconds=timing.execution_lease_seconds)
        ),
    }


#: A row in one of these states with no owner is waiting for a worker.
CLAIMABLE = (TaskStatus.PENDING.value, TaskStatus.FAILED.value)


def claim_order(row: Any) -> tuple[Any, ...]:
    """Which waiting row goes first: the order the claim index is built in."""
    return (-row.priority, row.scheduled_for or row.created_at, row.created_at, row.id)


def seconds_left(row: Any, now: datetime) -> float:
    """What remains of a started attempt, measured on the database clock."""
    return (row.execution_deadline_at - now).total_seconds()


def renewal_changes(
    row: Any,
    assignment: Assignment,
    owner: str,
    now: datetime,
    timing: Timing,
) -> dict[str, Any] | None:
    if not owns(row, assignment, owner):
        return None
    if row.lease_expires_at <= now or row.execution_deadline_at <= now:
        return None
    # Assignment leases are fixed: a child must start before the dispatch deadline.
    # A live, not-yet-started assignment needs no renewal, but remains valid.
    if row.status == TaskStatus.DISPATCHING.value:
        return {}
    if row.status != TaskStatus.PROCESSING.value:
        return None
    return {
        "lease_expires_at": min(
            row.execution_deadline_at, now + timedelta(seconds=timing.execution_lease_seconds)
        ),
        "updated_at": now,
    }


def outcome_changes(
    row: Any,
    assignment: Assignment,
    owner: str,
    execution_token: str,
    now: datetime,
    failure: BaseException | None,
) -> dict[str, Any] | None:
    if not owns(row, assignment, owner) or row.status != TaskStatus.PROCESSING.value:
        return None
    if row.execution_token != execution_token:
        return None
    if row.lease_expires_at <= now or row.execution_deadline_at <= now:
        return None
    if getattr(row, "cancel_requested", False):
        if failure is None:
            return None
        return dict(
            CLEAR_NATIVE,
            execution_token=None,
            status=TaskStatus.DEAD.value,
            error="cancelled by operator",
            latest_outcome="cancelled",
            updated_at=now,
        )
    if failure is None:
        return dict(
            CLEAR_NATIVE,
            execution_token=None,
            status=TaskStatus.COMPLETED.value,
            completed_at=now,
            error="",
            latest_outcome="completed",
            updated_at=now,
        )
    outcome = classify_failure(
        failure,
        policy=resolve_policy(row.task_type),
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        now=now,
    )
    return dict(
        CLEAR_NATIVE,
        execution_token=None,
        status=outcome.status.value,
        scheduled_for=outcome.retry_at,
        error=outcome.reason,
        latest_outcome="handler_failure",
        updated_at=now,
    )


def recovery_changes(row: Any, now: datetime, *, stopped: bool = False) -> dict[str, Any] | None:
    validate(row)
    if not tagged(row) or (
        not stopped and row.lease_expires_at > now and row.execution_deadline_at > now
    ):
        return None
    base = dict(CLEAR_NATIVE, execution_token=None, dispatching_at=None, updated_at=now)
    if getattr(row, "cancel_requested", False):
        return dict(
            base,
            status=TaskStatus.DEAD.value,
            latest_outcome="cancelled",
            error="cancelled by operator",
        )
    if row.status == TaskStatus.DISPATCHING.value:
        if row.expires_at is not None and row.expires_at <= now:
            return dict(
                base, status=TaskStatus.EXPIRED.value, expired_at=now, latest_outcome="expired"
            )
        return dict(
            base,
            status=(
                TaskStatus.DEAD.value
                if row.attempts >= row.max_attempts
                else TaskStatus.PENDING.value
            ),
            latest_outcome="abandoned",
        )
    kind = "timed_out" if row.execution_deadline_at <= now else "interrupted"
    if row.attempts >= row.max_attempts:
        return dict(
            base, status=TaskStatus.DEAD.value, latest_outcome=kind, error="attempts exhausted"
        )
    return dict(
        base,
        status=TaskStatus.FAILED.value,
        scheduled_for=now + resolve_policy(row.task_type).delay_for(row.attempts),
        latest_outcome=kind,
        error=kind,
    )


def reconcile(row: Any, assignment: Assignment, owner: str) -> Reconciliation:
    if row is None:
        return Reconciliation.INVALIDATED
    if owns(row, assignment, owner):
        return Reconciliation.ACTIVE
    # No historical audit is implied: cleared/rotated ownership is sufficient to
    # release this obligation, but cannot prove this particular attempt succeeded.
    return Reconciliation.INVALIDATED
