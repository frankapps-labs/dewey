"""Shared execution decisions — one implementation for every backend.

The SQLAlchemy sync, SQLAlchemy async, and Django executors all differ in how
they load and save rows, and not at all in what a failure *means*. That decision
lives here so the three paths cannot drift apart.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from dewey.core.backoff import BackoffFn
from dewey.core.states import TaskStatus, should_die
from dewey.errors import RetryAfter, UnknownTaskTypeError
from dewey.policy import TaskPolicy, resolve_policy

FAILURE_DETAILS = ("type", "full")
_failure_details = "type"


def configure_failure_details(level: str) -> None:
    """How much of a handler's exception the failed-attempt log record carries.

    ``"type"`` (the default) names the exception class only, with no message
    and no traceback: an exception's text may quote handler input or SQL
    parameters, and the log pipeline is not the ledger's ``--reveal`` boundary.
    ``"full"`` adds the message and ``exc_info``, for an application whose log
    pipeline scrubs what it collects. The row's stored error is not affected.
    """
    if level not in FAILURE_DETAILS:
        raise ValueError("failure_details must be type or full")
    global _failure_details
    _failure_details = level


def failure_details() -> str:
    return _failure_details


@dataclass(frozen=True)
class FailureOutcome:
    """What to write to the row after a handler raised."""

    status: TaskStatus
    retry_at: datetime | None
    reason: str

    @property
    def is_dead(self) -> bool:
        return self.status == TaskStatus.DEAD


def classify_failure(
    exc: BaseException,
    *,
    policy: TaskPolicy,
    attempts: int,
    max_attempts: int,
    now: datetime,
    backoff: BackoffFn | None = None,
) -> FailureOutcome:
    """Decide whether a failed attempt retries, and when.

    ``attempts`` is the number of attempts *including* the one that just failed.

    Precedence:

    1. ``fail_fast_on`` (and anything outside ``retry_on``) dead-letters at once,
       however much of the attempt budget is left — retrying a malformed request
       just burns capacity.
    2. A spent attempt budget dead-letters.
    3. Otherwise retry, no earlier than the policy backoff. A handler raising
       ``RetryAfter(n)`` can ask for *later* than the policy would, never
       earlier, so a misbehaving provider cannot pull retries in past your own
       rate budget.
    """
    requested_delay: timedelta | None = None
    if isinstance(exc, RetryAfter):
        requested_delay = timedelta(seconds=exc.seconds)
    elif not policy.is_retryable(exc):
        return FailureOutcome(TaskStatus.DEAD, None, "non-retryable")

    if should_die(attempts, max_attempts):
        return FailureOutcome(TaskStatus.DEAD, None, "attempts exhausted")

    delay = (backoff or policy.delay_for)(attempts)
    if requested_delay is not None and requested_delay > delay:
        delay = requested_delay
    return FailureOutcome(TaskStatus.FAILED, now + delay, "retry")


def resolve_handler(
    task_type: str,
    explicit: Callable[..., Any] | None,
    *,
    policy: TaskPolicy | None = None,
) -> Callable[..., Any]:
    """Return the callable for ``task_type``.

    An explicitly passed handler wins — that is the escape hatch for tests and
    for in-process use. Otherwise the registered handler is used.

    Raises:
        UnknownTaskTypeError: nothing is registered and nothing was passed. The
            executor treats this as an ordinary failed attempt rather than a
            crash, so a worker that predates the handler's deploy retries with
            backoff instead of destroying the work.
    """
    if explicit is not None:
        return explicit
    policy = policy or resolve_policy(task_type)
    if policy.handler is None:
        raise UnknownTaskTypeError(
            f"No handler registered for task type {task_type!r}. Declare one with "
            f"@dewey.task({task_type!r}) and make sure the worker imports that "
            f"module, or pass handler= explicitly."
        )
    return policy.handler


def log_outcome(
    log: logging.Logger,
    *,
    task_id: str,
    task_type: str,
    queue: str,
    attempt: int,
    max_attempts: int,
    status: TaskStatus | str,
    reason: str,
    retry_at: datetime | None = None,
    exc: BaseException | None = None,
) -> None:
    """One log record per failed attempt, the same from every executor and the worker.

    A failed attempt that will be retried is a WARNING; the one that leaves the
    task dead is an ERROR. The record names the exception's type, and with
    ``failure_details="full"`` also carries its message and ``exc_info``, when
    the process that logs it is the one that caught it; and the task's identity
    as structured fields (``task_id``, ``task_type``, ``queue``, ``attempt``,
    ``max_attempts``, ``outcome``, ``reason``) so a JSON or OpenTelemetry log
    handler can index and count them.

    Every value on the record (its arguments and its fields) is a string, a
    number or None. A JSON handler serialises the arguments too, and one that
    meets an object it has no encoder for may go looking for one: Logfire
    imports every optional library it knows (pandas, numpy, ...) to find a
    match, which takes tens of seconds on a small CPU. The exception travels as
    ``exc_info`` only; its text is in the message.
    """
    status = TaskStatus(status)
    if exc is None:
        error = ""
    elif _failure_details == "full":
        error = f"{type(exc).__name__}: {exc}"
    else:
        error = type(exc).__name__
        exc = None  # the message and traceback stay out of the log pipeline
    fields = {
        "task_id": task_id,
        "task_type": task_type,
        "queue": queue,
        "attempt": attempt,
        "max_attempts": max_attempts,
        "outcome": status.value,
        "reason": reason,
    }
    if status == TaskStatus.DEAD:
        log.error(
            "Task dead id=%s type=%s queue=%s attempt=%d/%d reason=%s error=%s",
            task_id,
            task_type,
            queue,
            attempt,
            max_attempts,
            reason,
            error,
            exc_info=exc,
            extra=fields,
        )
    else:
        log.warning(
            "Task failed id=%s type=%s queue=%s attempt=%d/%d reason=%s retry_at=%s error=%s",
            task_id,
            task_type,
            queue,
            attempt,
            max_attempts,
            reason,
            None if retry_at is None else retry_at.isoformat(),
            error,
            exc_info=exc,
            extra={**fields, "retry_at": None if retry_at is None else retry_at.isoformat()},
        )


__all__ = [
    "FAILURE_DETAILS",
    "FailureOutcome",
    "classify_failure",
    "configure_failure_details",
    "failure_details",
    "log_outcome",
    "resolve_handler",
]
