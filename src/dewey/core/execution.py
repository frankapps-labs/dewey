"""Shared execution decisions — one implementation for every backend.

The SQLAlchemy sync, SQLAlchemy async, and Django executors all differ in how
they load and save rows, and not at all in what a failure *means*. That decision
lives here so the three paths cannot drift apart.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from dewey.core.backoff import BackoffFn
from dewey.core.states import TaskStatus, should_die
from dewey.errors import RetryAfter, UnknownTaskTypeError
from dewey.policy import TaskPolicy, resolve_policy


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


__all__ = ["FailureOutcome", "classify_failure", "resolve_handler"]
