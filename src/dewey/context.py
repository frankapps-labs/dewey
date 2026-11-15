"""Stable handler identity for application-owned effect idempotency.

A key is useful only when the receiving system durably deduplicates it. This
module neither makes effects atomic with the ledger nor prevents handler retries.
"""

from __future__ import annotations

import hashlib
import re
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

_OPERATION = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,63}\Z")


@dataclass(frozen=True)
class TaskContext:
    """Identity of the currently admitted handler, independent of its transport.

    ``attempt`` includes this admitted execution and is diagnostic only. Never
    include it, a dispatch token, or a worker instance in an effect dedupe key.
    """

    task_id: str
    task_type: str
    attempt: int
    #: When the worker stops this attempt. None when run inline, with no worker.
    deadline: datetime | None = None
    #: The task policy's database budgets, for handlers that manage their own
    #: connections. The Django worker applies them to its connection for you.
    statement_timeout_s: float | None = None
    lock_timeout_s: float | None = None
    #: The deadline on this process's monotonic clock, set by the worker.
    _stops_at: float | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, str) or not self.task_id or "\0" in self.task_id:
            raise ValueError("Task identity must be a nonempty string without NUL")
        if not isinstance(self.task_type, str) or not self.task_type:
            raise ValueError("Task type must be a nonempty string")
        if isinstance(self.attempt, bool) or not isinstance(self.attempt, int) or self.attempt < 1:
            raise ValueError("Task attempt must be a positive integer")

    def remaining(self) -> timedelta | None:
        """Time left before the worker stops this attempt; None without a deadline.

        Check it between units of work and return early (or raise
        ``dewey.RetryAfter``) rather than be cut off mid-write.
        """
        if self._stops_at is not None:
            # Counted from the database's clock at start: host clock skew cannot stretch it.
            return timedelta(seconds=max(self._stops_at - time.monotonic(), 0.0))
        if self.deadline is None:
            return None
        return max(self.deadline - datetime.now(UTC), timedelta(0))

    def effect_key(self, operation: str) -> str:
        """Return a stable 64-hex key for one named effect across retries.

        Use a different stable name for each effect (e.g. ``charge`` and ``email``).
        Do not rename the operation while retrying existing tasks. Verify the
        provider's key scope, size and retention window. Separate task rows are
        separate obligations: creation-time business-request dedupe remains the
        job of ``create_or_get_task`` or the application's business identity.
        """
        if not isinstance(operation, str) or _OPERATION.fullmatch(operation) is None:
            raise ValueError(
                "Effect operation must be 1–64 ASCII letters/digits/._-, starting with a letter"
            )
        value = "dewey-task-effect:v1\0" + self.task_id + "\0" + operation
        return hashlib.sha256(value.encode("utf-8")).hexdigest()


_current: ContextVar[TaskContext | None] = ContextVar("dewey_task_context", default=None)


def current_task() -> TaskContext:
    """Get the admitted handler's identity; raise outside a Dewey handler.

    Context follows awaited async work. It is not a licence to detach background
    work past the handler lifetime; native runtimes reject leftover async tasks.
    """
    value = _current.get()
    if value is None:
        raise RuntimeError("No admitted Dewey task is executing in this context")
    return value


def current_task_or_none() -> TaskContext | None:
    """The admitted handler's identity, or None outside a Dewey handler.

    For a handler that is also called directly (a management command, a test,
    a request): ``if (task := dewey.current_task_or_none()) and task.remaining()
    < limit`` needs no ``try``/``except RuntimeError``.
    """
    return _current.get()


@contextmanager
def _bind_task(value: TaskContext) -> Iterator[TaskContext]:
    token = _current.set(value)
    try:
        yield value
    finally:
        _current.reset(token)


__all__ = ["TaskContext", "current_task", "current_task_or_none"]
