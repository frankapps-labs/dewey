"""Pure Python dataclasses — framework-agnostic type definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from dewey.core.states import TaskStatus


@dataclass
class TaskEntry:
    """Read-only snapshot of a task row. Used for query results and type safety."""

    id: str
    task_type: str
    status: TaskStatus
    args: list[Any]
    kwargs: dict[str, Any]
    queue: str
    priority: int
    attempts: int
    max_attempts: int
    error: str
    created_at: datetime
    updated_at: datetime
    scheduled_for: datetime | None = None
    dispatching_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    #: Absolute deadline for starting handler execution. A task observed before
    #: invocation at ``now == expires_at`` is expired. None means no deadline.
    expires_at: datetime | None = None
    #: Immutable snapshot of the creation-time schedule. ``scheduled_for`` mutates
    #: on retry, so idempotent creation matches against this internal field instead.
    #: Written once at creation and never updated.
    initial_scheduled_for: datetime | None = None
    #: Audit timestamp: when Dewey observed the deadline had passed. The status
    #: EXPIRED is the reason; this records the moment.
    expired_at: datetime | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status.is_terminal

    @property
    def is_retryable(self) -> bool:
        return self.status == TaskStatus.FAILED and self.attempts < self.max_attempts
