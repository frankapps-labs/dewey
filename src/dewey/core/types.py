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
    payload: dict[str, Any]
    queue: str
    priority: int
    attempts: int
    max_attempts: int
    error: str
    created_at: datetime
    updated_at: datetime
    process_after: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    idempotency_key: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.status.is_terminal

    @property
    def is_retryable(self) -> bool:
        return self.status == TaskStatus.FAILED and self.attempts < self.max_attempts
