"""Abstract adapter interface for queue transports."""

from __future__ import annotations

from typing import Any, Protocol


class BaseAdapter(Protocol):
    """
    Protocol for queue transport adapters.

    Adapters bridge dewey (Postgres) to your task queue (Huey, Celery, etc.).
    The adapter's job is simple: take a task ID and put it on a queue.
    """

    def enqueue(self, task_id: str, queue: str = "default", priority: int = 0) -> Any:
        """Enqueue a task ID for processing by a worker."""
        ...

    def enqueue_sweep(self) -> Any:
        """Trigger a sweep (re-enqueue failed/stuck tasks)."""
        ...
