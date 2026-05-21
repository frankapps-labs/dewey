"""Abstract adapter interface for queue transports."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

ProcessTaskFn = Callable[[str], Any]


class DispatcherAdapter(Protocol):
    """
    Target protocol for Dewey-driven dispatch.

    Producers create rows only. A Dewey dispatcher claims ready rows and calls
    ``dispatch(task_id)`` after commit; the transport adapter then hands that
    task ID to its worker pool. ``register(process_fn)`` wires the worker-side
    processor that receives the task ID.

    Existing adapters may still expose the legacy ``enqueue`` API until they are
    migrated to this protocol.
    """

    def register(self, process_fn: ProcessTaskFn) -> None:
        """Register the worker-side function that processes a task ID."""
        ...

    def dispatch(self, task_id: str) -> Any:
        """Dispatch a claimed task ID to the transport worker pool."""
        ...


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
