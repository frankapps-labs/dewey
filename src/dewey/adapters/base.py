"""Abstract adapter interface for queue transports."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

ProcessTaskFn = Callable[[str], Any]


@runtime_checkable
class DispatcherAdapter(Protocol):
    """
    Target protocol for Dewey-driven dispatch.

    Producers create rows only. A Dewey dispatcher claims ready rows and
    calls :meth:`dispatch` after commit; the transport adapter then hands
    the task ID to its worker pool. :meth:`register` wires the worker-side
    processor that receives the task ID.

    Existing adapters may still expose the legacy ``enqueue`` API until
    they are migrated to this protocol.

    Lifecycle
    ---------
    1. **Construction** — build the adapter with its transport handle
       (Huey instance, Celery app, etc.) in the worker process *and* in
       any producer process that will call :meth:`dispatch`.
    2. **register(process_fn)** — called once per worker process, before
       the worker pool starts consuming. The protocol does not require
       re-registration to be idempotent; adapters are free to raise if
       called twice. Producer processes that only call :meth:`dispatch`
       do not need to call :meth:`register`.
    3. **dispatch(task_id)** — called by the Dewey dispatcher after the
       claimed row has been committed to ``PROCESSING``. Must be safe to
       call concurrently from multiple producer processes and must not
       block on the task completing.

    The protocol is :func:`runtime_checkable`, so adapters can be
    validated with ``isinstance(adapter, DispatcherAdapter)`` at wiring
    time. Note that runtime checks verify method presence only, not
    signatures — see :mod:`tests.test_adapter_protocol` for signature
    contract tests.
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
