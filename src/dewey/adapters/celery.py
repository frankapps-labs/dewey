"""Celery adapter — enqueue tasks and register periodic sweep."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from celery import Celery

logger = logging.getLogger(__name__)


class CeleryAdapter:
    """
    Adapter that bridges dewey to Celery.

    Usage::

        from celery import Celery
        from dewey.adapters.celery import CeleryAdapter

        app = Celery("myapp", broker="redis://localhost:6379/0")
        adapter = CeleryAdapter(app)

        # Register the worker task + periodic sweep
        adapter.setup(process_fn=my_process_function, sweep_fn=my_sweep_function)

        # Then enqueue tasks:
        adapter.enqueue(task_id="abc-123", queue="default", priority=5)

    The ``process_fn`` you provide receives a task_id (str) and should call
    ``dewey.sqlalchemy.executor.process_task()`` (or the Django equivalent)
    with the appropriate session and handler.

    Unlike Huey, Celery supports per-call queue routing and priority natively.
    """

    def __init__(self, app: Celery) -> None:
        self._app = app
        self._process_task: Any = None
        self._sweep_task: Any = None

    def setup(
        self,
        process_fn: Callable[[str], Any],
        sweep_fn: Callable[[], Any] | None = None,
        sweep_interval_seconds: int = 300,
        task_name: str = "dewey.process",
        sweep_task_name: str = "dewey.sweep",
    ) -> None:
        """
        Register Celery tasks for processing and sweeping.

        Args:
            process_fn: Called with task_id. Should open a session and call process_task().
            sweep_fn: Called with no args. Should open a session and call sweep().
                      If None, no periodic sweep is registered.
            sweep_interval_seconds: How often to run the sweep (default: 300s = 5 min).
            task_name: Celery task name for the process task.
            sweep_task_name: Celery task name for the sweep task.
        """

        # Register the process task
        @self._app.task(name=task_name, bind=False)
        def _dewey_process(task_id: str) -> Any:
            return process_fn(task_id)

        self._process_task = _dewey_process

        if sweep_fn:

            @self._app.task(name=sweep_task_name, bind=False)
            def _dewey_sweep() -> Any:
                return sweep_fn()

            self._sweep_task = _dewey_sweep

            # Register periodic beat schedule
            existing = getattr(self._app.conf, "beat_schedule", None) or {}
            self._app.conf.beat_schedule = {
                **existing,
                "dewey-sweep": {
                    "task": sweep_task_name,
                    "schedule": sweep_interval_seconds,
                },
            }

        logger.info(
            "CeleryAdapter setup complete sweep_interval=%ds",
            sweep_interval_seconds,
        )

    def enqueue(self, task_id: str, queue: str = "default", priority: int = 0) -> Any:
        """
        Enqueue a task ID for processing.

        Celery supports per-call queue routing and priority natively,
        so both parameters are forwarded to ``apply_async``.

        Args:
            task_id: The dewey task ID to process.
            queue: Celery queue to route the task to.
            priority: Celery task priority (0-9, broker-dependent).

        Returns:
            Celery AsyncResult.
        """
        if self._process_task is None:
            raise RuntimeError(
                "CeleryAdapter.setup() must be called before enqueue(). "
                "Register your process_fn first."
            )

        result = self._process_task.apply_async(
            args=[task_id],
            queue=queue,
            priority=priority,
        )
        logger.debug("Enqueued task_id=%s queue=%s priority=%d", task_id, queue, priority)
        return result

    def enqueue_sweep(self) -> Any:
        """Manually trigger a sweep outside the periodic schedule."""
        if self._sweep_task is None:
            raise RuntimeError("No sweep_fn was registered in setup().")
        return self._sweep_task.apply_async()
