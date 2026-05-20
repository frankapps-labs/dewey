"""Huey adapter — enqueue tasks and register periodic sweep."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from huey import Huey

logger = logging.getLogger(__name__)


class HueyAdapter:
    """
    Adapter that bridges dewey to Huey.

    Usage::

        from huey import RedisHuey
        from dewey.adapters.huey import HueyAdapter

        huey = RedisHuey("myapp")
        adapter = HueyAdapter(huey)

        # Register the worker task + periodic sweep
        adapter.setup(process_fn=my_process_function)

        # Then enqueue tasks:
        adapter.enqueue(task_id="abc-123", queue="default")

    The ``process_fn`` you provide receives a task_id (str) and should call
    ``dewey.sqlalchemy.executor.process_task()`` with the appropriate
    session and handler.
    """

    def __init__(self, huey: Huey) -> None:
        self._huey = huey
        self._process_task: Any = None
        self._sweep_task: Any = None

    def setup(
        self,
        process_fn: Callable[[str], Any],
        sweep_fn: Callable[[], Any] | None = None,
        sweep_interval_minutes: int = 5,
        retries: int = 0,
        retry_delay: int = 0,
    ) -> None:
        """
        Register Huey tasks for processing and sweeping.

        Args:
            process_fn: Called with task_id. Should open a session and call process_task().
            sweep_fn: Called with no args. Should open a session and call sweep().
                      If None, no periodic sweep is registered.
            sweep_interval_minutes: How often to run the sweep (default: 5 min).
            retries: Huey-level retries for the process task (default: 0, dewey handles retries).
            retry_delay: Huey-level retry delay (default: 0).
        """
        from huey import crontab

        @self._huey.task(retries=retries, retry_delay=retry_delay)
        def _dewey_process(task_id: str) -> Any:
            return process_fn(task_id)

        self._process_task = _dewey_process

        if sweep_fn:

            @self._huey.periodic_task(crontab(minute=f"*/{sweep_interval_minutes}"))
            def _dewey_sweep() -> Any:
                return sweep_fn()

            self._sweep_task = _dewey_sweep

        logger.info(
            "HueyAdapter setup complete sweep_interval=%dm",
            sweep_interval_minutes,
        )

    def enqueue(self, task_id: str, queue: str = "default", priority: int = 0) -> Any:
        """Enqueue a task ID for processing."""
        if self._process_task is None:
            raise RuntimeError(
                "HueyAdapter.setup() must be called before enqueue(). "
                "Register your process_fn first."
            )

        if queue != "default":
            logger.debug(
                "HueyAdapter: queue=%s is informational only — "
                "Huey routes tasks via registration, not per-enqueue",
                queue,
            )
        if priority != 0:
            logger.debug(
                "HueyAdapter: priority=%d is informational only — "
                "use PriorityRedisHuey and configure priority at task registration level",
                priority,
            )
        result = self._process_task(task_id)
        logger.debug("Enqueued task_id=%s queue=%s priority=%d", task_id, queue, priority)
        return result

    def enqueue_sweep(self) -> Any:
        """Manually trigger a sweep outside the periodic schedule."""
        if self._sweep_task is None:
            raise RuntimeError("No sweep_fn was registered in setup().")
        return self._sweep_task()
