"""Huey adapter — transport only.

Huey's job is to carry a task ID from the dispatcher to a worker. It does not
decide when work runs, how often it retries, or what happens when it fails. That
is Dewey's, in Postgres.

Registering with ``retries=0`` is deliberate: two retry engines fighting over one
task is how work gets run twice and how attempt counters stop meaning anything.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from huey import Huey

logger = logging.getLogger(__name__)

ProcessTaskFn = Callable[[str], Any]


class HueyAdapter:
    """Bridge Dewey's dispatcher to a Huey worker pool.

    Put the wiring in a module both processes import — the Huey instance, the
    adapter, and the ``register`` call::

        # myapp/tasks.py
        huey = RedisHuey("myapp")
        adapter = HueyAdapter(huey)

        def _process(task_id: str) -> bool:
            with Session(engine) as session:
                return process_task(session, task_id)

        adapter.register(_process)

    The worker runs ``huey_consumer myapp.tasks.huey``. The dispatcher imports the
    same module and hands IDs over::

        dispatcher = Dispatcher(SQLAlchemyDispatchBackend(engine), adapter.dispatch)
        dispatcher.run()

    Producers import none of this. They call ``create_task`` and commit.
    """

    def __init__(self, huey: Huey, *, task_name: str = "dewey_process_task") -> None:
        self._huey = huey
        self._task_name = task_name
        self._process_task: Any = None

    def register(self, process_fn: ProcessTaskFn) -> None:
        """Register the callable that processes a task ID.

        Call once per process, at import time, before the consumer starts.

        ``retries=0`` is not an oversight: Dewey owns retry scheduling and the
        attempt budget. A broker retrying underneath it would re-run a handler
        Dewey had deliberately scheduled for later.
        """
        if self._process_task is not None:
            raise RuntimeError(
                "HueyAdapter.register() was called twice in this process. Register once "
                "at import time — a second call would bind two callables to the task "
                f"name {self._task_name!r}."
            )

        @self._huey.task(name=self._task_name, retries=0)
        def _dewey_process_task(task_id: str) -> Any:
            return process_fn(task_id)

        self._process_task = _dewey_process_task
        logger.info("HueyAdapter registered processor as %r", self._task_name)

    def dispatch(self, task_id: str) -> Any:
        """Hand a claimed task ID to the Huey worker pool.

        Called only by the dispatcher, only after the claim is committed.

        Raising when the broker is unreachable is correct and expected: the
        dispatcher returns the row to PENDING, backs off, and tries again. Nothing
        is lost, because Postgres — not Redis — is holding the backlog.
        """
        if self._process_task is None:
            raise RuntimeError(
                "HueyAdapter.register() must be called before dispatch(). Import the "
                "module that wires the adapter (the same one your worker uses) in the "
                "dispatcher process, so both agree on the task name."
            )
        return self._process_task(task_id)


__all__ = ["HueyAdapter", "ProcessTaskFn"]
