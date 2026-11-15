"""Internal native child execution, behind the pool's synchronous lifecycle."""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import ExitStack
from typing import Any, Protocol

from dewey._worker.loop import ChildLoop
from dewey._worker.pool import Assignment
from dewey.context import TaskContext, _bind_task
from dewey.core.execution import resolve_handler
from dewey.core.logging import restore_trace
from dewey.policy import resolve_policy

#: How long a task process keeps asking a ledger that is too slow to answer in
#: time before it gives the task up to recovery.
LEDGER_PATIENCE_SECONDS = 60.0


class LedgerStorage(Protocol):
    def call(self, operation: str, *args: Any) -> Any: ...


class LedgerRuntime:
    """Construct inside a spawned child with child-local storage and registry.

    Resource disposal must complete before the child loop is closed. Sync and
    async disposal callbacks are supported; no shared engine/session ownership.
    """

    def __init__(self, storage: LedgerStorage, *, close_resources: Callable[[], Any]):
        self.storage = storage
        self.loop = ChildLoop()
        self.close_resources = close_resources
        try:
            self.loop.invoke(storage.call, "readiness")
        except BaseException:
            self.close()
            raise

    def _ledger(self, operation: str, *args: Any) -> Any:
        """A ledger call that ran out of time rolled back, so ask again for a while.

        Both calls are fenced by the row's ownership: a late one changes nothing.
        """
        give_up = time.monotonic() + LEDGER_PATIENCE_SECONDS
        pause = 0.1
        while True:
            try:
                return self.loop.invoke(self.storage.call, operation, *args)
            except TimeoutError:
                if time.monotonic() >= give_up:
                    raise
                time.sleep(pause)
                pause = min(2 * pause, 2.0)

    def execute(self, assignment: Assignment) -> None:
        invocation = self._ledger("start", assignment)
        if invocation is None:
            return
        failure = None
        try:
            handler = resolve_handler(invocation["task_type"], None)
            policy = resolve_policy(invocation["task_type"])
            left = invocation.get("execution_seconds_left")
            context = TaskContext(
                assignment.task_id,
                invocation["task_type"],
                invocation["attempts"],
                deadline=invocation.get("execution_deadline_at"),
                statement_timeout_s=policy.statement_timeout_s,
                lock_timeout_s=policy.lock_timeout_s,
                _stops_at=None if left is None else time.monotonic() + left,
            )
            with ExitStack() as stack:
                stack.enter_context(_bind_task(context))
                stack.enter_context(restore_trace(invocation.get("trace")))
                # A storage may prepare its framework for the handler (Django
                # applies the task's database budgets to its connection).
                scope = getattr(self.storage, "handler_scope", None)
                if scope is not None:
                    stack.enter_context(scope(context))
                self.loop.invoke_handler(handler, *invocation["args"], **invocation["kwargs"])
        except Exception as exc:
            failure = exc
        # RETURNED still only means this callback returned. Ownership loss or an
        # uncertain commit must be reconciled by the parent against the ledger.
        self._ledger("finish", assignment, invocation["execution_token"], failure)

    def close(self) -> None:
        try:
            self.loop.invoke(self.close_resources)
        finally:
            self.loop.close()
