"""Child-owned async bridge behind the pool's synchronous lifecycle protocol.

Construct only after spawn. One task executes at a time; this is not a threaded
handler executor or a multi-task scheduler. Hard cancellation belongs to the
supervising process, not to this event-loop bridge.
"""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import os
import threading
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

T = TypeVar("T")


class UnsafeBackgroundTasks(BaseException):
    """Abort the child lifecycle rather than reuse a contaminated event loop."""


async def _await_result(value: Awaitable[T]) -> T:
    return await value


class ChildLoop:
    """Reuse one isolated event loop for async storage and handler operations."""

    def __init__(self) -> None:
        self._owner = (os.getpid(), threading.get_ident())
        self._runner = asyncio.Runner()
        self._closed = False

    def _check(self) -> None:
        if self._owner != (os.getpid(), threading.get_ident()):
            raise RuntimeError("ChildLoop cannot cross process or thread boundaries")
        if self._closed:
            raise RuntimeError("ChildLoop is closed")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise RuntimeError("ChildLoop synchronous bridge cannot nest a running loop")

    def invoke(self, handler: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Invoke once; await coroutine/awaitable results, reject async generators.

        Handler exceptions propagate to the integration's fenced outcome writer.
        A synchronous callable returning an awaitable is supported as well.
        """
        self._check()
        result = handler(*args, **kwargs)
        if inspect.isasyncgen(result):
            self._runner.run(_await_result(result.aclose()))
            raise TypeError("Async generator handlers are not supported")
        if inspect.isawaitable(result):
            # asyncio.Runner otherwise reuses the context captured at its first
            # run, leaking/missing per-assignment identity on a reusable loop.
            result = self._runner.run(_await_result(result), context=contextvars.copy_context())
            if inspect.iscoroutine(result):
                result.close()
                raise TypeError("Nested coroutine results are not supported")
            if inspect.isasyncgen(result):
                self._runner.run(_await_result(result.aclose()))
                raise TypeError("Async generator handlers are not supported")
            if inspect.isawaitable(result):
                raise TypeError("Nested awaitable results are not supported")
        return result

    async def _cancel_created(self, previous: set[asyncio.Task[Any]]) -> bool:
        current = asyncio.current_task()
        created = asyncio.all_tasks() - previous - {current}
        for task in created:
            task.cancel()
        if created:
            await asyncio.gather(*created, return_exceptions=True)
        if asyncio.all_tasks() - previous - {current}:
            raise UnsafeBackgroundTasks("Handler background tasks survived cancellation")
        return bool(created)

    def invoke_handler(self, handler: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Do not carry handler-created background tasks into another assignment.

        Async storage may own legitimate persistent tasks, so this boundary is
        applied only to handlers. Cancellation-resistant code still requires the
        parent's process hard stop; there is no in-loop termination guarantee.
        """
        self._check()
        previous = asyncio.all_tasks(self._runner.get_loop())
        try:
            result = self.invoke(handler, *args, **kwargs)
        except BaseException:
            self._runner.run(self._cancel_created(previous))
            raise
        if self._runner.run(self._cancel_created(previous)):
            raise TypeError("Handler left unawaited background tasks")
        return result

    def close(self) -> None:
        """Close after integration resources have been disposed on this loop."""
        if self._closed:
            return
        self._check()
        try:
            self._runner.close()
        finally:
            self._closed = True
