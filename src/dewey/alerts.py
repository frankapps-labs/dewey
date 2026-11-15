"""One hook for the events an operator wants to be told about.

Register a callable that takes a JSON-safe dict; route it to whatever you use
(an error tracker, a log pipeline, a webhook)::

    import dewey

    @dewey.on_alert
    def page(event: dict) -> None:
        if event["event"] == "task_dead":
            ...

Events and their fields:

``task_dead``                 ``task_id``, ``task_type``, ``queue``, ``attempts``, ``reason``
``execution_recovered``       ``task_id``, ``task_type``, ``queue``, ``reason``, ``status``
``schedule_tick_missed``      ``schedule``, ``missed``
``worker_heartbeat_stale``    ``message`` (raised by ``doctor``)

The hook runs in the process that made the change, after its transaction
commits. A pull-runner host owns that commit: on Django its alerts follow the
host's commit by themselves; on SQLAlchemy the host calls
``runner.deliver_alerts()`` after committing. It must be quick. An exception it raises is logged and goes no
further: an alert can never change what the ledger did.
"""

from __future__ import annotations

import importlib
import inspect
import logging
from collections import deque
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

AlertHook = Callable[[dict[str, Any]], None]
_hook: AlertHook | None = None


def on_alert(hook: AlertHook | None) -> AlertHook | None:
    """Set the process-wide alert hook (``None`` removes it). Usable as a decorator."""
    global _hook
    if hook is not None and not callable(hook):
        raise TypeError("An alert hook is a callable taking one dict")
    if inspect.iscoroutinefunction(hook):
        # Nothing awaits the hook: its alerts would silently never be sent.
        raise TypeError("An alert hook is a plain function, not a coroutine function")
    _hook = hook
    return hook


def load_alert_hook(path: str | None) -> None:
    """Set the hook from a ``module:function`` path; ``None`` leaves it alone."""
    if path is None:
        return
    module, _, name = path.partition(":")
    if not module or not name:
        raise ValueError("An alert hook path is module:function")
    on_alert(getattr(importlib.import_module(module), name))


def emit(event: str, **fields: Any) -> None:
    if _hook is None:
        return
    try:
        _hook({"event": event, **fields})
    except Exception:
        logger.exception("Alert hook failed for %s", event)


class AlertOutbox:
    """Alerts noticed inside a transaction, sent only once it has committed."""

    def __init__(self) -> None:
        # Bounded: a caller that owns its own commit may never flush.
        self._pending: deque[tuple[str, dict[str, Any]]] = deque(maxlen=256)

    def __len__(self) -> int:
        return len(self._pending)

    def add(self, event: str, **fields: Any) -> None:
        self._pending.append((event, fields))

    def task_changed(self, row: Any, reason: str | None, *, recovered: bool = False) -> None:
        """Record what a settled or recovered task row means for an operator."""
        identity = {"task_id": str(row.id), "task_type": row.task_type, "queue": row.queue}
        if recovered:
            self.add("execution_recovered", **identity, reason=reason, status=row.status)
        if row.status == "dead":
            self.add("task_dead", **identity, attempts=row.attempts, reason=reason)

    def operation_done(self, name: str, result: Any) -> None:
        """An operator killing a task is a dead letter too."""
        if name == "tasks.kill" and isinstance(result, dict) and result.get("status") == "dead":
            self.add(
                "task_dead",
                task_id=result["id"],
                task_type=result["task_type"],
                queue=result["queue"],
                attempts=result["attempts"],
                reason="killed",
            )

    def flush(self) -> None:
        while self._pending:
            event, fields = self._pending.popleft()
            emit(event, **fields)

    def discard(self) -> None:
        self._pending.clear()


__all__ = ["AlertOutbox", "emit", "load_alert_hook", "on_alert"]
