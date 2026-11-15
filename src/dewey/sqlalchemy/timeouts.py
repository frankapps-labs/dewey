"""Apply the running task's database budgets to a connection the handler owns."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession
from sqlalchemy.orm import Session

from dewey.context import current_task

_SET = text("SELECT set_config(:name, :value, true)")


def _budgets() -> list[dict[str, str]]:
    task = current_task()
    budgets = {
        "statement_timeout": task.statement_timeout_s,
        "lock_timeout": task.lock_timeout_s,
    }
    return [
        {"name": name, "value": f"{max(1, int(seconds * 1000))}ms"}
        for name, seconds in budgets.items()
        if seconds is not None
    ]


def apply_task_timeouts(target: Session | Connection) -> None:
    """Bound the current transaction by the task policy's ``statement_timeout_s``/``lock_timeout_s``.

    Call inside a handler, on a session or connection with an open transaction.
    The settings are transaction-local (``SET LOCAL``): they end with it, and a
    policy without budgets changes nothing.
    """
    for budget in _budgets():
        target.execute(_SET, budget)


async def apply_task_timeouts_async(target: AsyncSession | AsyncConnection) -> None:
    """Async equivalent of :func:`apply_task_timeouts`."""
    for budget in _budgets():
        await target.execute(_SET, budget)


__all__ = ["apply_task_timeouts", "apply_task_timeouts_async"]
