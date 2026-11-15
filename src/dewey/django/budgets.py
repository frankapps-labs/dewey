"""The task policy's database budgets, applied to a Django connection for one handler.

The worker applies them around every attempt; ``process_task`` applies them the
same way, so a handler tested inline runs under the budgets it will have in
production. No worker module is imported here: producers import the executor.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from django.db import connections

from dewey.context import TaskContext


@contextmanager
def task_budgets(using: str, context: TaskContext) -> Iterator[None]:
    """Set ``statement_timeout``/``lock_timeout`` on the alias while the handler runs.

    Restores the connection's previous settings afterwards, or closes it when
    the handler left it unusable (a fresh connection starts clean). A policy
    without budgets changes nothing.
    """
    budgets = {
        "statement_timeout": context.statement_timeout_s,
        "lock_timeout": context.lock_timeout_s,
    }
    budgets = {name: seconds for name, seconds in budgets.items() if seconds is not None}
    if not budgets:
        yield
        return
    connection = connections[using]
    with connection.cursor() as cursor:
        cursor.execute("SELECT " + ", ".join(f"current_setting('{name}')" for name in budgets))
        previous = cursor.fetchone()
        for name, seconds in budgets.items():
            # set_config, not SET: SET takes no bound parameters.
            cursor.execute(
                "SELECT set_config(%s, %s, false)", [name, f"{max(1, int(seconds * 1000))}ms"]
            )
    try:
        yield
    finally:
        try:
            with connection.cursor() as cursor:
                for name, value in zip(budgets, previous, strict=True):
                    cursor.execute("SELECT set_config(%s, %s, false)", [name, value])
        except Exception:
            # The handler left the connection unusable; a fresh one starts clean.
            connection.close()


__all__ = ["task_budgets"]
