"""Monotonic operation budget for native ledger SQL statements.

Drivers still need bounded connection/transport configuration. A SQL timeout is
not proof that a network read or COMMIT acknowledgement cannot hang.
"""

import time
from contextlib import closing, contextmanager
from typing import Any


class DatabaseBudget:
    def __init__(self, seconds: float):
        self.deadline = time.monotonic() + seconds

    def milliseconds(self) -> int:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Native database operation exceeded its budget")
        return max(1, int(remaining * 1000))

    @contextmanager
    def preserve(self, cursor_factory):
        """Restore caller settings outside the per-statement budget hooks.

        An aborted PostgreSQL root cannot execute SET; preserve the original
        error in that case. The caller's mandatory root rollback restores GUCs.
        """
        with closing(cursor_factory()) as cursor:
            cursor.execute(
                "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')"
            )
            settings = cursor.fetchone()

        def restore():
            with closing(cursor_factory()) as cursor:
                for name, value in zip(
                    ("statement_timeout", "lock_timeout"), settings, strict=True
                ):
                    # Values come from PostgreSQL, not task input; quote as SQL
                    # literals for both psycopg and asyncpg's DBAPI adaptation.
                    quoted = value.replace("'", "''")
                    cursor.execute(f"SET LOCAL {name} = '{quoted}'")

        try:
            yield
        except BaseException:
            try:
                restore()
            except Exception:
                # No savepoint/rollback here: provisional host writes belong to
                # the caller, and the original failure must remain observable.
                pass
            raise
        else:
            restore()

    def configure(self, cursor) -> None:
        # Integers are generated locally, never interpolated from task inputs.
        cursor.execute(f"SET LOCAL statement_timeout = {self.milliseconds()}")
        cursor.execute(f"SET LOCAL lock_timeout = {self.milliseconds()}")

    def before_sqlalchemy(self, connection, cursor, statement, parameters, context, many):
        self.configure(cursor)

    def django_execute(self, execute, sql, params, many, context):
        # A query may use a named/server-side cursor, which cannot execute SET.
        with context["connection"].connection.cursor() as control:
            self.configure(control)
        return execute(sql, params, many, context)


#: Counting a large ledger takes longer than a worker's own short budget.
OPERATOR_WAIT_SECONDS = 30.0


def wait_seconds(operation: str, timing: Any) -> float:
    """How long one storage operation may spend on the database."""
    return OPERATOR_WAIT_SECONDS if operation == "ops" else timing.db_wait_seconds


# query_canceled (statement_timeout) and lock_not_available (lock_timeout)
_OUT_OF_TIME = frozenset({"57014", "55P03"})


def out_of_time(error: BaseException | None) -> bool:
    """Whether a driver error is Postgres enforcing a time limit on one statement.

    That statement, and so the transaction, did not happen; a lost connection
    proves nothing of the kind.
    """
    seen: set[int] = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if {getattr(error, "sqlstate", None), getattr(error, "pgcode", None)} & _OUT_OF_TIME:
            return True
        error = getattr(error, "orig", None) or error.__cause__
    return False


@contextmanager
def definite_timeouts():
    """Report a rolled-back, out-of-time operation as the TimeoutError callers may retry."""
    try:
        yield
    except TimeoutError:
        raise
    except Exception as error:
        if out_of_time(error):
            raise TimeoutError("Native database operation exceeded its budget") from error
        raise
