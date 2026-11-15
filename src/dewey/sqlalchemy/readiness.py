"""Read-only startup checks; never migrate tables or claim work implicitly."""

from __future__ import annotations

from typing import cast

from sqlalchemy import BOOLEAN, TIMESTAMP, VARCHAR, Float, Table, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dewey.sqlalchemy.models import (
    HistoryStateModel,
    ScheduleEntryModel,
    TaskEntryModel,
    TaskEventModel,
)


class ReadinessError(RuntimeError):
    """The application must fix its schema/transaction contract before serving work."""


def _check_connection(connection: Connection) -> None:
    if connection.dialect.name != "postgresql":
        raise ReadinessError("Dewey delivery requires PostgreSQL")
    if connection.get_isolation_level() != "READ COMMITTED":
        raise ReadinessError("Dewey requires READ COMMITTED isolation and dedicated sessions")
    table = cast(Table, TaskEntryModel.__table__)
    inspector = inspect(connection)
    schema = connection.schema_for_object(table)
    if not inspector.has_table(table.name, schema=schema):
        raise ReadinessError("Dewey task_entries table is missing; apply application migrations")
    for model in (ScheduleEntryModel, TaskEventModel, HistoryStateModel):
        other = cast(Table, model.__table__)
        if not inspector.has_table(other.name, schema=connection.schema_for_object(other)):
            raise ReadinessError(
                f"Dewey {other.name} table is missing; apply application migrations"
            )
    # Without it every outcome is still recorded on the row, but `tasks show` has no history.
    installed = connection.execute(
        text(
            "SELECT 1 FROM pg_trigger WHERE tgname = 'dewey_task_event_transition'"
            " AND tgrelid = to_regclass(:table) AND NOT tgisinternal"
        ),
        {"table": f"{schema}.{table.name}" if schema else table.name},
    ).first()
    if installed is None:
        raise ReadinessError(
            "Dewey task history trigger is missing; run "
            "dewey.dashboard.history.install_task_history in a migration"
        )
    columns = {
        column["name"]: column for column in inspector.get_columns(table.name, schema=schema)
    }
    missing = set(table.columns.keys()) - columns.keys()
    if missing:
        raise ReadinessError("Dewey task_entries schema is outdated; apply application migrations")
    for name, length in (
        ("execution_token", 36),
        ("dispatch_token", 36),
        ("worker_id", 36),
        ("latest_outcome", 32),
    ):
        column = columns[name]
        if (
            not column["nullable"]
            or not isinstance(column["type"], VARCHAR)
            or column["type"].length != length
        ):
            raise ReadinessError(
                f"Dewey {name} must be nullable varchar({length}); review migrations"
            )
    if not columns["execution_timeout_s"]["nullable"] or not isinstance(
        columns["execution_timeout_s"]["type"], Float
    ):
        raise ReadinessError("Dewey execution_timeout_s must be nullable float; review migrations")
    if columns["cancel_requested"]["nullable"] or not isinstance(
        columns["cancel_requested"]["type"], BOOLEAN
    ):
        raise ReadinessError("Dewey cancel_requested must be non-null boolean; review migrations")
    for name in ("lease_expires_at", "execution_deadline_at"):
        column = columns[name]
        if (
            not column["nullable"]
            or not isinstance(column["type"], TIMESTAMP)
            or not column["type"].timezone
        ):
            raise ReadinessError(f"Dewey {name} must be nullable timestamptz; review migrations")


def check_readiness(session: Session) -> None:
    """Check schema/isolation at startup in a dedicated session; caller ends its transaction.

    Raises ReadinessError for known configuration/schema mismatches. Connection and
    permission errors propagate. No migrations, commits or task mutations occur.
    """
    _check_connection(session.connection())


async def check_readiness_async(session: AsyncSession) -> None:
    """Async equivalent of check_readiness; caller ends the inspection transaction."""
    connection = await session.connection()
    await connection.run_sync(_check_connection)
