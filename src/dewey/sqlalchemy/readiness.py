"""Read-only startup checks; never migrate tables or claim work implicitly."""

from __future__ import annotations

from typing import cast

from sqlalchemy import VARCHAR, Table, inspect
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from dewey.sqlalchemy.models import TaskEntryModel


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
    columns = {
        column["name"]: column for column in inspector.get_columns(table.name, schema=schema)
    }
    missing = set(table.columns.keys()) - columns.keys()
    if missing:
        raise ReadinessError("Dewey task_entries schema is outdated; apply application migrations")
    token = columns["execution_token"]
    if (
        not token["nullable"]
        or not isinstance(token["type"], VARCHAR)
        or token["type"].length != 36
    ):
        raise ReadinessError(
            "Dewey execution_token must be nullable varchar(36); review migrations"
        )


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
