"""Additive ownership migration preserves legacy data on isolated real Postgres."""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.orm import Session

from dewey.core.states import TaskStatus


def test_sqlalchemy_additive_sql_preserves_legacy_row(engine):
    from dewey.sqlalchemy.models import TaskEntryModel

    task_id = str(uuid4())
    with engine.begin() as connection:
        # Recreate the pre-token shape only inside this owned test database/transaction.
        connection.execute(text("ALTER TABLE task_entries DROP COLUMN execution_token"))
        connection.execute(
            text(
                "INSERT INTO task_entries (id, task_type, status, args, kwargs, metadata, queue, "
                "priority, attempts, max_attempts, error, created_at, updated_at) "
                "VALUES (:id, 'synthetic.legacy', 'processing', '[]', '{}', '{}', 'default', "
                "0, 1, 3, 'synthetic-old-error', now(), now())"
            ),
            {"id": task_id},
        )
        before = connection.execute(
            text(
                "SELECT status, attempts, error, created_at, updated_at FROM task_entries WHERE id=:id"
            ),
            {"id": task_id},
        ).one()
        connection.execute(
            text("ALTER TABLE task_entries ADD COLUMN execution_token varchar(36) NULL")
        )
        after = connection.execute(
            text(
                "SELECT status, attempts, error, created_at, updated_at FROM task_entries WHERE id=:id"
            ),
            {"id": task_id},
        ).one()
        assert after == before
    with Session(engine) as session:
        row = session.get(TaskEntryModel, task_id)
        assert row is not None
        assert row.execution_token is None
        assert row.status == TaskStatus.PROCESSING.value
        assert row.attempts == 1
