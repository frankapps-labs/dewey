"""Additive ownership migration preserves legacy data on isolated real Postgres."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from dewey.core.states import TaskStatus


@pytest.mark.django_db(transaction=True)
def test_django_legacy_processing_row_survives_additive_migration_and_recovers():
    from django.core.management import call_command
    from django.db import connection
    from django.db.migrations.executor import MigrationExecutor

    from dewey.django.executor import process_task
    from dewey.django.models import TaskEntry
    from dewey.django.sweep import sweep_stuck

    old_target = [("dewey", "0002_task_expiry_and_dispatcher_heartbeat")]
    new_target = [("dewey", "0003_execution_token")]
    executor = MigrationExecutor(connection)
    executor.migrate(old_target)
    try:
        old_model = executor.loader.project_state(old_target).apps.get_model("dewey", "TaskEntry")
        legacy = old_model.objects.create(
            task_type="synthetic.legacy",
            status=TaskStatus.PROCESSING.value,
            attempts=1,
            max_attempts=3,
            error="synthetic-old-error",
            started_at=datetime.now(UTC) - timedelta(minutes=1),
        )
        fields = ("status", "attempts", "error", "started_at", "created_at", "updated_at")
        before = {field: getattr(legacy, field) for field in fields}
        MigrationExecutor(connection).migrate(new_target)
        row = TaskEntry.objects.get(pk=legacy.pk)
        assert row.execution_token is None
        assert {field: getattr(row, field) for field in fields} == before
        assert sweep_stuck(stuck_threshold_minutes=0) == [str(legacy.pk)]

        def handler():
            owned = TaskEntry.objects.get(pk=legacy.pk)
            assert owned.execution_token
            assert owned.attempts == 2

        assert process_task(str(legacy.pk), handler) is True
        row.refresh_from_db()
        assert row.status == TaskStatus.COMPLETED.value
        assert row.execution_token is None
        call_command("makemigrations", "dewey", check=True, dry_run=True, verbosity=0)
    finally:
        MigrationExecutor(connection).migrate(new_target)


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
