"""History installation is additive, reversible and never guesses old DEAD time."""

import pytest
from django.db import connection
from django.db.migrations.executor import MigrationExecutor

from dewey.django.models import HistoryState, TaskEntry, TaskEvent


@pytest.mark.django_db(transaction=True)
def test_history_migration_preserves_old_dead_without_backfill():
    old_target = [("dewey", "0004_native_ownership")]
    executor = MigrationExecutor(connection)
    latest = executor.loader.graph.leaf_nodes("dewey")
    executor.migrate(old_target)
    try:
        old_model = executor.loader.project_state(old_target).apps.get_model("dewey", "TaskEntry")
        old = old_model.objects.create(
            task_type="test.old-dead",
            status="dead",
            attempts=3,
            max_attempts=3,
            error="TEST-PRIVATE",
        )
        before = {
            f: getattr(old, f)
            for f in (
                "id",
                "status",
                "attempts",
                "max_attempts",
                "error",
                "created_at",
                "updated_at",
            )
        }
        MigrationExecutor(connection).migrate(latest)
        row = TaskEntry.objects.get(pk=old.pk)
        assert {f: getattr(row, f) for f in before} == before
        assert not TaskEvent.objects.filter(task=row).exists()
        assert HistoryState.objects.get(pk="task-events").started_at > row.created_at
        TaskEntry.objects.filter(pk=row.pk).update(status="pending")
        TaskEntry.objects.filter(pk=row.pk).update(status="dead")
        assert TaskEvent.objects.get(task=row).attempt == 3
        MigrationExecutor(connection).migrate(old_target)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM pg_trigger WHERE tgname='dewey_task_event_transition'"
            )
            assert cursor.fetchone()[0] == 0
    finally:
        MigrationExecutor(connection).migrate(latest)
