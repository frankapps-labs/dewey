"""Retention migration is additive/default-off and preserves existing ledger/history."""

import pytest
from django.db import connection
from django.db.migrations.executor import MigrationExecutor

from dewey.django.models import TaskEntry, TaskEvent
from dewey.django.retention import retention_state


@pytest.mark.django_db(transaction=True)
def test_retention_migration_preserves_tasks_and_history_without_enabling_cleanup():
    executor = MigrationExecutor(connection)
    latest = executor.loader.graph.leaf_nodes("dewey")
    old = [("dewey", "0005_task_history")]
    executor.migrate(old)
    try:
        model = executor.loader.project_state(old).apps.get_model("dewey", "TaskEntry")
        row = model.objects.create(task_type="test.retention-upgrade", status="pending")
        model.objects.filter(pk=row.pk).update(status="completed")
        original_fields = [field.name for field in model._meta.concrete_fields]
        before = list(model.objects.values())
        events = list(TaskEvent.objects.values())
        MigrationExecutor(connection).migrate(latest)
        assert (
            list(TaskEntry.objects.values(*original_fields)) == before
            and list(TaskEvent.objects.values()) == events
        )
        state = retention_state()
        assert state["config"] == {"days": 30, "enabled": False, "revision": 0}
        assert state["last_run"] is None and state["audits"] == []
        with connection.cursor() as cursor:
            indexes = connection.introspection.get_constraints(cursor, "task_entries")
            assert "ix_task_completed_retention" in indexes
        MigrationExecutor(connection).migrate(old)
        assert list(model.objects.values()) == before and list(TaskEvent.objects.values()) == events
    finally:
        MigrationExecutor(connection).migrate(latest)
