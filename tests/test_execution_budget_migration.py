"""Budget migration preserves existing rows and rejects invalid stored budgets."""

import pytest
from django.db import IntegrityError, connection, transaction
from django.db.migrations.executor import MigrationExecutor


@pytest.mark.django_db(transaction=True)
def test_budget_migration_is_additive_and_validates_database_writes():
    executor = MigrationExecutor(connection)
    latest = executor.loader.graph.leaf_nodes("dewey")
    old = [("dewey", "0006_task_retention")]
    executor.migrate(old)
    try:
        old_model = executor.loader.project_state(old).apps.get_model("dewey", "TaskEntry")
        original = old_model.objects.create(task_type="test.budget-upgrade", attempts=2)
        before = old_model.objects.values().get(pk=original.pk)
        MigrationExecutor(connection).migrate(latest)
        from dewey.django.models import TaskEntry

        row = TaskEntry.objects.get(pk=original.pk)
        assert TaskEntry.objects.values(*before).get(pk=original.pk) == before
        assert row.execution_timeout_s is None
        assert row.cancel_requested is False
        for invalid in (0, -1, float("inf"), float("nan")):
            with pytest.raises(IntegrityError), transaction.atomic():
                TaskEntry.objects.filter(pk=row.pk).update(execution_timeout_s=invalid)
        TaskEntry.objects.filter(pk=row.pk).update(execution_timeout_s=600)
        assert TaskEntry.objects.get(pk=row.pk).execution_timeout_s == 600
    finally:
        MigrationExecutor(connection).migrate(latest)
