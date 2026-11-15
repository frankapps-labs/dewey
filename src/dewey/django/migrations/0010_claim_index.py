from django.contrib.postgres.operations import AddIndexConcurrently
from django.db import migrations, models
from django.db.models.functions import Coalesce


class Migration(migrations.Migration):
    # Built without blocking writes: the ledger stays in use during the upgrade.
    atomic = False

    dependencies = [
        ("dewey", "0009_schedules"),
    ]

    operations = [
        AddIndexConcurrently(
            model_name="taskentry",
            index=models.Index(
                models.F("queue"),
                models.OrderBy(models.F("priority"), descending=True),
                Coalesce("scheduled_for", "created_at"),
                models.F("created_at"),
                models.F("id"),
                condition=models.Q(
                    ("dispatch_token__isnull", True), ("status__in", ["pending", "failed"])
                ),
                name="ix_task_claim",
            ),
        ),
    ]
