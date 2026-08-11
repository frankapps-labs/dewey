# Dewey 0.5: task expiry fields, EXPIRED status, and dispatcher heartbeats.
# Purely additive and reversible — no data migration, no field removal.

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("dewey", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="DispatcherHeartbeat",
            fields=[
                (
                    "instance_id",
                    models.CharField(max_length=36, primary_key=True, serialize=False),
                ),
                ("dewey_version", models.CharField(max_length=50)),
                ("backend", models.CharField(max_length=50)),
                ("database", models.CharField(max_length=200)),
                ("queues", models.JSONField(blank=True, null=True)),
                ("started_at", models.DateTimeField()),
                ("last_seen_at", models.DateTimeField()),
            ],
            options={
                "db_table": "dewey_dispatcher_heartbeats",
                "indexes": [
                    models.Index(fields=["last_seen_at"], name="ix_heartbeat_last_seen"),
                    models.Index(fields=["backend", "database"], name="ix_heartbeat_backend_db"),
                ],
            },
        ),
        migrations.AddField(
            model_name="taskentry",
            name="expires_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="taskentry",
            name="expired_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="taskentry",
            name="initial_scheduled_for",
            field=models.DateTimeField(editable=False, null=True),
        ),
        migrations.AlterField(
            model_name="taskentry",
            name="status",
            field=models.CharField(
                choices=[
                    ("pending", "Pending"),
                    ("dispatching", "Dispatching"),
                    ("processing", "Processing"),
                    ("completed", "Completed"),
                    ("failed", "Failed"),
                    ("dead", "Dead"),
                    ("expired", "Expired"),
                ],
                db_index=True,
                default="pending",
                max_length=20,
            ),
        ),
        migrations.AddIndex(
            model_name="taskentry",
            index=models.Index(
                condition=models.Q(
                    ("expires_at__isnull", False),
                    ("status__in", ["pending", "dispatching", "processing", "failed"]),
                ),
                fields=["expires_at"],
                name="ix_task_expires",
            ),
        ),
    ]
