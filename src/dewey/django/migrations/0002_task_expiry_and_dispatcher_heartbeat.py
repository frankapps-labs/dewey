# Dewey 0.5: task expiry fields, EXPIRED status, and dispatcher heartbeats.
# The forward schema change is additive. On rollback, 0.5-expired rows are mapped
# to DEAD before the 0.4 status choices are restored.

from django.db import migrations, models


def restore_legacy_terminal_status(apps, schema_editor):
    """Keep terminal 0.5 rows interpretable by the 0.4 runtime on downgrade."""
    task_entry = apps.get_model("dewey", "TaskEntry")
    task_entry.objects.using(schema_editor.connection.alias).filter(status="expired").update(
        status="dead"
    )


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
        migrations.RunPython(
            code=migrations.RunPython.noop,
            reverse_code=restore_legacy_terminal_status,
        ),
        migrations.AddIndex(
            model_name="taskentry",
            index=models.Index(
                condition=models.Q(
                    ("expires_at__isnull", False),
                    ("status__in", ["pending", "dispatching", "failed"]),
                ),
                fields=["expires_at"],
                name="ix_task_expires",
            ),
        ),
    ]
