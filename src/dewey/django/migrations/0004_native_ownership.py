from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("dewey", "0003_execution_token")]

    operations = [
        migrations.AddField(
            model_name="taskentry",
            name="dispatch_token",
            field=models.CharField(max_length=36, null=True, editable=False),
        ),
        migrations.AddField(
            model_name="taskentry",
            name="worker_id",
            field=models.CharField(max_length=36, null=True, editable=False),
        ),
        migrations.AddField(
            model_name="taskentry",
            name="lease_expires_at",
            field=models.DateTimeField(null=True, editable=False),
        ),
        migrations.AddField(
            model_name="taskentry",
            name="execution_deadline_at",
            field=models.DateTimeField(null=True, editable=False),
        ),
        migrations.AddField(
            model_name="taskentry",
            name="latest_outcome",
            field=models.CharField(max_length=32, null=True, editable=False),
        ),
        migrations.AddIndex(
            model_name="taskentry",
            index=models.Index(
                fields=["lease_expires_at"],
                name="ix_task_native_lease",
                condition=models.Q(lease_expires_at__isnull=False),
            ),
        ),
    ]
