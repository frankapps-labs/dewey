from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("dewey", "0002_task_expiry_and_dispatcher_heartbeat")]

    operations = [
        migrations.AddField(
            model_name="taskentry",
            name="execution_token",
            field=models.CharField(editable=False, max_length=36, null=True),
        ),
    ]
