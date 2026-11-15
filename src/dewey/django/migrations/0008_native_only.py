# Dewey 0.6: the native worker is the only runner. Release rows that the removed
# broker path left claimed (stop the old workers before applying this), and rename
# the heartbeat model to what now writes it. The table keeps its name.
from django.db import migrations

from dewey.core.upgrade import RESET_LEGACY_CLAIMS_SQL


class Migration(migrations.Migration):
    dependencies = [("dewey", "0007_task_execution_budget")]

    operations = [
        migrations.RunSQL(RESET_LEGACY_CLAIMS_SQL, migrations.RunSQL.noop),
        migrations.RenameModel("DispatcherHeartbeat", "WorkerHeartbeat"),
    ]
