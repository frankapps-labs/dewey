"""One bounded retention batch, invoked by the host's explicit scheduler."""

import json

from django.core.management.base import BaseCommand, CommandError

from dewey.django.retention import RetentionCleanupError, cleanup_completed_batch


class Command(BaseCommand):
    help = "Run one managed completed-task retention batch; no implicit schedule or enablement."

    def add_arguments(self, parser):
        parser.add_argument(
            "--database",
            default=None,
            help="Writable Dewey alias; defaults to the task write router.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Task bound1–1000; independent event bound1000.",
        )

    def handle(self, *args, **options):
        try:
            result = cleanup_completed_batch(
                using=options["database"], batch_size=options["batch_size"]
            )
        except (RetentionCleanupError, ValueError) as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(json.dumps(result, sort_keys=True))
        if result["run"]["status"] in ("blocked", "failed"):
            raise CommandError(
                "Managed retention did not run; inspect the recorded run ID and host safeguards."
            )
