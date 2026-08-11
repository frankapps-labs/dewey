"""Run the Dewey dispatcher: ``python manage.py dewey_dispatcher``."""

from __future__ import annotations

import logging
import signal
from typing import Any

from django.core.management.base import BaseCommand

from dewey.dispatcher import Dispatcher
from dewey.django.conf import get_dispatch_fn, get_settings
from dewey.django.dispatch import DjangoDispatchBackend

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Claim ready Dewey tasks from Postgres and hand their IDs to the configured "
        "transport. Also runs the recovery sweep, without which failed tasks never "
        "become eligible for retry."
    )

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--queues",
            help="Comma-separated queues to serve. Defaults to every queue.",
        )
        parser.add_argument("--batch-size", type=int, help="Rows to claim per round trip.")
        parser.add_argument(
            "--idle-poll",
            type=float,
            dest="idle_poll_seconds",
            help="Maximum seconds to wait for a notification before polling anyway.",
        )
        parser.add_argument(
            "--sweep-interval",
            type=float,
            dest="sweep_interval_seconds",
            help="Seconds between recovery sweeps. 0 disables the sweep.",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Run a single claim-and-dispatch pass, then exit. For cron and smoke tests.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        config = get_settings()
        dispatch_fn = get_dispatch_fn()

        queues = config["QUEUES"]
        if options.get("queues"):
            queues = [queue.strip() for queue in options["queues"].split(",") if queue.strip()]

        sweep_interval = options.get("sweep_interval_seconds")
        if sweep_interval is None:
            sweep_interval = config["SWEEP_INTERVAL_SECONDS"]
        if sweep_interval == 0:
            sweep_interval = None

        backend = DjangoDispatchBackend(
            queues=queues,
            stuck_threshold_minutes=config["STUCK_THRESHOLD_MINUTES"],
            dispatch_timeout_seconds=config["DISPATCH_TIMEOUT_SECONDS"],
            sweep_limit=config["SWEEP_LIMIT"],
            using=config["DATABASE"],
        )
        dispatcher = Dispatcher(
            backend,
            dispatch_fn,
            batch_size=options.get("batch_size") or config["BATCH_SIZE"],
            idle_poll_seconds=options.get("idle_poll_seconds") or config["IDLE_POLL_SECONDS"],
            sweep_interval_seconds=sweep_interval,
        )

        if options.get("once"):
            dispatcher.maybe_sweep()
            dispatched = dispatcher.dispatch_batch()
            backend.close()
            self.stdout.write(self.style.SUCCESS(f"Dispatched {dispatched} task(s)."))
            return

        # SIGTERM is how a container asks us to stop. Finish the current pass and
        # exit rather than dropping a claim mid-flight.
        for signal_name in ("SIGINT", "SIGTERM"):
            signal_number = getattr(signal, signal_name, None)
            if signal_number is not None:
                signal.signal(signal_number, lambda *_: dispatcher.stop())

        self.stdout.write("Dewey dispatcher running. Ctrl-C to stop.")
        dispatcher.run()
        self.stdout.write("Dewey dispatcher stopped.")
