"""``manage.py dewey <command>``: the ``dewey`` CLI with this project's settings."""

from __future__ import annotations

import argparse
import sys
from typing import Any

from django.core.management.base import BaseCommand, CommandError, SystemCheckError


class Command(BaseCommand):
    help = "Run a Dewey worker and operate the ledger. See `manage.py dewey --help`."

    def run_from_argv(self, argv: list[str]) -> None:
        # From the shell the CLI owns its own argument parsing and exit statuses.
        from dewey.cli import main
        from dewey.django.worker import worker_settings

        prog = f"{argv[0]} dewey"
        try:
            self.check()  # as every management command does: a misconfigured worker must not start
        except SystemCheckError as exc:
            print(f"{prog}: error: {exc}", file=sys.stderr)
            sys.exit(2)
        sys.exit(main(argv[2:], defaults=worker_settings(), prog=prog))

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("arguments", nargs=argparse.REMAINDER, help="a dewey command line")

    def handle(self, *args: Any, **options: Any) -> None:
        """``call_command("dewey", "queues", "--json")``: output to ``stdout``, failure raises."""
        from dewey.cli import main
        from dewey.django.worker import worker_settings

        status = main(
            options["arguments"],
            defaults=worker_settings(),
            prog="manage.py dewey",
            stdout=self.stdout,
        )
        if status:
            raise CommandError(f"dewey exited with status {status}", returncode=status)
