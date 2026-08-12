"""Active Dewey readiness checks with human and stable JSON output."""

from __future__ import annotations

import json
from typing import Any

from django.core.checks import ERROR, run_checks
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from django.db.migrations.recorder import MigrationRecorder


class Command(BaseCommand):
    help = "Check Dewey configuration, schema, registry, and dispatcher readiness."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--format", choices=("human", "json"), default="human")
        parser.add_argument(
            "--queues",
            help="Comma-separated queues that must have a fresh matching dispatcher.",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        from dewey.django.conf import get_settings
        from dewey.django.queries import get_dispatchers
        from dewey.policy import registry

        findings: list[dict[str, str]] = []

        def add(finding_id: str, level: str, message: str) -> None:
            findings.append({"id": finding_id, "level": level, "message": message})

        for message in run_checks():
            add(message.id, "error" if message.level >= ERROR else "warning", str(message.msg))

        try:
            config = get_settings()
            if not config["DISPATCH"]:
                add(
                    "dewey.doctor.dispatch",
                    "error",
                    "DEWEY['DISPATCH'] is required for dispatcher readiness.",
                )
            alias = config["DATABASE"]
            connection = connections[alias]
            connection.ensure_connection()
            if connection.vendor != "postgresql":
                add("dewey.doctor.database_backend", "error", "Dewey requires PostgreSQL.")
            tables = set(connection.introspection.table_names())
            if "task_entries" not in tables:
                add("dewey.doctor.schema", "error", "task_entries is not reachable.")
            if "dewey_dispatcher_heartbeats" not in tables:
                add(
                    "dewey.doctor.heartbeat_schema",
                    "error",
                    "dewey_dispatcher_heartbeats is not reachable; apply Dewey migrations.",
                )
            applied = set(MigrationRecorder(connection).applied_migrations())
            if ("dewey", "0002_task_expiry_and_dispatcher_heartbeat") not in applied:
                add("dewey.doctor.migrations", "error", "Dewey migration 0002 is not applied.")

            configured_queues = config["QUEUES"]
            requested = (
                tuple(part.strip() for part in options["queues"].split(",") if part.strip())
                if options.get("queues")
                else tuple(configured_queues)
                if configured_queues
                else None
            )
            fresh = get_dispatchers(
                using=alias,
                database=alias,
                queues=requested,
            )
            if requested is None:
                # No queue restriction means readiness for the whole backlog; a
                # scoped dispatcher cannot satisfy that fail-closed requirement.
                fresh = [heartbeat for heartbeat in fresh if heartbeat.queues is None]
            if not fresh:
                queue_text = ", ".join(requested) if requested else "all configured queues"
                add(
                    "dewey.doctor.dispatcher_heartbeat",
                    "error",
                    f"No fresh dispatcher heartbeat for alias {alias!r} and {queue_text}.",
                )
        except Exception as exc:
            add("dewey.doctor.database", "error", f"Database readiness failed: {exc}")

        task_types = registry.task_types()
        if not task_types:
            add(
                "dewey.doctor.handlers",
                "warning",
                "No task handlers are registered after normal app imports. Without an "
                "expected-task manifest, doctor cannot prove registry completeness.",
            )

        errors = [finding for finding in findings if finding["level"] == "error"]
        payload = {"ok": not errors, "findings": findings}
        if options["format"] == "json":
            self.stdout.write(json.dumps(payload, sort_keys=True))
        else:
            if not findings:
                self.stdout.write(self.style.SUCCESS("Dewey doctor: ready"))
            for finding in findings:
                self.stdout.write(
                    f"{finding['level'].upper()} {finding['id']}: {finding['message']}"
                )
            self.stdout.write(f"Dewey doctor: {'ready' if not errors else 'not ready'}")

        if errors:
            raise CommandError("Dewey doctor found readiness errors")
