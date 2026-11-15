"""The ``dewey`` command: run a worker and operate a ledger.

Every command takes the application factory the worker uses
(``--app module:function`` or ``DEWEY_APP``), so it talks to the same database
with the same handlers and schedules registered. Django projects use
``manage.py dewey ...`` instead, which needs no factory.

Operator commands are thin wrappers over ``dewey.ops``; ``--json`` prints the
operation's result unchanged.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from uuid import uuid4

from dewey import alerts
from dewey.core.states import TaskStatus

# Exit statuses beyond the worker's own (0, 70, 71).
NOT_OK = 1  # doctor found errors, or the task does not exist
CHANGES = 2  # `schedules plan` found differences; also argparse's usage errors

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def duration(text: str) -> float:
    """``90s``, ``10m``, ``2h`` or ``1d`` as seconds."""
    match = re.fullmatch(r"(\d+)([smhd])", text)
    if match is None or int(match[1]) == 0:
        raise argparse.ArgumentTypeError("a duration looks like 90s, 10m, 2h or 1d")
    return float(int(match[1]) * _UNITS[match[2]])


def names(text: str) -> list[str]:
    values = [part.strip() for part in text.split(",") if part.strip()]
    if not values:
        raise argparse.ArgumentTypeError("expected a comma-separated list")
    return values


def build_parser(prog: str, *, needs_app: bool) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=prog, description="Run and operate Dewey.")
    if needs_app:
        parser.add_argument(
            "--app",
            default=os.environ.get("DEWEY_APP"),
            help="application factory, module:function (default: $DEWEY_APP)",
        )
    parser.add_argument(
        "--config", type=Path, help="worker settings as JSON; flags override the file"
    )
    parser.add_argument("--json", action="store_true", help="print results as JSON")
    # Also accepted after the command (`dewey run --app ...`, `dewey queues --json`),
    # without resetting there what was given before it.
    common = argparse.ArgumentParser(add_help=False, argument_default=argparse.SUPPRESS)
    if needs_app:
        common.add_argument("--app", help=argparse.SUPPRESS)
    common.add_argument("--config", type=Path, help=argparse.SUPPRESS)
    common.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    commands = parser.add_subparsers(dest="command", required=True, metavar="command")

    run = commands.add_parser("run", parents=[common], help="run a worker until stopped")
    run.add_argument("--queues", type=names, help="comma-separated queues to serve")
    run.add_argument("--concurrency", type=int, help="task processes")
    run.add_argument(
        "--auto-apply-schedules",
        action="store_true",
        default=None,
        help="apply declared schedules at start (single-node installs)",
    )

    commands.add_parser("queues", parents=[common], help="counts by status for every queue")
    commands.add_parser("workers", parents=[common], help="workers with a fresh heartbeat")

    def task_filters(target: argparse.ArgumentParser, *, status: bool) -> None:
        if status:
            target.add_argument("--status", choices=[s.value for s in TaskStatus])
        target.add_argument("--queue")
        target.add_argument("--type", dest="task_type")
        target.add_argument("--limit", type=int, default=50)
        target.add_argument("--reveal", action="store_true", help="include arguments and errors")

    tasks = commands.add_parser("tasks", parents=[common], help="list, show, retry or kill tasks")
    task_commands = tasks.add_subparsers(dest="action", required=True, metavar="action")
    task_filters(
        task_commands.add_parser("list", parents=[common], help="newest first"), status=True
    )
    show = task_commands.add_parser("show", parents=[common], help="one task with its history")
    show.add_argument("task_id")
    show.add_argument("--reveal", action="store_true", help="include arguments and the error")
    for action, text in (("retry", "back to pending"), ("kill", "stop retrying")):
        task_commands.add_parser(action, parents=[common], help=text).add_argument("task_id")

    dlq = commands.add_parser("dlq", parents=[common], help="dead letters")
    dlq_commands = dlq.add_subparsers(dest="action", required=True, metavar="action")
    task_filters(
        dlq_commands.add_parser("list", parents=[common], help="dead tasks, newest first"),
        status=False,
    )
    dlq_commands.add_parser(
        "retry", parents=[common], help="a dead task back to pending"
    ).add_argument("task_id")

    schedules = commands.add_parser(
        "schedules", parents=[common], help="list, plan, apply, pause or resume"
    )
    schedule_commands = schedules.add_subparsers(dest="action", required=True, metavar="action")
    schedule_commands.add_parser(
        "list", parents=[common], help="applied schedules and their clocks"
    )
    schedule_commands.add_parser(
        "plan",
        parents=[common],
        help=f"show what apply would change; exits {CHANGES} when it would",
    )
    apply = schedule_commands.add_parser(
        "apply", parents=[common], help="make the table match the code"
    )
    apply.add_argument(
        "--keep-pending",
        action="store_true",
        help="leave a removed schedule's unstarted ticks to run",
    )
    for action in ("pause", "resume"):
        target = schedule_commands.add_parser(action, parents=[common])
        target.add_argument("names", nargs="*", metavar="NAME")
        target.add_argument("--all", action="store_true", help="every schedule")
        if action == "pause":
            target.add_argument(
                "--for",
                dest="seconds",
                type=duration,
                metavar="DURATION",
                help="resume by itself after e.g. 10m; ticks due meanwhile follow catch-up",
            )

    doctor = commands.add_parser(
        "doctor", parents=[common], help=f"readiness check; exits {NOT_OK} on errors"
    )
    doctor.add_argument("--queues", type=names, help="queues that must have a running worker")
    return parser


# --- Human output -------------------------------------------------------------


def table(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> str:
    if not rows:
        return "(none)"
    cells = [[("" if row.get(c) is None else str(row[c])) for c in columns] for row in rows]
    widths = [max(len(c), *(len(line[i]) for line in cells)) for i, c in enumerate(columns)]
    lines = ["  ".join(c.upper().ljust(w) for c, w in zip(columns, widths, strict=True))]
    lines += ["  ".join(v.ljust(w) for v, w in zip(line, widths, strict=True)) for line in cells]
    return "\n".join(line.rstrip() for line in lines)


_TASK_COLUMNS = ("id", "task_type", "queue", "status", "attempts", "created_at", "schedule")


def render(command: str, result: Any) -> str:
    """A terminal rendering of an operation's result."""
    if command == "queues":
        return table(result, ("queue", *(status.value for status in TaskStatus)))
    if command in ("tasks.list", "dlq.list"):
        return table(result, _TASK_COLUMNS)
    if command == "workers":
        return table(result, ("instance_id", "backend", "database", "queues", "last_seen_at"))
    if command == "schedules.list":
        return table(
            result,
            ("name", "task_type", "when", "catch_up", "paused", "next_due_at", "missed_count"),
        )
    if command in ("schedules.plan", "schedules.apply"):
        removed = [
            f"{item['name']} ({item['unstarted_ticks']} unstarted)" for item in result["removed"]
        ]
        lines = [
            f"{label}: {', '.join(values) or '-'}"
            for label, values in (
                ("add", result["added"]),
                ("change", result["changed"]),
                ("remove", removed),
                ("unchanged", result["unchanged"]),
            )
        ]
        return "\n".join(lines)
    if command == "doctor":
        lines = [f"{f['level'].upper()} {f['id']}: {f['message']}" for f in result["findings"]]
        return "\n".join([*lines, f"Dewey doctor: {'ready' if result['ok'] else 'not ready'}"])
    if isinstance(result, list):
        return ", ".join(map(str, result)) or "(none)"
    if isinstance(result, dict):
        return "\n".join(f"{key}: {value}" for key, value in result.items())
    return str(result)


# --- Commands -----------------------------------------------------------------


def worker_settings(args: argparse.Namespace, defaults: Mapping[str, Any]) -> dict[str, Any]:
    settings = dict(defaults)
    if args.config is not None:
        settings.update(json.loads(args.config.read_text()))
    if getattr(args, "app", None):
        settings["factory"] = args.app
    for flag in ("queues", "concurrency", "auto_apply_schedules"):
        value = getattr(args, flag, None)
        if value is not None:
            settings[flag] = value
    if "factory" not in settings:
        raise ValueError("an application factory is required: --app module:function or DEWEY_APP")
    return settings


def operation(args: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    """Map a parsed command line to a ``dewey.ops`` operation and its arguments."""
    command, action = args.command, getattr(args, "action", None)
    if command in ("queues", "workers"):
        return command, {}
    if command == "doctor":
        return "doctor", {"queues": args.queues}
    if action == "list" and command in ("tasks", "dlq"):
        return "tasks.list", {
            "status": TaskStatus.DEAD.value if command == "dlq" else args.status,
            "queue": args.queue,
            "task_type": args.task_type,
            "limit": args.limit,
            "reveal": args.reveal,
        }
    if command == "tasks" and action == "show":
        return "tasks.show", {"task_id": args.task_id, "reveal": args.reveal}
    if command in ("tasks", "dlq"):
        return f"tasks.{action}", {"task_id": args.task_id}
    if action in ("pause", "resume"):
        if bool(args.names) == args.all:
            raise ValueError(f"name the schedules to {action}, or pass --all")
        arguments: dict[str, Any] = {"names": None if args.all else args.names}
        if action == "pause":
            arguments["seconds"] = args.seconds
        return f"schedules.{action}", arguments
    if action == "apply":
        return "schedules.apply", {"keep_pending": args.keep_pending}
    return f"schedules.{action}", {}


def operate(config: Any, name: str, arguments: Mapping[str, Any]) -> Any:
    """Run one operation through the application's own storage, then release it."""
    from dewey._worker.loop import ChildLoop
    from dewey.worker import WorkerContext
    from dewey.worker._process import application

    alerts.load_alert_hook(config.alert_hook)
    app = application(WorkerContext(config, str(uuid4()), "parent"))
    loop = ChildLoop()
    try:
        return loop.invoke(app.storage.call, "ops", name, dict(arguments))
    finally:
        try:
            loop.invoke(app.close_resources)
        finally:
            loop.close()


def main(
    argv: Sequence[str] | None = None,
    *,
    defaults: Mapping[str, Any] | None = None,
    prog: str = "dewey",
    stdout: Any = None,
) -> int:
    """Entry point. ``defaults`` are worker settings a host framework supplies; results go to ``stdout``."""
    from dewey.worker import WorkerConfig, run_worker

    parser = build_parser(prog, needs_app="factory" not in (defaults or {}))
    args = parser.parse_args(argv)
    try:
        config = WorkerConfig.from_dict(worker_settings(args, defaults or {}))
        if args.command == "run":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
            return run_worker(config)
        name, arguments = operation(args)
        if args.command == "dlq" and args.action == "retry":
            task = operate(config, "tasks.show", arguments)
            if task is not None and task["status"] != TaskStatus.DEAD.value:
                raise ValueError(f"task {args.task_id} is {task['status']}, not dead")
        result = operate(config, name, arguments)
    except (OSError, ValueError, TypeError) as exc:
        parser.error(str(exc))
    except Exception as exc:
        # Usually the database: say what failed without a traceback.
        reason = str(exc).strip().splitlines()[0] if str(exc).strip() else "no details"
        print(f"{prog}: {type(exc).__name__}: {reason}", file=sys.stderr)
        return NOT_OK
    if result is None:
        print(f"No task {args.task_id}", file=sys.stderr)
        return NOT_OK
    label = f"{args.command}.{args.action}" if hasattr(args, "action") else args.command
    text = json.dumps(result, sort_keys=True) if args.json else render(label, result)
    (stdout or sys.stdout).write(text + "\n")
    if name == "doctor":
        return 0 if result["ok"] else NOT_OK
    if name == "schedules.plan" and (result["added"] or result["changed"] or result["removed"]):
        return CHANGES
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
