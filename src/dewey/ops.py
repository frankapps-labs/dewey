"""The operations contract: what an operator can ask of a ledger, as plain JSON.

Framework-neutral. ``dewey.sqlalchemy.ops.run`` and ``dewey.django.ops.run``
execute an operation by name inside the caller's transaction and return a
JSON-safe value; the ``dewey`` CLI, an admin page or an MCP server are thin
transports over the same names:

=====================  ==========================================================
``queues``             counts by status for every queue
``tasks.list``         ``status``, ``queue``, ``task_type``, ``limit``, ``reveal``
``tasks.show``         ``task_id``, ``reveal`` — one task with its history
``tasks.retry``        ``task_id`` — a failed or dead task back to pending
``tasks.kill``         ``task_id`` — stop retrying; the task becomes dead
``workers``            workers with a fresh heartbeat
``schedules.list``     every applied schedule with its clock state
``schedules.plan``     what ``schedules.apply`` would change
``schedules.apply``    ``keep_pending`` — make the table match the declarations
``schedules.pause``    ``names`` (None: all), ``seconds`` (None: until resumed)
``schedules.resume``   ``names`` (None: all)
``doctor``             ``queues`` — readiness findings; ``ok`` is False on any error
=====================  ==========================================================

Arguments and errors are never part of a result unless ``reveal`` is set:
they can hold personal data, and listings end up in terminals and tickets.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

from dewey import alerts, scheduling
from dewey.core.heartbeat import WorkerHeartbeat
from dewey.core.types import TaskEntry
from dewey.scheduling import SchedulePlan, ScheduleStore

Operation = Callable[..., Any]

#: A lease this far past its expiry means nothing is recovering work.
RECOVERY_GRACE = timedelta(minutes=2)
#: A schedule this far past due, while not paused, means nothing creates ticks.
TICK_GRACE = timedelta(minutes=2)
MAX_LIMIT = 500


def stamp(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def bounded_limit(limit: int) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit is a whole number from 1 to {MAX_LIMIT}")
    return limit


def task_summary(task: TaskEntry, *, reveal: bool = False) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "id": task.id,
        "task_type": task.task_type,
        "queue": task.queue,
        "status": task.status.value,
        "priority": task.priority,
        "attempts": task.attempts,
        "max_attempts": task.max_attempts,
        "schedule": task.metadata.get("schedule") if isinstance(task.metadata, dict) else None,
        "created_at": stamp(task.created_at),
        "scheduled_for": stamp(task.scheduled_for),
        "started_at": stamp(task.started_at),
        "completed_at": stamp(task.completed_at),
        "expires_at": stamp(task.expires_at),
        "has_error": bool(task.error),
    }
    if reveal:
        summary.update(args=task.args, kwargs=task.kwargs, error=task.error)
    return summary


def worker_summary(worker: WorkerHeartbeat) -> dict[str, Any]:
    return {
        "instance_id": worker.instance_id,
        "dewey_version": worker.dewey_version,
        "backend": worker.backend,
        "database": worker.database,
        "queues": list(worker.queues or ()),
        "started_at": stamp(worker.started_at),
        "last_seen_at": stamp(worker.last_seen_at),
    }


def schedule_operations(store: ScheduleStore) -> dict[str, Operation]:
    """The schedule operations, identical for every storage."""

    def plan() -> dict[str, Any]:
        return scheduling.plan(store).to_dict()

    def apply(keep_pending: bool = False) -> dict[str, Any]:
        return scheduling.apply(store, keep_pending=keep_pending).to_dict()

    def pause(names: Sequence[str] | None = None, seconds: float | None = None) -> list[str]:
        return scheduling.pause(store, names, seconds=seconds)

    def resume(names: Sequence[str] | None = None) -> list[str]:
        return scheduling.resume(store, names)

    return {
        "schedules.list": lambda: scheduling.status(store),
        "schedules.plan": plan,
        "schedules.apply": apply,
        "schedules.pause": pause,
        "schedules.resume": resume,
    }


def call(operations: Mapping[str, Operation], name: str, **arguments: Any) -> Any:
    try:
        operation = operations[name]
    except KeyError:
        raise ValueError(
            f"Unknown operation {name!r}. Known: {', '.join(sorted(operations))}"
        ) from None
    if not isinstance(arguments.get("reveal", False), bool):
        # A transport's "false" must not pass for consent to show payloads.
        raise ValueError("reveal is true or false")
    result = operation(**arguments)
    if name == "doctor":
        # Doctor only reads, so there is no commit to wait for.
        for finding in result["findings"]:
            if finding["id"].endswith(("no_worker", "queue_unserved")):
                alerts.emit("worker_heartbeat_stale", message=finding["message"])
    return result


def schema_finding(detail: str) -> dict[str, Any]:
    """The doctor result when the ledger itself cannot be read."""
    message = f"The ledger schema is not usable ({detail}). Apply Dewey's migrations."
    return {
        "ok": False,
        "findings": [{"id": "dewey.doctor.schema", "level": "error", "message": message}],
    }


def diagnose(
    *,
    now: datetime,
    workers: Sequence[WorkerHeartbeat],
    waiting: Mapping[str, Sequence[str]],
    overdue_leases: int,
    schedule_status: Sequence[Mapping[str, Any]],
    schedule_plan: SchedulePlan,
    known_task_types: Sequence[str],
    queues: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Turn facts read from a ledger into readiness findings.

    ``waiting`` maps each queue that has unfinished work to the task types
    waiting in it. ``known_task_types`` and the schedule plan come from this
    process's registries, so run doctor from the application's own code.
    """
    findings: list[dict[str, str]] = []

    def add(finding: str, level: str, message: str) -> None:
        findings.append({"id": f"dewey.doctor.{finding}", "level": level, "message": message})

    served = {queue for worker in workers for queue in worker.queues or ()}
    required = set(queues) if queues else set(waiting)
    if not workers:
        add("no_worker", "error", "No worker has a fresh heartbeat; nothing is running tasks.")
    for queue in sorted(required - served):
        if workers:
            add("queue_unserved", "error", f"No running worker serves queue {queue!r}.")

    known = set(known_task_types)
    unknown = sorted({kind for kinds in waiting.values() for kind in kinds} - known)
    if unknown:
        add(
            "unknown_task_type",
            "error",
            f"Waiting tasks have no registered handler here: {', '.join(unknown)}. "
            "Workers leave them unclaimed.",
        )
    if overdue_leases:
        add(
            "unrecovered_work",
            "error",
            f"{overdue_leases} task(s) hold a lease that expired over "
            f"{int(RECOVERY_GRACE.total_seconds())}s ago; no worker on their queue is recovering.",
        )

    if schedule_plan.has_changes:
        parts = [
            f"{label} {', '.join(change.name for change in changes)}"
            for label, changes in (
                ("to add:", schedule_plan.added),
                ("to change:", schedule_plan.changed),
                ("to remove:", schedule_plan.removed),
            )
            if changes
        ]
        add(
            "schedule_drift",
            "error",
            f"Declared schedules differ from the applied ones ({'; '.join(parts)}). "
            "Run `dewey schedules apply`.",
        )
    for item in schedule_status:
        if item["task_type"] not in known:
            add(
                "schedule_unknown_task",
                "error",
                f"Schedule {item['name']!r} creates {item['task_type']!r}, "
                "which has no registered handler here.",
            )
        due = datetime.fromisoformat(item["next_due_at"])
        if not item["paused"] and due <= now - TICK_GRACE:
            add(
                "schedule_late",
                "error",
                f"Schedule {item['name']!r} has been due since {item['next_due_at']}; "
                "no worker is creating its ticks.",
            )
        if item["paused"]:
            until = f" until {item['paused_until']}" if item["paused_until"] else ""
            add("schedule_paused", "warning", f"Schedule {item['name']!r} is paused{until}.")

    return {
        "ok": not any(finding["level"] == "error" for finding in findings),
        "findings": findings,
    }


__all__ = [
    "bounded_limit",
    "call",
    "diagnose",
    "schedule_operations",
    "schema_finding",
    "stamp",
    "task_summary",
    "worker_summary",
]
