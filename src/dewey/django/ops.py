"""Operations on the Django ORM: ``run(using, name, **arguments)``.

Call inside ``transaction.atomic(using=...)``. See ``dewey.ops`` for the
operations and their results.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from django.db import connections
from django.db.models import Count

from dewey import ops
from dewey.core.states import TaskStatus
from dewey.django import queries
from dewey.django.models import (
    ScheduleEntry,
    TaskEntry,
    TaskEvent,
    WorkerHeartbeat,
    resolve_db_alias,
)
from dewey.django.schedules import DjangoScheduleStore
from dewey.policy import registry
from dewey.scheduling import plan as plan_schedules
from dewey.scheduling import status as schedule_status

_UNFINISHED = tuple(status.value for status in TaskStatus if not status.is_terminal)


def _queues(using: str) -> list[dict[str, Any]]:
    counts: dict[str, dict[str, int]] = {}
    rows = TaskEntry.objects.using(using).values_list("queue", "status").annotate(n=Count("id"))
    for queue, status, count in rows:
        counts.setdefault(queue, dict.fromkeys((s.value for s in TaskStatus), 0))[status] = count
    return [{"queue": queue, **counts[queue]} for queue in sorted(counts)]


def _tasks(
    using: str,
    status: str | None = None,
    queue: str | None = None,
    task_type: str | None = None,
    limit: int = 50,
    reveal: bool = False,
) -> list[dict[str, Any]]:
    rows = TaskEntry.objects.using(using).order_by("-created_at", "id")
    if status is not None:
        rows = rows.filter(status=TaskStatus(status).value)
    if queue is not None:
        rows = rows.filter(queue=queue)
    if task_type is not None:
        rows = rows.filter(task_type=task_type)
    return [
        ops.task_summary(row.to_dataclass(), reveal=reveal)
        for row in rows[: ops.bounded_limit(limit)]
    ]


def _show(using: str, task_id: str, reveal: bool = False) -> dict[str, Any] | None:
    task = TaskEntry.objects.using(using).filter(id=task_id).first()
    if task is None:
        return None
    events = (
        TaskEvent.objects.using(using)
        .filter(task_id=task_id)
        .order_by("occurred_at", "id")
        .values_list("kind", "attempt", "occurred_at")
    )
    return {
        **ops.task_summary(task.to_dataclass(), reveal=reveal),
        "history": [
            {"kind": kind, "attempt": attempt, "at": ops.stamp(at)} for kind, attempt, at in events
        ],
    }


def _changed(task) -> dict[str, Any] | None:
    return None if task is None else ops.task_summary(task)


def _doctor(using: str, queues: Sequence[str] | None = None) -> dict[str, Any]:
    store = DjangoScheduleStore(using)
    now = store.now()
    tasks = TaskEntry.objects.using(using)
    waiting: dict[str, list[str]] = {}
    for queue, task_type in (
        tasks.filter(status__in=_UNFINISHED).values_list("queue", "task_type").distinct()[:500]
    ):
        waiting.setdefault(queue, []).append(task_type)
    return ops.diagnose(
        now=now,
        workers=queries.get_workers(using=using, now=now),
        waiting=waiting,
        overdue_leases=tasks.filter(lease_expires_at__lt=now - ops.RECOVERY_GRACE).count(),
        schedule_status=schedule_status(store),
        schedule_plan=plan_schedules(store),
        known_task_types=registry.task_types(),
        queues=queues,
    )


def _doctor_or_schema(using: str, **arguments: Any) -> dict[str, Any]:
    # Inspect first: a failed read would poison the caller's transaction.
    connection = connections[using]
    tables = set(connection.introspection.table_names())
    missing = sorted(
        model._meta.db_table
        for model in (TaskEntry, TaskEvent, ScheduleEntry, WorkerHeartbeat)
        if model._meta.db_table not in tables
    )
    if missing:
        return ops.schema_finding(f"missing table(s): {', '.join(missing)}")
    return _doctor(using, **arguments)


def run(using: str | None, name: str, **arguments: Any) -> Any:
    """Execute one operation on ``using``, inside the caller's atomic block."""
    alias = resolve_db_alias(using)
    store = DjangoScheduleStore(alias)
    return ops.call(
        {
            **ops.schedule_operations(store),
            "queues": lambda: _queues(alias),
            "tasks.list": lambda **kw: _tasks(alias, **kw),
            "tasks.show": lambda **kw: _show(alias, **kw),
            "tasks.retry": lambda task_id: _changed(queries.retry_task(task_id, using=alias)),
            "tasks.kill": lambda task_id: _changed(queries.kill_task(task_id, using=alias)),
            "workers": lambda: [
                ops.worker_summary(worker)
                for worker in queries.get_workers(using=alias, now=store.now())
            ],
            "doctor": lambda **kw: _doctor_or_schema(alias, **kw),
        },
        name,
        **arguments,
    )


__all__ = ["run"]
