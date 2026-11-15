"""Operations on SQLAlchemy: ``run(session, name, **arguments)``.

The caller owns the transaction. See ``dewey.ops`` for the operations and
their results. With an ``AsyncSession``::

    result = await session.run_sync(run, "queues")
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dewey import ops
from dewey.core.states import TaskStatus
from dewey.policy import registry
from dewey.scheduling import plan as plan_schedules
from dewey.scheduling import status as schedule_status
from dewey.sqlalchemy import queries
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.models import TaskEventModel as Event
from dewey.sqlalchemy.readiness import ReadinessError, check_readiness
from dewey.sqlalchemy.schedules import SQLAlchemyScheduleStore

_UNFINISHED = tuple(status.value for status in TaskStatus if not status.is_terminal)


def _queues(session: Session) -> list[dict[str, Any]]:
    counts: dict[str, dict[str, int]] = {}
    for queue, status, count in session.execute(
        select(Task.queue, Task.status, func.count()).group_by(Task.queue, Task.status)
    ):
        counts.setdefault(queue, dict.fromkeys((s.value for s in TaskStatus), 0))[status] = count
    return [{"queue": queue, **counts[queue]} for queue in sorted(counts)]


def _tasks(
    session: Session,
    status: str | None = None,
    queue: str | None = None,
    task_type: str | None = None,
    limit: int = 50,
    reveal: bool = False,
) -> list[dict[str, Any]]:
    statement = (
        select(Task).order_by(Task.created_at.desc(), Task.id).limit(ops.bounded_limit(limit))
    )
    if status is not None:
        statement = statement.where(Task.status == TaskStatus(status).value)
    if queue is not None:
        statement = statement.where(Task.queue == queue)
    if task_type is not None:
        statement = statement.where(Task.task_type == task_type)
    rows = session.execute(statement.execution_options(populate_existing=True)).scalars()
    return [ops.task_summary(queries._to_dataclass(row), reveal=reveal) for row in rows]


def _show(session: Session, task_id: str, reveal: bool = False) -> dict[str, Any] | None:
    # The caller's session may hold this row from earlier: report the database's version.
    row = session.execute(
        select(Task).where(Task.id == task_id).execution_options(populate_existing=True)
    ).scalar_one_or_none()
    if row is None:
        return None
    task = queries._to_dataclass(row)
    events = session.execute(
        select(Event.kind, Event.attempt, Event.occurred_at)
        .where(Event.task_id == task_id)
        .order_by(Event.occurred_at, Event.id)
    )
    return {
        **ops.task_summary(task, reveal=reveal),
        "history": [
            {"kind": kind, "attempt": attempt, "at": ops.stamp(at)} for kind, attempt, at in events
        ],
    }


def _changed(task) -> dict[str, Any] | None:
    return None if task is None else ops.task_summary(task)


def _doctor(session: Session, queues: Sequence[str] | None = None) -> dict[str, Any]:
    store = SQLAlchemyScheduleStore(session)
    now = store.now()
    waiting: dict[str, list[str]] = {}
    for queue, task_type in session.execute(
        select(Task.queue, Task.task_type).where(Task.status.in_(_UNFINISHED)).distinct().limit(500)
    ):
        waiting.setdefault(queue, []).append(task_type)
    overdue = session.execute(
        select(func.count())
        .select_from(Task)
        .where(Task.lease_expires_at < now - ops.RECOVERY_GRACE)
    ).scalar_one()
    return ops.diagnose(
        now=now,
        workers=queries.get_workers(session, now=now),
        waiting=waiting,
        overdue_leases=overdue,
        schedule_status=schedule_status(store),
        schedule_plan=plan_schedules(store),
        known_task_types=registry.task_types(),
        queues=queues,
    )


def _doctor_or_schema(session: Session, **arguments: Any) -> dict[str, Any]:
    try:  # inspect first: a failed read would poison the caller's transaction
        check_readiness(session)
    except ReadinessError as exc:
        return ops.schema_finding(str(exc))
    return _doctor(session, **arguments)


def run(session: Session, name: str, **arguments: Any) -> Any:
    """Execute one operation in the caller's transaction; the caller commits."""
    return ops.call(
        {
            **ops.schedule_operations(SQLAlchemyScheduleStore(session)),
            "queues": lambda: _queues(session),
            "tasks.list": lambda **kw: _tasks(session, **kw),
            "tasks.show": lambda **kw: _show(session, **kw),
            "tasks.retry": lambda task_id: _changed(queries.retry_task(session, task_id)),
            "tasks.kill": lambda task_id: _changed(queries.kill_task(session, task_id)),
            "workers": lambda: [
                ops.worker_summary(worker)
                for worker in queries.get_workers(
                    session, now=SQLAlchemyScheduleStore(session).now()
                )
            ],
            "doctor": lambda **kw: _doctor_or_schema(session, **kw),
        },
        name,
        **arguments,
    )


__all__ = ["run"]
