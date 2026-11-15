"""Schedule state on SQLAlchemy. The rules live in ``dewey.scheduling``.

Wrap the caller's session in a store and pass it to the shared operations::

    from dewey import scheduling
    from dewey.sqlalchemy.schedules import SQLAlchemyScheduleStore

    with Session(engine) as session, session.begin():
        scheduling.apply(SQLAlchemyScheduleStore(session))

With an ``AsyncSession``, run the same call through ``session.run_sync``::

    await session.run_sync(lambda sync: scheduling.apply(SQLAlchemyScheduleStore(sync)))
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.orm import Session

from dewey._worker.ledger import CLEAR_NATIVE
from dewey.core.states import TaskStatus
from dewey.scheduling import (
    WRITER_LOCK_KEY,
    Tick,
    definition_hash,
    tick_key_length,
    tick_key_prefix,
)
from dewey.sqlalchemy.models import ScheduleEntryModel as Schedule
from dewey.sqlalchemy.models import TaskEntryModel as Task

_UNSTARTED = (
    TaskStatus.PENDING.value,
    TaskStatus.FAILED.value,
    TaskStatus.DISPATCHING.value,  # claimed, handler not yet running
)
_UNFINISHED = tuple(
    status.value
    for status in (
        TaskStatus.PENDING,
        TaskStatus.DISPATCHING,
        TaskStatus.PROCESSING,
        TaskStatus.FAILED,
    )
)


def _like_prefix(prefix: str) -> str:
    return prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


class SQLAlchemyScheduleStore:
    """One open transaction's view of the schedule table."""

    def __init__(self, session: Session):
        self.session = session

    def now(self) -> datetime:
        return self.session.execute(select(func.clock_timestamp())).scalar_one()

    def due(self, limit: int) -> Sequence[Schedule]:
        now = self.now()
        return (
            self.session.execute(
                select(Schedule)
                .where(
                    Schedule.next_due_at <= now,
                    or_(Schedule.paused.is_(False), Schedule.paused_until <= now),
                )
                .order_by(Schedule.next_due_at, Schedule.name)
                .limit(limit)
                .with_for_update(skip_locked=True)
                .execution_options(populate_existing=True)
            )
            .scalars()
            .all()
        )

    def all(self, *, lock: bool = False) -> Sequence[Schedule]:
        statement = select(Schedule).order_by(Schedule.name)
        if lock:
            self.session.execute(select(func.pg_advisory_xact_lock(WRITER_LOCK_KEY)))
            statement = statement.with_for_update().execution_options(populate_existing=True)
        return self.session.execute(statement).scalars().all()

    def add(
        self, name: str, definition: dict[str, Any], cursor: datetime, next_due: datetime
    ) -> None:
        self.session.add(
            Schedule(
                name=name,
                definition=definition,
                definition_hash=definition_hash(definition),
                cursor_at=cursor,
                next_due_at=next_due,
            )
        )
        self.session.flush()

    def save(self, row: Schedule) -> None:
        self.session.flush()

    def delete(self, row: Schedule) -> None:
        self.session.delete(row)
        self.session.flush()

    def task_unfinished(self, task_id: str) -> bool:
        row = self.session.execute(
            select(Task.status, Task.expires_at).where(Task.id == task_id)
        ).one_or_none()
        if row is None or row.status not in _UNFINISHED:
            return False
        # A tick still waiting past its expiry will never run: it is in nobody's way.
        waiting = row.status in (TaskStatus.PENDING.value, TaskStatus.FAILED.value)
        return not (waiting and row.expires_at is not None and row.expires_at <= self.now())

    def create_tick(self, name: str, definition: Mapping[str, Any], tick: Tick) -> tuple[str, bool]:
        fields = tick.task_fields(name, definition)
        existing = self.session.execute(
            select(Task.id).where(
                Task.task_type == fields["task_type"],
                Task.idempotency_key == fields["idempotency_key"],
            )
        ).scalar()
        if existing is not None:
            return existing, False
        task = Task(**fields, task_metadata=tick.task_metadata(name))
        self.session.add(task)
        self.session.flush()
        return task.id, True

    def _unstarted(self, name: str, definition: Mapping[str, Any]):
        # Waiting for a first attempt or a retry, or claimed by a worker that has
        # not started it: its start checks ownership, so taking the row is safe.
        return (
            Task.idempotency_key.like(_like_prefix(tick_key_prefix(name)), escape="\\"),
            func.length(Task.idempotency_key) == tick_key_length(name),
            Task.status.in_(_UNSTARTED),
        )

    def unstarted_ticks(self, name: str, definition: Mapping[str, Any]) -> int:
        return self.session.execute(
            select(func.count()).select_from(Task).where(*self._unstarted(name, definition))
        ).scalar_one()

    def expire_unstarted_ticks(self, name: str, definition: Mapping[str, Any]) -> int:
        now = self.now()
        result = self.session.execute(
            update(Task)
            .where(*self._unstarted(name, definition))
            .values(
                status=TaskStatus.EXPIRED.value,
                expired_at=now,
                latest_outcome="expired",
                error="Schedule removed",
                updated_at=now,
                dispatching_at=None,
                **CLEAR_NATIVE,
            )
            .execution_options(synchronize_session=False)
        )
        return result.rowcount  # type: ignore[attr-defined]


__all__ = ["SQLAlchemyScheduleStore"]
