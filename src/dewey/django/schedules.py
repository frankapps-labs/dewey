"""Schedule state on the Django ORM. The rules live in ``dewey.scheduling``.

Wrap an alias in a store, inside a transaction, and pass it to the shared
operations::

    from django.db import transaction
    from dewey import scheduling
    from dewey.django.schedules import DjangoScheduleStore

    with transaction.atomic():
        scheduling.apply(DjangoScheduleStore())
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from django.db import connections
from django.db.models import Q
from django.db.models.functions import Length

from dewey._worker.ledger import CLEAR_NATIVE
from dewey.core.states import TaskStatus
from dewey.django.models import ScheduleEntry, TaskEntry, resolve_db_alias
from dewey.scheduling import (
    WRITER_LOCK_KEY,
    Tick,
    definition_hash,
    tick_key_length,
    tick_key_prefix,
)

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


class DjangoScheduleStore:
    """One open transaction's view of the schedule table on ``using``."""

    def __init__(self, using: str | None = None):
        self.using = resolve_db_alias(using)
        self.schedules = ScheduleEntry.objects.using(self.using)
        self.tasks = TaskEntry.objects.using(self.using)

    def now(self) -> datetime:
        with connections[self.using].cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            return cursor.fetchone()[0]

    def due(self, limit: int) -> Sequence[ScheduleEntry]:
        now = self.now()
        return list(
            self.schedules.select_for_update(skip_locked=True)
            .filter(Q(paused=False) | Q(paused_until__lte=now), next_due_at__lte=now)
            .order_by("next_due_at", "name")[:limit]
        )

    def all(self, *, lock: bool = False) -> Sequence[ScheduleEntry]:
        rows = self.schedules.order_by("name")
        if lock:
            with connections[self.using].cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", [WRITER_LOCK_KEY])
        return list(rows.select_for_update() if lock else rows)

    def add(
        self, name: str, definition: dict[str, Any], cursor: datetime, next_due: datetime
    ) -> None:
        self.schedules.create(
            name=name,
            definition=definition,
            definition_hash=definition_hash(definition),
            cursor_at=cursor,
            next_due_at=next_due,
        )

    def save(self, row: ScheduleEntry) -> None:
        row.save(using=self.using)

    def delete(self, row: ScheduleEntry) -> None:
        row.delete(using=self.using)

    def task_unfinished(self, task_id: str) -> bool:
        # A tick still waiting past its expiry will never run: it is in nobody's way.
        waiting_expired = Q(
            status__in=(TaskStatus.PENDING.value, TaskStatus.FAILED.value),
            expires_at__lte=self.now(),
        )
        return (
            self.tasks.filter(id=task_id, status__in=_UNFINISHED).exclude(waiting_expired).exists()
        )

    def create_tick(self, name: str, definition: Mapping[str, Any], tick: Tick) -> tuple[str, bool]:
        fields = tick.task_fields(name, definition)
        existing = self.tasks.filter(
            task_type=fields["task_type"], idempotency_key=fields["idempotency_key"]
        ).first()
        if existing is not None:
            return str(existing.pk), False
        return str(self.tasks.create(**fields, metadata=tick.task_metadata(name)).pk), True

    def _unstarted(self, name: str, definition: Mapping[str, Any]):
        # Waiting for a first attempt or a retry, or claimed by a worker that has
        # not started it: its start checks ownership, so taking the row is safe.
        return self.tasks.annotate(key_length=Length("idempotency_key")).filter(
            idempotency_key__startswith=tick_key_prefix(name),
            key_length=tick_key_length(name),
            status__in=_UNSTARTED,
        )

    def unstarted_ticks(self, name: str, definition: Mapping[str, Any]) -> int:
        return self._unstarted(name, definition).count()

    def expire_unstarted_ticks(self, name: str, definition: Mapping[str, Any]) -> int:
        now = self.now()
        return self._unstarted(name, definition).update(
            status=TaskStatus.EXPIRED.value,
            expired_at=now,
            latest_outcome="expired",
            error="Schedule removed",
            updated_at=now,
            dispatching_at=None,
            **CLEAR_NATIVE,
        )


__all__ = ["DjangoScheduleStore"]
