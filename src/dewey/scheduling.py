"""Schedule declarations and the pure rules for which ticks are due.

A schedule turns time into ordinary task rows: one row per tick. This module
holds what code declares and decides, with no database. The storage layers
(``dewey.sqlalchemy.schedules``, ``dewey.django.schedules``) persist a cursor
per schedule and create the rows; ``plan``/``apply`` there reconcile these
declarations with the table.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Literal, Protocol

from dewey.core.cron import Trigger, trigger_from_dict
from dewey.errors import DeweyError
from dewey.policy import resolve_policy
from dewey.serialization import encode_args, encode_kwargs

CatchUp = Literal["skip", "once", "all"]
Overlap = Literal["skip", "allow"]

#: ``catch_up="all"`` creates at most this many ticks after an outage; older
#: ones are recorded as missed.
CATCH_UP_LIMIT = 100
#: ``catch_up="skip"`` still creates a tick this fresh; anything later is missed.
SKIP_GRACE = timedelta(seconds=60)
#: Task rows one pass creates at most; a longer catch-up continues on the next pass.
TICKS_PER_PASS = 25
#: Ticks examined per call, so one transaction stays small after a long outage.
SCAN_LIMIT = 1000

_NAME_LENGTH = 100
# What a task row can hold: a tick that cannot be stored must fail at declaration.
_TASK_TYPE_LENGTH = 100
_QUEUE_LENGTH = 50


class DuplicateScheduleError(DeweyError):
    """Two different schedules were declared under one name."""


@dataclass(frozen=True)
class Schedule:
    """One declared schedule. Its name is its identity, not the handler's path."""

    name: str
    task_type: str
    when: Trigger
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=dict)
    catch_up: CatchUp = "once"
    overlap: Overlap = "skip"
    expire_at_next_tick: bool = True
    queue: str | None = None
    priority: int | None = None

    def definition(self) -> dict[str, Any]:
        """Everything a tick needs, resolved now so creating one needs no handler code.

        Queue, priority and attempt budget come from the task's policy at plan
        time: the process that applies a schedule has the code that declares it;
        the worker that later creates its ticks may not (a rolling deploy).
        """
        policy = resolve_policy(self.task_type)
        definition = {
            "task_type": self.task_type,
            "when": self.when.to_dict(),
            "args": encode_args(self.args),
            "kwargs": encode_kwargs(dict(self.kwargs)),
            "catch_up": self.catch_up,
            "overlap": self.overlap,
            "expire_at_next_tick": self.expire_at_next_tick,
            "queue": policy.queue if self.queue is None else self.queue,
            "priority": policy.priority if self.priority is None else self.priority,
            "max_attempts": policy.max_attempts,
        }
        if policy.execution_timeout_s is not None:
            # Only when set: a schedule applied before this field existed keeps
            # its hash, so an upgrade does not report every schedule as changed.
            definition["execution_timeout_s"] = policy.execution_timeout_s
        return definition


def definition_hash(definition: Mapping[str, Any]) -> str:
    """Stable identity of a definition; operational state (pause, cursor) is not part of it."""
    canonical = json.dumps(definition, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class ScheduleRegistry:
    """Process-local map of schedule name to declaration, filled at import time."""

    def __init__(self) -> None:
        self._schedules: dict[str, Schedule] = {}

    def register(self, schedule: Schedule) -> Schedule:
        existing = self._schedules.get(schedule.name)
        if existing is not None and existing != schedule:
            raise DuplicateScheduleError(
                f"Schedule {schedule.name!r} is already declared for task "
                f"{existing.task_type!r}. A schedule's name is its identity — pick a "
                "distinct one."
            )
        self._schedules[schedule.name] = schedule
        return schedule

    def get(self, name: str) -> Schedule | None:
        return self._schedules.get(name)

    def all(self) -> tuple[Schedule, ...]:
        return tuple(self._schedules[name] for name in sorted(self._schedules))

    def unregister(self, name: str) -> None:
        self._schedules.pop(name, None)

    def clear(self) -> None:
        self._schedules.clear()


#: The process-local registry ``dewey.schedule`` writes to and plan/apply reads.
schedules = ScheduleRegistry()


def schedule(
    name: str,
    *,
    task: str,
    when: Trigger,
    args: Sequence[Any] = (),
    kwargs: Mapping[str, Any] | None = None,
    catch_up: CatchUp = "once",
    overlap: Overlap | None = None,
    expire: Literal["next_tick"] | None = "next_tick",
    queue: str | None = None,
    priority: int | None = None,
) -> Schedule:
    """Declare that ``task`` runs on ``when``. Takes effect at ``dewey schedules apply``.

    ``catch_up`` decides what happens to ticks that came due while nothing ran:
    ``"once"`` runs the latest and records the rest as missed, ``"skip"`` runs
    none, ``"all"`` runs each one (at most 100). ``overlap="skip"`` creates no
    tick while an earlier one of this schedule is unfinished. With
    ``expire="next_tick"`` a tick that has not started when the next is due
    expires instead of running late. ``catch_up="all"`` means every period
    matters, so it defaults to ``overlap="allow"`` and its ticks never expire.
    """
    if not isinstance(name, str) or not name or len(name) > _NAME_LENGTH or "\0" in name:
        raise ValueError(f"A schedule name is 1-{_NAME_LENGTH} characters without NUL")
    if not isinstance(task, str) or not 1 <= len(task) <= _TASK_TYPE_LENGTH:
        raise ValueError(
            f"A schedule needs the task type it creates, 1-{_TASK_TYPE_LENGTH} characters"
        )
    if not isinstance(when, Trigger):
        raise TypeError("when= takes dewey.cron(...) or dewey.every(...)")
    if catch_up not in ("skip", "once", "all"):
        raise ValueError('catch_up is "skip", "once" or "all"')
    every_period_matters = catch_up == "all"
    if overlap is None:
        overlap = "allow" if every_period_matters else "skip"
    if overlap not in ("skip", "allow"):
        raise ValueError('overlap is "skip" or "allow"')
    if expire not in ("next_tick", None):
        raise ValueError('expire is "next_tick" or None')
    if every_period_matters:
        if overlap == "skip":
            raise ValueError('catch_up="all" runs every tick: it needs overlap="allow"')
        expire = None  # a tick that must run cannot expire
    if queue is not None and (not isinstance(queue, str) or not 1 <= len(queue) <= _QUEUE_LENGTH):
        raise ValueError(f"queue is 1-{_QUEUE_LENGTH} characters")
    if priority is not None and (isinstance(priority, bool) or not isinstance(priority, int)):
        raise TypeError("priority is an integer")
    if priority is not None and not -(2**31) <= priority < 2**31:
        raise ValueError("priority must fit a 32-bit integer")
    # A string is not a list of arguments; check the shape before copying anything.
    encode_args(args)
    encode_kwargs(kwargs)
    declared = Schedule(
        name=name,
        task_type=task,
        when=when,
        # Copies: a declaration must not follow later changes to the caller's objects.
        args=tuple(deepcopy(list(args))),
        kwargs=deepcopy(dict(kwargs or {})),
        catch_up=catch_up,
        overlap=overlap,
        expire_at_next_tick=expire == "next_tick",
        queue=queue,
        priority=priority,
    )
    declared.definition()  # arguments must be JSON-safe now, not at 03:15
    return schedules.register(declared)


# --- Which ticks are due ------------------------------------------------------


@dataclass(frozen=True)
class TickPlan:
    """What one pass over a schedule should do. ``more`` means call again."""

    create: tuple[datetime, ...]
    missed: int
    cursor: datetime
    next_due: datetime
    more: bool = False


def plan_ticks(when: Trigger, cursor: datetime, now: datetime, *, catch_up: str) -> TickPlan:
    """Decide the ticks to create for everything due in ``(cursor, now]``.

    ``cursor`` is the last tick already accounted for (or the moment the
    schedule was applied). The caller persists ``plan.cursor`` in the same
    transaction as the rows it creates, which is what makes a tick happen once.
    """
    due: list[datetime] = []
    tick = when.next_after(cursor)
    while tick <= now and len(due) < SCAN_LIMIT:
        due.append(tick)
        tick = when.next_after(tick)
    if not due:
        return TickPlan((), 0, cursor, tick)
    if tick <= now:
        # A longer backlog than one pass examines. Write off all but the newest
        # few, which may still be among the ones to run, and continue next pass.
        kept = due[-CATCH_UP_LIMIT:]
        return TickPlan((), len(due) - len(kept), due[-CATCH_UP_LIMIT - 1], kept[0], more=True)
    if catch_up == "all":
        create = due[-CATCH_UP_LIMIT:]
    elif catch_up == "once" or now - due[-1] <= SKIP_GRACE:
        create = due[-1:]
    else:
        create = []
    return TickPlan(tuple(create), len(due) - len(create), due[-1], tick)


def tick_key(name: str, tick: datetime) -> str:
    """The idempotency key of one tick: one row per schedule per instant."""
    return f"{tick_key_prefix(name)}{tick.strftime('%Y-%m-%dT%H:%M:%SZ')}"


def tick_key_prefix(name: str) -> str:
    return f"schedule:{name}:"


def tick_key_length(name: str) -> int:
    """Every tick key of one schedule has this length: its prefix and a fixed-width instant.

    Matching on it as well keeps "poll" from claiming the ticks of "poll:eu".
    """
    return len(tick_key_prefix(name)) + len("0000-00-00T00:00:00Z")


#: Advisory lock held by whoever changes the set of schedules. Row locks cannot
#: cover a row that does not exist yet, so two concurrent applies would both add it.
WRITER_LOCK_KEY = 0x6465776579736368


@dataclass(frozen=True)
class Tick:
    """One task row to create for a schedule."""

    at: datetime
    expires_at: datetime | None

    def task_fields(self, name: str, definition: Mapping[str, Any]) -> dict[str, Any]:
        """Column values for the tick's task row, the same for every storage."""
        return {
            "task_type": definition["task_type"],
            "args": list(definition["args"]),
            "kwargs": dict(definition["kwargs"]),
            "queue": definition["queue"],
            "priority": definition["priority"],
            "max_attempts": definition["max_attempts"],
            "execution_timeout_s": definition.get("execution_timeout_s"),
            "expires_at": self.expires_at,
            "idempotency_key": tick_key(name, self.at),
        }

    def task_metadata(self, name: str) -> dict[str, Any]:
        return {"schedule": name, "tick": self.at.isoformat()}


@dataclass(frozen=True)
class Advance:
    """One pass over a stored schedule: rows to create and the state to persist."""

    ticks: tuple[Tick, ...]
    missed: int
    cursor: datetime
    next_due: datetime


def advance(definition: Mapping[str, Any], cursor: datetime, now: datetime) -> Advance:
    """Apply a stored definition to the clock. Pure: storage persists the result."""
    when = trigger_from_dict(definition["when"])
    plan = plan_ticks(when, cursor, now, catch_up=definition["catch_up"])
    expires = definition["expire_at_next_tick"]
    ticks = tuple(Tick(at, when.next_after(at) if expires else None) for at in plan.create)
    return Advance(ticks, plan.missed, plan.cursor, plan.next_due)


# --- Plan and apply -----------------------------------------------------------


@dataclass(frozen=True)
class ScheduleChange:
    name: str
    definition: dict[str, Any] | None  # None for a removal
    #: For a removal: ticks created but not yet started.
    unstarted_ticks: int = 0


@dataclass(frozen=True)
class SchedulePlan:
    """The difference between the schedules code declares and the table holds."""

    added: tuple[ScheduleChange, ...] = ()
    changed: tuple[ScheduleChange, ...] = ()
    removed: tuple[ScheduleChange, ...] = ()
    unchanged: tuple[str, ...] = ()

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.changed or self.removed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "added": [change.name for change in self.added],
            "changed": [change.name for change in self.changed],
            "removed": [
                {"name": change.name, "unstarted_ticks": change.unstarted_ticks}
                for change in self.removed
            ],
            "unchanged": list(self.unchanged),
        }


def diff_schedules(
    declared: Sequence[Schedule],
    stored: Mapping[str, str],
    unstarted_ticks: Mapping[str, int] | None = None,
) -> SchedulePlan:
    """Compare declarations with the stored ``{name: definition_hash}``."""
    added, changed, unchanged = [], [], []
    for item in declared:
        definition = item.definition()
        if item.name not in stored:
            added.append(ScheduleChange(item.name, definition))
        elif stored[item.name] != definition_hash(definition):
            changed.append(ScheduleChange(item.name, definition))
        else:
            unchanged.append(item.name)
    names = {item.name for item in declared}
    removed = [
        ScheduleChange(name, None, (unstarted_ticks or {}).get(name, 0))
        for name in sorted(stored.keys() - names)
    ]
    return SchedulePlan(tuple(added), tuple(changed), tuple(removed), tuple(unchanged))


# --- Storage-driven operations --------------------------------------------------


class ScheduleStore(Protocol):
    """What a storage backend provides; every decision above it is shared.

    One store wraps one open transaction. Rows are the backend's own schedule
    objects, read and mutated by attribute, then handed back to ``save``.
    """

    def now(self) -> datetime:
        """The database clock."""
        ...

    def due(self, limit: int) -> Sequence[Any]:
        """Lock and return schedules that are due and not paused, skipping locked ones."""
        ...

    def all(self, *, lock: bool = False) -> Sequence[Any]:
        """Every schedule; with ``lock``, as the only writer until the transaction ends."""
        ...

    def add(self, name: str, definition: dict[str, Any], cursor: datetime, next_due: datetime): ...

    def save(self, row: Any) -> None: ...

    def delete(self, row: Any) -> None: ...

    def task_unfinished(self, task_id: str) -> bool: ...

    def create_tick(self, name: str, definition: Mapping[str, Any], tick: Tick) -> tuple[str, bool]:
        """Insert the tick's task row unless its key exists; return its id and whether it is new."""
        ...

    def unstarted_ticks(self, name: str, definition: Mapping[str, Any]) -> int: ...

    def expire_unstarted_ticks(self, name: str, definition: Mapping[str, Any]) -> int: ...


def create_due_ticks(
    store: ScheduleStore, limit: int = 20, max_ticks: int = TICKS_PER_PASS
) -> list[dict[str, Any]]:
    """Turn due ticks into task rows; safe to call from any number of processes.

    Each schedule is locked, its ticks created and its cursor advanced in the
    caller's one transaction, so a tick becomes exactly one row or none. A pass
    handles at most ``max_ticks`` ticks, so a large catch-up is worked off over
    several short transactions instead of one that may not fit its time budget.
    Returns one JSON-safe report per schedule that created, missed or skipped
    something.
    """
    rows = store.due(limit)
    now = store.now()  # after the locks, not before a wait
    reports = []
    for row in rows:
        if max_ticks <= 0:
            break  # still due: the next pass takes it
        if row.paused:  # a timed pause that has run out
            row.paused, row.paused_until = False, None
        step = advance(row.definition, row.cursor_at, now)
        if len(step.ticks) > max_ticks:
            # Stop at the last tick this pass handles; the rest stay due.
            ticks = step.ticks[:max_ticks]
            when = trigger_from_dict(row.definition["when"])
            step = Advance(ticks, step.missed, ticks[-1].at, when.next_after(ticks[-1].at))
        max_ticks -= len(step.ticks)
        created, skipped = [], 0
        for tick in step.ticks:
            if (
                row.definition["overlap"] == "skip"
                and row.last_task_id is not None
                and store.task_unfinished(row.last_task_id)
            ):
                skipped += 1
                continue
            row.last_task_id, is_new = store.create_tick(row.name, row.definition, tick)
            row.last_fired_at = tick.at
            if is_new:  # the unique key already holds this tick otherwise
                created.append(row.last_task_id)
        row.cursor_at, row.next_due_at = step.cursor, step.next_due
        row.missed_count += step.missed
        row.skipped_count += skipped
        store.save(row)
        if created or step.missed or skipped:
            reports.append(
                {
                    "schedule": row.name,
                    "created": created,
                    "missed": step.missed,
                    "skipped": skipped,
                }
            )
    return reports


def plan(store: ScheduleStore, declared: Sequence[Schedule] | None = None) -> SchedulePlan:
    """What ``apply`` would change. Reads only."""
    declared = schedules.all() if declared is None else declared
    rows = {row.name: row for row in store.all()}
    names = {item.name for item in declared}
    unstarted = {
        name: store.unstarted_ticks(name, row.definition)
        for name, row in rows.items()
        if name not in names
    }
    return diff_schedules(
        declared, {name: row.definition_hash for name, row in rows.items()}, unstarted
    )


def apply(
    store: ScheduleStore,
    declared: Sequence[Schedule] | None = None,
    *,
    keep_pending: bool = False,
) -> SchedulePlan:
    """Make the table match the declarations. The only writer of definitions.

    A new schedule starts from now: no backfill. A changed one keeps its cursor
    and its paused state. A removed one is deleted and its unstarted ticks
    expire, unless ``keep_pending``.
    """
    rows = {row.name: row for row in store.all(lock=True)}
    changes = plan(store, declared)
    now = store.now()
    for change in changes.added:
        assert change.definition is not None
        store.add(
            change.name,
            change.definition,
            now,
            trigger_from_dict(change.definition["when"]).next_after(now),
        )
    for change in changes.changed:
        assert change.definition is not None
        row = rows[change.name]
        row.definition = change.definition
        row.definition_hash = definition_hash(change.definition)
        row.next_due_at = trigger_from_dict(change.definition["when"]).next_after(row.cursor_at)
        store.save(row)
    for change in changes.removed:
        row = rows[change.name]
        if not keep_pending:
            store.expire_unstarted_ticks(row.name, row.definition)
        store.delete(row)
    return changes


def _selected(store: ScheduleStore, names: Sequence[str] | None) -> list[Any]:
    rows = {row.name: row for row in store.all(lock=True)}
    if names is None:
        return list(rows.values())
    unknown = sorted(set(names) - rows.keys())
    if unknown:
        raise ValueError(f"No applied schedule named {', '.join(unknown)}")
    return [rows[name] for name in names]


def pause(
    store: ScheduleStore, names: Sequence[str] | None = None, *, seconds: float | None = None
) -> list[str]:
    """Stop creating ticks for the named schedules (all when ``names`` is None).

    With ``seconds`` the pause ends by itself. Ticks that come due meanwhile
    are handled by each schedule's catch-up policy when it resumes.
    """
    if seconds is not None and seconds <= 0:
        raise ValueError("A timed pause lasts a positive number of seconds")
    until = None if seconds is None else store.now() + timedelta(seconds=seconds)
    rows = _selected(store, names)
    for row in rows:
        row.paused, row.paused_until = True, until
        store.save(row)
    return [row.name for row in rows]


def resume(store: ScheduleStore, names: Sequence[str] | None = None) -> list[str]:
    rows = _selected(store, names)
    for row in rows:
        row.paused, row.paused_until = False, None
        store.save(row)
    return [row.name for row in rows]


def status(store: ScheduleStore) -> list[dict[str, Any]]:
    """Every applied schedule with its clock state, JSON-safe."""
    now = store.now()

    def stamp(value: datetime | None) -> str | None:
        return None if value is None else value.isoformat()

    return [
        {
            "name": row.name,
            "task_type": row.definition["task_type"],
            "when": str(trigger_from_dict(row.definition["when"])),
            "catch_up": row.definition["catch_up"],
            "overlap": row.definition["overlap"],
            "paused": row.paused and (row.paused_until is None or row.paused_until > now),
            "paused_until": stamp(row.paused_until),
            "next_due_at": stamp(row.next_due_at),
            "late": row.next_due_at <= now,
            "last_fired_at": stamp(row.last_fired_at),
            "missed_count": row.missed_count,
            "skipped_count": row.skipped_count,
        }
        for row in sorted(store.all(), key=lambda row: row.name)
    ]


__all__ = [
    "CATCH_UP_LIMIT",
    "WRITER_LOCK_KEY",
    "Advance",
    "DuplicateScheduleError",
    "Schedule",
    "ScheduleChange",
    "SchedulePlan",
    "ScheduleRegistry",
    "ScheduleStore",
    "Tick",
    "TickPlan",
    "advance",
    "apply",
    "create_due_ticks",
    "definition_hash",
    "diff_schedules",
    "pause",
    "plan",
    "plan_ticks",
    "resume",
    "schedule",
    "schedules",
    "status",
    "tick_key",
    "tick_key_length",
    "tick_key_prefix",
]
