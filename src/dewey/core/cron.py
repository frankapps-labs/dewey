"""When a schedule fires: cron expressions and fixed intervals. Pure, no database.

Both kinds answer one question, ``next_after(instant)``, in UTC. A cron
expression is evaluated on the wall clock of its timezone, so daylight-saving
changes need a rule:

* A schedule that names its hours (``15 3 * * *``) is anchored to the wall
  clock. A local time that does not exist that day fires once, at the first
  valid instant after the gap. A local time that occurs twice fires once, on
  its first occurrence.
* A schedule that runs every hour (``*/10 * * * *``) follows elapsed time: it
  keeps its cadence through both changes, with no burst and no silent hour.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, runtime_checkable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_MINUTE = timedelta(minutes=1)
# Longer than any daylight-saving shift; how far back to look near a transition.
_TRANSITION_WINDOW = timedelta(hours=3)
# 29 February on a given weekday can be decades apart; anything rarer than
# this is a mistake (30 February), not a schedule.
_SEARCH_YEARS = 40

_MONTHS = {
    name: number
    for number, name in enumerate(
        ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), 1
    )
}
_WEEKDAYS = {
    name: number for number, name in enumerate(("sun", "mon", "tue", "wed", "thu", "fri", "sat"))
}
_MACROS = {
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}
# name, lowest, highest, names
_FIELDS: tuple[tuple[str, int, int, dict[str, int]], ...] = (
    ("minute", 0, 59, {}),
    ("hour", 0, 23, {}),
    ("day of month", 1, 31, {}),
    ("month", 1, 12, _MONTHS),
    ("day of week", 0, 7, _WEEKDAYS),
)


@runtime_checkable
class Trigger(Protocol):
    """Anything a schedule can fire on."""

    def next_after(self, instant: datetime) -> datetime:
        """The first fire time strictly after ``instant``, as an aware UTC datetime."""
        ...

    def to_dict(self) -> dict[str, Any]:
        """A JSON-safe definition that :func:`trigger_from_dict` restores."""
        ...


def _aware_utc(instant: datetime) -> datetime:
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("Schedule instants must be timezone-aware")
    return instant.astimezone(UTC)


def _number(token: str, name: str, low: int, high: int, names: dict[str, int]) -> int:
    value = names.get(token.lower())
    if value is None:
        if not token.isdigit():
            raise ValueError(f"Invalid {name} value {token!r} in cron expression")
        value = int(token)
    if not low <= value <= high:
        raise ValueError(f"{name.capitalize()} value {value} is outside {low}-{high}")
    return value


def _parse_field(text: str, name: str, low: int, high: int, names: dict[str, int]) -> frozenset:
    values: set[int] = set()
    for part in text.split(","):
        base, slash, step_text = part.partition("/")
        if not base or (slash and not step_text.isdigit()) or (slash and int(step_text) < 1):
            raise ValueError(f"Invalid {name} field {text!r} in cron expression")
        step = int(step_text) if slash else 1
        if base == "*":
            first, last = low, high
        else:
            start, dash, end = base.partition("-")
            if not start or (dash and not end):
                raise ValueError(f"Invalid {name} field {text!r} in cron expression")
            first = _number(start, name, low, high, names)
            # "5/15" means "from 5, every 15", like the classic implementations.
            last = _number(end, name, low, high, names) if dash else (high if slash else first)
            if last < first:
                raise ValueError(f"{name.capitalize()} range {base!r} runs backwards")
        values.update(range(first, last + 1, step))
    return frozenset(values)


@dataclass(frozen=True)
class Cron:
    """A standard five-field cron expression evaluated in ``tz``."""

    expression: str
    tz: str = "UTC"
    _minutes: frozenset = field(init=False, repr=False, compare=False)
    _hours: frozenset = field(init=False, repr=False, compare=False)
    _days: frozenset = field(init=False, repr=False, compare=False)
    _months: frozenset = field(init=False, repr=False, compare=False)
    _weekdays: frozenset = field(init=False, repr=False, compare=False)
    _any_day: bool = field(init=False, repr=False, compare=False)
    _any_weekday: bool = field(init=False, repr=False, compare=False)
    _zone: ZoneInfo = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not isinstance(self.expression, str) or not isinstance(self.tz, str):
            raise TypeError("A cron expression and its timezone are strings")
        expression = " ".join(self.expression.split())
        parts = _MACROS.get(expression.lower(), expression).split(" ")
        if len(parts) != 5:
            raise ValueError(
                f"Cron expression {self.expression!r} needs five fields (minute hour "
                "day-of-month month day-of-week); seconds and years are not supported"
            )
        parsed = [_parse_field(text, *spec) for text, spec in zip(parts, _FIELDS, strict=True)]
        try:
            zone = ZoneInfo(self.tz)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"Unknown timezone {self.tz!r}") from exc
        # Normalising the spelling keeps equality and the definition hash stable.
        object.__setattr__(self, "expression", expression)
        for name, value in zip(("_minutes", "_hours", "_days", "_months"), parsed, strict=False):
            object.__setattr__(self, name, value)
        # 0 and 7 are both Sunday; Python's weekday() counts from Monday.
        object.__setattr__(self, "_weekdays", frozenset((day - 1) % 7 for day in parsed[4]))
        # As in classic cron, a stepped star ("*/2") still counts as a star.
        object.__setattr__(self, "_any_day", parts[2].startswith("*"))
        object.__setattr__(self, "_any_weekday", parts[4].startswith("*"))
        object.__setattr__(self, "_zone", zone)
        self._next_wall(datetime(2000, 1, 1))  # reject dates that never occur

    @property
    def _follows_elapsed_time(self) -> bool:
        return len(self._hours) == 24

    def _day_matches(self, moment: datetime) -> bool:
        by_date = moment.day in self._days
        by_weekday = moment.weekday() in self._weekdays
        if self._any_day or self._any_weekday:
            return by_date and by_weekday
        # Both restricted: the classic rule fires when either matches.
        return by_date or by_weekday

    def _next_wall(self, wall: datetime) -> datetime:
        """The first matching naive wall-clock minute strictly after ``wall``."""
        moment = wall.replace(second=0, microsecond=0) + _MINUTE
        limit = moment.year + _SEARCH_YEARS
        while moment.year <= limit:
            if moment.month not in self._months:
                year, month = divmod(moment.month, 12)
                moment = datetime(moment.year + year, month + 1, 1)
            elif not self._day_matches(moment):
                moment = datetime(moment.year, moment.month, moment.day) + timedelta(days=1)
            elif moment.hour not in self._hours:
                moment = moment.replace(minute=0) + timedelta(hours=1)
            elif moment.minute not in self._minutes:
                moment += _MINUTE
            else:
                return moment
        raise ValueError(f"Cron expression {self.expression!r} never fires")

    def _gap_end(self, before: datetime, after: datetime) -> datetime:
        """The first instant on the new offset, between two instants straddling a gap."""
        offset = after.astimezone(self._zone).utcoffset()
        while after - before > _MINUTE:
            middle = before + (after - before) / 2
            middle = middle.replace(second=0, microsecond=0)
            if middle.astimezone(self._zone).utcoffset() == offset:
                after = middle
            else:
                before = middle
        return after

    def _instants(self, wall: datetime) -> tuple[datetime, ...]:
        first = wall.replace(tzinfo=self._zone, fold=0).astimezone(UTC)
        second = wall.replace(tzinfo=self._zone, fold=1).astimezone(UTC)
        if first == second:
            return (first,)
        if first < second:  # the wall time occurs twice
            return (first, second) if self._follows_elapsed_time else (first,)
        # The wall time does not exist: ``second`` is before the gap, ``first`` after.
        return () if self._follows_elapsed_time else (self._gap_end(second, first),)

    def next_after(self, instant: datetime) -> datetime:
        instant = _aware_utc(instant)
        wall = instant.astimezone(self._zone).replace(tzinfo=None)
        offsets = {
            (instant + shift).astimezone(self._zone).utcoffset()
            for shift in (-_TRANSITION_WINDOW, timedelta(0), _TRANSITION_WINDOW)
        }
        # Around a change, wall-clock order and real order disagree: look at every
        # wall time in the window and take the earliest real instant.
        near_change = len(offsets) > 1
        horizon = wall + _TRANSITION_WINDOW
        if near_change:
            wall -= _TRANSITION_WINDOW
        best: datetime | None = None
        while True:
            wall = self._next_wall(wall)
            for candidate in self._instants(wall):
                if candidate > instant and (best is None or candidate < best):
                    best = candidate
            if best is not None and (not near_change or wall >= horizon):
                return best

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "cron", "expression": self.expression, "tz": self.tz}

    def __str__(self) -> str:
        return self.expression if self.tz == "UTC" else f"{self.expression} ({self.tz})"


@dataclass(frozen=True)
class Every:
    """A fixed interval of whole seconds, aligned to the Unix epoch."""

    seconds: int

    def __post_init__(self) -> None:
        if isinstance(self.seconds, bool) or not isinstance(self.seconds, int):
            raise TypeError("An interval is a whole number of seconds")
        if self.seconds < 60:
            raise ValueError("The shortest schedule interval is one minute")

    def next_after(self, instant: datetime) -> datetime:
        elapsed = int(_aware_utc(instant).timestamp()) // self.seconds
        return datetime.fromtimestamp((elapsed + 1) * self.seconds, UTC)

    def to_dict(self) -> dict[str, Any]:
        return {"kind": "every", "seconds": self.seconds}

    def __str__(self) -> str:
        return f"every {timedelta(seconds=self.seconds)}"


def cron(expression: str, tz: str = "UTC") -> Cron:
    """A five-field cron expression: lists, ranges, steps, month and weekday names.

    Day-of-month and day-of-week combine the classic way: when both are
    restricted, either one matching is enough.
    """
    return Cron(expression, tz)


def every(*, days: int = 0, hours: int = 0, minutes: int = 0, seconds: int = 0) -> Every:
    """A fixed interval of at least one minute, e.g. ``every(minutes=5)``."""
    parts = (days, hours, minutes, seconds)
    if any(isinstance(part, bool) or not isinstance(part, int) or part < 0 for part in parts):
        raise TypeError("Interval parts are non-negative whole numbers")
    return Every(days * 86400 + hours * 3600 + minutes * 60 + seconds)


def trigger_from_dict(value: dict[str, Any]) -> Trigger:
    """Restore a trigger from :meth:`Trigger.to_dict`."""
    kind = value.get("kind")
    if kind == "cron":
        return Cron(value["expression"], value["tz"])
    if kind == "every":
        return Every(value["seconds"])
    raise ValueError(f"Unknown schedule trigger kind {kind!r}")


__all__ = ["Cron", "Every", "Trigger", "cron", "every", "trigger_from_dict"]
