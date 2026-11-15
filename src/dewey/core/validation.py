"""Producer-side argument validation shared by every framework integration."""

from __future__ import annotations

from datetime import datetime
from math import isfinite


def require_execution_timeout(value: float | None) -> None:
    """A per-attempt budget must be positive and finite, or absent."""
    if value is not None and (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
        or value <= 0
    ):
        raise ValueError("execution_timeout_s must be positive and finite")


def require_timezone_aware(value: datetime | None, field: str) -> None:
    """
    Reject naive datetimes with a stable, field-specific message.

    Every producer path calls this before touching the database, so a bad
    argument can never leave behind a row.
    """
    if value is not None and value.utcoffset() is None:
        raise ValueError(f"{field} must be a timezone-aware datetime")
