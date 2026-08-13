"""Producer-side argument validation shared by every framework integration."""

from __future__ import annotations

from datetime import datetime


def require_timezone_aware(value: datetime | None, field: str) -> None:
    """
    Reject naive datetimes with a stable, field-specific message.

    Every producer path calls this before touching the database, so a bad
    argument can never leave behind a row or a NOTIFY.
    """
    if value is not None and value.utcoffset() is None:
        raise ValueError(f"{field} must be a timezone-aware datetime")
