"""Postgres LISTEN/NOTIFY helpers for wake-on-insert workers.

These helpers are optional accelerators: Dewey's durable source of truth is
still the task/notification tables. A missed or dropped NOTIFY is harmless as
long as workers retain a periodic fallback poll/sweep.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DEFAULT_WORK_CHANNEL = "dewey_work_available"


@dataclass(frozen=True)
class WorkNotification:
    """A best-effort wake-up payload delivered over Postgres NOTIFY."""

    kind: str
    id: str
    queue: str | None = None


def _is_postgresql_bind(bind: Any) -> bool:
    dialect = getattr(bind, "dialect", None)
    return getattr(dialect, "name", None) == "postgresql"


def _payload(kind: str, entry_id: str, queue: str | None = None) -> str:
    return json.dumps({"kind": kind, "id": entry_id, "queue": queue}, separators=(",", ":"))


def _parse_payload(payload: str) -> WorkNotification:
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return WorkNotification(kind="unknown", id=payload)

    kind = data.get("kind")
    entry_id = data.get("id")
    queue = data.get("queue")
    return WorkNotification(
        kind=kind if isinstance(kind, str) else "unknown",
        id=entry_id if isinstance(entry_id, str) else "",
        queue=queue if isinstance(queue, str) else None,
    )


def notify_work_available(
    session: Session,
    *,
    kind: str,
    entry_id: str,
    queue: str | None = None,
    channel: str = DEFAULT_WORK_CHANNEL,
) -> bool:
    """Schedule a Postgres NOTIFY for commit time when using PostgreSQL.

    Returns ``True`` when a NOTIFY was queued and ``False`` for non-Postgres
    binds. Callers do not need to branch for SQLite test databases.
    """
    bind = session.get_bind()
    if not _is_postgresql_bind(bind):
        return False

    session.execute(
        text("SELECT pg_notify(:channel, :payload)"),
        {"channel": channel, "payload": _payload(kind, entry_id, queue)},
    )
    return True


async def notify_work_available_async(
    session: AsyncSession,
    *,
    kind: str,
    entry_id: str,
    queue: str | None = None,
    channel: str = DEFAULT_WORK_CHANNEL,
) -> bool:
    """Async variant of :func:`notify_work_available`."""
    bind = session.get_bind()
    if not _is_postgresql_bind(bind):
        return False

    await session.execute(
        text("SELECT pg_notify(:channel, :payload)"),
        {"channel": channel, "payload": _payload(kind, entry_id, queue)},
    )
    return True


class AsyncPostgresWorkListener:
    """Dedicated asyncpg-backed listener for Dewey work wake-ups.

    Pass a SQLAlchemy ``AsyncEngine`` that uses the ``postgresql+asyncpg``
    dialect. The listener owns one dedicated connection while entered.
    ``wait()`` returns after the first notification (plus any immediately
    queued siblings) or after the timeout. Workers should still run a fallback
    poll/sweep on timeout to cover delayed tasks and any missed NOTIFY.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        channel: str = DEFAULT_WORK_CHANNEL,
        max_queue_size: int = 1000,
    ) -> None:
        if not _is_postgresql_bind(engine):
            raise ValueError("AsyncPostgresWorkListener requires a PostgreSQL AsyncEngine")
        self.engine = engine
        self.channel = channel
        self._queue: asyncio.Queue[WorkNotification] = asyncio.Queue(maxsize=max_queue_size)
        self._conn: Any | None = None
        self._raw: Any | None = None
        self._driver: Any | None = None

    async def __aenter__(self) -> AsyncPostgresWorkListener:
        self._conn = await self.engine.connect()
        self._raw = await self._conn.get_raw_connection()
        self._driver = getattr(self._raw, "driver_connection", None)
        if self._driver is None or not hasattr(self._driver, "add_listener"):
            await self._conn.close()
            self._conn = None
            raise RuntimeError(
                "AsyncPostgresWorkListener requires an asyncpg-backed SQLAlchemy engine"
            )
        await self._driver.add_listener(self.channel, self._on_notify)
        logger.info("Listening for Dewey work on Postgres channel %s", self.channel)
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._driver is not None and hasattr(self._driver, "remove_listener"):
            await self._driver.remove_listener(self.channel, self._on_notify)
        if self._conn is not None:
            await self._conn.close()
        self._driver = None
        self._raw = None
        self._conn = None

    def _on_notify(self, connection: object, pid: int, channel: str, payload: str) -> None:
        del connection, pid, channel
        try:
            self._queue.put_nowait(_parse_payload(payload))
        except asyncio.QueueFull:
            logger.warning("Dewey work notification queue full; dropping wake payload")

    async def wait(self, timeout: float | None = None) -> list[WorkNotification]:
        """Wait for work notifications, returning an empty list on timeout."""
        try:
            first = await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except TimeoutError:
            return []

        notifications = [first]
        while True:
            try:
                notifications.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                return notifications


__all__ = [
    "DEFAULT_WORK_CHANNEL",
    "AsyncPostgresWorkListener",
    "WorkNotification",
    "notify_work_available",
    "notify_work_available_async",
]
