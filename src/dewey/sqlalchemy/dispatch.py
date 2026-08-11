"""SQLAlchemy dispatch backend — the claim query and its recovery passes."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from sqlalchemy import Engine, and_, case, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, sessionmaker

from dewey.core.states import TaskStatus
from dewey.listen_sync import DEFAULT_WORK_CHANNEL, SyncWorkListener
from dewey.sqlalchemy.async_sweep import sweep_async
from dewey.sqlalchemy.listen import AsyncPostgresWorkListener
from dewey.sqlalchemy.models import TaskEntryModel
from dewey.sqlalchemy.sweep import (
    DEFAULT_DISPATCH_TIMEOUT_SECONDS,
    DEFAULT_STUCK_THRESHOLD_MINUTES,
    sweep,
)

logger = logging.getLogger(__name__)


class SQLAlchemyDispatchBackend:
    """Claim, release and sweep against a SQLAlchemy engine.

    Args:
        engine: Engine pointed at your Dewey database. On PostgreSQL the claim uses
            ``FOR UPDATE SKIP LOCKED``, so any number of dispatchers cooperate
            without a leader election.
        queues: Restrict this dispatcher to these queues. ``None`` means all of
            them.
        stuck_threshold_minutes: How long a row may sit in PROCESSING before the
            sweep assumes the worker died.
        dispatch_timeout_seconds: How long a row may sit in DISPATCHING before the
            sweep reclaims it. Must exceed the worst-case wait in your broker.
        channel: Postgres channel to listen on for wake-ups.
        session_factory: Override how sessions are made (tests, custom pooling).
    """

    def __init__(
        self,
        engine: Engine,
        *,
        queues: Sequence[str] | None = None,
        stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
        dispatch_timeout_seconds: int = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
        sweep_limit: int = 100,
        channel: str = DEFAULT_WORK_CHANNEL,
        session_factory: Callable[[], Session] | None = None,
    ) -> None:
        self.engine = engine
        self.queues = list(queues) if queues else None
        self.stuck_threshold_minutes = stuck_threshold_minutes
        self.dispatch_timeout_seconds = dispatch_timeout_seconds
        self.sweep_limit = sweep_limit
        self._session_factory = session_factory or sessionmaker(bind=engine, expire_on_commit=False)
        self._supports_skip_locked = engine.dialect.name == "postgresql"
        if not self._supports_skip_locked:
            logger.warning(
                "Dewey dispatcher: %s does not support SELECT ... FOR UPDATE SKIP "
                "LOCKED. Run a single dispatcher against this database.",
                engine.dialect.name,
            )
        self._listener = SyncWorkListener(self._raw_connection, channel=channel)
        self._listener_opened = False

    # --- claim / release ---

    def claim(self, limit: int) -> list[str]:
        """Atomically expire deadlines and claim due PENDING or retryable FAILED rows."""
        now = datetime.now(UTC)
        due = or_(TaskEntryModel.scheduled_for.is_(None), TaskEntryModel.scheduled_for <= now)
        claimable = or_(
            and_(TaskEntryModel.status == TaskStatus.PENDING.value, due),
            and_(
                TaskEntryModel.status == TaskStatus.FAILED.value,
                TaskEntryModel.scheduled_for <= now,
                TaskEntryModel.attempts < TaskEntryModel.max_attempts,
            ),
        )
        effective_due = func.coalesce(TaskEntryModel.scheduled_for, TaskEntryModel.created_at)

        with self._session_factory() as session:
            expired = select(TaskEntryModel.id).where(
                TaskEntryModel.status.in_([TaskStatus.PENDING.value, TaskStatus.FAILED.value]),
                TaskEntryModel.expires_at.is_not(None),
                TaskEntryModel.expires_at <= now,
            )
            if self.queues is not None:
                expired = expired.where(TaskEntryModel.queue.in_(self.queues))
            if self._supports_skip_locked:
                expired = expired.with_for_update(skip_locked=True)
            expired_ids = list(session.execute(expired.limit(limit)).scalars())
            if expired_ids:
                session.execute(
                    update(TaskEntryModel)
                    .where(TaskEntryModel.id.in_(expired_ids))
                    .values(
                        status=TaskStatus.EXPIRED.value,
                        expired_at=now,
                        dispatching_at=None,
                    )
                )

            candidates = (
                select(TaskEntryModel.id)
                .where(
                    claimable,
                    or_(TaskEntryModel.expires_at.is_(None), TaskEntryModel.expires_at > now),
                )
                .order_by(
                    TaskEntryModel.priority.desc(),
                    effective_due.asc(),
                    TaskEntryModel.created_at.asc(),
                )
                .limit(limit)
            )
            if self.queues is not None:
                candidates = candidates.where(TaskEntryModel.queue.in_(self.queues))
            if self._supports_skip_locked:
                candidates = candidates.with_for_update(skip_locked=True)
            task_ids = list(session.execute(candidates).scalars())
            if not task_ids:
                session.commit()
                return []
            claimed = set(
                session.execute(
                    update(TaskEntryModel)
                    .where(
                        TaskEntryModel.id.in_(task_ids),
                        TaskEntryModel.status.in_(
                            [TaskStatus.PENDING.value, TaskStatus.FAILED.value]
                        ),
                    )
                    .values(status=TaskStatus.DISPATCHING.value, dispatching_at=now)
                    .returning(TaskEntryModel.id)
                ).scalars()
            )
            session.commit()
        return [task_id for task_id in task_ids if task_id in claimed]

    def next_due(self) -> datetime | None:
        """Earliest future schedule or deadline in this dispatcher's queue scope."""
        now = datetime.now(UTC)
        wake_at = case(
            (
                and_(
                    TaskEntryModel.expires_at.is_not(None),
                    TaskEntryModel.expires_at < TaskEntryModel.scheduled_for,
                ),
                TaskEntryModel.expires_at,
            ),
            else_=TaskEntryModel.scheduled_for,
        )
        stmt = select(func.min(wake_at)).where(
            TaskEntryModel.scheduled_for.is_not(None),
            or_(
                TaskEntryModel.status == TaskStatus.PENDING.value,
                and_(
                    TaskEntryModel.status == TaskStatus.FAILED.value,
                    TaskEntryModel.attempts < TaskEntryModel.max_attempts,
                ),
            ),
        )
        if self.queues is not None:
            stmt = stmt.where(TaskEntryModel.queue.in_(self.queues))
        with self._session_factory() as session:
            due_at = session.execute(stmt).scalar_one_or_none()
            session.rollback()
        if due_at is not None and due_at.tzinfo is None:
            due_at = due_at.replace(tzinfo=UTC)
        return due_at if due_at is None or due_at >= now else now

    def release(self, task_ids: Sequence[str]) -> None:
        """Return claimed rows to PENDING, leaving the attempt count untouched."""
        if not task_ids:
            return
        with self._session_factory() as session:
            session.execute(
                update(TaskEntryModel)
                .where(
                    TaskEntryModel.id.in_(list(task_ids)),
                    TaskEntryModel.status == TaskStatus.DISPATCHING.value,
                )
                .values(status=TaskStatus.PENDING.value, dispatching_at=None)
            )
            session.commit()

    # --- recovery ---

    def run_sweep(self) -> dict[str, list[str]]:
        with self._session_factory() as session:
            result = sweep(
                session,
                stuck_threshold_minutes=self.stuck_threshold_minutes,
                dispatch_timeout_seconds=self.dispatch_timeout_seconds,
                limit=self.sweep_limit,
            )
            session.commit()
        return result

    # --- wake-up ---

    def wait_for_work(self, timeout: float) -> bool:
        if not self._listener_opened:
            self._listener_opened = True
            try:
                self._listener.open()
            except Exception:
                logger.warning(
                    "Dewey dispatcher: could not start LISTEN; polling only", exc_info=True
                )
        if self._listener.supported:
            return self._listener.wait(timeout)
        # No listener: the poll interval is the whole wake-up story.
        return _sleep(timeout)

    def close(self) -> None:
        self._listener.close()

    def _raw_connection(self):
        """Take a connection out of the pool and keep it.

        A connection parked in LISTEN cannot serve anything else, so it must not
        be shared with the claim queries. ``detach()`` removes it from the pool
        entirely — without that, closing the listener would close a connection
        SQLAlchemy still believed it owned, and every later checkout of it would
        fail with "connection already closed".
        """
        fairy = self.engine.raw_connection()
        raw = fairy.driver_connection
        # Read the driver connection *before* detaching: detach() clears the fairy's
        # reference, and returning None would silently disable LISTEN.
        fairy.detach()
        return raw


def _sleep(timeout: float) -> bool:
    import time

    time.sleep(max(0.0, timeout))
    return False


class AsyncSQLAlchemyDispatchBackend:
    """Claim, release and sweep against an async SQLAlchemy engine.

    The async twin of :class:`SQLAlchemyDispatchBackend`, for asyncpg deployments that
    should not have to add a synchronous driver and a second engine just to run a
    dispatcher.

    Wake-up uses Dewey's asyncpg listener, which lives on the event loop rather than
    owning a blocking thread. As always, polling is the correctness path.

    Args:
        engine: An ``AsyncEngine``. On PostgreSQL the claim uses
            ``FOR UPDATE SKIP LOCKED``, so any number of dispatchers cooperate.
        queues: Restrict this dispatcher to these queues. ``None`` means all of them.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        queues: Sequence[str] | None = None,
        stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
        dispatch_timeout_seconds: int = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
        sweep_limit: int = 100,
        channel: str = DEFAULT_WORK_CHANNEL,
        session_factory: Callable[[], AsyncSession] | None = None,
    ) -> None:
        self.engine = engine
        self.queues = list(queues) if queues else None
        self.stuck_threshold_minutes = stuck_threshold_minutes
        self.dispatch_timeout_seconds = dispatch_timeout_seconds
        self.sweep_limit = sweep_limit
        self.channel = channel
        self._session_factory = session_factory or async_sessionmaker(
            bind=engine, expire_on_commit=False
        )
        self._supports_skip_locked = engine.dialect.name == "postgresql"
        if not self._supports_skip_locked:
            logger.warning(
                "Dewey dispatcher: %s does not support SELECT ... FOR UPDATE SKIP "
                "LOCKED. Run a single dispatcher against this database.",
                engine.dialect.name,
            )
        self._listener: AsyncPostgresWorkListener | None = None
        self._listener_started = False

    # --- claim / release ---

    async def claim(self, limit: int) -> list[str]:
        """Atomically expire deadlines and claim due PENDING or retryable FAILED rows."""
        now = datetime.now(UTC)
        due = or_(TaskEntryModel.scheduled_for.is_(None), TaskEntryModel.scheduled_for <= now)
        claimable = or_(
            and_(TaskEntryModel.status == TaskStatus.PENDING.value, due),
            and_(
                TaskEntryModel.status == TaskStatus.FAILED.value,
                TaskEntryModel.scheduled_for <= now,
                TaskEntryModel.attempts < TaskEntryModel.max_attempts,
            ),
        )
        effective_due = func.coalesce(TaskEntryModel.scheduled_for, TaskEntryModel.created_at)

        async with self._session_factory() as session:
            expired = select(TaskEntryModel.id).where(
                TaskEntryModel.status.in_([TaskStatus.PENDING.value, TaskStatus.FAILED.value]),
                TaskEntryModel.expires_at.is_not(None),
                TaskEntryModel.expires_at <= now,
            )
            if self.queues is not None:
                expired = expired.where(TaskEntryModel.queue.in_(self.queues))
            if self._supports_skip_locked:
                expired = expired.with_for_update(skip_locked=True)
            result = await session.execute(expired.limit(limit))
            expired_ids = list(result.scalars())
            if expired_ids:
                await session.execute(
                    update(TaskEntryModel)
                    .where(TaskEntryModel.id.in_(expired_ids))
                    .values(
                        status=TaskStatus.EXPIRED.value,
                        expired_at=now,
                        dispatching_at=None,
                    )
                )

            candidates = (
                select(TaskEntryModel.id)
                .where(
                    claimable,
                    or_(TaskEntryModel.expires_at.is_(None), TaskEntryModel.expires_at > now),
                )
                .order_by(
                    TaskEntryModel.priority.desc(),
                    effective_due.asc(),
                    TaskEntryModel.created_at.asc(),
                )
                .limit(limit)
            )
            if self.queues is not None:
                candidates = candidates.where(TaskEntryModel.queue.in_(self.queues))
            if self._supports_skip_locked:
                candidates = candidates.with_for_update(skip_locked=True)
            result = await session.execute(candidates)
            task_ids = list(result.scalars())
            if not task_ids:
                await session.commit()
                return []
            claimed_result = await session.execute(
                update(TaskEntryModel)
                .where(
                    TaskEntryModel.id.in_(task_ids),
                    TaskEntryModel.status.in_([TaskStatus.PENDING.value, TaskStatus.FAILED.value]),
                )
                .values(status=TaskStatus.DISPATCHING.value, dispatching_at=now)
                .returning(TaskEntryModel.id)
            )
            claimed = set(claimed_result.scalars())
            await session.commit()
        return [task_id for task_id in task_ids if task_id in claimed]

    async def next_due(self) -> datetime | None:
        """Earliest future schedule or deadline in this dispatcher's queue scope."""
        now = datetime.now(UTC)
        wake_at = case(
            (
                and_(
                    TaskEntryModel.expires_at.is_not(None),
                    TaskEntryModel.expires_at < TaskEntryModel.scheduled_for,
                ),
                TaskEntryModel.expires_at,
            ),
            else_=TaskEntryModel.scheduled_for,
        )
        stmt = select(func.min(wake_at)).where(
            TaskEntryModel.scheduled_for.is_not(None),
            or_(
                TaskEntryModel.status == TaskStatus.PENDING.value,
                and_(
                    TaskEntryModel.status == TaskStatus.FAILED.value,
                    TaskEntryModel.attempts < TaskEntryModel.max_attempts,
                ),
            ),
        )
        if self.queues is not None:
            stmt = stmt.where(TaskEntryModel.queue.in_(self.queues))
        async with self._session_factory() as session:
            result = await session.execute(stmt)
            due_at = result.scalar_one_or_none()
            await session.rollback()
        if due_at is not None and due_at.tzinfo is None:
            due_at = due_at.replace(tzinfo=UTC)
        return due_at if due_at is None or due_at >= now else now

    async def release(self, task_ids: Sequence[str]) -> None:
        """Return claimed rows to PENDING, leaving the attempt count untouched."""
        if not task_ids:
            return
        async with self._session_factory() as session:
            await session.execute(
                update(TaskEntryModel)
                .where(
                    TaskEntryModel.id.in_(list(task_ids)),
                    TaskEntryModel.status == TaskStatus.DISPATCHING.value,
                )
                .values(status=TaskStatus.PENDING.value, dispatching_at=None)
            )
            await session.commit()

    # --- recovery ---

    async def run_sweep(self) -> dict[str, list[str]]:
        async with self._session_factory() as session:
            result = await sweep_async(
                session,
                stuck_threshold_minutes=self.stuck_threshold_minutes,
                dispatch_timeout_seconds=self.dispatch_timeout_seconds,
                limit=self.sweep_limit,
            )
            await session.commit()
        return result

    # --- wake-up ---

    async def wait_for_work(self, timeout: float) -> bool:
        if not self._listener_started:
            self._listener_started = True
            listener = AsyncPostgresWorkListener(self.engine, channel=self.channel)
            try:
                await listener.__aenter__()
                self._listener = listener
            except Exception:
                logger.warning(
                    "Dewey dispatcher: could not start LISTEN; polling only", exc_info=True
                )
        if self._listener is not None:
            return bool(await self._listener.wait(timeout=timeout))
        await asyncio.sleep(max(0.0, timeout))
        return False

    async def close(self) -> None:
        if self._listener is not None:
            await self._listener.__aexit__(None, None, None)
            self._listener = None


__all__ = ["AsyncSQLAlchemyDispatchBackend", "SQLAlchemyDispatchBackend"]
