"""SQLAlchemy dispatch backend — the claim query and its recovery passes."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from sqlalchemy import Engine, or_, select, update
from sqlalchemy.orm import Session, sessionmaker

from dewey.core.states import TaskStatus
from dewey.listen_sync import DEFAULT_WORK_CHANNEL, SyncWorkListener
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
        """Move up to ``limit`` ready rows to DISPATCHING, committed before return.

        Ready means PENDING and due: either unscheduled, or ``scheduled_for`` has
        passed. Ordering is highest ``priority`` first, then by due time, then
        oldest first — so a jumped queue does not starve ordinary work of its
        place in line.
        """
        now = datetime.now(UTC)
        candidates = (
            select(TaskEntryModel.id)
            .where(
                TaskEntryModel.status == TaskStatus.PENDING.value,
                or_(
                    TaskEntryModel.scheduled_for.is_(None),
                    TaskEntryModel.scheduled_for <= now,
                ),
            )
            .order_by(
                TaskEntryModel.priority.desc(),
                TaskEntryModel.scheduled_for.asc().nulls_first(),
                TaskEntryModel.created_at.asc(),
            )
            .limit(limit)
        )
        if self.queues is not None:
            candidates = candidates.where(TaskEntryModel.queue.in_(self.queues))
        if self._supports_skip_locked:
            candidates = candidates.with_for_update(skip_locked=True)

        # Lock first, then update the locked IDs. Folding this into one
        # `UPDATE ... WHERE id IN (SELECT ... LIMIT n FOR UPDATE SKIP LOCKED)` looks
        # tidier and is wrong: Postgres may run that subquery once per candidate row,
        # and every row then finds itself in its own execution's result — the claim
        # silently takes the whole table instead of one batch.
        with self._session_factory() as session:
            task_ids = list(session.execute(candidates).scalars().all())
            if not task_ids:
                session.rollback()
                return []
            claimed = list(
                session.execute(
                    update(TaskEntryModel)
                    .where(
                        TaskEntryModel.id.in_(task_ids),
                        TaskEntryModel.status == TaskStatus.PENDING.value,
                    )
                    .values(status=TaskStatus.DISPATCHING.value, dispatching_at=now)
                    .returning(TaskEntryModel.id)
                ).scalars()
            )
            session.commit()
        # Preserve claim order: RETURNING order is not guaranteed, and the dispatcher
        # should hand out the highest-priority work first.
        claimed_set = set(claimed)
        return [task_id for task_id in task_ids if task_id in claimed_set]

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


__all__ = ["SQLAlchemyDispatchBackend"]
