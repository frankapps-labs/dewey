"""Internal Postgres native ledger adapter, shared by sync and async sessions.

No handler runs in a ledger transaction. AsyncSession.run_sync adapts only these
SQLAlchemy database operations; it never wraps application code in a thread.
Callers must provide dedicated session factories and bounded driver connection
establishment. Starting a worker never creates or migrates tables.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, delete, event, func, or_, select, text, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, sessionmaker

from dewey._worker import ledger
from dewey._worker.budget import DatabaseBudget, definite_timeouts, wait_seconds
from dewey._worker.contracts import Reconciliation, Timing
from dewey._worker.pool import Assignment
from dewey.alerts import AlertOutbox
from dewey.core.heartbeat import DEFAULT_HEARTBEAT_RETENTION_DAYS, heartbeat_values
from dewey.core.logging import extract_trace_context
from dewey.core.states import TaskStatus
from dewey.scheduling import create_due_ticks
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.models import WorkerHeartbeatModel as Heartbeat
from dewey.sqlalchemy.readiness import check_readiness
from dewey.sqlalchemy.schedules import SQLAlchemyScheduleStore

HEARTBEAT_RETENTION = timedelta(days=DEFAULT_HEARTBEAT_RETENTION_DAYS)


def _apply(row: Task, changes: dict[str, Any]) -> None:
    for field, value in changes.items():
        setattr(row, field, value)


def _invalid_ownership():
    fields = [getattr(Task, name) for name in ledger.OWNERSHIP_FIELDS]
    return and_(
        or_(*(field.is_not(None) for field in fields)),
        or_(
            *(field.is_(None) for field in fields),
            Task.status.not_in((TaskStatus.DISPATCHING.value, TaskStatus.PROCESSING.value)),
            and_(Task.status == TaskStatus.PROCESSING.value, Task.execution_token.is_(None)),
        ),
    )


#: Returns the task types a worker may claim; None claims every type in its queues.
TaskTypes = Callable[[], Sequence[str]] | None


class _LedgerOperations:
    def __init__(
        self, owner: str, timing: Timing, queues: Sequence[str], task_types: TaskTypes = None
    ):
        if not owner or len(owner) > 36:
            raise ValueError("Owner must be a nonempty instance ID of at most 36 characters")
        if not queues or any(not queue or len(queue) > 50 for queue in queues):
            raise ValueError("Native storage requires explicit nonempty served queues")
        self.owner = owner
        self.timing = timing
        self.queues = tuple(queues)
        self.task_types = task_types
        self.alerts = AlertOutbox()

    def prepare(self, session: Session) -> None:
        if session.get_bind().dialect.name != "postgresql":
            raise RuntimeError("Native storage requires Postgres")

    def now(self, session: Session) -> datetime:
        return session.execute(select(func.clock_timestamp())).scalar_one()

    def row(self, session: Session, task_id: str) -> Task | None:
        return session.execute(
            select(Task)
            .where(Task.id == task_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        ).scalar_one_or_none()

    def claim(self, session: Session, limit: int) -> list[Assignment]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("Claim limit must be positive")
        # A row whose type this process cannot run stays for a worker that can.
        known = None if self.task_types is None else tuple(self.task_types())
        if known is not None and not known:
            return []
        now = self.now(session)
        # One queue per statement: its filter and order are exactly the claim
        # index (ix_task_claim), so the first rows come straight off it however
        # long the backlog is. Across several queues the best rows win; the
        # others were only locked, and are free again at commit.
        waiting = select(Task).where(
            Task.status.in_(ledger.CLAIMABLE),
            Task.dispatch_token.is_(None),
            Task.worker_id.is_(None),
            Task.lease_expires_at.is_(None),
            Task.execution_deadline_at.is_(None),
            Task.attempts < Task.max_attempts,
            Task.task_type.in_(known) if known is not None else true(),
            or_(Task.scheduled_for.is_(None), Task.scheduled_for <= now),
        )
        rows = [
            row
            for queue in self.queues
            for row in session.execute(
                waiting.where(Task.queue == queue)
                .order_by(
                    Task.priority.desc(),
                    func.coalesce(Task.scheduled_for, Task.created_at),
                    Task.created_at,
                    Task.id,
                )
                .limit(limit)
                .with_for_update(skip_locked=True)
            ).scalars()
        ]
        rows = sorted(rows, key=ledger.claim_order)[:limit]
        # Refresh time after locks, not transaction-start time.
        now = self.now(session)
        assignments = []
        for row in rows:
            if row.expires_at is not None and row.expires_at <= now:
                _apply(
                    row,
                    {
                        "status": TaskStatus.EXPIRED.value,
                        "expired_at": now,
                        "latest_outcome": "expired",
                        "updated_at": now,
                    },
                )
                continue
            changes = ledger.assignment_changes(row, self.owner, now, self.timing)
            _apply(row, changes)
            assert row.dispatch_token is not None
            assignments.append(Assignment(row.id, row.dispatch_token))
        return assignments

    def start(self, session: Session, assignment: Assignment) -> dict[str, Any] | None:
        row = self.row(session, assignment.task_id)
        if row is None:
            return None
        now = self.now(session)
        changes = ledger.admission_changes(row, assignment, self.owner, now, self.timing)
        if changes is None:
            return None
        _apply(row, changes)
        if row.status != TaskStatus.PROCESSING.value:
            self.alerts.task_changed(row, row.latest_outcome)  # out of attempts: a dead letter
            return None
        return {
            "task_type": row.task_type,
            "args": list(row.args),
            "kwargs": dict(row.kwargs),
            "execution_token": row.execution_token,
            "attempts": row.attempts,
            "execution_timeout_s": row.execution_timeout_s,
            "execution_deadline_at": row.execution_deadline_at,
            "execution_seconds_left": ledger.seconds_left(row, now),
            "trace": extract_trace_context(row.task_metadata),
        }

    def finish(
        self,
        session: Session,
        assignment: Assignment,
        token: str,
        failure: BaseException | None = None,
    ) -> bool:
        row = self.row(session, assignment.task_id)
        if row is None:
            return False
        changes = ledger.outcome_changes(
            row, assignment, self.owner, token, self.now(session), failure
        )
        if changes is None:
            return False
        _apply(row, changes)
        self.alerts.task_changed(row, row.latest_outcome)
        return True

    def renew(self, session: Session, assignment: Assignment) -> bool:
        row = self.row(session, assignment.task_id)
        if row is None:
            return False
        changes = ledger.renewal_changes(
            row, assignment, self.owner, self.now(session), self.timing
        )
        if changes is None:
            return False
        _apply(row, changes)
        return True

    def abandon(self, session: Session, assignment: Assignment) -> Reconciliation:
        """Caller must first establish that this local ticket cannot execute."""
        row = self.row(session, assignment.task_id)
        if row is None or not ledger.owns(row, assignment, self.owner):
            return Reconciliation.INVALIDATED
        changes = ledger.recovery_changes(row, self.now(session), stopped=True)
        if changes is not None:
            _apply(row, changes)
            # The same news as when another worker recovers it after a crash.
            self.alerts.task_changed(
                row, row.latest_outcome, recovered=row.latest_outcome != "abandoned"
            )
        return Reconciliation.SETTLED

    def reconcile(self, session: Session, assignment: Assignment) -> Reconciliation:
        return ledger.reconcile(self.row(session, assignment.task_id), assignment, self.owner)

    def recover(self, session: Session, limit: int) -> list[str]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("Recovery limit must be positive")
        self.report_invalid(session)
        now = self.now(session)
        rows = (
            session.execute(
                select(Task)
                .where(
                    Task.queue.in_(self.queues),
                    ~_invalid_ownership(),
                    Task.dispatch_token.is_not(None),
                    or_(Task.lease_expires_at <= now, Task.execution_deadline_at <= now),
                )
                .order_by(Task.lease_expires_at, Task.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            .scalars()
            .all()
        )
        now = self.now(session)
        recovered = []
        for row in rows:
            changes = ledger.recovery_changes(row, now)
            if changes is not None:
                _apply(row, changes)
                recovered.append(row.id)
                # An unstarted assignment handed back is routine, not an incident.
                self.alerts.task_changed(
                    row, row.latest_outcome, recovered=row.latest_outcome != "abandoned"
                )
        return recovered

    def report_invalid(self, session: Session) -> None:
        rows = session.execute(
            select(Task)
            .where(Task.queue.in_(self.queues), _invalid_ownership())
            .order_by(Task.id)
            .limit(ledger.QUARANTINE_SCAN_LIMIT)
        ).scalars()
        for row in rows:
            ledger.report_invalid(row)

    def readiness(self, session: Session) -> None:
        check_readiness(session)
        self.report_invalid(session)
        isolation = session.execute(text("SHOW transaction_isolation")).scalar_one()
        if isolation != "read committed":
            raise RuntimeError("Native storage requires READ COMMITTED")

    def tick(self, session: Session, limit: int = 20) -> list[dict[str, Any]]:
        """Create task rows for due schedule ticks; any number of callers is safe."""
        reports = create_due_ticks(SQLAlchemyScheduleStore(session), limit)
        for report in reports:
            if report["missed"]:
                self.alerts.add(
                    "schedule_tick_missed", schedule=report["schedule"], missed=report["missed"]
                )
        return reports

    def heartbeat(self, session: Session) -> None:
        now = self.now(session)
        values = heartbeat_values(
            self.owner, "sqlalchemy", session.get_bind().engine.url.database or "", self.queues, now
        )
        session.execute(
            insert(Heartbeat)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[Heartbeat.instance_id],
                set_={"last_seen_at": now, "queues": values["queues"]},
            )
        )
        session.execute(delete(Heartbeat).where(Heartbeat.last_seen_at < now - HEARTBEAT_RETENTION))

    def ops(self, session: Session, name: str, arguments: dict[str, Any] | None = None) -> Any:
        from dewey.sqlalchemy.ops import run

        arguments = arguments or {}
        # Only a kill that changes something is news.
        killing = name == "tasks.kill" and run(session, "tasks.show", **arguments)
        result = run(session, name, **arguments)
        if killing and killing["status"] != "dead":
            self.alerts.operation_done(name, result)
        return result

    def call(self, session: Session, operation: str, *args: Any) -> Any:
        allowed = {
            "claim",
            "tick",
            "heartbeat",
            "ops",
            "start",
            "finish",
            "renew",
            "abandon",
            "reconcile",
            "recover",
            "readiness",
        }
        if operation not in allowed:
            raise ValueError("Unknown native storage operation")
        self.prepare(session)
        return getattr(self, operation)(session, *args)


class NativeSQLAlchemyStorage:
    """Every call commits before returning, including read/reconciliation calls."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        owner: str,
        timing: Timing,
        queues: Sequence[str],
        task_types: TaskTypes = None,
    ):
        self.sessions = session_factory
        self.operations = _LedgerOperations(owner, timing, queues, task_types)

    def call(self, operation: str, *args: Any) -> Any:
        budget = DatabaseBudget(wait_seconds(operation, self.operations.timing))
        alerts = self.operations.alerts
        alerts.discard()
        with definite_timeouts(), self.sessions() as session, session.begin():
            connection = session.connection()
            event.listen(connection, "before_cursor_execute", budget.before_sqlalchemy)
            try:
                result = self.operations.call(session, operation, *args)
                session.flush()
                budget.milliseconds()
            finally:
                event.remove(connection, "before_cursor_execute", budget.before_sqlalchemy)
        alerts.flush()  # committed
        return result


class AsyncNativeSQLAlchemyStorage:
    """Same ledger decisions on an async driver; await each committed operation."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        owner: str,
        timing: Timing,
        queues: Sequence[str],
        task_types: TaskTypes = None,
    ):
        self.sessions = session_factory
        self.operations = _LedgerOperations(owner, timing, queues, task_types)

    async def call(self, operation: str, *args: Any) -> Any:
        budget = DatabaseBudget(wait_seconds(operation, self.operations.timing))
        alerts = self.operations.alerts
        alerts.discard()

        def execute(sync):
            connection = sync.connection()
            event.listen(connection, "before_cursor_execute", budget.before_sqlalchemy)
            try:
                result = self.operations.call(sync, operation, *args)
                sync.flush()
                budget.milliseconds()
                return result
            finally:
                event.remove(connection, "before_cursor_execute", budget.before_sqlalchemy)

        with definite_timeouts():
            async with self.sessions() as session, session.begin():
                result = await session.run_sync(execute)
        alerts.flush()  # committed
        return result
