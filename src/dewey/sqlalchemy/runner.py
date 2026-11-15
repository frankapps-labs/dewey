"""Caller-owned PostgreSQL pull-runner transactions, sync and async.

Bind a Session to an already-open Connection root with
join_transaction_mode='rollback_only'. The host joins its reservation, output,
receipt and audit writes in that same root. Commit before acknowledging anything.
"""

from __future__ import annotations

from contextlib import ExitStack
from typing import Any

from sqlalchemy import Connection, event, select, text
from sqlalchemy.engine import Transaction
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, object_mapper

from dewey._worker import remote
from dewey._worker.budget import DatabaseBudget
from dewey.alerts import AlertOutbox
from dewey.runner import FailureReport, RunnerAssignment, RunnerPolicy, RunnerRenewal
from dewey.sqlalchemy.models import Base
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.native import _apply, _LedgerOperations


def _root(session: Session) -> tuple[Connection, Transaction]:
    connection = session.bind
    if (
        not isinstance(connection, Connection)
        or connection.dialect.name != "postgresql"
        or not connection.in_transaction()
        or connection.in_nested_transaction()
        or session.in_nested_transaction()
        or not session.is_active
        or session.join_transaction_mode != "rollback_only"
    ):
        raise ValueError("Expected an active Connection root and rollback_only Session, no nesting")
    root = connection.get_transaction()
    if root is None or not root.is_active:
        raise ValueError("Caller root is not active")
    # Check BEFORE any query/autoflush: default bind alone does not exclude
    # mapper-routed tasks, history, or foreign pending domain objects.
    mappers = set(Base.registry.mappers)
    mappers.update(
        object_mapper(obj)
        for obj in (*session.identity_map.values(), *session.new, *session.dirty, *session.deleted)
    )
    if any(session.get_bind(mapper=mapper) is not connection for mapper in mappers):
        raise ValueError("All participating mappings must use the caller Connection root")
    return connection, root


def _type_filter(policy: RunnerPolicy):
    types = policy.task_types
    return None if types is None else (lambda: types)


class _Operations(_LedgerOperations):
    def __init__(self, policy: RunnerPolicy):
        super().__init__(policy.owner, policy.timing, policy.queues, _type_filter(policy))
        self.policy = policy
        self.bounds = []

    def row(self, session: Session, task_id: str) -> Task | None:
        row = session.execute(
            select(Task)
            .where(Task.id == task_id, Task.queue.in_(self.queues))
            .with_for_update()
            .execution_options(populate_existing=True)
        ).scalar_one_or_none()
        if row is not None:
            self.bounds.extend(
                v for v in (row.lease_expires_at, row.execution_deadline_at) if v is not None
            )
        return row

    def renew_execution(
        self, session: Session, assignment: RunnerAssignment, token: str
    ) -> RunnerRenewal | None:
        row = self.row(session, assignment.task_id)
        if row is None:
            return None
        changes = remote.renew_changes(row, assignment, self.policy, token, self.now(session))
        if changes is None:
            return None
        _apply(row, changes)
        return RunnerRenewal(cancel_requested=row.cancel_requested)

    def request_cancel(self, session: Session, assignment: RunnerAssignment, token: str) -> bool:
        row = self.row(session, assignment.task_id)
        if row is None:
            return False
        changes = remote.cancellation_changes(
            row, assignment, self.policy, token, self.now(session)
        )
        if changes is None:
            return False
        _apply(row, changes)
        return True

    def release(self, session: Session, assignment: RunnerAssignment) -> bool:
        row = self.row(session, assignment.task_id)
        if row is None:
            return False
        changes = remote.release_changes(row, assignment, self.policy, self.now(session))
        if changes is None:
            return False
        _apply(row, changes)
        return True


class SQLAlchemyRunner:
    """Server-side facade; every result is PROVISIONAL until caller-root commit.

    No authentication, network listener, process stop or output validity implied.
    On ANY error the host MUST roll back its root; never catch and commit writes.
    Calls bound SQL timeouts within that root; drivers still need transport bounds.
    """

    def __init__(self, session: Session, policy: RunnerPolicy):
        if not isinstance(policy, RunnerPolicy):
            raise ValueError("Expected server-owned RunnerPolicy")
        self.session, self.policy = session, policy
        self._alerts = AlertOutbox()
        self._watched: Connection | None = None

    def deliver_alerts(self) -> None:
        """Send the alerts for what this runner's calls changed. Call after your commit.

        SQLAlchemy has no hook that runs once a Connection's commit has succeeded,
        so the host that owns the commit says when. Alerts of a root that rolled
        back are dropped; at most the latest 256 are held.
        """
        self._alerts.flush()

    def _drop_on_rollback(self, connection: Connection) -> None:
        if self._watched is not connection:
            event.listen(connection, "rollback", lambda _: self._alerts.discard())
            self._watched = connection

    def _call(self, operation: str, *args: Any) -> Any:
        session = self.session
        connection, root = _root(session)
        operations = _Operations(self.policy)
        operations.alerts = self._alerts
        budget = DatabaseBudget(self.policy.timing.db_wait_seconds)
        with ExitStack() as stack:
            stack.enter_context(budget.preserve(connection.connection.cursor))
            event.listen(connection, "before_cursor_execute", budget.before_sqlalchemy)
            stack.callback(
                event.remove, connection, "before_cursor_execute", budget.before_sqlalchemy
            )
            with session.no_autoflush:
                if (
                    session.execute(text("SHOW transaction_isolation")).scalar_one()
                    != "read committed"
                ):
                    raise ValueError("Runner operations require READ COMMITTED")
                # Only AFTER validating every participating bind. Otherwise a
                # populate_existing refresh could discard a pending host cancel,
                # queue change or other authoritative task write in this root.
                session.flush()
                result = getattr(operations, operation)(session, *args)
                session.flush()
                if operation == "claim":
                    for assignment in result:
                        operations.row(session, assignment.task_id)
                if result and operation in (
                    "claim",
                    "start",
                    "finish",
                    "renew_execution",
                    "request_cancel",
                ):
                    remote.assert_live(operations.bounds, operations.now(session))
                budget.milliseconds()
                if connection.get_transaction() is not root or not root.is_active:
                    raise RuntimeError("Caller root changed during runner operation")
                if self._alerts:
                    self._drop_on_rollback(connection)
                return result

    def check_readiness(self) -> None:
        self._call("readiness")

    def claim(self, limit: int = 1) -> list[RunnerAssignment]:
        remote.bounded_limit(limit)
        return self._call("claim", limit)

    def start(self, assignment: RunnerAssignment) -> dict[str, Any] | None:
        return self._call("start", assignment)

    def renew(self, assignment: RunnerAssignment, execution_token: str) -> RunnerRenewal | None:
        remote.execution_token(execution_token)
        return self._call("renew_execution", assignment, execution_token)

    def request_cancel(self, assignment: RunnerAssignment, execution_token: str) -> bool:
        """Trusted host request, not stop evidence or permission to release resources."""
        remote.execution_token(execution_token)
        return self._call("request_cancel", assignment, execution_token)

    def complete(self, assignment: RunnerAssignment, execution_token: str) -> bool:
        remote.execution_token(execution_token)
        return self._call("finish", assignment, execution_token)

    def fail(
        self, assignment: RunnerAssignment, execution_token: str, report: FailureReport
    ) -> bool:
        remote.execution_token(execution_token)
        if not isinstance(report, FailureReport):
            raise ValueError("Expected a FailureReport")
        return self._call("finish", assignment, execution_token, report.exception())

    def release(self, assignment: RunnerAssignment) -> bool:
        """Give back an unstarted assignment; NEVER abandon processing execution."""
        return self._call("release", assignment)

    def recover(self, limit: int = 100) -> list[str]:
        """Trusted host maintenance only, not an untrusted runner command."""
        remote.bounded_limit(limit)
        return self._call("recover", limit)

    def tick(self, limit: int = 20) -> list[dict[str, Any]]:
        """Trusted host maintenance: create task rows for due schedule ticks.

        Safe from any number of hosts; a tick becomes one row. Returns one report
        per schedule (``schedule``, ``created`` task ids, ``missed``, ``skipped``).
        """
        remote.bounded_limit(limit)
        return self._call("tick", limit)


class AsyncSQLAlchemyRunner:
    """Identical contract on an AsyncConnection-bound rollback_only AsyncSession."""

    def __init__(self, session: AsyncSession, policy: RunnerPolicy):
        self.session, self.policy = session, policy
        self._runner: SQLAlchemyRunner | None = None

    async def _call(self, method: str, *args: Any) -> Any:
        def call(session: Session) -> Any:
            if self._runner is None:  # one runner, so its alerts outlive a single call
                self._runner = SQLAlchemyRunner(session, self.policy)
            return getattr(self._runner, method)(*args)

        return await self.session.run_sync(call)

    def deliver_alerts(self) -> None:
        """Send the alerts for what this runner's calls changed. Call after your commit."""
        if self._runner is not None:
            self._runner.deliver_alerts()

    async def check_readiness(self) -> None:
        await self._call("check_readiness")

    async def claim(self, limit: int = 1) -> list[RunnerAssignment]:
        return await self._call("claim", limit)

    async def start(self, assignment: RunnerAssignment) -> dict[str, Any] | None:
        return await self._call("start", assignment)

    async def renew(
        self, assignment: RunnerAssignment, execution_token: str
    ) -> RunnerRenewal | None:
        return await self._call("renew", assignment, execution_token)

    async def request_cancel(self, assignment: RunnerAssignment, execution_token: str) -> bool:
        return await self._call("request_cancel", assignment, execution_token)

    async def complete(self, assignment: RunnerAssignment, execution_token: str) -> bool:
        return await self._call("complete", assignment, execution_token)

    async def fail(
        self, assignment: RunnerAssignment, execution_token: str, report: FailureReport
    ) -> bool:
        return await self._call("fail", assignment, execution_token, report)

    async def release(self, assignment: RunnerAssignment) -> bool:
        return await self._call("release", assignment)

    async def recover(self, limit: int = 100) -> list[str]:
        return await self._call("recover", limit)

    async def tick(self, limit: int = 20) -> list[dict[str, Any]]:
        """Trusted host maintenance: create task rows for due schedule ticks."""
        remote.bounded_limit(limit)
        return await self._call("tick", limit)
