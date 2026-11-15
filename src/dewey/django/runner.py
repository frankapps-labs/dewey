"""Pull-runner operations in the host's existing outermost atomic transaction."""

from __future__ import annotations

from typing import Any

from django.db import connections, transaction

from dewey._worker import remote
from dewey._worker.budget import DatabaseBudget
from dewey.django.native import NativeDjangoStorage
from dewey.runner import FailureReport, RunnerAssignment, RunnerPolicy, RunnerRenewal


class _Operations(NativeDjangoStorage):
    def __init__(self, policy: RunnerPolicy, using: str):
        types = policy.task_types
        super().__init__(
            owner=policy.owner,
            timing=policy.timing,
            queues=policy.queues,
            using=using,
            task_types=None if types is None else (lambda: types),
        )
        self.policy, self.bounds = policy, []

    def row(self, task_id):
        row = self.rows().select_for_update().filter(id=task_id, queue__in=self.queues).first()
        if row is not None:
            self.bounds.extend(
                v for v in (row.lease_expires_at, row.execution_deadline_at) if v is not None
            )
        return row

    def renew_execution(self, assignment, token):
        row = self.row(assignment.task_id)
        if row is None:
            return None
        changes = remote.renew_changes(row, assignment, self.policy, token, self.now())
        if changes is None:
            return None
        self.apply(row, changes)
        return RunnerRenewal(cancel_requested=row.cancel_requested)

    def request_cancel(self, assignment, token):
        row = self.row(assignment.task_id)
        if row is None:
            return False
        changes = remote.cancellation_changes(row, assignment, self.policy, token, self.now())
        if changes is None:
            return False
        self.apply(row, changes)
        return True

    def release(self, assignment):
        row = self.row(assignment.task_id)
        if row is None:
            return False
        changes = remote.release_changes(row, assignment, self.policy, self.now())
        if changes is None:
            return False
        self.apply(row, changes)
        return True


class DjangoRunner:
    """PROVISIONAL results: caller owns outer atomic commit/rollback and lifetime.

    Rejects autocommit, nested/savepoint and rollback-pending blocks. Use an
    explicitly selected alias and commit before responding or executing anything.
    On ANY error, roll back the entire caller root. No process-stop claim implied.
    """

    def __init__(self, policy: RunnerPolicy, *, using: str = "default"):
        if not isinstance(policy, RunnerPolicy):
            raise ValueError("Expected server-owned RunnerPolicy")
        self.policy, self.using = policy, using

    def _call(self, operation: str, *args: Any) -> Any:
        connection = connections[self.using]
        if (
            connection.vendor != "postgresql"
            or not connection.in_atomic_block
            or connection.savepoint_ids
            or connection.needs_rollback
            or connection.get_autocommit()
        ):
            raise ValueError("Expected an active outermost PostgreSQL atomic block")
        operations = _Operations(self.policy, self.using)
        budget = DatabaseBudget(self.policy.timing.db_wait_seconds)
        with (
            budget.preserve(connection.connection.cursor),
            connection.execute_wrapper(budget.django_execute),
        ):
            with connection.cursor() as cursor:
                cursor.execute("SHOW transaction_isolation")
                if cursor.fetchone()[0] != "read committed":
                    raise ValueError("Runner operations require READ COMMITTED")
            result = getattr(operations, operation)(*args)
            if operation == "claim":
                for assignment in result:
                    operations.row(assignment.task_id)
            if result and operation in (
                "claim",
                "start",
                "finish",
                "renew_execution",
                "request_cancel",
            ):
                remote.assert_live(operations.bounds, operations.now())
            budget.milliseconds()
            if operations.alerts:
                # The host owns the commit; a rollback drops the callback with the work.
                transaction.on_commit(operations.alerts.flush, using=self.using)
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
        """Trusted host request, never physical stop evidence."""
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
