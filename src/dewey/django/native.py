"""Internal transactional Postgres native ledger adapter for Django."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

from django.db import connections, transaction
from django.db.models import F, Q
from django.db.models.functions import Coalesce

from dewey._worker import ledger
from dewey._worker.budget import DatabaseBudget, definite_timeouts, wait_seconds
from dewey._worker.contracts import Reconciliation, Timing
from dewey._worker.pool import Assignment
from dewey.alerts import AlertOutbox
from dewey.context import TaskContext
from dewey.core.heartbeat import DEFAULT_HEARTBEAT_RETENTION_DAYS, heartbeat_values
from dewey.core.logging import extract_trace_context
from dewey.core.states import TaskStatus
from dewey.django.models import TaskEntry, WorkerHeartbeat, resolve_db_alias
from dewey.django.schedules import DjangoScheduleStore
from dewey.scheduling import create_due_ticks


def _invalid_ownership():
    tagged = Q(
        *(Q(**{f"{field}__isnull": False}) for field in ledger.OWNERSHIP_FIELDS),
        _connector=Q.OR,
    )
    incomplete = Q(
        *(Q(**{f"{field}__isnull": True}) for field in ledger.OWNERSHIP_FIELDS),
        _connector=Q.OR,
    )
    return Q(
        tagged,
        Q(
            incomplete,
            ~Q(status__in=(TaskStatus.DISPATCHING.value, TaskStatus.PROCESSING.value)),
            Q(status=TaskStatus.PROCESSING.value, execution_token__isnull=True),
            _connector=Q.OR,
        ),
    )


class NativeDjangoStorage:
    """Dedicated committed calls; never run a handler in an atomic block."""

    def __init__(
        self,
        *,
        owner: str,
        timing: Timing,
        queues: Sequence[str],
        using: str = "default",
        task_types: Callable[[], Sequence[str]] | None = None,
    ):
        if not owner or len(owner) > 36:
            raise ValueError("Owner must be a nonempty instance ID of at most 36 characters")
        if not queues or any(not queue or len(queue) > 50 for queue in queues):
            raise ValueError("Native storage requires explicit nonempty served queues")
        self.owner, self.timing, self.queues = owner, timing, tuple(queues)
        self.using = resolve_db_alias(using)
        self.task_types = task_types
        self.alerts = AlertOutbox()

    def now(self):
        with connections[self.using].cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            return cursor.fetchone()[0]

    def rows(self):
        return TaskEntry.objects.using(self.using)

    def row(self, task_id):
        return self.rows().select_for_update().filter(id=task_id).first()

    def apply(self, row, changes):
        for field, value in changes.items():
            setattr(row, field, value)
        row.save(using=self.using, update_fields=list(changes))

    def claim(self, limit):
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("Claim limit must be positive")
        # A row whose type this process cannot run stays for a worker that can.
        rows = self.rows()
        if self.task_types is not None:
            known = tuple(self.task_types())
            if not known:
                return []
            rows = rows.filter(task_type__in=known)
        now = self.now()
        # One queue per statement: its filter and order are exactly the claim
        # index (ix_task_claim), so the first rows come straight off it however
        # long the backlog is. Across several queues the best rows win; the
        # others were only locked, and are free again at commit.
        waiting = rows.select_for_update(skip_locked=True).filter(
            Q(scheduled_for__isnull=True) | Q(scheduled_for__lte=now),
            status__in=ledger.CLAIMABLE,
            dispatch_token__isnull=True,
            worker_id__isnull=True,
            lease_expires_at__isnull=True,
            execution_deadline_at__isnull=True,
            attempts__lt=F("max_attempts"),
        )
        candidates = [
            row
            for queue in self.queues
            for row in waiting.filter(queue=queue).order_by(
                "-priority", Coalesce("scheduled_for", "created_at"), "created_at", "id"
            )[:limit]
        ]
        candidates = sorted(candidates, key=ledger.claim_order)[:limit]
        now = self.now()
        assignments = []
        for row in candidates:
            if row.expires_at is not None and row.expires_at <= now:
                self.apply(
                    row,
                    {
                        "status": TaskStatus.EXPIRED.value,
                        "expired_at": now,
                        "latest_outcome": "expired",
                        "updated_at": now,
                    },
                )
                continue
            self.apply(row, ledger.assignment_changes(row, self.owner, now, self.timing))
            assignments.append(Assignment(row.id, row.dispatch_token))
        return assignments

    def start(self, assignment):
        row = self.row(assignment.task_id)
        if row is None:
            return None
        now = self.now()
        changes = ledger.admission_changes(row, assignment, self.owner, now, self.timing)
        if changes is None:
            return None
        self.apply(row, changes)
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
            "trace": extract_trace_context(row.metadata),
        }

    def finish(self, assignment, token, failure=None):
        row = self.row(assignment.task_id)
        if row is None:
            return False
        changes = ledger.outcome_changes(row, assignment, self.owner, token, self.now(), failure)
        if changes is None:
            return False
        self.apply(row, changes)
        self.alerts.task_changed(row, row.latest_outcome)
        return True

    def renew(self, assignment):
        row = self.row(assignment.task_id)
        if row is None:
            return False
        changes = ledger.renewal_changes(row, assignment, self.owner, self.now(), self.timing)
        if changes is None:
            return False
        self.apply(row, changes)
        return True

    def abandon(self, assignment):
        """Only after the caller establishes that this ticket cannot execute."""
        row = self.row(assignment.task_id)
        if row is None or not ledger.owns(row, assignment, self.owner):
            return Reconciliation.INVALIDATED
        changes = ledger.recovery_changes(row, self.now(), stopped=True)
        if changes is not None:
            self.apply(row, changes)
            # The same news as when another worker recovers it after a crash.
            self.alerts.task_changed(
                row, row.latest_outcome, recovered=row.latest_outcome != "abandoned"
            )
        return Reconciliation.SETTLED

    def reconcile(self, assignment):
        return ledger.reconcile(self.row(assignment.task_id), assignment, self.owner)

    def recover(self, limit):
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("Recovery limit must be positive")
        self.report_invalid()
        now = self.now()
        rows = list(
            self.rows()
            .select_for_update(skip_locked=True)
            .exclude(_invalid_ownership())
            .filter(
                Q(lease_expires_at__lte=now) | Q(execution_deadline_at__lte=now),
                queue__in=self.queues,
                dispatch_token__isnull=False,
            )
            .order_by("lease_expires_at", "id")[:limit]
        )
        now = self.now()
        recovered = []
        for row in rows:
            changes = ledger.recovery_changes(row, now)
            if changes is not None:
                self.apply(row, changes)
                recovered.append(row.id)
                # An unstarted assignment handed back is routine, not an incident.
                self.alerts.task_changed(
                    row, row.latest_outcome, recovered=row.latest_outcome != "abandoned"
                )
        return recovered

    def report_invalid(self):
        rows = (
            self.rows()
            .filter(_invalid_ownership(), queue__in=self.queues)
            .order_by("id")[: ledger.QUARANTINE_SCAN_LIMIT]
        )
        for row in rows:
            ledger.report_invalid(row)

    def readiness(self):
        with connections[self.using].cursor() as cursor:
            cursor.execute(
                "SELECT attname, format_type(atttypid, atttypmod), attnotnull "
                "FROM pg_attribute WHERE attrelid='task_entries'::regclass "
                "AND attnum > 0 AND NOT attisdropped"
            )
            columns = {name: (kind, required) for name, kind, required in cursor.fetchall()}
        for name, kind in (
            ("execution_token", "character varying(36)"),
            ("dispatch_token", "character varying(36)"),
            ("worker_id", "character varying(36)"),
            ("latest_outcome", "character varying(32)"),
            ("lease_expires_at", "timestamp with time zone"),
            ("execution_deadline_at", "timestamp with time zone"),
            ("execution_timeout_s", "double precision"),
        ):
            if columns.get(name) != (kind, False):
                raise RuntimeError("Native ownership schema is outdated or incompatible")
        if columns.get("cancel_requested") != ("boolean", True):
            raise RuntimeError("Native cancellation schema is outdated or incompatible")
        with connections[self.using].cursor() as cursor:
            cursor.execute("SELECT to_regclass('dewey_schedules')")
            if cursor.fetchone()[0] is None:
                raise RuntimeError("dewey_schedules is missing; apply Dewey migrations")
        self.report_invalid()
        with connections[self.using].cursor() as cursor:
            cursor.execute("SHOW transaction_isolation")
            if cursor.fetchone()[0] != "read committed":
                raise RuntimeError("Native storage requires READ COMMITTED")

    def tick(self, limit: int = 20) -> list[dict[str, Any]]:
        """Create task rows for due schedule ticks; any number of callers is safe."""
        reports = create_due_ticks(DjangoScheduleStore(self.using), limit)
        for report in reports:
            if report["missed"]:
                self.alerts.add(
                    "schedule_tick_missed", schedule=report["schedule"], missed=report["missed"]
                )
        return reports

    def heartbeat(self) -> None:
        now = self.now()
        values = heartbeat_values(self.owner, "django", self.using, self.queues, now)
        heartbeats = WorkerHeartbeat.objects.using(self.using)
        heartbeats.update_or_create(
            instance_id=values.pop("instance_id"),
            defaults={"last_seen_at": now, "queues": values["queues"]},
            create_defaults=values,
        )
        heartbeats.filter(
            last_seen_at__lt=now - timedelta(days=DEFAULT_HEARTBEAT_RETENTION_DAYS)
        ).delete()

    def ops(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        from dewey.django.ops import run

        arguments = arguments or {}
        # Only a kill that changes something is news.
        killing = name == "tasks.kill" and run(self.using, "tasks.show", **arguments)
        result = run(self.using, name, **arguments)
        if killing and killing["status"] != "dead":
            self.alerts.operation_done(name, result)
        return result

    @contextmanager
    def handler_scope(self, context: TaskContext) -> Iterator[None]:
        """Apply the task's database budgets to this alias while its handler runs."""
        budgets = {
            "statement_timeout": context.statement_timeout_s,
            "lock_timeout": context.lock_timeout_s,
        }
        budgets = {name: seconds for name, seconds in budgets.items() if seconds is not None}
        if not budgets:
            yield
            return
        connection = connections[self.using]
        with connection.cursor() as cursor:
            cursor.execute("SELECT " + ", ".join(f"current_setting('{name}')" for name in budgets))
            previous = cursor.fetchone()
            for name, seconds in budgets.items():
                # set_config, not SET: SET takes no bound parameters.
                cursor.execute(
                    "SELECT set_config(%s, %s, false)", [name, f"{max(1, int(seconds * 1000))}ms"]
                )
        try:
            yield
        finally:
            try:
                with connection.cursor() as cursor:
                    for name, value in zip(budgets, previous, strict=True):
                        cursor.execute("SELECT set_config(%s, %s, false)", [name, value])
            except Exception:
                # The handler left the connection unusable; a fresh one starts clean.
                connection.close()

    def call(self, operation: str, *args: Any) -> Any:
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
        connection = connections[self.using]
        if connection.vendor != "postgresql" or connection.in_atomic_block:
            raise RuntimeError("Native storage requires dedicated Postgres transactions")
        budget = DatabaseBudget(wait_seconds(operation, self.timing))
        self.alerts.discard()
        with (
            definite_timeouts(),
            transaction.atomic(using=self.using),
            connection.execute_wrapper(budget.django_execute),
        ):
            result = getattr(self, operation)(*args)
            budget.milliseconds()
        self.alerts.flush()  # committed
        return result
