"""Internal bounded native orchestration; integrations own bootstrap and signals.

This is an experimental step-driven runner, not a public worker command. Call
step on the configured cadence. Any database/ownership uncertainty fail-stops
local children and leaves the ledger recoverable; it never reports success.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import Any

from dewey._worker.contracts import Reconciliation, Timing, round_budget, stop_budget
from dewey._worker.loop import ChildLoop
from dewey._worker.pool import Assignment, Outcome, WorkerEvent, WorkerPool
from dewey._worker.runtime import LedgerStorage

logger = logging.getLogger("dewey.worker")


class NativeRunner:
    def __init__(
        self,
        pool: WorkerPool,
        storage: LedgerStorage,
        *,
        timing: Timing,
        close_resources: Callable[[], Any] | None = None,
        progress: Callable[[], None] | None = None,
        can_claim: Callable[[], bool] | None = None,
        auto_apply_schedules: bool = False,
    ):
        if pool.task_timeout_seconds != timing.task_timeout_seconds:
            raise ValueError("Pool timeout must match the durable hard runtime limit")
        if 3 * pool.termination_timeout_seconds > timing.termination_seconds:
            raise ValueError("Pool termination timeout exceeds the timing safety budget")
        # A worst-case reconciliation/renewal round must fit inside the lease.
        budget = round_budget(timing, pool.concurrency)
        if timing.renewal_interval_seconds + budget >= timing.execution_lease_seconds:
            raise ValueError("Execution lease is too short for this concurrency and DB budget")
        self.pool, self.storage, self.timing = pool, storage, timing
        self.loop = ChildLoop()
        self.assignments: dict[str, Assignment] = {}
        self._renew_at = 0.0
        # A slow database is waited out only while every lease is certainly live:
        # until this moment, measured from the last round that renewed them all.
        self._lease_slack = timing.execution_lease_seconds - stop_budget(timing, pool.concurrency)
        self._fail_stop_at = 0.0
        self._slow = False
        self._closed = False
        self._started = False
        self._observations: list[WorkerEvent] = []
        self._close_resources = close_resources
        self._resources_closed = False
        self._progress = progress
        self._can_claim = can_claim
        self._auto_apply_schedules = auto_apply_schedules
        if pool.start_method == "fork":
            # A forked task process must not inherit this process's connections:
            # release them before every fork (Django and SQLAlchemy reconnect on
            # next use). Without a way to do that, fork is not safe to run.
            if close_resources is None:
                raise ValueError(
                    "start_method=fork needs close_resources: a forked task process must "
                    "not inherit this process's database connections"
                )
            if pool.prepare is not None:
                raise ValueError(
                    "The pool already has a prepare hook; NativeRunner releases the "
                    "connections before each fork itself"
                )
            pool.prepare = self._release_connections

    def _release_connections(self) -> None:
        assert self._close_resources is not None
        self.loop.invoke(self._close_resources)

    def call(self, operation, *args):
        try:
            return self.loop.invoke(self.storage.call, operation, *args)
        finally:
            if self._started and not self._closed:
                # Poll between bounded DB operations, not just once per batch.
                self._observations.extend(self.pool.poll())
            if self._progress is not None:
                self._progress()

    def start(self) -> None:
        try:
            self.call("readiness")
            self._reconcile_schedules()
            self.pool.start()
            self._started = True
            self._fail_stop_at = time.monotonic() + self._lease_slack
        except BaseException:
            self.close()
            raise

    def _reconcile_schedules(self) -> None:
        """Workers report schedule drift; only a single-node install applies it here."""
        operation = "schedules.apply" if self._auto_apply_schedules else "schedules.plan"
        changes = self.call("ops", operation) or {}
        if not any(changes.get(kind) for kind in ("added", "changed", "removed")):
            return
        summary = json.dumps(changes, sort_keys=True)
        if self._auto_apply_schedules:
            logger.info("Applied schedules: %s", summary)
        else:
            logger.warning(
                "Declared schedules differ from the applied ones (%s); run `dewey schedules apply`",
                summary,
            )

    def step(self) -> int:
        """Reconcile, renew/recover, then claim at most available capacity.

        A database too slow for one operation's budget is not a reason to stop
        work in progress: that operation rolled back, so the round is abandoned
        and tried again. Once the leases can no longer be trusted the worker
        stops, as it does for every other failure.
        """
        if self._closed or not self._started:
            raise RuntimeError("Native runner is not running")
        try:
            claimed = self._round()
        except TimeoutError:
            if time.monotonic() >= self._fail_stop_at:
                self.close()
                raise
            if not self._slow:
                logger.warning(
                    "The database is too slow for db_wait_seconds=%s; holding work in "
                    "progress and retrying while its leases last",
                    self.timing.db_wait_seconds,
                )
                self._slow = True
            return 0
        except BaseException:
            # An uncertain claim/send may not have a known ticket. Do not retry
            # claims here: expired durable leases let another instance recover.
            self.close()
            raise
        if self._slow:
            logger.info("The database is answering in time again")
            self._slow = False
        return claimed

    def _round(self) -> int:
        self._observations.extend(self.pool.poll())
        observations, self._observations = self._observations, []
        for index, event in enumerate(observations):
            try:
                self._settle(event)
            except TimeoutError:
                self._observations[:0] = observations[index:]  # seen once, settled later
                raise
        now = time.monotonic()
        if now >= self._renew_at:
            self._renew(now)
        capacity = self.pool.capacity
        if not capacity or (self._can_claim is not None and not self._can_claim()):
            return 0
        assignments = self.call("claim", capacity)
        for assignment in assignments:
            ticket = self.pool.submit(assignment)
            self.assignments[ticket] = assignment
        return len(assignments)

    def _settle(self, event: WorkerEvent) -> None:
        """Bring the ledger and the pool into line with what a task process reported."""
        if event.ticket not in self.assignments:
            return  # already reconciled during a renewal/completion race
        if event.outcome != Outcome.RETURNED:
            self.pool.stop(event.ticket)
        state = self.call("reconcile", event.assignment)
        if state.durable_release and event.outcome in (Outcome.TIMED_OUT, Outcome.STOPPED):
            # A committed outcome can also precede local timeout while
            # RETURNED is delayed. This is not a crashed runtime.
            self.pool.stop(event.ticket, recycle=True)
        if not state.durable_release:
            # RETURNED may leave an active row; task timeout/stops may
            # also arrive before recovery. They consume task attempts,
            # not crash-health budget. Observed errors/deaths still do.
            if state != Reconciliation.ACTIVE:
                raise RuntimeError("Native assignment ownership is unknown")
            self.pool.stop(
                event.ticket,
                recycle=event.outcome in (Outcome.RETURNED, Outcome.TIMED_OUT, Outcome.STOPPED),
            )
            state = self.call("abandon", event.assignment)
        if not state.durable_release:
            raise RuntimeError("Native assignment remains unresolved")
        self.pool.acknowledge(event.ticket)
        del self.assignments[event.ticket]

    def _renew(self, now: float) -> None:
        """Renew every lease, recover what other workers lost, then do maintenance."""
        for ticket, assignment in list(self.assignments.items()):
            if self._has_returned(ticket):
                continue  # finished: the next step settles it
            if not self.call("renew", assignment):
                state = self.call("reconcile", assignment)
                if state == Reconciliation.UNKNOWN:
                    raise RuntimeError("Native assignment ownership is unknown")
                if state.durable_release and self._has_returned(ticket):
                    # Its own process committed the outcome and has now said
                    # so: a healthy process, not one to stop and replace.
                    continue
                # Confirmed expiry and completion/operator invalidation
                # are local lifecycle events, not service-health failures.
                # Reap this child before recovery or capacity reuse.
                self.pool.stop(ticket, recycle=True)
                if state == Reconciliation.ACTIVE:
                    state = self.call("abandon", assignment)
                if not state.durable_release:
                    raise RuntimeError("Native assignment remains unresolved")
                self.pool.acknowledge(ticket)
                del self.assignments[ticket]
        self._fail_stop_at = now + self._lease_slack  # every lease is at least this fresh
        self.call("recover", self.pool.concurrency)
        # Maintenance any worker may do: say we are alive, and turn due
        # schedule ticks into task rows (one row per tick, however many ask).
        self.call("heartbeat")
        self._create_due_ticks()
        self._renew_at = now + self.timing.renewal_interval_seconds

    def _create_due_ticks(self) -> None:
        """One pass takes a bounded batch; keep going while there was work, for about one call's time."""
        until = time.monotonic() + self.timing.db_wait_seconds
        while self.call("tick") and time.monotonic() < until:
            pass

    def _has_returned(self, ticket: str) -> bool:
        """Whether the task's process has reported back (``call`` polls after each operation)."""
        return any(
            event.ticket == ticket and event.outcome == Outcome.RETURNED
            for event in self._observations
        )

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.pool.shutdown(grace_seconds=0)
        finally:
            # Keep the runner retryable if OS reaping failed; never claim cleanup
            # succeeded merely because shutdown raised. Loop has no DB resources.
            try:
                if self._close_resources is not None and not self._resources_closed:
                    self.loop.invoke(self._close_resources)
                    self._resources_closed = True
            finally:
                self.loop.close()
        self._closed = True
