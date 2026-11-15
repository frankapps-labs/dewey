"""A database too slow for one operation is waited out while the leases last."""

import logging
import time
from types import SimpleNamespace

import pytest
from sqlalchemy import text

import dewey
from dewey._worker import runtime as runtime_module
from dewey._worker.contracts import Reconciliation, stop_budget
from dewey._worker.pool import Assignment, Outcome, WorkerEvent
from dewey._worker.runner import NativeRunner
from dewey._worker.runtime import LedgerRuntime
from tests._helpers.native_runtime import TIMING
from tests.native.test_storage import create, snapshot


def test_an_operation_postgres_cut_short_is_a_timeout_that_changed_nothing(store, engine):
    task_id = create(engine)
    (assignment,) = store("claim", 1)
    with engine.connect() as holder:  # someone else holds the row for longer than the budget
        holder.execute(
            text("SELECT 1 FROM task_entries WHERE id = :id FOR UPDATE"), {"id": task_id}
        )
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            store("start", assignment)
        assert time.monotonic() - started < 5
        holder.rollback()
    assert snapshot(engine, task_id)["status"] == "dispatching"
    assert store("start", assignment)["attempts"] == 1  # the same call, asked again


class SlowLedger:
    """A ledger that times out while ``slow`` is set, and records what it was asked."""

    def __init__(self):
        self.slow = False
        self.asked = []

    def call(self, operation, *args):
        self.asked.append(operation)
        if self.slow and operation not in ("readiness", "ops"):
            raise TimeoutError("synthetic slow database")
        if operation == "reconcile":
            return Reconciliation.SETTLED
        if operation == "renew":
            return True
        return [] if operation in ("claim", "recover", "tick") else None


@pytest.fixture
def worker():
    ledger = SlowLedger()
    events, stopped, acknowledged = [], [], []
    pool = SimpleNamespace(
        start_method="spawn",
        prepare=None,
        concurrency=2,
        capacity=1,
        task_timeout_seconds=TIMING.task_timeout_seconds,
        termination_timeout_seconds=0.2,
        start=lambda: None,
        poll=lambda: [events.pop(0)] if events else [],
        stop=lambda ticket, **options: stopped.append(ticket),
        acknowledge=acknowledged.append,
        shutdown=lambda **options: stopped.append("everything"),
    )
    runner = NativeRunner(pool, ledger, timing=TIMING)
    runner.start()
    yield SimpleNamespace(
        runner=runner, ledger=ledger, events=events, stopped=stopped, acknowledged=acknowledged
    )
    runner.close()


def test_a_slow_database_pauses_the_worker_and_loses_nothing_it_was_told(worker, caplog):
    assignment = Assignment("task-1", "dispatch-1")
    worker.runner.assignments["ticket-1"] = assignment
    worker.events.append(WorkerEvent("ticket-1", assignment, 4321, Outcome.RETURNED))
    worker.ledger.slow = True
    with caplog.at_level(logging.WARNING, logger="dewey.worker"):
        assert [worker.runner.step() for _ in range(3)] == [0, 0, 0]
    assert worker.stopped == [] and worker.acknowledged == []  # work in progress is left alone
    assert [record.message for record in caplog.records].count(caplog.records[0].message) == 1
    assert "too slow" in caplog.records[0].message
    worker.ledger.slow = False
    worker.runner.step()
    assert worker.acknowledged == ["ticket-1"] and worker.runner.assignments == {}
    assert worker.stopped == []


def test_slowness_that_outlasts_the_leases_stops_the_worker(worker):
    slack = TIMING.execution_lease_seconds - stop_budget(TIMING, 2)
    assert 0 < slack < TIMING.execution_lease_seconds
    worker.runner.assignments["ticket-1"] = Assignment("task-1", "dispatch-1")  # a lease at stake
    worker.ledger.slow = True
    assert worker.runner.step() == 0
    worker.runner._fail_stop_at = time.monotonic()  # as if `slack` seconds had passed
    with pytest.raises(TimeoutError):
        worker.runner.step()
    assert worker.stopped == ["everything"]


def test_any_other_failure_still_stops_the_worker_at_once(worker):
    worker.ledger.call = lambda operation, *args: (_ for _ in ()).throw(OSError("connection lost"))
    with pytest.raises(OSError):
        worker.runner.step()
    assert worker.stopped == ["everything"]


def test_a_task_process_asks_again_instead_of_abandoning_finished_work(monkeypatch):
    ran, asked = [], []
    dewey.task("work.slow-ledger")(lambda: ran.append("handler"))

    class Storage:
        failures = {"start": 1, "finish": 2}

        def call(self, operation, *args):
            asked.append(operation)
            if self.failures.get(operation):
                self.failures[operation] -= 1
                raise TimeoutError("synthetic slow database")
            if operation == "start":
                return {
                    "task_type": "work.slow-ledger",
                    "args": [],
                    "kwargs": {},
                    "execution_token": "token",
                    "attempts": 1,
                }
            return None

    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    runtime = LedgerRuntime(Storage(), close_resources=lambda: None)
    try:
        runtime.execute(Assignment("task-1", "dispatch-1"))
        assert ran == ["handler"]  # once, however often the ledger had to be asked
        assert asked == ["readiness", "start", "start", "finish", "finish", "finish"]
        Storage.failures = {"start": 10**6}
        monkeypatch.setattr(runtime_module, "LEDGER_PATIENCE_SECONDS", 0.0)
        with pytest.raises(TimeoutError):  # patience runs out: recovery takes over
            runtime.execute(Assignment("task-2", "dispatch-2"))
    finally:
        runtime.close()
        dewey.registry.clear()
