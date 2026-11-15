"""Real-PG/spawn regressions for isolated expiry and safe lifecycle recycling."""

import multiprocessing
import time
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update

from dewey._worker.contracts import Reconciliation
from dewey._worker.pool import PoolBroken, WorkerPool
from dewey._worker.runner import NativeRunner
from dewey.sqlalchemy.models import TaskEntryModel as Task
from tests._helpers.native_runtime import TIMING
from tests.native.test_runner import StorageBridge, drive
from tests.native.test_storage import create, snapshot


def runner_for(store, monkeypatch, **options):
    monkeypatch.setenv("DEWEY_NATIVE_TEST_MODE", store.mode)
    monkeypatch.setenv("DEWEY_NATIVE_TEST_OWNER", store.owner)
    pool = WorkerPool(
        "tests._helpers.native_lifecycle_runtime:create_runtime",
        concurrency=options.pop("concurrency", 2),
        task_timeout_seconds=TIMING.task_timeout_seconds,
        termination_timeout_seconds=0.2,
        **options,
    )
    return NativeRunner(pool, StorageBridge(store), timing=TIMING)


def wait_capacity(runner, capacity=2):
    """Task processes start one after another; these tests need every slot up."""
    until = time.monotonic() + 10
    while runner.pool.capacity < capacity and time.monotonic() < until:
        assert runner.pool.poll() == []  # nothing is assigned yet, so no events
        time.sleep(0.005)
    assert runner.pool.capacity == capacity


def wait_status(engine, ids, status):
    until = time.monotonic() + 8
    while time.monotonic() < until:
        if all(snapshot(engine, task_id)["status"] == status for task_id in ids):
            return
        time.sleep(0.005)
    pytest.fail("Synthetic tasks did not reach expected ledger phase")


@pytest.mark.parametrize("observation", ["renewal", "timeout"])
def test_expiry_reaps_only_affected_child_and_sibling_completes(
    store, engine, monkeypatch, tmp_path, observation
):
    """One expired generation must not interrupt a healthy sibling's attempt."""
    release, effect = tmp_path / "release", tmp_path / "effect"
    first = create(engine, task_type="native.synthetic.barrier", args=[str(release), str(effect)])
    sibling_effect = tmp_path / "sibling-effect"
    sibling = create(
        engine, task_type="native.synthetic.barrier", args=[str(release), str(sibling_effect)]
    )
    runner = runner_for(store, monkeypatch, max_restarts=0)
    try:
        runner.start()
        wait_capacity(runner)
        pids = set(runner.pool.worker_pids)
        assert runner.step() == 2
        wait_status(engine, [first, sibling], "processing")
        before = snapshot(engine, sibling)
        with engine.begin() as connection:
            connection.execute(
                update(Task)
                .where(Task.id == first)
                .values(execution_deadline_at=datetime.now(UTC) - timedelta(seconds=1))
            )
        if observation == "timeout":
            slot = next(s for s in runner.pool._slots if s.assignment.task_id == first)
            slot.assigned_at -= TIMING.task_timeout_seconds
        runner._renew_at = 0
        runner.step()
        live = {p.pid for p in multiprocessing.active_children()}
        assert len(pids & live) == 1
        expired = snapshot(engine, first)
        assert expired["status"] == "failed"
        assert expired["latest_outcome"] == "timed_out"
        assert expired["attempts"] == 1
        assert expired["dispatch_token"] is None
        healthy = snapshot(engine, sibling)
        for field in ("status", "attempts", "dispatch_token", "execution_token", "worker_id"):
            assert healthy[field] == before[field]
        release.touch()
        completed = drive(runner, engine, sibling, "completed")
        assert completed["attempts"] == 1
        assert sibling_effect.exists()
        assert not effect.exists()
    finally:
        runner.close()


@pytest.mark.parametrize("observation", ["renewal", "timeout", "during_renewal"])
def test_task_timeout_routes_preserve_health_budget_and_exhaust_task_attempts(
    store, engine, monkeypatch, tmp_path, observation
):
    """Task-level timeout policy must not depend on which poll sees the event."""
    effect = tmp_path / "effect"
    task_id = create(
        engine,
        task_type="native.synthetic.barrier",
        args=[str(tmp_path / "never-release"), str(effect)],
        max_attempts=2,
    )
    runner = runner_for(store, monkeypatch, concurrency=1, max_restarts=0)
    original = runner.storage.call
    timeout_on_renewal = False

    def observe_during_renewal(operation, *args):
        nonlocal timeout_on_renewal
        result = original(operation, *args)
        if timeout_on_renewal and operation == "renew":
            timeout_on_renewal = False
            runner.pool._slots[0].assigned_at -= TIMING.task_timeout_seconds
        return result

    runner.storage.call = observe_during_renewal
    try:
        runner.start()
        for attempt in (1, 2):
            until = time.monotonic() + 8
            while not runner.assignments and time.monotonic() < until:
                runner.step()
                time.sleep(0.005)
            assert len(runner.assignments) == 1
            wait_status(engine, [task_id], "processing")
            old_pids = set(runner.pool.worker_pids)
            with engine.begin() as connection:
                connection.execute(
                    update(Task)
                    .where(Task.id == task_id)
                    .values(execution_deadline_at=datetime.now(UTC) - timedelta(seconds=1))
                )
            if observation == "timeout":
                runner.pool._slots[0].assigned_at -= TIMING.task_timeout_seconds
            timeout_on_renewal = observation == "during_renewal"
            runner._renew_at = 0
            runner.step()
            row = snapshot(engine, task_id)
            assert row["status"] == ("failed" if attempt == 1 else "dead")
            assert row["latest_outcome"] == "timed_out"
            assert row["attempts"] == attempt
            assert row["dispatch_token"] is None
            assert runner.assignments == {}
            assert runner.pool._slots[0].restarts == 0
            assert not old_pids.intersection(p.pid for p in multiprocessing.active_children())
            if attempt == 1:
                with engine.begin() as connection:
                    connection.execute(
                        update(Task)
                        .where(Task.id == task_id)
                        .values(scheduled_for=datetime.now(UTC) - timedelta(seconds=1))
                    )
        assert not effect.exists()
        assert store("claim", 1) == []
    finally:
        runner.close()


@pytest.mark.parametrize("observation", ["renewal", "timeout"])
def test_committed_success_delayed_return_preserves_health_budget(
    store, engine, monkeypatch, tmp_path, observation
):
    """Repeated durable successes with delayed IPC can recycle even at zero budget."""
    monkeypatch.setenv("DEWEY_NATIVE_TEST_HOLD_RETURN", "1")
    runner = runner_for(store, monkeypatch, concurrency=1, max_restarts=0)
    try:
        runner.start()
        for index in range(3):
            effect = tmp_path / f"effect-{index}"
            task_id = create(engine, task_type="native.synthetic.sync", args=[str(effect)])
            until = time.monotonic() + 8
            while not runner.assignments and time.monotonic() < until:
                runner.step()
                time.sleep(0.005)
            old_pids = set(runner.pool.worker_pids)
            wait_status(engine, [task_id], "completed")
            if observation == "timeout":
                # Force the pool's monotonic timeout observation, not DB expiry.
                runner.pool._slots[0].assigned_at -= TIMING.task_timeout_seconds
            runner._renew_at = 0
            runner.step()
            assert runner.assignments == {}
            assert not old_pids.intersection(p.pid for p in multiprocessing.active_children())
            row = snapshot(engine, task_id)
            assert row["status"] == "completed" and row["attempts"] == 1
            assert effect.exists()
    finally:
        runner.close()


@pytest.mark.parametrize("fault", ["unknown", "database"])
def test_uncertain_renewal_still_fail_stops_siblings(store, engine, monkeypatch, tmp_path, fault):
    release = tmp_path / "never-release"
    ids = [
        create(
            engine,
            task_type="native.synthetic.barrier",
            args=[str(release), str(tmp_path / str(i))],
        )
        for i in range(2)
    ]
    runner = runner_for(store, monkeypatch)
    try:
        runner.start()
        wait_capacity(runner)
        pids = set(runner.pool.worker_pids)
        runner.step()
        wait_status(engine, ids, "processing")
        original = runner.storage.call

        def uncertain(operation, *args):
            if operation == "renew":
                if fault == "database":
                    raise ConnectionError("Synthetic unavailable ledger")
                return False
            if operation == "reconcile":
                return Reconciliation.UNKNOWN
            return original(operation, *args)

        runner.storage.call = uncertain
        runner._renew_at = 0
        with pytest.raises((RuntimeError, ConnectionError)):
            runner.step()
        assert not pids.intersection(p.pid for p in multiprocessing.active_children())
        assert all(snapshot(engine, task_id)["attempts"] == 1 for task_id in ids)
    finally:
        runner.close()


def test_crash_still_exhausts_health_budget(store, engine, monkeypatch):
    create(engine, task_type="native.synthetic.crash")
    runner = runner_for(store, monkeypatch, concurrency=1, max_restarts=0)
    try:
        runner.start()
        runner.step()
        until = time.monotonic() + 8
        with pytest.raises(PoolBroken, match="budget exhausted"):
            while time.monotonic() < until:
                runner.step()
                time.sleep(0.005)
    finally:
        runner.close()
