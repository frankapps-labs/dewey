"""Actual dispatch → spawned handler → durable outcome → crash recovery."""

import json
import multiprocessing
import time
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update

from dewey._worker.pool import WorkerPool
from dewey._worker.runner import NativeRunner
from dewey.sqlalchemy.models import TaskEntryModel as Task
from tests._helpers.native_runtime import TIMING
from tests.native.test_storage import create, expire_lease, snapshot


class StorageBridge:
    def __init__(self, call):
        self.call = call


def runner_for(store, monkeypatch):
    # The storage fixture exposes only a callable. Capture owner/mode through its
    # explicit fixture attributes, not inherited engines or session objects.
    monkeypatch.setenv("DEWEY_NATIVE_TEST_MODE", store.mode)
    monkeypatch.setenv("DEWEY_NATIVE_TEST_OWNER", store.owner)
    pool = WorkerPool(
        "tests._helpers.native_runtime:create_runtime",
        concurrency=2,
        task_timeout_seconds=TIMING.task_timeout_seconds,
        termination_timeout_seconds=0.2,
    )
    return NativeRunner(pool, StorageBridge(store), timing=TIMING)


def drive(runner, engine, task_id, expected):
    until = time.monotonic() + 15
    while time.monotonic() < until:
        runner.step()
        row = snapshot(engine, task_id)
        if row["status"] == expected:
            return row
        time.sleep(0.005)
    pytest.fail(f"Task did not reach expected durable status {expected}")


@pytest.mark.parametrize("handler", ["sync", "async"])
def test_native_dispatch_executes_handler_and_commits_outcome(
    store, engine, tmp_path, monkeypatch, handler
):
    effect = tmp_path / "effect"
    task_id = create(engine, task_type=f"native.synthetic.{handler}", args=[str(effect)])
    runner = runner_for(store, monkeypatch)
    try:
        runner.start()
        pids = runner.pool.worker_pids
        assert runner.step() == 1
        assert len(runner.assignments) == 1
        row = drive(runner, engine, task_id, "completed")
        assert effect.exists()
        assert row["attempts"] == 1
        assert row["latest_outcome"] == "completed"
        until = time.monotonic() + 5
        while runner.assignments and time.monotonic() < until:
            runner.step()
            time.sleep(0.005)
        assert runner.assignments == {}
    finally:
        runner.close()
    assert not set(pids).intersection(p.pid for p in multiprocessing.active_children())


@pytest.mark.parametrize("handler", ["sync", "async"])
def test_native_handler_context_preserves_effect_key_across_retry(
    store, engine, tmp_path, monkeypatch, handler
):
    effect = tmp_path / "synthetic-effect-keys"
    task_id = create(
        engine, task_type=f"native.synthetic.context.{handler}", args=[str(effect)], max_attempts=2
    )
    runner = runner_for(store, monkeypatch)
    try:
        runner.start()
        final = drive(runner, engine, task_id, "completed")
        records = [json.loads(line) for line in effect.read_text().splitlines()]
        assert [item["task_id"] for item in records] == [task_id, task_id]
        assert [item["attempt"] for item in records] == [1, 2]
        assert records[0]["effect_key"] == records[1]["effect_key"]
        assert final["attempts"] == 2 and final["latest_outcome"] == "completed"
    finally:
        runner.close()


def test_native_child_crash_recovers_and_exhausts_attempt_budget(store, engine, monkeypatch):
    task_id = create(engine, task_type="native.synthetic.crash", max_attempts=2)
    runner = runner_for(store, monkeypatch)
    try:
        runner.start()
        first = drive(runner, engine, task_id, "failed")
        assert first["attempts"] == 1
        assert first["latest_outcome"] == "interrupted"
        with engine.begin() as connection:
            connection.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(scheduled_for=datetime.now(UTC) - timedelta(seconds=1))
            )
        final = drive(runner, engine, task_id, "dead")
        assert final["attempts"] == 2
        assert final["latest_outcome"] == "interrupted"
    finally:
        runner.close()


@pytest.mark.parametrize("fault,expected", [("before", "dead"), ("after", "completed")])
def test_child_completion_uncertainty_is_reconciled_not_replayed(
    store, engine, tmp_path, monkeypatch, fault, expected
):
    monkeypatch.setenv("DEWEY_NATIVE_TEST_COMPLETION_FAULT", fault)
    effect = tmp_path / "effect"
    task_id = create(engine, task_type="native.synthetic.async", args=[str(effect)], max_attempts=1)
    runner = runner_for(store, monkeypatch)
    try:
        runner.start()
        row = drive(runner, engine, task_id, expected)
        assert effect.exists()
        assert row["attempts"] == 1
        until = time.monotonic() + 5
        while runner.assignments and time.monotonic() < until:
            runner.step()
            time.sleep(0.005)
        assert runner.assignments == {}
        assert snapshot(engine, task_id)["status"] == expected
    finally:
        runner.close()


def test_uncertain_parent_claim_fail_stops_and_recovers_committed_assignment(
    store, engine, tmp_path, monkeypatch
):
    effect = tmp_path / "effect"
    task_id = create(engine, task_type="native.synthetic.sync", args=[str(effect)])
    runner = runner_for(store, monkeypatch)
    original = runner.storage.call

    def uncertain(operation, *args):
        result = original(operation, *args)
        if operation == "claim":
            raise ConnectionError("Synthetic lost claim acknowledgement")
        return result

    runner.storage.call = uncertain
    try:
        runner.start()
        pids = runner.pool.worker_pids
        with pytest.raises(ConnectionError, match="Synthetic lost claim"):
            runner.step()
        assert not set(pids).intersection(p.pid for p in multiprocessing.active_children())
        assert not effect.exists()
        row = snapshot(engine, task_id)
        assert row["status"] == "dispatching"
        assert row["attempts"] == 0
        expire_lease(engine, task_id)
        assert store("recover", 1) == [task_id]
        assert snapshot(engine, task_id)["attempts"] == 0
    finally:
        runner.close()
    replacement = runner_for(store, monkeypatch)
    try:
        replacement.start()
        row = drive(replacement, engine, task_id, "completed")
        assert row["attempts"] == 1
        assert effect.exists()
    finally:
        replacement.close()


def test_a_task_that_finishes_as_leases_are_renewed_keeps_its_process(
    store, engine, tmp_path, monkeypatch
):
    # A finished task's row no longer renews. That alone is not a reason to stop and
    # replace a healthy process: under load it cost a process start per task.
    task_ids = [
        create(engine, task_type="native.synthetic.sync", args=[str(tmp_path / f"effect-{n}")])
        for n in range(30)
    ]
    runner = runner_for(store, monkeypatch)
    try:
        runner.start()
        pids = set(runner.pool.worker_pids)
        until = time.monotonic() + 30
        while time.monotonic() < until:
            runner._renew_at = 0  # renew on every step, so the race is the common case
            runner.step()
            if not runner.assignments and all(
                snapshot(engine, task_id)["status"] == "completed" for task_id in task_ids
            ):
                break
        else:
            pytest.fail("Tasks did not complete")
        assert set(runner.pool.worker_pids) == pids
    finally:
        runner.close()
