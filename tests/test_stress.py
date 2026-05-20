"""Stress tests — concurrent processing, sweep races, high volume, two-phase visibility."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.executor import create_task, process_task
from dewey.sqlalchemy.models import TaskEntryModel
from dewey.sqlalchemy.sweep import sweep, sweep_failed


class TestConcurrentSameTask:
    """When multiple workers race to process the same task, exactly one wins."""

    def test_only_one_worker_processes(self, engine):
        with Session(engine) as session:
            task = create_task(session, task_type="test.concurrent")
            session.commit()
            task_id = task.id

        results = []
        errors = []

        def worker():
            try:
                with Session(engine) as s:
                    result = process_task(s, task_id, lambda t, p: None)
                    results.append(result)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Worker errors: {errors}"
        assert results.count(True) == 1, f"Expected exactly 1 success, got {results.count(True)}"
        assert results.count(False) == 9

        with Session(engine) as session:
            task = session.get(TaskEntryModel, task_id)
            assert task.status == TaskStatus.COMPLETED.value
            assert task.attempts == 1

    def test_concurrent_with_failing_handler(self, engine):
        """Even with failures, only one worker claims the task."""
        with Session(engine) as session:
            task = create_task(session, task_type="test.concurrent.fail", max_attempts=5)
            session.commit()
            task_id = task.id

        results = []

        def worker():
            with Session(engine) as s:
                result = process_task(
                    s, task_id, lambda t, p: (_ for _ in ()).throw(ValueError("boom"))
                )
                results.append(result)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        # One worker claimed it (and failed), rest returned False (not pending)
        assert results.count(False) == 5  # The one that claimed fails, rest skip

        with Session(engine) as session:
            task = session.get(TaskEntryModel, task_id)
            assert task.status == TaskStatus.FAILED.value
            assert task.attempts == 1


class TestConcurrentManyTasks:
    """Multiple tasks processed by a pool of concurrent workers."""

    def test_100_tasks_10_workers(self, engine):
        task_ids = []
        with Session(engine) as session:
            for i in range(100):
                task = create_task(session, task_type="test.batch", payload={"i": i})
                task_ids.append(task.id)
            session.commit()

        def worker(tid):
            with Session(engine) as s:
                return process_task(s, tid, lambda t, p: None)

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(worker, tid): tid for tid in task_ids}
            results = []
            for f in as_completed(futures):
                results.append(f.result())

        assert all(results), f"Some tasks failed: {results.count(False)} failures"
        assert len(results) == 100

        with Session(engine) as session:
            count = session.execute(
                select(func.count())
                .select_from(TaskEntryModel)
                .where(TaskEntryModel.status == TaskStatus.COMPLETED.value)
            ).scalar()
            assert count == 100


class TestSweepStuckIntegration:
    """Sweep finds tasks stuck in PROCESSING — the two-phase commit payoff."""

    def test_sweep_finds_abandoned_task(self, engine):
        """Simulate worker death: task left in PROCESSING, sweep resets to PENDING."""
        with Session(engine) as session:
            task = create_task(session, task_type="test.stuck")
            session.commit()
            task_id = task.id

        # Simulate: worker claimed the task (set PROCESSING) then died
        with Session(engine) as session:
            session.execute(
                update(TaskEntryModel)
                .where(TaskEntryModel.id == task_id)
                .values(
                    status=TaskStatus.PROCESSING.value,
                    started_at=datetime.now(UTC) - timedelta(minutes=30),
                    attempts=1,
                )
            )
            session.commit()

        # Sweep should find it
        with Session(engine) as session:
            result = sweep(session, stuck_threshold_minutes=10)
            session.commit()
            assert task_id in result["stuck"]

        # Now PENDING again — can be reprocessed
        with Session(engine) as session:
            task = session.get(TaskEntryModel, task_id)
            assert task.status == TaskStatus.PENDING.value

        # Process successfully
        with Session(engine) as session:
            ok = process_task(session, task_id, lambda t, p: None)
            assert ok is True

        with Session(engine) as session:
            task = session.get(TaskEntryModel, task_id)
            assert task.status == TaskStatus.COMPLETED.value

    def test_processing_visible_to_other_sessions(self, engine):
        """While handler runs, PROCESSING is visible to other connections."""
        with Session(engine) as session:
            task = create_task(session, task_type="test.visible")
            session.commit()
            task_id = task.id

        observed_statuses = []
        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_handler(task_type, payload):
            handler_started.set()
            handler_continue.wait(timeout=10)

        def observer():
            handler_started.wait(timeout=10)
            time.sleep(0.1)  # Ensure commit happened
            with Session(engine) as obs_session:
                task = obs_session.get(TaskEntryModel, task_id)
                observed_statuses.append(task.status)
            handler_continue.set()

        t = threading.Thread(target=observer)
        t.start()

        with Session(engine) as session:
            process_task(session, task_id, slow_handler)

        t.join(timeout=15)
        assert observed_statuses == [TaskStatus.PROCESSING.value]


class TestConcurrentSweepAndProcess:
    """Sweep and processing running at the same time — no crashes, consistent state."""

    def test_sweep_and_process_no_conflict(self, engine):
        # Create 20 failed tasks ready for retry
        task_ids = []
        with Session(engine) as session:
            for _ in range(20):
                task = create_task(session, task_type="test.sweep_race")
                task_ids.append(task.id)
            session.commit()

        # Mark them as failed with past process_after
        with Session(engine) as session:
            session.execute(
                update(TaskEntryModel)
                .where(TaskEntryModel.id.in_(task_ids))
                .values(
                    status=TaskStatus.FAILED.value,
                    process_after=datetime.now(UTC) - timedelta(minutes=5),
                    attempts=1,
                )
            )
            session.commit()

        errors = []

        def run_sweep():
            try:
                with Session(engine) as s:
                    sweep(s)
                    s.commit()
            except Exception as e:
                errors.append(("sweep", e))

        def run_process(tid):
            try:
                with Session(engine) as s:
                    process_task(s, tid, lambda t, p: None)
            except Exception as e:
                errors.append(("process", tid, e))

        with ThreadPoolExecutor(max_workers=12) as pool:
            # Fire sweep and processing concurrently
            pool.submit(run_sweep)
            for tid in task_ids:
                pool.submit(run_process, tid)

        assert not errors, f"Errors during concurrent sweep+process: {errors}"

        # All tasks should be in a valid terminal or pending state
        with Session(engine) as session:
            tasks = (
                session.execute(select(TaskEntryModel).where(TaskEntryModel.id.in_(task_ids)))
                .scalars()
                .all()
            )
            for t in tasks:
                assert t.status in (
                    TaskStatus.COMPLETED.value,
                    TaskStatus.PENDING.value,
                    TaskStatus.FAILED.value,
                ), f"Unexpected status {t.status} for task {t.id}"


class TestHighVolume:
    """Throughput test — create and process many tasks."""

    def test_500_tasks_20_workers(self, engine):
        task_ids = []
        with Session(engine) as session:
            for i in range(500):
                task = create_task(session, task_type="test.volume", payload={"i": i})
                task_ids.append(task.id)
            session.commit()

        def worker(tid):
            with Session(engine) as s:
                return process_task(s, tid, lambda t, p: None)

        with ThreadPoolExecutor(max_workers=20) as pool:
            results = list(pool.map(worker, task_ids))

        assert all(results), f"{results.count(False)} tasks failed to process"

        with Session(engine) as session:
            count = session.execute(
                select(func.count())
                .select_from(TaskEntryModel)
                .where(TaskEntryModel.status == TaskStatus.COMPLETED.value)
            ).scalar()
            assert count == 500


class TestRetryLifecycle:
    """Full lifecycle: create → fail → sweep → retry → succeed."""

    def test_full_retry_cycle(self, engine):
        # Create task
        with Session(engine) as session:
            task = create_task(session, task_type="test.lifecycle", max_attempts=3)
            session.commit()
            task_id = task.id

        # First attempt: fail
        with Session(engine) as session:
            result = process_task(
                session, task_id, lambda t, p: (_ for _ in ()).throw(ValueError("fail1"))
            )
            assert result is False

        with Session(engine) as session:
            task = session.get(TaskEntryModel, task_id)
            assert task.status == TaskStatus.FAILED.value
            assert task.attempts == 1
            assert task.process_after is not None

        # Fast-forward process_after for sweep to pick up
        with Session(engine) as session:
            session.execute(
                update(TaskEntryModel)
                .where(TaskEntryModel.id == task_id)
                .values(process_after=datetime.now(UTC) - timedelta(minutes=1))
            )
            session.commit()

        # Sweep picks it up
        with Session(engine) as session:
            result = sweep_failed(session)
            session.commit()
            assert task_id in result

        # Now PENDING again
        with Session(engine) as session:
            task = session.get(TaskEntryModel, task_id)
            assert task.status == TaskStatus.PENDING.value

        # Second attempt: succeed
        with Session(engine) as session:
            result = process_task(session, task_id, lambda t, p: None)
            assert result is True

        with Session(engine) as session:
            task = session.get(TaskEntryModel, task_id)
            assert task.status == TaskStatus.COMPLETED.value
            assert task.attempts == 2  # Two attempts total

    def test_dead_letter_after_exhausting_retries(self, engine):
        """Task fails max_attempts times → DEAD."""
        with Session(engine) as session:
            task = create_task(session, task_type="test.exhaust", max_attempts=2)
            session.commit()
            task_id = task.id

        # Attempt 1: fail
        with Session(engine) as session:
            process_task(session, task_id, lambda t, p: (_ for _ in ()).throw(ValueError("fail")))

        # Fast-forward and sweep
        with Session(engine) as session:
            session.execute(
                update(TaskEntryModel)
                .where(TaskEntryModel.id == task_id)
                .values(process_after=datetime.now(UTC) - timedelta(minutes=1))
            )
            session.commit()
        with Session(engine) as session:
            sweep_failed(session)
            session.commit()

        # Attempt 2: fail again → should die
        with Session(engine) as session:
            process_task(session, task_id, lambda t, p: (_ for _ in ()).throw(ValueError("fail")))

        with Session(engine) as session:
            task = session.get(TaskEntryModel, task_id)
            assert task.status == TaskStatus.DEAD.value
            assert task.attempts == 2
