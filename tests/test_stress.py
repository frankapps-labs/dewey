"""Stress tests — concurrent processing, sweep races, high volume, two-phase visibility."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.executor import create_task, process_task
from dewey.sqlalchemy.models import TaskEntryModel


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
                    result = process_task(s, task_id, lambda **kw: None)
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
                    s, task_id, lambda **kw: (_ for _ in ()).throw(ValueError("boom"))
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
                task = create_task(session, task_type="test.batch", kwargs={"i": i})
                task_ids.append(task.id)
            session.commit()

        def worker(tid):
            with Session(engine) as s:
                return process_task(s, tid, lambda **kw: None)

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


class TestProcessingVisibility:
    """Sweep finds tasks stuck in PROCESSING — the two-phase commit payoff."""

    def test_processing_visible_to_other_sessions(self, engine):
        """While handler runs, PROCESSING is visible to other connections."""
        with Session(engine) as session:
            task = create_task(session, task_type="test.visible")
            session.commit()
            task_id = task.id

        observed_statuses = []
        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_handler(**kwargs):
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


class TestHighVolume:
    """Throughput test — create and process many tasks."""

    def test_500_tasks_20_workers(self, engine):
        task_ids = []
        with Session(engine) as session:
            for i in range(500):
                task = create_task(session, task_type="test.volume", kwargs={"i": i})
                task_ids.append(task.id)
            session.commit()

        def worker(tid):
            with Session(engine) as s:
                return process_task(s, tid, lambda **kw: None)

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
