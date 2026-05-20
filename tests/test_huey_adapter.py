"""Tests for the Huey adapter — uses SqliteHuey(immediate=True) as in-memory backend."""

import logging
from datetime import UTC

import pytest
from huey import SqliteHuey

from dewey.adapters.huey import HueyAdapter


@pytest.fixture
def huey():
    """Huey instance in immediate mode — tasks execute synchronously, no Redis needed."""
    return SqliteHuey(filename="/tmp/test_dewey_huey.db", immediate=True)


@pytest.fixture
def adapter(huey):
    return HueyAdapter(huey)


class TestSetup:
    def test_setup_registers_process_task(self, adapter):
        called_with = []
        adapter.setup(process_fn=lambda tid: called_with.append(tid))

        assert adapter._process_task is not None

    def test_setup_registers_sweep_when_provided(self, adapter):
        adapter.setup(
            process_fn=lambda tid: None,
            sweep_fn=lambda: "swept",
        )
        assert adapter._sweep_task is not None

    def test_setup_no_sweep_when_omitted(self, adapter):
        adapter.setup(process_fn=lambda tid: None)
        assert adapter._sweep_task is None


class TestEnqueue:
    def test_enqueue_calls_process_fn(self, adapter):
        called_with = []
        adapter.setup(process_fn=lambda tid: called_with.append(tid))

        adapter.enqueue(task_id="task-abc-123")

        assert called_with == ["task-abc-123"]

    def test_enqueue_returns_result(self, adapter):
        adapter.setup(process_fn=lambda tid: f"processed-{tid}")

        result = adapter.enqueue(task_id="task-42")
        # In immediate mode, Huey returns the result directly
        assert result.get() == "processed-task-42"

    def test_enqueue_multiple_tasks(self, adapter):
        called_with = []
        adapter.setup(process_fn=lambda tid: called_with.append(tid))

        adapter.enqueue(task_id="task-1")
        adapter.enqueue(task_id="task-2")
        adapter.enqueue(task_id="task-3")

        assert called_with == ["task-1", "task-2", "task-3"]

    def test_enqueue_before_setup_raises(self, adapter):
        with pytest.raises(RuntimeError, match="setup\\(\\) must be called before enqueue"):
            adapter.enqueue(task_id="task-1")

    def test_enqueue_non_default_queue_logs_warning(self, adapter, caplog):
        adapter.setup(process_fn=lambda tid: None)

        with caplog.at_level(logging.DEBUG):
            adapter.enqueue(task_id="task-1", queue="critical")

        assert "queue=critical is informational only" in caplog.text

    def test_enqueue_nonzero_priority_logs_warning(self, adapter, caplog):
        adapter.setup(process_fn=lambda tid: None)

        with caplog.at_level(logging.DEBUG):
            adapter.enqueue(task_id="task-1", priority=10)

        assert "priority=10 is informational only" in caplog.text

    def test_enqueue_default_queue_no_warning(self, adapter, caplog):
        adapter.setup(process_fn=lambda tid: None)

        with caplog.at_level(logging.DEBUG):
            adapter.enqueue(task_id="task-1", queue="default", priority=0)

        assert "informational only" not in caplog.text


class TestEnqueueSweep:
    def test_enqueue_sweep_calls_sweep_fn(self, adapter):
        sweep_calls = []
        adapter.setup(
            process_fn=lambda tid: None,
            sweep_fn=lambda: sweep_calls.append("swept"),
        )

        adapter.enqueue_sweep()

        assert sweep_calls == ["swept"]

    def test_enqueue_sweep_without_sweep_fn_raises(self, adapter):
        adapter.setup(process_fn=lambda tid: None)

        with pytest.raises(RuntimeError, match="No sweep_fn was registered"):
            adapter.enqueue_sweep()


class TestProcessFnExceptions:
    def test_handler_exception_wrapped_by_huey(self, adapter):
        """Exceptions from process_fn are wrapped in Huey's TaskException."""
        from huey.exceptions import TaskException

        def failing_handler(tid):
            raise ValueError(f"handler failed for {tid}")

        adapter.setup(process_fn=failing_handler)

        result = adapter.enqueue(task_id="task-bad")
        # Huey wraps exceptions in TaskException in immediate mode
        with pytest.raises(TaskException, match="handler failed for task-bad"):
            result.get()


class TestIntegrationWithTaskledger:
    """End-to-end: adapter → executor → DB, using immediate mode."""

    def test_full_lifecycle(self, adapter, engine):
        """Create task in DB, enqueue via adapter, verify it gets processed."""
        from sqlalchemy import select
        from sqlalchemy.orm import Session

        from dewey.core.states import TaskStatus
        from dewey.sqlalchemy.executor import create_task, process_task
        from dewey.sqlalchemy.models import TaskEntryModel

        # Create a task in the DB
        with Session(engine) as session:
            task = create_task(session, task_type="test.huey", payload={"key": "val"})
            session.commit()
            task_id = task.id

        # Wire up the adapter with a real process_fn
        handler_calls = []

        def my_handler(task_type, payload):
            handler_calls.append((task_type, payload))

        def process_fn(tid):
            with Session(engine) as session:
                return process_task(session, tid, my_handler)

        adapter.setup(process_fn=process_fn)

        # Enqueue — in immediate mode, this processes synchronously
        adapter.enqueue(task_id=task_id)

        # Verify task completed in DB
        with Session(engine) as session:
            updated = session.execute(
                select(TaskEntryModel).where(TaskEntryModel.id == task_id)
            ).scalar_one()
            assert updated.status == TaskStatus.COMPLETED.value
            assert updated.attempts == 1

        assert handler_calls == [("test.huey", {"key": "val"})]

    def test_sweep_integration(self, adapter, engine):
        """Sweep via adapter picks up failed tasks."""
        from datetime import datetime, timedelta

        from sqlalchemy import select, update
        from sqlalchemy.orm import Session

        from dewey.core.states import TaskStatus
        from dewey.sqlalchemy.executor import create_task
        from dewey.sqlalchemy.models import TaskEntryModel
        from dewey.sqlalchemy.sweep import sweep

        # Create a failed task ready for retry
        with Session(engine) as session:
            task = create_task(session, task_type="test.sweep.huey")
            session.commit()
            task_id = task.id

        with Session(engine) as session:
            session.execute(
                update(TaskEntryModel)
                .where(TaskEntryModel.id == task_id)
                .values(
                    status=TaskStatus.FAILED.value,
                    process_after=datetime.now(UTC) - timedelta(minutes=5),
                    attempts=1,
                )
            )
            session.commit()

        # Wire up adapter with sweep
        def sweep_fn():
            with Session(engine) as session:
                result = sweep(session)
                session.commit()
                return result

        adapter.setup(
            process_fn=lambda tid: None,
            sweep_fn=sweep_fn,
        )

        adapter.enqueue_sweep()

        # Task should be back to PENDING
        with Session(engine) as session:
            updated = session.execute(
                select(TaskEntryModel).where(TaskEntryModel.id == task_id)
            ).scalar_one()
            assert updated.status == TaskStatus.PENDING.value
