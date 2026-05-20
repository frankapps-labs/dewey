"""Tests for the Celery adapter — uses task_always_eager for synchronous execution."""

import logging
from datetime import UTC

import pytest
from celery import Celery

from dewey.adapters.celery import CeleryAdapter


@pytest.fixture
def celery_app():
    """Celery app in eager mode — tasks execute synchronously, no broker needed."""
    app = Celery("test_dewey")
    app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True,
        broker_url="memory://",
        result_backend="cache+memory://",
    )
    return app


@pytest.fixture
def adapter(celery_app):
    return CeleryAdapter(celery_app)


class TestSetup:
    def test_setup_registers_process_task(self, adapter):
        adapter.setup(process_fn=lambda tid: None)
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

    def test_setup_registers_beat_schedule(self, adapter, celery_app):
        adapter.setup(
            process_fn=lambda tid: None,
            sweep_fn=lambda: None,
            sweep_interval_seconds=120,
        )
        assert "dewey-sweep" in celery_app.conf.beat_schedule
        assert celery_app.conf.beat_schedule["dewey-sweep"]["schedule"] == 120

    def test_setup_preserves_existing_beat_schedule(self, adapter, celery_app):
        celery_app.conf.beat_schedule = {"existing-task": {"task": "other", "schedule": 60}}
        adapter.setup(
            process_fn=lambda tid: None,
            sweep_fn=lambda: None,
        )
        assert "existing-task" in celery_app.conf.beat_schedule
        assert "dewey-sweep" in celery_app.conf.beat_schedule

    def test_setup_custom_task_names(self, adapter, celery_app):
        adapter.setup(
            process_fn=lambda tid: None,
            sweep_fn=lambda: None,
            task_name="myapp.process_ledger",
            sweep_task_name="myapp.sweep_ledger",
        )
        assert adapter._process_task.name == "myapp.process_ledger"
        assert adapter._sweep_task.name == "myapp.sweep_ledger"
        assert celery_app.conf.beat_schedule["dewey-sweep"]["task"] == "myapp.sweep_ledger"


class TestEnqueue:
    def test_enqueue_calls_process_fn(self, adapter):
        called_with = []
        adapter.setup(process_fn=lambda tid: called_with.append(tid))

        adapter.enqueue(task_id="task-abc-123")

        assert called_with == ["task-abc-123"]

    def test_enqueue_returns_async_result(self, adapter):
        adapter.setup(process_fn=lambda tid: f"processed-{tid}")

        result = adapter.enqueue(task_id="task-42")
        # In eager mode, result is immediately available
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

    def test_enqueue_with_queue_routing(self, adapter):
        """Celery supports per-call queue routing — no warning, just works."""
        called_with = []
        adapter.setup(process_fn=lambda tid: called_with.append(tid))

        adapter.enqueue(task_id="task-1", queue="critical")

        assert called_with == ["task-1"]

    def test_enqueue_with_priority(self, adapter):
        """Celery supports per-call priority — no warning, just works."""
        called_with = []
        adapter.setup(process_fn=lambda tid: called_with.append(tid))

        adapter.enqueue(task_id="task-1", priority=9)

        assert called_with == ["task-1"]

    def test_enqueue_no_spurious_warnings(self, adapter, caplog):
        """Unlike Huey, Celery shouldn't warn about queue/priority — it's native."""
        adapter.setup(process_fn=lambda tid: None)

        with caplog.at_level(logging.DEBUG):
            adapter.enqueue(task_id="task-1", queue="critical", priority=5)

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

    def test_enqueue_sweep_returns_async_result(self, adapter):
        adapter.setup(
            process_fn=lambda tid: None,
            sweep_fn=lambda: {"failed": 2, "stuck": 1},
        )

        result = adapter.enqueue_sweep()
        assert result.get() == {"failed": 2, "stuck": 1}


class TestProcessFnExceptions:
    def test_handler_exception_propagates_in_eager_mode(self, adapter):
        """With task_eager_propagates=True, exceptions propagate directly."""

        def failing_handler(tid):
            raise ValueError(f"handler failed for {tid}")

        adapter.setup(process_fn=failing_handler)

        with pytest.raises(ValueError, match="handler failed for task-bad"):
            adapter.enqueue(task_id="task-bad")


class TestIntegrationWithTaskledger:
    """End-to-end: adapter → executor → DB, using eager mode."""

    def test_full_lifecycle(self, adapter, engine):
        """Create task in DB, enqueue via adapter, verify it gets processed."""
        from sqlalchemy import select
        from sqlalchemy.orm import Session

        from dewey.core.states import TaskStatus
        from dewey.sqlalchemy.executor import create_task, process_task
        from dewey.sqlalchemy.models import TaskEntryModel

        # Create a task in the DB
        with Session(engine) as session:
            task = create_task(session, task_type="test.celery", payload={"key": "val"})
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

        # Enqueue — in eager mode, this processes synchronously
        adapter.enqueue(task_id=task_id)

        # Verify task completed in DB
        with Session(engine) as session:
            updated = session.execute(
                select(TaskEntryModel).where(TaskEntryModel.id == task_id)
            ).scalar_one()
            assert updated.status == TaskStatus.COMPLETED.value
            assert updated.attempts == 1

        assert handler_calls == [("test.celery", {"key": "val"})]

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
            task = create_task(session, task_type="test.sweep.celery")
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
