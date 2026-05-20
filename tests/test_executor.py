"""Tests for the SQLAlchemy executor."""

import threading
import time
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.executor import create_task, process_task
from dewey.sqlalchemy.models import TaskEntryModel


class TestCreateTask:
    def test_creates_with_defaults(self, session):
        task = create_task(session, task_type="order.confirmed")
        assert task.id is not None
        assert task.task_type == "order.confirmed"
        assert task.status == TaskStatus.PENDING.value
        assert task.payload == {}
        assert task.queue == "default"
        assert task.priority == 0
        assert task.max_attempts == 5

    def test_creates_with_payload(self, session):
        task = create_task(
            session,
            task_type="order.confirmed",
            payload={"order_id": "ORD-123"},
            queue="critical",
            priority=100,
            max_attempts=3,
        )
        assert task.payload == {"order_id": "ORD-123"}
        assert task.queue == "critical"
        assert task.priority == 100
        assert task.max_attempts == 3

    def test_creates_with_idempotency_key(self, session):
        task = create_task(
            session,
            task_type="order.confirmed",
            idempotency_key="order-123-confirm",
        )
        assert task.idempotency_key == "order-123-confirm"

    def test_creates_with_process_after(self, session):
        future = datetime(2026, 12, 1, tzinfo=UTC)
        task = create_task(
            session,
            task_type="reminder.send",
            process_after=future,
        )
        assert task.process_after == future

    def test_creates_with_metadata(self, session):
        task = create_task(
            session,
            task_type="test.task",
            metadata={"source": "api", "version": 2},
        )
        assert task.task_metadata == {"source": "api", "version": 2}


class TestProcessTask:
    def test_successful_processing(self, session):
        task = create_task(session, task_type="test.task", payload={"x": 1})
        session.commit()

        def handler(task_type, payload):
            assert task_type == "test.task"
            assert payload == {"x": 1}

        result = process_task(session, task.id, handler)
        assert result is True

        # Re-query to see committed state
        updated = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == task.id)
        ).scalar_one()
        assert updated.status == TaskStatus.COMPLETED.value
        assert updated.completed_at is not None
        assert updated.attempts == 1
        assert updated.error == ""

    def test_failed_processing(self, session):
        task = create_task(session, task_type="test.task", max_attempts=5)
        session.commit()

        def handler(task_type, payload):
            raise ValueError("Something went wrong")

        result = process_task(session, task.id, handler)
        assert result is False

        updated = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == task.id)
        ).scalar_one()
        assert updated.status == TaskStatus.FAILED.value
        assert updated.attempts == 1
        assert "Something went wrong" in updated.error
        assert updated.process_after is not None  # Backoff set

    def test_failed_processing_backoff_starts_at_failure_time(self, session):
        task = create_task(session, task_type="test.task", max_attempts=5)
        session.commit()
        failure_time = None

        def handler(task_type, payload):
            nonlocal failure_time
            time.sleep(0.01)
            failure_time = datetime.now(UTC)
            raise ValueError("late failure")

        result = process_task(session, task.id, handler, backoff=lambda attempts: timedelta(0))
        assert result is False

        updated = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == task.id)
        ).scalar_one()
        assert updated.process_after >= failure_time

    def test_dead_letter_after_max_attempts(self, session):
        task = create_task(session, task_type="test.task", max_attempts=1)
        session.commit()

        def handler(task_type, payload):
            raise ValueError("Fatal")

        result = process_task(session, task.id, handler)
        assert result is False

        updated = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == task.id)
        ).scalar_one()
        assert updated.status == TaskStatus.DEAD.value
        assert updated.attempts == 1

    def test_skip_already_completed(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.COMPLETED.value
        session.commit()

        called = False

        def handler(task_type, payload):
            nonlocal called
            called = True

        result = process_task(session, task.id, handler)
        assert result is False
        assert called is False

    def test_skip_nonexistent_task(self, session):
        def handler(task_type, payload):
            pass

        result = process_task(session, "nonexistent-id", handler)
        assert result is False

    def test_skip_processing_task(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.PROCESSING.value
        session.commit()

        result = process_task(session, task.id, lambda t, p: None)
        assert result is False

    def test_skip_task_not_ready(self, session):
        """Tasks with future process_after should be skipped."""
        future = datetime.now(UTC) + timedelta(hours=1)
        task = create_task(session, task_type="test.task", process_after=future)
        session.commit()

        result = process_task(session, task.id, lambda t, p: None)
        assert result is False

        updated = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == task.id)
        ).scalar_one()
        assert updated.status == TaskStatus.PENDING.value

    def test_processes_task_past_process_after(self, session):
        """Tasks with past process_after should be processed."""
        past = datetime.now(UTC) - timedelta(minutes=5)
        task = create_task(session, task_type="test.task", process_after=past)
        session.commit()

        result = process_task(session, task.id, lambda t, p: None)
        assert result is True

    def test_processing_state_committed(self, session, engine):
        """PROCESSING state is visible to other sessions (two-phase commit)."""
        import threading

        task = create_task(session, task_type="test.visible")
        session.commit()
        task_id = task.id

        observed_status = [None]
        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_handler(task_type, payload):
            handler_started.set()
            handler_continue.wait(timeout=10)

        def observer():
            handler_started.wait(timeout=10)
            # Small delay to ensure commit happened
            import time

            time.sleep(0.1)
            from sqlalchemy.orm import Session as S

            with S(engine) as obs_session:
                t = obs_session.get(TaskEntryModel, task_id)
                observed_status[0] = t.status
            handler_continue.set()

        t = threading.Thread(target=observer)
        t.start()

        process_task(session, task_id, slow_handler)
        t.join(timeout=15)

        assert observed_status[0] == TaskStatus.PROCESSING.value

    def test_task_deleted_during_success(self, engine):
        """If task is deleted mid-processing, Phase 3b handles it gracefully."""
        with Session(engine) as session:
            task = create_task(session, task_type="test.disappear")
            session.commit()
            task_id = task.id

        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_handler(task_type, payload):
            handler_started.set()
            handler_continue.wait(timeout=10)

        def deleter():
            handler_started.wait(timeout=10)
            time.sleep(0.1)
            with Session(engine) as s:
                s.execute(delete(TaskEntryModel).where(TaskEntryModel.id == task_id))
                s.commit()
            handler_continue.set()

        t = threading.Thread(target=deleter)
        t.start()

        with Session(engine) as session:
            result = process_task(session, task_id, slow_handler)

        t.join(timeout=15)
        # Should return False gracefully, not crash
        assert result is False

    def test_task_deleted_during_failure(self, engine):
        """If task is deleted while handler fails, Phase 3a handles it gracefully."""
        with Session(engine) as session:
            task = create_task(session, task_type="test.disappear.fail", max_attempts=5)
            session.commit()
            task_id = task.id

        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_failing_handler(task_type, payload):
            handler_started.set()
            handler_continue.wait(timeout=10)
            raise ValueError("fail after delete")

        def deleter():
            handler_started.wait(timeout=10)
            time.sleep(0.1)
            with Session(engine) as s:
                s.execute(delete(TaskEntryModel).where(TaskEntryModel.id == task_id))
                s.commit()
            handler_continue.set()

        t = threading.Thread(target=deleter)
        t.start()

        with Session(engine) as session:
            result = process_task(session, task_id, slow_failing_handler)

        t.join(timeout=15)
        assert result is False

    def test_task_killed_during_success(self, engine):
        """If task is killed mid-processing, Phase 3b respects the DEAD status."""
        with Session(engine) as session:
            task = create_task(session, task_type="test.killed")
            session.commit()
            task_id = task.id

        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_handler(task_type, payload):
            handler_started.set()
            handler_continue.wait(timeout=10)

        def killer():
            handler_started.wait(timeout=10)
            time.sleep(0.1)
            with Session(engine) as s:
                s.execute(
                    update(TaskEntryModel)
                    .where(TaskEntryModel.id == task_id)
                    .values(status=TaskStatus.DEAD.value)
                )
                s.commit()
            handler_continue.set()

        t = threading.Thread(target=killer)
        t.start()

        with Session(engine) as session:
            result = process_task(session, task_id, slow_handler)

        t.join(timeout=15)
        # Should return False — task was killed, not overwrite with COMPLETED
        assert result is False

        with Session(engine) as session:
            task = session.get(TaskEntryModel, task_id)
            assert task.status == TaskStatus.DEAD.value

    def test_task_killed_during_failure(self, engine):
        """If task is killed while handler fails, Phase 3a respects the DEAD status."""
        with Session(engine) as session:
            task = create_task(session, task_type="test.killed.fail", max_attempts=5)
            session.commit()
            task_id = task.id

        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_failing_handler(task_type, payload):
            handler_started.set()
            handler_continue.wait(timeout=10)
            raise ValueError("fail after kill")

        def killer():
            handler_started.wait(timeout=10)
            time.sleep(0.1)
            with Session(engine) as s:
                s.execute(
                    update(TaskEntryModel)
                    .where(TaskEntryModel.id == task_id)
                    .values(status=TaskStatus.DEAD.value)
                )
                s.commit()
            handler_continue.set()

        t = threading.Thread(target=killer)
        t.start()

        with Session(engine) as session:
            result = process_task(session, task_id, slow_failing_handler)

        t.join(timeout=15)
        assert result is False

        with Session(engine) as session:
            task = session.get(TaskEntryModel, task_id)
            # DEAD should be preserved, not overwritten with FAILED
            assert task.status == TaskStatus.DEAD.value
