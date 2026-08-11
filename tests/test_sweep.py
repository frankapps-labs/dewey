"""Tests for the sweep module."""

from datetime import UTC, datetime, timedelta

from sqlalchemy import update

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.executor import create_task
from dewey.sqlalchemy.models import TaskEntryModel
from dewey.sqlalchemy.sweep import sweep, sweep_dispatching, sweep_failed, sweep_stuck


class TestSweepFailed:
    def test_re_enqueues_failed_tasks_past_scheduled_for(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.FAILED.value
        task.scheduled_for = datetime.now(UTC) - timedelta(minutes=1)
        session.commit()

        ids = sweep_failed(session)
        assert task.id in ids

        session.expire_all()
        updated = session.get(TaskEntryModel, task.id)
        assert updated.status == TaskStatus.PENDING.value

    def test_skips_failed_tasks_not_yet_ready(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.FAILED.value
        task.scheduled_for = datetime.now(UTC) + timedelta(hours=1)
        session.commit()

        ids = sweep_failed(session)
        assert ids == []

        session.expire_all()
        updated = session.get(TaskEntryModel, task.id)
        assert updated.status == TaskStatus.FAILED.value

    def test_dead_letters_exhausted_failed_tasks(self, session):
        task = create_task(session, task_type="test.task", max_attempts=1)
        task.status = TaskStatus.FAILED.value
        task.attempts = 1
        task.scheduled_for = datetime.now(UTC) - timedelta(minutes=1)
        session.commit()

        ids = sweep_failed(session)
        assert task.id not in ids

        session.expire_all()
        updated = session.get(TaskEntryModel, task.id)
        assert updated.status == TaskStatus.DEAD.value

    def test_returns_empty_when_no_failed(self, session):
        create_task(session, task_type="test.task")
        session.commit()

        ids = sweep_failed(session)
        assert ids == []


class TestSweepStuck:
    def test_unsticks_processing_tasks(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.PROCESSING.value
        task.started_at = datetime.now(UTC) - timedelta(minutes=15)
        session.commit()

        ids = sweep_stuck(session, stuck_threshold_minutes=10)
        assert task.id in ids

        session.expire_all()
        updated = session.get(TaskEntryModel, task.id)
        assert updated.status == TaskStatus.PENDING.value

    def test_skips_recently_started(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.PROCESSING.value
        task.started_at = datetime.now(UTC) - timedelta(minutes=2)
        session.commit()

        ids = sweep_stuck(session, stuck_threshold_minutes=10)
        assert ids == []

        session.expire_all()
        updated = session.get(TaskEntryModel, task.id)
        assert updated.status == TaskStatus.PROCESSING.value

    def test_dead_letters_exhausted_stuck_tasks(self, session):
        task = create_task(session, task_type="test.task", max_attempts=1)
        task.status = TaskStatus.PROCESSING.value
        task.attempts = 1
        task.started_at = datetime.now(UTC) - timedelta(minutes=15)
        session.commit()

        ids = sweep_stuck(session, stuck_threshold_minutes=10)
        assert task.id not in ids

        session.expire_all()
        updated = session.get(TaskEntryModel, task.id)
        assert updated.status == TaskStatus.DEAD.value


class TestSweepCombined:
    def test_sweep_runs_both(self, session):
        # One failed task ready for retry
        failed = create_task(session, task_type="test.fail")
        failed.status = TaskStatus.FAILED.value
        failed.scheduled_for = datetime.now(UTC) - timedelta(minutes=1)

        # One stuck processing task
        stuck = create_task(session, task_type="test.stuck")
        stuck.status = TaskStatus.PROCESSING.value
        stuck.started_at = datetime.now(UTC) - timedelta(minutes=20)

        session.commit()

        result = sweep(session)
        assert failed.id in result["failed"]
        assert stuck.id in result["stuck"]


class TestSweepDispatching:
    """Reclaim rows a dispatcher claimed but no worker ever started."""

    def _dispatching(self, session, *, age_seconds: int, attempts: int = 0, max_attempts: int = 5):
        task = create_task(session, task_type="test.dispatch", max_attempts=max_attempts)
        session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == task.id)
            .values(
                status=TaskStatus.DISPATCHING.value,
                dispatching_at=datetime.now(UTC) - timedelta(seconds=age_seconds),
                attempts=attempts,
            )
        )
        session.commit()
        return task.id

    def test_reclaims_a_row_past_the_dispatch_timeout(self, session):
        task_id = self._dispatching(session, age_seconds=600)

        reclaimed = sweep_dispatching(session, dispatch_timeout_seconds=300)
        session.commit()

        assert reclaimed == [task_id]
        row = session.get(TaskEntryModel, task_id)
        assert row.status == TaskStatus.PENDING.value
        assert row.dispatching_at is None

    def test_leaves_a_recent_claim_alone(self, session):
        """A healthy broker backlog must not be reclaimed and dispatched twice."""
        task_id = self._dispatching(session, age_seconds=10)

        assert sweep_dispatching(session, dispatch_timeout_seconds=300) == []
        assert session.get(TaskEntryModel, task_id).status == TaskStatus.DISPATCHING.value

    def test_dead_letters_when_the_attempt_budget_is_spent(self, session):
        task_id = self._dispatching(session, age_seconds=600, attempts=5, max_attempts=5)

        assert sweep_dispatching(session, dispatch_timeout_seconds=300) == []
        session.commit()
        assert session.get(TaskEntryModel, task_id).status == TaskStatus.DEAD.value

    def test_sweep_runs_the_dispatching_pass(self, session):
        task_id = self._dispatching(session, age_seconds=600)

        result = sweep(session, dispatch_timeout_seconds=300)
        session.commit()

        assert result["dispatching"] == [task_id]
