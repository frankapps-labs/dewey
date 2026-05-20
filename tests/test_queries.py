"""Tests for the query & action API."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from dewey.core.states import TaskStatus
from dewey.core.types import TaskEntry
from dewey.sqlalchemy.executor import create_task
from dewey.sqlalchemy.models import TaskEntryModel
from dewey.sqlalchemy.queries import (
    bulk_retry,
    get_dead,
    get_failed,
    get_pending,
    get_processing,
    get_recent,
    get_stats,
    get_stuck,
    get_task,
    kill_task,
    purge_completed,
    retry_task,
)


class TestGetStats:
    def test_empty_stats(self, session):
        stats = get_stats(session)
        assert stats == {
            "pending": 0,
            "processing": 0,
            "completed": 0,
            "failed": 0,
            "dead": 0,
        }

    def test_counts_by_status(self, session):
        create_task(session, task_type="a")
        create_task(session, task_type="b")
        t3 = create_task(session, task_type="c")
        t3.status = TaskStatus.COMPLETED.value
        session.flush()

        stats = get_stats(session)
        assert stats["pending"] == 2
        assert stats["completed"] == 1


class TestGetPending:
    def test_returns_pending_tasks(self, session):
        create_task(session, task_type="test.a")
        create_task(session, task_type="test.b")
        t3 = create_task(session, task_type="test.c")
        t3.status = TaskStatus.COMPLETED.value
        session.flush()

        results = get_pending(session)
        assert len(results) == 2
        assert all(isinstance(r, TaskEntry) for r in results)

    def test_filter_by_task_type(self, session):
        create_task(session, task_type="order.confirmed")
        create_task(session, task_type="email.send")
        session.flush()

        results = get_pending(session, task_type="order.confirmed")
        assert len(results) == 1
        assert results[0].task_type == "order.confirmed"


class TestGetProcessing:
    def test_returns_processing_tasks(self, session):
        t1 = create_task(session, task_type="test.a")
        t1.status = TaskStatus.PROCESSING.value
        t1.started_at = datetime.now(UTC)
        create_task(session, task_type="test.b")  # pending — not returned
        session.flush()

        results = get_processing(session)
        assert len(results) == 1
        assert results[0].status == TaskStatus.PROCESSING


class TestGetStuck:
    def test_returns_stuck_tasks(self, session):
        t1 = create_task(session, task_type="test.stuck")
        t1.status = TaskStatus.PROCESSING.value
        t1.started_at = datetime.now(UTC) - timedelta(minutes=30)
        session.flush()

        results = get_stuck(session, older_than_minutes=10)
        assert len(results) == 1

    def test_skips_recent_processing(self, session):
        t1 = create_task(session, task_type="test.recent")
        t1.status = TaskStatus.PROCESSING.value
        t1.started_at = datetime.now(UTC) - timedelta(minutes=2)
        session.flush()

        results = get_stuck(session, older_than_minutes=10)
        assert len(results) == 0


class TestGetFailed:
    def test_returns_failed_tasks(self, session):
        t1 = create_task(session, task_type="test.fail")
        t1.status = TaskStatus.FAILED.value
        t1.error = "boom"
        create_task(session, task_type="test.ok")  # pending — not returned
        session.flush()

        results = get_failed(session)
        assert len(results) == 1
        assert results[0].status == TaskStatus.FAILED


class TestGetDead:
    def test_returns_dead_tasks(self, session):
        t1 = create_task(session, task_type="test.dead")
        t1.status = TaskStatus.DEAD.value
        create_task(session, task_type="test.ok")  # pending — not returned
        session.flush()

        results = get_dead(session)
        assert len(results) == 1
        assert results[0].status == TaskStatus.DEAD


class TestGetTask:
    def test_returns_task(self, session):
        task = create_task(session, task_type="test.task", payload={"x": 1})
        session.flush()

        result = get_task(session, task.id)
        assert result is not None
        assert result.id == task.id
        assert result.payload == {"x": 1}

    def test_returns_none_for_missing(self, session):
        assert get_task(session, "nonexistent") is None


class TestGetRecent:
    def test_returns_recent(self, session):
        create_task(session, task_type="a")
        create_task(session, task_type="b")
        session.flush()

        results = get_recent(session)
        assert len(results) == 2

    def test_filter_by_status(self, session):
        create_task(session, task_type="a")
        t2 = create_task(session, task_type="b")
        t2.status = TaskStatus.COMPLETED.value
        session.flush()

        results = get_recent(session, status=TaskStatus.COMPLETED)
        assert len(results) == 1
        assert results[0].task_type == "b"

    def test_filter_by_since(self, session):
        t1 = create_task(session, task_type="old")
        t1.created_at = datetime.now(UTC) - timedelta(days=7)
        create_task(session, task_type="new")
        session.flush()

        since = datetime.now(UTC) - timedelta(days=1)
        results = get_recent(session, since=since)
        assert len(results) == 1
        assert results[0].task_type == "new"


class TestRetryTask:
    def test_retry_failed_task(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.FAILED.value
        task.error = "some error"
        task.attempts = 3
        session.flush()

        result = retry_task(session, task.id)
        assert result is not None
        assert result.status == TaskStatus.PENDING
        assert result.attempts == 0  # Reset
        assert result.error == ""

    def test_retry_dead_task(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.DEAD.value
        task.attempts = 5
        session.flush()

        result = retry_task(session, task.id)
        assert result is not None
        assert result.status == TaskStatus.PENDING
        assert result.attempts == 0  # Reset

    def test_retry_nonexistent(self, session):
        assert retry_task(session, "nope") is None

    def test_retry_pending_noop(self, session):
        task = create_task(session, task_type="test.task")
        session.flush()

        result = retry_task(session, task.id)
        assert result is not None
        assert result.status == TaskStatus.PENDING

    def test_retry_completed_noop(self, session):
        """Can't retry a completed task — no transition COMPLETED → PENDING."""
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.COMPLETED.value
        session.flush()

        result = retry_task(session, task.id)
        assert result is not None
        assert result.status == TaskStatus.COMPLETED  # Unchanged


class TestBulkRetry:
    def test_retries_all_failed(self, session):
        t1 = create_task(session, task_type="a")
        t1.status = TaskStatus.FAILED.value
        t1.attempts = 3
        t2 = create_task(session, task_type="b")
        t2.status = TaskStatus.FAILED.value
        t2.attempts = 2
        create_task(session, task_type="c")  # pending — not touched
        session.flush()

        count = bulk_retry(session)
        assert count == 2

        # Verify attempts reset
        session.expire_all()
        updated = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == t1.id)
        ).scalar_one()
        assert updated.attempts == 0

    def test_filter_by_task_type(self, session):
        t1 = create_task(session, task_type="order.confirmed")
        t1.status = TaskStatus.FAILED.value
        t2 = create_task(session, task_type="email.send")
        t2.status = TaskStatus.FAILED.value
        session.flush()

        count = bulk_retry(session, task_type="order.confirmed")
        assert count == 1

    def test_rejects_invalid_source_status(self, session):
        """bulk_retry should raise ValueError for statuses that can't transition to PENDING."""
        with pytest.raises(ValueError, match="Cannot retry tasks in 'completed' state"):
            bulk_retry(session, status=TaskStatus.COMPLETED)

    def test_accepts_dead_status(self, session):
        """bulk_retry should accept DEAD as source status (DEAD → PENDING is valid)."""
        t1 = create_task(session, task_type="test.dead")
        t1.status = TaskStatus.DEAD.value
        t1.attempts = 5
        session.flush()

        count = bulk_retry(session, status=TaskStatus.DEAD)
        assert count == 1

        session.expire_all()
        updated = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == t1.id)
        ).scalar_one()
        assert updated.status == TaskStatus.PENDING.value
        assert updated.attempts == 0


class TestKillTask:
    def test_kills_pending_task(self, session):
        task = create_task(session, task_type="test.task")
        session.flush()

        result = kill_task(session, task.id)
        assert result is not None
        assert result.status == TaskStatus.DEAD

    def test_kills_failed_task(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.FAILED.value
        session.flush()

        result = kill_task(session, task.id)
        assert result is not None
        assert result.status == TaskStatus.DEAD

    def test_kill_nonexistent(self, session):
        assert kill_task(session, "nope") is None

    def test_kill_completed_noop(self, session):
        """Can't kill a completed task — COMPLETED is fully terminal."""
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.COMPLETED.value
        session.flush()

        result = kill_task(session, task.id)
        assert result is not None
        assert result.status == TaskStatus.COMPLETED  # Unchanged

    def test_kill_dead_noop(self, session):
        """Killing an already dead task is a no-op."""
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.DEAD.value
        session.flush()

        result = kill_task(session, task.id)
        assert result is not None
        assert result.status == TaskStatus.DEAD  # Unchanged


class TestPurgeCompleted:
    def test_purges_old_completed(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.COMPLETED.value
        task.completed_at = datetime.now(UTC) - timedelta(days=60)
        session.flush()

        count = purge_completed(session, older_than_days=30)
        assert count == 1

    def test_keeps_recent_completed(self, session):
        task = create_task(session, task_type="test.task")
        task.status = TaskStatus.COMPLETED.value
        task.completed_at = datetime.now(UTC) - timedelta(days=5)
        session.flush()

        count = purge_completed(session, older_than_days=30)
        assert count == 0
