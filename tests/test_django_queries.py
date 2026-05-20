"""Tests for Django queries — stats, list, actions."""

# ruff: noqa: E402 — django.setup() must run before model imports

import os
from datetime import timedelta

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.utils import timezone

from dewey.core.states import TaskStatus
from dewey.django.executor import create_task
from dewey.django.models import TaskEntry
from dewey.django.queries import (
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


@pytest.fixture(autouse=True)
def clean_db():
    yield
    TaskEntry.objects.all().delete()


@pytest.mark.django_db(transaction=True)
class TestStats:
    def test_empty_stats(self):
        stats = get_stats()
        assert stats == {s.value: 0 for s in TaskStatus}

    def test_counts_by_status(self):
        create_task(task_type="a")
        create_task(task_type="b")
        t3 = create_task(task_type="c")
        TaskEntry.objects.filter(id=t3.id).update(status=TaskStatus.COMPLETED.value)

        stats = get_stats()
        assert stats["pending"] == 2
        assert stats["completed"] == 1


@pytest.mark.django_db(transaction=True)
class TestGetPending:
    def test_returns_pending_tasks(self):
        create_task(task_type="test.a")
        create_task(task_type="test.b")
        t3 = create_task(task_type="test.c")
        TaskEntry.objects.filter(id=t3.id).update(status=TaskStatus.COMPLETED.value)

        results = get_pending()
        assert len(results) == 2
        assert all(r.status == TaskStatus.PENDING for r in results)

    def test_filter_by_task_type(self):
        create_task(task_type="order.confirmed")
        create_task(task_type="email.send")

        results = get_pending(task_type="order.confirmed")
        assert len(results) == 1
        assert results[0].task_type == "order.confirmed"


@pytest.mark.django_db(transaction=True)
class TestGetProcessing:
    def test_returns_processing_tasks(self):
        t1 = create_task(task_type="test.a")
        TaskEntry.objects.filter(id=t1.id).update(
            status=TaskStatus.PROCESSING.value,
            started_at=timezone.now(),
        )
        create_task(task_type="test.b")  # pending — not returned

        results = get_processing()
        assert len(results) == 1
        assert results[0].status == TaskStatus.PROCESSING


@pytest.mark.django_db(transaction=True)
class TestGetStuck:
    def test_returns_stuck_tasks(self):
        t1 = create_task(task_type="test.stuck")
        TaskEntry.objects.filter(id=t1.id).update(
            status=TaskStatus.PROCESSING.value,
            started_at=timezone.now() - timedelta(minutes=30),
        )

        results = get_stuck(older_than_minutes=10)
        assert len(results) == 1

    def test_skips_recent_processing(self):
        t1 = create_task(task_type="test.recent")
        TaskEntry.objects.filter(id=t1.id).update(
            status=TaskStatus.PROCESSING.value,
            started_at=timezone.now() - timedelta(minutes=2),
        )

        results = get_stuck(older_than_minutes=10)
        assert len(results) == 0


@pytest.mark.django_db(transaction=True)
class TestGetFailed:
    def test_returns_failed_tasks(self):
        t1 = create_task(task_type="test.fail")
        TaskEntry.objects.filter(id=t1.id).update(
            status=TaskStatus.FAILED.value,
            error="boom",
        )
        create_task(task_type="test.ok")

        results = get_failed()
        assert len(results) == 1
        assert results[0].status == TaskStatus.FAILED


@pytest.mark.django_db(transaction=True)
class TestGetDead:
    def test_returns_dead_tasks(self):
        t1 = create_task(task_type="test.dead")
        TaskEntry.objects.filter(id=t1.id).update(status=TaskStatus.DEAD.value)
        create_task(task_type="test.ok")

        results = get_dead()
        assert len(results) == 1
        assert results[0].status == TaskStatus.DEAD


@pytest.mark.django_db(transaction=True)
class TestGetTask:
    def test_returns_task(self):
        task = create_task(task_type="test.task", payload={"x": 1})
        result = get_task(task.id)
        assert result is not None
        assert result.task_type == "test.task"
        assert result.payload == {"x": 1}

    def test_returns_none_for_missing(self):
        assert get_task("nonexistent") is None


@pytest.mark.django_db(transaction=True)
class TestGetRecent:
    def test_returns_recent(self):
        create_task(task_type="a")
        create_task(task_type="b")
        results = get_recent()
        assert len(results) == 2

    def test_filter_by_status(self):
        create_task(task_type="a")
        t2 = create_task(task_type="b")
        TaskEntry.objects.filter(id=t2.id).update(status=TaskStatus.COMPLETED.value)

        results = get_recent(status=TaskStatus.COMPLETED)
        assert len(results) == 1
        assert results[0].status == TaskStatus.COMPLETED


@pytest.mark.django_db(transaction=True)
class TestRetryTask:
    def test_retry_failed_task(self):
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(
            status=TaskStatus.FAILED.value, error="boom", attempts=3
        )

        result = retry_task(task.id)
        assert result is not None
        assert result.status == TaskStatus.PENDING
        assert result.attempts == 0  # Reset
        assert result.error == ""

    def test_retry_dead_task(self):
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(status=TaskStatus.DEAD.value, attempts=5)

        result = retry_task(task.id)
        assert result is not None
        assert result.status == TaskStatus.PENDING
        assert result.attempts == 0  # Reset

    def test_retry_nonexistent(self):
        assert retry_task("nope") is None

    def test_retry_pending_noop(self):
        task = create_task(task_type="test.task")
        result = retry_task(task.id)
        assert result is not None
        assert result.status == TaskStatus.PENDING  # unchanged

    def test_retry_completed_noop(self):
        """Can't retry a completed task."""
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(status=TaskStatus.COMPLETED.value)

        result = retry_task(task.id)
        assert result is not None
        assert result.status == TaskStatus.COMPLETED  # Unchanged


@pytest.mark.django_db(transaction=True)
class TestBulkRetry:
    def test_retries_all_failed(self):
        t1 = create_task(task_type="a")
        t2 = create_task(task_type="b")
        create_task(task_type="c")  # pending — not touched
        TaskEntry.objects.filter(id__in=[t1.id, t2.id]).update(
            status=TaskStatus.FAILED.value, attempts=3
        )

        count = bulk_retry()
        assert count == 2

        # Verify attempts reset
        updated = TaskEntry.objects.get(id=t1.id)
        assert updated.attempts == 0

    def test_filter_by_task_type(self):
        t1 = create_task(task_type="order.confirmed")
        t2 = create_task(task_type="email.send")
        TaskEntry.objects.filter(id__in=[t1.id, t2.id]).update(status=TaskStatus.FAILED.value)

        count = bulk_retry(task_type="order.confirmed")
        assert count == 1

    def test_rejects_invalid_source_status(self):
        """bulk_retry should raise ValueError for statuses that can't transition to PENDING."""
        with pytest.raises(ValueError, match="Cannot retry tasks in 'completed' state"):
            bulk_retry(status=TaskStatus.COMPLETED)

    def test_accepts_dead_status(self):
        """bulk_retry should accept DEAD as source status (DEAD → PENDING is valid)."""
        t1 = create_task(task_type="test.dead")
        TaskEntry.objects.filter(id=t1.id).update(status=TaskStatus.DEAD.value, attempts=5)

        count = bulk_retry(status=TaskStatus.DEAD)
        assert count == 1

        updated = TaskEntry.objects.get(id=t1.id)
        assert updated.status == TaskStatus.PENDING.value
        assert updated.attempts == 0


@pytest.mark.django_db(transaction=True)
class TestKillTask:
    def test_kills_pending_task(self):
        task = create_task(task_type="test.task")
        result = kill_task(task.id)
        assert result is not None
        assert result.status == TaskStatus.DEAD

    def test_kills_failed_task(self):
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(status=TaskStatus.FAILED.value)

        result = kill_task(task.id)
        assert result is not None
        assert result.status == TaskStatus.DEAD

    def test_kill_nonexistent(self):
        assert kill_task("nope") is None

    def test_kill_completed_noop(self):
        """Can't kill a completed task — COMPLETED is fully terminal."""
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(status=TaskStatus.COMPLETED.value)

        result = kill_task(task.id)
        assert result is not None
        assert result.status == TaskStatus.COMPLETED  # Unchanged

    def test_kill_dead_noop(self):
        """Killing an already dead task is a no-op."""
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(status=TaskStatus.DEAD.value)

        result = kill_task(task.id)
        assert result is not None
        assert result.status == TaskStatus.DEAD  # Unchanged


@pytest.mark.django_db(transaction=True)
class TestPurgeCompleted:
    def test_purges_old_completed(self):
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(
            status=TaskStatus.COMPLETED.value,
            completed_at=timezone.now() - timedelta(days=60),
        )

        count = purge_completed(older_than_days=30)
        assert count == 1

    def test_keeps_recent_completed(self):
        task = create_task(task_type="test.task")
        TaskEntry.objects.filter(id=task.id).update(
            status=TaskStatus.COMPLETED.value,
            completed_at=timezone.now() - timedelta(days=5),
        )

        count = purge_completed(older_than_days=30)
        assert count == 0
