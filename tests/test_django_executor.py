"""Tests for Django executor — create_task and process_task."""

# ruff: noqa: E402 — django.setup() must run before model imports

import os
import time
from datetime import UTC, datetime, timedelta

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.utils import timezone

from dewey.core.states import TaskStatus
from dewey.django.executor import create_task, process_task


@pytest.fixture(autouse=True)
def clean_db():
    """Wipe task_entries between tests."""
    from dewey.django.models import TaskEntry

    yield
    TaskEntry.objects.all().delete()


@pytest.mark.django_db(transaction=True)
class TestCreateTask:
    def test_creates_with_defaults(self):
        task = create_task(task_type="order.confirmed")
        assert task.task_type == "order.confirmed"
        assert task.status == TaskStatus.PENDING
        assert task.kwargs == {}
        assert task.max_attempts == 5
        assert task.attempts == 0

    def test_creates_with_payload(self):
        task = create_task(
            task_type="order.confirmed",
            kwargs={"order_id": 42},
            queue="high",
            priority=10,
        )
        assert task.kwargs == {"order_id": 42}
        assert task.queue == "high"
        assert task.priority == 10

    def test_creates_with_idempotency_key(self):
        task = create_task(
            task_type="email.send",
            idempotency_key="email-abc-123",
        )
        assert task.idempotency_key == "email-abc-123"

    def test_creates_with_metadata(self):
        task = create_task(
            task_type="test.task",
            metadata={"source": "api"},
        )
        assert task.metadata == {"source": "api"}

    def test_id_is_string(self):
        """ID should always be a string, never a UUID object."""
        task = create_task(task_type="test.task")
        assert isinstance(task.id, str)
        assert len(task.id) == 36  # UUID format


@pytest.mark.django_db(transaction=True)
class TestProcessTask:
    def test_successful_processing(self):
        task = create_task(task_type="test.task", kwargs={"x": 1})
        result = process_task(task.id, handler=lambda **kw: None)
        assert result is True

        from dewey.django.models import TaskEntry

        updated = TaskEntry.objects.get(id=task.id)
        assert updated.status == TaskStatus.COMPLETED.value
        assert updated.attempts == 1
        assert updated.completed_at is not None

    def test_failed_processing(self):
        task = create_task(task_type="test.task", max_attempts=5)

        def bad_handler(**kwargs):
            raise ValueError("boom")

        result = process_task(task.id, handler=bad_handler)
        assert result is False

        from dewey.django.models import TaskEntry

        updated = TaskEntry.objects.get(id=task.id)
        assert updated.status == TaskStatus.FAILED.value
        assert updated.attempts == 1
        assert "boom" in updated.error

    def test_failed_processing_backoff_starts_at_failure_time(self):
        task = create_task(task_type="test.task", max_attempts=5)
        failure_time = None

        def bad_handler(**kwargs):
            nonlocal failure_time
            time.sleep(0.01)
            failure_time = timezone.now()
            raise ValueError("late boom")

        result = process_task(task.id, handler=bad_handler, backoff=lambda attempts: timedelta(0))
        assert result is False

        from dewey.django.models import TaskEntry

        updated = TaskEntry.objects.get(id=task.id)
        assert updated.scheduled_for >= failure_time

    def test_dead_letter_after_max_attempts(self):
        task = create_task(task_type="test.task", max_attempts=1)

        result = process_task(
            task.id,
            handler=lambda **kw: (_ for _ in ()).throw(ValueError("fail")),
        )
        assert result is False

        from dewey.django.models import TaskEntry

        updated = TaskEntry.objects.get(id=task.id)
        assert updated.status == TaskStatus.DEAD.value

    def test_skip_already_completed(self):
        task = create_task(task_type="test.task")
        process_task(task.id, handler=lambda **kw: None)
        # Process again — should skip
        result = process_task(task.id, handler=lambda **kw: None)
        assert result is False

    def test_skip_nonexistent_task(self):
        result = process_task("nonexistent-id", handler=lambda **kw: None)
        assert result is False

    def test_skip_task_not_ready(self):
        """Tasks with future scheduled_for should be skipped."""
        future = timezone.now() + timedelta(hours=1)
        task = create_task(task_type="test.task", scheduled_for=future)

        result = process_task(task.id, handler=lambda **kw: None)
        assert result is False

        from dewey.django.models import TaskEntry

        updated = TaskEntry.objects.get(id=task.id)
        assert updated.status == TaskStatus.PENDING.value

    def test_processing_state_committed(self):
        """PROCESSING state should be visible to other transactions (two-phase commit)."""
        import threading
        import time

        task = create_task(task_type="test.visible")
        task_id = task.id

        observed_status = [None]
        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_handler(**kwargs):
            handler_started.set()
            handler_continue.wait(timeout=10)

        def observer():
            handler_started.wait(timeout=10)
            time.sleep(0.1)
            from django.db import connections

            # Use a separate connection to see committed state
            with connections["default"].cursor() as cursor:
                cursor.execute("SELECT status FROM task_entries WHERE id = %s", [task_id])
                row = cursor.fetchone()
                if row:
                    observed_status[0] = row[0]
            handler_continue.set()

        t = threading.Thread(target=observer)
        t.start()

        process_task(task_id, handler=slow_handler)
        t.join(timeout=15)

        assert observed_status[0] == TaskStatus.PROCESSING.value

    def test_task_deleted_during_success(self):
        """If task is deleted mid-processing, Phase 3b handles it gracefully."""
        import threading
        import time

        task = create_task(task_type="test.disappear")
        task_id = task.id

        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_handler(**kwargs):
            handler_started.set()
            handler_continue.wait(timeout=10)

        def deleter():
            handler_started.wait(timeout=10)
            time.sleep(0.1)
            from django.db import connections

            with connections["default"].cursor() as cursor:
                cursor.execute("DELETE FROM task_entries WHERE id = %s", [task_id])
            handler_continue.set()

        t = threading.Thread(target=deleter)
        t.start()

        result = process_task(task_id, handler=slow_handler)
        t.join(timeout=15)
        assert result is False

    def test_task_killed_during_success(self):
        """If task is killed mid-processing, Phase 3b respects the DEAD status."""
        import threading
        import time

        from dewey.django.models import TaskEntry

        task = create_task(task_type="test.killed")
        task_id = task.id

        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_handler(**kwargs):
            handler_started.set()
            handler_continue.wait(timeout=10)

        def killer():
            handler_started.wait(timeout=10)
            time.sleep(0.1)
            from django.db import connections

            with connections["default"].cursor() as cursor:
                cursor.execute(
                    "UPDATE task_entries SET status = %s WHERE id = %s",
                    [TaskStatus.DEAD.value, task_id],
                )
            handler_continue.set()

        t = threading.Thread(target=killer)
        t.start()

        result = process_task(task_id, handler=slow_handler)
        t.join(timeout=15)
        assert result is False

        updated = TaskEntry.objects.get(id=task_id)
        assert updated.status == TaskStatus.DEAD.value

    def test_skip_failed_task(self):
        """Tasks in FAILED state should not be processed (only PENDING tasks)."""
        from dewey.django.models import TaskEntry as TaskEntryModel

        task = create_task(task_type="test.failed")
        # Manually set to FAILED (simulating a previous failed attempt)
        TaskEntryModel.objects.filter(id=task.id).update(status=TaskStatus.FAILED.value)

        result = process_task(task.id, handler=lambda **kw: None)
        assert result is False

        updated = TaskEntryModel.objects.get(id=task.id)
        assert updated.status == TaskStatus.FAILED.value

    def test_task_deleted_during_failure(self):
        """If task is deleted mid-processing and handler fails, Phase 3a handles gracefully."""
        import threading
        import time

        task = create_task(task_type="test.disappear.fail")
        task_id = task.id

        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_failing_handler(**kwargs):
            handler_started.set()
            handler_continue.wait(timeout=10)
            raise ValueError("boom")

        def deleter():
            handler_started.wait(timeout=10)
            time.sleep(0.1)
            from django.db import connections

            with connections["default"].cursor() as cursor:
                cursor.execute("DELETE FROM task_entries WHERE id = %s", [task_id])
            handler_continue.set()

        t = threading.Thread(target=deleter)
        t.start()

        result = process_task(task_id, handler=slow_failing_handler)
        t.join(timeout=15)
        assert result is False

    def test_task_killed_during_failure(self):
        """If task is killed mid-processing and handler fails, Phase 3a respects DEAD."""
        import threading
        import time

        from dewey.django.models import TaskEntry

        task = create_task(task_type="test.killed.fail")
        task_id = task.id

        handler_started = threading.Event()
        handler_continue = threading.Event()

        def slow_failing_handler(**kwargs):
            handler_started.set()
            handler_continue.wait(timeout=10)
            raise ValueError("boom")

        def killer():
            handler_started.wait(timeout=10)
            time.sleep(0.1)
            from django.db import connections

            with connections["default"].cursor() as cursor:
                cursor.execute(
                    "UPDATE task_entries SET status = %s WHERE id = %s",
                    [TaskStatus.DEAD.value, task_id],
                )
            handler_continue.set()

        t = threading.Thread(target=killer)
        t.start()

        result = process_task(task_id, handler=slow_failing_handler)
        t.join(timeout=15)
        assert result is False

        updated = TaskEntry.objects.get(id=task_id)
        assert updated.status == TaskStatus.DEAD.value


@pytest.mark.django_db(transaction=True)
class TestExpiry:
    def test_deadline_is_checked_after_the_task_lock_is_acquired(self, monkeypatch):
        from django.db.models.query import QuerySet

        import dewey.django.executor as executor_module
        from dewey.django.models import TaskEntry

        before = datetime.now(UTC)
        deadline = before + timedelta(minutes=1)
        after = deadline + timedelta(seconds=1)
        task = create_task(task_type="deadline", expires_at=deadline)
        observed = [before]

        class LockAwareClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return observed[0]

        real_get = QuerySet.get

        def get_after_wait(self, *args, **kwargs):
            observed[0] = after
            return real_get(self, *args, **kwargs)

        monkeypatch.setattr(executor_module, "datetime", LockAwareClock)
        monkeypatch.setattr(QuerySet, "get", get_after_wait)
        runs = []

        assert process_task(task.id, lambda: runs.append(1)) is False
        assert runs == []
        assert TaskEntry.objects.get(id=task.id).status == TaskStatus.EXPIRED.value

    def test_creation_persists_deadline_and_original_schedule(self):
        scheduled = timezone.now() + timedelta(minutes=5)
        expires = scheduled + timedelta(minutes=10)
        task = create_task(
            task_type="deadline",
            scheduled_for=scheduled,
            expires_at=expires,
        )
        assert task.initial_scheduled_for == scheduled
        assert task.expires_at == expires

    def test_naive_deadline_is_rejected(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            create_task(task_type="deadline", expires_at=timezone.now().replace(tzinfo=None))

    def test_expired_delivery_never_invokes_handler_or_consumes_attempt(self):
        from dewey.django.models import TaskEntry

        task = create_task(
            task_type="deadline",
            expires_at=timezone.now() - timedelta(seconds=1),
        )
        runs: list[int] = []

        assert process_task(task.id, lambda: runs.append(1)) is False
        row = TaskEntry.objects.get(id=task.id)
        assert runs == []
        assert row.status == TaskStatus.EXPIRED.value
        assert row.expired_at is not None
        assert row.attempts == 0
