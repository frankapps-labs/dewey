"""Tests for core type definitions."""

from datetime import UTC, datetime, timedelta

from dewey.core.states import TaskStatus
from dewey.core.types import TaskEntry


def _make_task(**kwargs):
    defaults = {
        "id": "test-123",
        "task_type": "order.confirmed",
        "status": TaskStatus.PENDING,
        "args": [],
        "kwargs": {"order_id": "ORD-1"},
        "queue": "default",
        "priority": 0,
        "attempts": 0,
        "max_attempts": 5,
        "error": "",
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
    }
    defaults.update(kwargs)
    return TaskEntry(**defaults)


class TestTaskEntry:
    def test_is_terminal_completed(self):
        task = _make_task(status=TaskStatus.COMPLETED)
        assert task.is_terminal is True

    def test_is_terminal_dead(self):
        task = _make_task(status=TaskStatus.DEAD)
        assert task.is_terminal is True

    def test_is_not_terminal_pending(self):
        task = _make_task(status=TaskStatus.PENDING)
        assert task.is_terminal is False

    def test_is_retryable(self):
        task = _make_task(status=TaskStatus.FAILED, attempts=2, max_attempts=5)
        assert task.is_retryable is True

    def test_is_not_retryable_at_max(self):
        task = _make_task(status=TaskStatus.FAILED, attempts=5, max_attempts=5)
        assert task.is_retryable is False

    def test_is_not_retryable_when_pending(self):
        task = _make_task(status=TaskStatus.PENDING, attempts=0)
        assert task.is_retryable is False

    def test_default_metadata(self):
        task = _make_task()
        assert task.metadata == {}


class TestDeadlineAndScheduleSnapshotFields:
    """expires_at / initial_scheduled_for / expired_at ship with 0.5."""

    def test_all_default_to_none(self):
        """Existing rows have no deadline — the fields must be optional."""
        task = _make_task()
        assert task.expires_at is None
        assert task.initial_scheduled_for is None
        assert task.expired_at is None

    def test_fields_carry_timezone_aware_values(self):
        deadline = datetime.now(UTC) + timedelta(hours=1)
        schedule = datetime.now(UTC) + timedelta(minutes=5)
        observed = datetime.now(UTC)
        task = _make_task(
            status=TaskStatus.EXPIRED,
            expires_at=deadline,
            initial_scheduled_for=schedule,
            expired_at=observed,
        )
        assert task.expires_at == deadline
        assert task.initial_scheduled_for == schedule
        assert task.expired_at == observed

    def test_expired_task_is_terminal(self):
        task = _make_task(status=TaskStatus.EXPIRED)
        assert task.is_terminal is True

    def test_expired_task_is_not_retryable(self):
        """EXPIRED is not FAILED — attempts remaining change nothing."""
        task = _make_task(status=TaskStatus.EXPIRED, attempts=1, max_attempts=5)
        assert task.is_retryable is False

    def test_snapshot_is_independent_of_scheduled_for(self):
        """scheduled_for mutates on retry; the snapshot keeps the creation-time value."""
        created = datetime.now(UTC)
        retried = created + timedelta(minutes=30)
        task = _make_task(scheduled_for=retried, initial_scheduled_for=created)
        assert task.initial_scheduled_for == created
        assert task.scheduled_for == retried
