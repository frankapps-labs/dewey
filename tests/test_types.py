"""Tests for core type definitions."""

from datetime import UTC, datetime

from dewey.core.states import TaskStatus
from dewey.core.types import TaskEntry


def _make_task(**kwargs):
    defaults = {
        "id": "test-123",
        "task_type": "order.confirmed",
        "status": TaskStatus.PENDING,
        "payload": {"order_id": "ORD-1"},
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
