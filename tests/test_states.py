"""Tests for core state machine."""

from dewey.core.states import (
    TaskStatus,
    should_die,
    should_retry,
)


class TestTaskStatus:
    def test_values(self):
        assert TaskStatus.PENDING == "pending"
        assert TaskStatus.PROCESSING == "processing"
        assert TaskStatus.COMPLETED == "completed"
        assert TaskStatus.FAILED == "failed"
        assert TaskStatus.DEAD == "dead"

    def test_terminal_states(self):
        """is_terminal means the task won't be auto-processed.
        DEAD is terminal for processing but allows manual retry."""
        assert TaskStatus.COMPLETED.is_terminal is True
        assert TaskStatus.DEAD.is_terminal is True
        assert TaskStatus.PENDING.is_terminal is False
        assert TaskStatus.PROCESSING.is_terminal is False
        assert TaskStatus.FAILED.is_terminal is False

    def test_dead_is_terminal_but_retryable(self):
        """DEAD is terminal (won't auto-process) but CAN transition to PENDING."""
        assert TaskStatus.DEAD.is_terminal is True
        assert TaskStatus.DEAD.can_transition_to(TaskStatus.PENDING) is True


class TestTransitions:
    def test_pending_to_processing(self):
        assert TaskStatus.PENDING.can_transition_to(TaskStatus.PROCESSING) is True

    def test_pending_to_dead(self):
        assert TaskStatus.PENDING.can_transition_to(TaskStatus.DEAD) is True

    def test_processing_to_completed(self):
        assert TaskStatus.PROCESSING.can_transition_to(TaskStatus.COMPLETED) is True

    def test_processing_to_failed(self):
        assert TaskStatus.PROCESSING.can_transition_to(TaskStatus.FAILED) is True

    def test_processing_to_dead(self):
        assert TaskStatus.PROCESSING.can_transition_to(TaskStatus.DEAD) is True

    def test_processing_to_pending(self):
        """sweep_stuck resets abandoned tasks: PROCESSING → PENDING."""
        assert TaskStatus.PROCESSING.can_transition_to(TaskStatus.PENDING) is True

    def test_failed_to_pending(self):
        assert TaskStatus.FAILED.can_transition_to(TaskStatus.PENDING) is True

    def test_failed_to_dead(self):
        assert TaskStatus.FAILED.can_transition_to(TaskStatus.DEAD) is True

    def test_dead_to_pending(self):
        """Manual retry: DEAD → PENDING."""
        assert TaskStatus.DEAD.can_transition_to(TaskStatus.PENDING) is True

    def test_completed_is_fully_terminal(self):
        """COMPLETED has no outbound transitions at all."""
        for status in TaskStatus:
            assert TaskStatus.COMPLETED.can_transition_to(status) is False

    def test_dead_only_to_pending(self):
        """DEAD can only go to PENDING (manual retry)."""
        for status in TaskStatus:
            if status == TaskStatus.PENDING:
                assert TaskStatus.DEAD.can_transition_to(status) is True
            else:
                assert TaskStatus.DEAD.can_transition_to(status) is False

    def test_invalid_transitions(self):
        assert TaskStatus.PENDING.can_transition_to(TaskStatus.COMPLETED) is False
        assert TaskStatus.PENDING.can_transition_to(TaskStatus.FAILED) is False
        assert TaskStatus.FAILED.can_transition_to(TaskStatus.COMPLETED) is False

    def test_cross_type_rejected(self):
        """TaskStatus.can_transition_to rejects NotificationStatus values."""
        from dewey.core.notifications import NotificationStatus

        assert TaskStatus.PENDING.can_transition_to(NotificationStatus.SENDING) is False  # type: ignore[arg-type]


class TestRetryLogic:
    def test_should_retry_when_under_max(self):
        assert should_retry(attempts=1, max_attempts=5) is True
        assert should_retry(attempts=4, max_attempts=5) is True

    def test_should_not_retry_at_max(self):
        assert should_retry(attempts=5, max_attempts=5) is False

    def test_should_not_retry_over_max(self):
        assert should_retry(attempts=6, max_attempts=5) is False

    def test_should_die_at_max(self):
        assert should_die(attempts=5, max_attempts=5) is True

    def test_should_not_die_under_max(self):
        assert should_die(attempts=4, max_attempts=5) is False
