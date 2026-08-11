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
        assert TaskStatus.EXPIRED == "expired"

    def test_terminal_states(self):
        """is_terminal means the task won't be auto-processed.
        DEAD is terminal for processing but allows manual retry."""
        assert TaskStatus.COMPLETED.is_terminal is True
        assert TaskStatus.DEAD.is_terminal is True
        assert TaskStatus.EXPIRED.is_terminal is True
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

    def test_a_foreign_status_value_is_rejected(self):
        """can_transition_to answers False for anything outside the task states."""
        from enum import Enum

        class OtherStatus(Enum):
            SENDING = "sending"

        assert TaskStatus.PENDING.can_transition_to(OtherStatus.SENDING) is False  # type: ignore[arg-type]


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


class TestDispatchingState:
    """DISPATCHING sits between a dispatcher's claim and a worker picking it up."""

    def test_pending_can_be_claimed_for_dispatch(self):
        assert TaskStatus.PENDING.can_transition_to(TaskStatus.DISPATCHING)

    def test_pending_can_still_go_straight_to_processing(self):
        """In-process execution has no broker in the path, so this stays legal."""
        assert TaskStatus.PENDING.can_transition_to(TaskStatus.PROCESSING)

    def test_dispatching_can_be_picked_up_by_a_worker(self):
        assert TaskStatus.DISPATCHING.can_transition_to(TaskStatus.PROCESSING)

    def test_dispatching_can_be_returned_to_pending(self):
        """Dispatch failed, or the dispatch-timeout sweep reclaimed it."""
        assert TaskStatus.DISPATCHING.can_transition_to(TaskStatus.PENDING)

    def test_dispatching_can_be_killed(self):
        assert TaskStatus.DISPATCHING.can_transition_to(TaskStatus.DEAD)

    def test_dispatching_cannot_complete_without_processing(self):
        assert not TaskStatus.DISPATCHING.can_transition_to(TaskStatus.COMPLETED)

    def test_dispatching_is_not_terminal(self):
        assert TaskStatus.DISPATCHING.is_terminal is False

    def test_terminal_states_cannot_be_redispatched(self):
        assert not TaskStatus.COMPLETED.can_transition_to(TaskStatus.DISPATCHING)
        assert not TaskStatus.DEAD.can_transition_to(TaskStatus.DISPATCHING)

    def test_failed_is_redispatched_through_pending(self):
        """The sweep resets FAILED to PENDING; it never dispatches directly."""
        assert not TaskStatus.FAILED.can_transition_to(TaskStatus.DISPATCHING)
        assert TaskStatus.FAILED.can_transition_to(TaskStatus.PENDING)


class TestExpiredState:
    """EXPIRED is a fully terminal deadline outcome — reachable from every
    nonterminal state, with no transitions out and no manual retry."""

    def test_expired_is_terminal(self):
        assert TaskStatus.EXPIRED.is_terminal is True

    def test_pending_can_expire(self):
        assert TaskStatus.PENDING.can_transition_to(TaskStatus.EXPIRED) is True

    def test_dispatching_can_expire(self):
        assert TaskStatus.DISPATCHING.can_transition_to(TaskStatus.EXPIRED) is True

    def test_processing_can_expire(self):
        """Enforced again before handler invocation — no attempt is consumed."""
        assert TaskStatus.PROCESSING.can_transition_to(TaskStatus.EXPIRED) is True

    def test_failed_can_expire(self):
        """A task awaiting retry expires instead of being redispatched."""
        assert TaskStatus.FAILED.can_transition_to(TaskStatus.EXPIRED) is True

    def test_completed_cannot_expire(self):
        assert TaskStatus.COMPLETED.can_transition_to(TaskStatus.EXPIRED) is False

    def test_dead_cannot_expire(self):
        """DEAD is already terminal; a deadline never rewrites the outcome."""
        assert TaskStatus.DEAD.can_transition_to(TaskStatus.EXPIRED) is False

    def test_expired_has_no_outbound_transitions(self):
        """Unlike DEAD, an expired task cannot be manually retried."""
        for status in TaskStatus:
            assert TaskStatus.EXPIRED.can_transition_to(status) is False
