"""Task status state machine — the heart of dewey."""

from enum import Enum


class TaskStatus(str, Enum):  # noqa: UP042 — keeping (str, Enum) intentional; StrEnum changes str() repr
    """Task lifecycle states."""

    PENDING = "pending"
    #: Claimed by a dispatcher and handed to the transport, but not yet picked up
    #: by a worker. A row that sits here past the dispatch timeout is reclaimed by
    #: the sweep — that is what makes a dispatcher crash survivable.
    DISPATCHING = "dispatching"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"
    #: The task's absolute deadline (``expires_at``) passed before it completed.
    #: Fully terminal — expired tasks stay queryable but are never retried.
    EXPIRED = "expired"

    @property
    def is_terminal(self) -> bool:
        """Terminal for auto-processing — sweep and process_task skip these.

        Note: DEAD is terminal but allows manual retry (DEAD → PENDING).
        COMPLETED and EXPIRED are fully terminal — no transitions out.
        """
        return self in _TERMINAL_STATES

    def can_transition_to(self, target: "TaskStatus") -> bool:
        """Check if transitioning to ``target`` is allowed by the state machine."""
        return target in _ALLOWED_TRANSITIONS.get(self, set())


# States where automatic processing won't pick up the task.
_TERMINAL_STATES = {TaskStatus.COMPLETED, TaskStatus.DEAD, TaskStatus.EXPIRED}

_ALLOWED_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING: {
        TaskStatus.DISPATCHING,  # a dispatcher claimed it
        TaskStatus.PROCESSING,  # in-process execution, with no broker in the path
        TaskStatus.DEAD,  # manual kill
        TaskStatus.EXPIRED,  # deadline passed before dispatch
    },
    TaskStatus.DISPATCHING: {
        TaskStatus.PROCESSING,  # a worker picked it up off the transport
        TaskStatus.PENDING,  # dispatch failed, or the dispatch timeout swept it
        TaskStatus.DEAD,  # manual kill, or attempts exhausted while stuck
        TaskStatus.EXPIRED,  # deadline passed while on the transport
    },
    TaskStatus.PROCESSING: {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.DEAD,
        TaskStatus.PENDING,  # sweep_stuck resets abandoned tasks
    },
    TaskStatus.FAILED: {
        TaskStatus.PENDING,
        TaskStatus.DISPATCHING,  # directly claimed when its retry is due
        TaskStatus.DEAD,
        TaskStatus.EXPIRED,  # deadline passed while waiting for a retry
    },
    TaskStatus.DEAD: {TaskStatus.PENDING},  # manual retry only
    # COMPLETED and EXPIRED are fully terminal — no transitions out.
}


def should_retry(attempts: int, max_attempts: int) -> bool:
    """Should the task be retried (FAILED → PENDING)?"""
    return attempts < max_attempts


def should_die(attempts: int, max_attempts: int) -> bool:
    """Should the task be dead-lettered (FAILED → DEAD)?"""
    return attempts >= max_attempts
