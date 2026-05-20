"""Task status state machine — the heart of dewey."""

from enum import Enum


class TaskStatus(str, Enum):  # noqa: UP042 — keeping (str, Enum) intentional; StrEnum changes str() repr
    """Task lifecycle states."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"

    @property
    def is_terminal(self) -> bool:
        """Terminal for auto-processing — sweep and process_task skip these.

        Note: DEAD is terminal but allows manual retry (DEAD → PENDING).
        COMPLETED is fully terminal — no transitions out.
        """
        return self in _TERMINAL_STATES

    def can_transition_to(self, target: "TaskStatus") -> bool:
        """Check if transitioning to ``target`` is allowed by the state machine."""
        return target in _ALLOWED_TRANSITIONS.get(self, set())


# States where automatic processing won't pick up the task.
_TERMINAL_STATES = {TaskStatus.COMPLETED, TaskStatus.DEAD}

_ALLOWED_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING: {TaskStatus.PROCESSING, TaskStatus.DEAD},
    TaskStatus.PROCESSING: {
        TaskStatus.COMPLETED,
        TaskStatus.FAILED,
        TaskStatus.DEAD,
        TaskStatus.PENDING,  # sweep_stuck resets abandoned tasks
    },
    TaskStatus.FAILED: {TaskStatus.PENDING, TaskStatus.DEAD},
    TaskStatus.DEAD: {TaskStatus.PENDING},  # manual retry only
    # COMPLETED is fully terminal — no transitions out.
}


def should_retry(attempts: int, max_attempts: int) -> bool:
    """Should the task be retried (FAILED → PENDING)?"""
    return attempts < max_attempts


def should_die(attempts: int, max_attempts: int) -> bool:
    """Should the task be dead-lettered (FAILED → DEAD)?"""
    return attempts >= max_attempts
