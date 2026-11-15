"""Version 1 server-side pull-runner contract, independent of any transport.

The host authenticates/enrolls workers, chooses policy/queues, reserves resources
and records winner outputs. These values grant no authority by themselves. All
adapter results are provisional until the caller commits its root transaction.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from dewey.errors import DeweyError, NonRetryableError, RetryAfter, TransientError

PROTOCOL_VERSION = 1


@dataclass(frozen=True)
class RunnerAssignment:
    """Small assignment envelope; ownership tokens are opaque secrets."""

    task_id: str
    dispatch_token: str

    def __post_init__(self) -> None:
        for name, value in (("task_id", self.task_id), ("dispatch_token", self.dispatch_token)):
            if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 256:
                raise ValueError(f"{name} must be a nonempty string of at most 256 UTF-8 bytes")


@dataclass(frozen=True)
class RunnerTiming:
    """Explicit experimental limits, not production defaults.

    Storage must enforce db_wait_seconds as a total operation budget, not just
    a per-statement timeout. The runner must maintain the polling bound. These
    checks cannot themselves guarantee scheduling or physical termination.
    """

    assignment_lease_seconds: float
    execution_lease_seconds: float
    renewal_interval_seconds: float
    db_wait_seconds: float
    poll_interval_seconds: float
    termination_seconds: float
    task_timeout_seconds: float

    def __post_init__(self) -> None:
        for name, value in vars(self).items():
            if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        margin = self.db_wait_seconds + self.poll_interval_seconds + self.termination_seconds
        if self.renewal_interval_seconds + margin >= self.execution_lease_seconds:
            raise ValueError("Execution lease must exceed renewal interval plus safety margin")
        if margin >= self.assignment_lease_seconds:
            raise ValueError("Assignment lease must exceed admission and termination margin")
        if margin >= self.task_timeout_seconds:
            raise ValueError("Task timeout must exceed admission and termination margin")


@dataclass(frozen=True)
class RunnerPolicy:
    """Trusted host configuration; never construct directly from runner requests.

    ``owner`` identifies an enrolled worker's current *process instance*, not the
    machine or actor. Timing, queues and their authorization are server-owned.
    Empty/bare-string queue scopes fail closed, there is no unscoped runner.
    ``task_types`` optionally narrows claims to the types this runner's catalog can
    execute; rows of any other type stay unclaimed for a runner that can.
    """

    owner: str
    queues: Sequence[str]
    timing: RunnerTiming
    task_types: Sequence[str] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.owner, str) or not self.owner or len(self.owner) > 36:
            raise ValueError("Owner must be a nonempty instance ID of at most 36 characters")
        if isinstance(self.queues, (str, bytes)) or not isinstance(self.queues, Sequence):
            raise ValueError("Queues must be an explicit nonempty sequence")
        queues = tuple(self.queues)
        if not queues or any(not isinstance(q, str) or not q or len(q) > 50 for q in queues):
            raise ValueError("Queues must contain nonempty names of at most 50 characters")
        if not isinstance(self.timing, RunnerTiming):
            raise ValueError("Expected server-owned RunnerTiming")
        object.__setattr__(self, "queues", queues)
        if self.task_types is not None:
            types = self.task_types
            if isinstance(types, (str, bytes)) or not isinstance(types, Sequence):
                raise ValueError("Task types must be a sequence of names")
            types = tuple(types)
            if any(not isinstance(t, str) or not t or len(t) > 100 for t in types):
                raise ValueError("Task types must be nonempty names of at most 100 characters")
            object.__setattr__(self, "task_types", types)


@dataclass(frozen=True)
class RunnerRenewal:
    """Accepted live renewal; cancellation requests cooperative termination.

    A request is not proof of process stop. Invalid generations return None.
    """

    cancel_requested: bool = False


@dataclass(frozen=True)
class FailureReport:
    """Data-only failure; server policy still decides retry/backoff/exhaustion.

    Messages must be host-redacted. Retry-after is a floor (at most one year),
    never a policy override. Arbitrary exception classes are not a wire format.
    """

    kind: Literal["transient", "non_retryable"] = "transient"
    message: str = ""
    retry_after_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.kind not in ("transient", "non_retryable"):
            raise ValueError("Unsupported failure kind")
        if not isinstance(self.message, str) or len(self.message) > 2000:
            raise ValueError("Failure message must be a string of at most 2000 characters")
        value = self.retry_after_seconds
        if value is not None and (
            self.kind != "transient"
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or not 0 <= value <= 365 * 86400
        ):
            raise ValueError("Retry-after must be a finite transient delay from zero to one year")

    def exception(self) -> Exception:
        if self.kind == "non_retryable":
            return NonRetryableError(self.message)
        if self.retry_after_seconds is not None:
            return RetryAfter(self.retry_after_seconds, self.message)
        return TransientError(self.message)


class LeaseExpired(DeweyError):
    """Lease elapsed during an operation. Caller MUST roll back the entire root.

    No method commits, rolls back, closes or reports success for caller resources.
    This also applies to database/timeout errors, even after provisional writes.
    """


__all__ = [
    "PROTOCOL_VERSION",
    "FailureReport",
    "LeaseExpired",
    "RunnerAssignment",
    "RunnerPolicy",
    "RunnerRenewal",
    "RunnerTiming",
]
