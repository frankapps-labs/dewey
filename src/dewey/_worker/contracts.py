"""Internal native-runner contracts; no database or framework dependencies."""

from __future__ import annotations

from enum import StrEnum

from dewey.runner import RunnerTiming

# Keep existing internal imports and pickles resolving to the public value type.
Timing = RunnerTiming


def round_budget(timing: RunnerTiming, concurrency: int) -> float:
    """Worst-case seconds for one supervision round, which must fit inside a lease.

    Four ledger calls per slot, plus claim, recover, heartbeat, schedule ticks
    and a spare; then stopping every slot, then the pause between rounds.
    """
    return (
        (4 * concurrency + 5) * timing.db_wait_seconds
        + concurrency * timing.termination_seconds
        + timing.poll_interval_seconds
    )


def stop_budget(timing: RunnerTiming, concurrency: int) -> float:
    """Worst-case seconds to give up and stop every task process: no database needed.

    A call already in flight may run out its budget and its commit first.
    """
    return (
        2 * timing.db_wait_seconds
        + concurrency * timing.termination_seconds
        + timing.poll_interval_seconds
    )


class Reconciliation(StrEnum):
    """Durable observations, never inferred from an IPC return value.

    SETTLED means an outcome/recovery committed for this generation. INVALIDATED
    means this assignment no longer owns the ledger row, not that it succeeded.
    Both still require local process safety before pool acknowledgement.
    ACTIVE and UNKNOWN must retain the occupied slot.
    """

    SETTLED = "settled"
    INVALIDATED = "invalidated"
    ACTIVE = "active"
    UNKNOWN = "unknown"

    @property
    def durable_release(self) -> bool:
        return self in (self.SETTLED, self.INVALIDATED)
