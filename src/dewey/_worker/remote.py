"""Remote-only guards layered on the SAME native lifecycle, not another ledger."""

from datetime import datetime
from typing import Any

from dewey._worker import ledger
from dewey.core.states import TaskStatus
from dewey.runner import LeaseExpired, RunnerAssignment, RunnerPolicy


def execution_token(token: str) -> None:
    if not isinstance(token, str) or not token or len(token) > 36:
        raise ValueError("Execution token must be a nonempty string of at most 36 characters")


def bounded_limit(limit: int) -> None:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise ValueError("Limit must be an integer from 1 to 100")


def renew_changes(
    row: Any, assignment: RunnerAssignment, policy: RunnerPolicy, token: str, now: datetime
) -> dict[str, Any] | None:
    # The local supervisor deliberately renews an assignment on behalf of its
    # child. A remote execution must instead present the execution generation.
    if row.status != TaskStatus.PROCESSING.value or row.execution_token != token:
        return None
    return ledger.renewal_changes(row, assignment, policy.owner, now, policy.timing)


def cancellation_changes(
    row: Any, assignment: RunnerAssignment, policy: RunnerPolicy, token: str, now: datetime
) -> dict[str, Any] | None:
    """Host-authorized cooperative request, fenced like a renewal."""
    if renew_changes(row, assignment, policy, token, now) is None:
        return None
    return {"cancel_requested": True, "updated_at": now}


def release_changes(
    row: Any, assignment: RunnerAssignment, policy: RunnerPolicy, now: datetime
) -> dict[str, Any] | None:
    # Never expose abandon(stopped=True) to an untrusted processing runner.
    # DISPATCHING has not consumed an attempt or admitted execution.
    if row.status != TaskStatus.DISPATCHING.value or not ledger.owns(row, assignment, policy.owner):
        return None
    return ledger.recovery_changes(row, now, stopped=True)


def assert_live(bounds: list[datetime], now: datetime) -> None:
    if any(bound <= now for bound in bounds):
        raise LeaseExpired("Lease/deadline elapsed during operation; roll back caller root")
