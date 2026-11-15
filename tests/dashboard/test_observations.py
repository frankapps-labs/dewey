"""Recovery copy explains ledger facts without implying a healthy worker."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from dewey.dashboard.observations import recovery_observation

NOW = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def task():
    return SimpleNamespace(
        status="pending",
        attempts=0,
        max_attempts=2,
        expires_at=None,
        scheduled_for=None,
        lease_expires_at=None,
        execution_deadline_at=None,
        worker_id=None,
        execution_token=None,
        dispatch_token=None,
    )


@pytest.mark.parametrize(
    "status,title",
    [
        ("pending", "Ready"),
        ("failed", "Ready to retry"),
        ("processing", "Running"),
        ("dispatching", "Being assigned"),
        ("completed", "Task completed"),
        ("dead", "Needs attention"),
        ("expired", "Task expired"),
        ("unknown", "State unavailable"),
    ],
)
def test_status_explanations_do_not_mutate_task(task, status, title):
    task.status = status
    before = vars(task).copy()
    assert recovery_observation(task, NOW)["title"] == title
    assert vars(task) == before


@pytest.mark.parametrize("status", ["dispatching", "processing"])
def test_elapsed_lease_is_not_a_crash_diagnosis_or_recovery_promise(task, status):
    task.status = status
    task.lease_expires_at = NOW
    result = recovery_observation(task, NOW)
    assert result["title"] == "Awaiting recovery"
    assert "does not prove" in result["explanation"]
    assert "availability is unknown" in result["next_step"]
    task.lease_expires_at = NOW + timedelta(seconds=1)
    assert recovery_observation(task, NOW)["title"] != "Awaiting recovery"


@pytest.mark.parametrize(
    "marker,value",
    [
        ("execution_token", "TEST-PRIVATE-MARKER"),
        ("dispatch_token", "TEST-PRIVATE-MARKER"),
        ("worker_id", "TEST-PRIVATE-MARKER"),
        ("execution_token", ""),
        ("dispatch_token", ""),
        ("worker_id", ""),
        ("lease_expires_at", NOW),
        ("execution_deadline_at", NOW),
    ],
)
def test_unresolved_ownership_prevents_ready_claim(task, marker, value):
    setattr(task, marker, value)
    result = recovery_observation(task, NOW)
    assert result["title"] == "Ownership unresolved"
    assert "TEST-PRIVATE-MARKER" not in str(result)


def test_expiry_and_budget_override_future_schedule(task):
    task.scheduled_for = NOW + timedelta(minutes=5)
    task.expires_at = NOW
    assert recovery_observation(task, NOW)["title"] == "Lifetime elapsed"
    task.expires_at = None
    task.attempts = task.max_attempts
    assert recovery_observation(task, NOW)["title"] == "Attempt budget exhausted"


@pytest.mark.parametrize("status,title", [("pending", "Scheduled"), ("failed", "Waiting to retry")])
def test_scheduled_is_not_human_wait_or_guaranteed_execution(task, status, title):
    task.status = status
    task.scheduled_for = NOW + timedelta(seconds=1)
    result = recovery_observation(task, NOW)
    assert result["title"] == title
    assert "not evidence of a human-approval wait" in result["explanation"]
    assert "worker is still required" in result["next_step"]
    task.scheduled_for = NOW
    assert recovery_observation(task, NOW)["title"] in ("Ready", "Ready to retry")


def test_completion_is_not_business_success_even_after_expiry(task):
    task.status = "completed"
    task.expires_at = NOW
    result = recovery_observation(task, NOW)
    assert result["title"] == "Task completed"
    assert "business record" in result["next_step"]
