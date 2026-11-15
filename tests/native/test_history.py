"""Same transactional event capture for native sync/async/Django writers."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dewey.sqlalchemy.models import TaskEventModel
from tests.native.test_storage import create, expire_lease, snapshot


def events(engine, task_id):
    with engine.connect() as connection:
        return list(
            connection.execute(
                select(TaskEventModel.__table__)
                .where(TaskEventModel.task_id == task_id)
                .order_by(TaskEventModel.occurred_at)
            ).mappings()
        )


def test_native_history_records_actual_outcomes_and_rejects_stale_finish(store, engine):
    task_id = create(engine, max_attempts=1)
    assignment = store("claim", 1)[0]
    invocation = store("start", assignment)
    assert not store("finish", assignment, str(uuid4()))
    assert events(engine, task_id) == []
    assert store("finish", assignment, invocation["execution_token"])
    event = events(engine, task_id)[0]
    assert (event["kind"], event["attempt"], event["queue"]) == ("completed", 1, "default")
    assert event["diagnostics"] == event["audit"] == {}
    assert not store("finish", assignment, invocation["execution_token"])
    assert events(engine, task_id) == [event]


def test_parent_observed_interruption_has_local_dead_event_without_remote_ref(store, engine):
    task_id = create(engine, max_attempts=1)
    assignment = store("claim", 1)[0]
    store("start", assignment)
    expire_lease(engine, task_id, deadline=True)
    assert store("recover", 1) == [task_id]
    event = events(engine, task_id)[0]
    assert (event["kind"], event["attempt"]) == ("dead", 1)
    assert event["diagnostics"] == {}
    assert snapshot(engine, task_id)["latest_outcome"] == "timed_out"


def test_native_failure_and_expiry_are_captured_without_child_reporting(store, engine):
    failure = create(engine, max_attempts=1)
    expired = create(engine, expires_at=datetime.now(UTC) - timedelta(seconds=1))
    assignment = store("claim", 2)[0]
    invocation = store("start", assignment)
    assert store("finish", assignment, invocation["execution_token"], ValueError("TEST-PRIVATE"))
    assert [(e["kind"], e["attempt"]) for e in events(engine, failure)] == [("dead", 1)]
    assert [(e["kind"], e["attempt"]) for e in events(engine, expired)] == [("expired", 0)]
    assert "TEST-PRIVATE" not in str(events(engine, failure))


def test_dashboard_single_retry_can_be_consumed_by_each_native_backend(store, engine, settings):
    from django.db import connections

    from dewey.dashboard.operations import Diagnostics, FailureCategory, RetryDecision
    from dewey.django.operations import record_diagnostics, retry_task_once

    task_id = create(engine, max_attempts=1)
    assignment = store("claim", 1)[0]
    invocation = store("start", assignment)
    assert store("finish", assignment, invocation["execution_token"], RuntimeError("Test outage"))
    failure_event = events(engine, task_id)[0]
    config = deepcopy(connections.databases["default"])
    config["NAME"] = engine.url.database
    connections.databases["operations"] = config
    settings.DEWEY_DASHBOARD_RETRY_POLICY = lambda task, event: RetryDecision(
        True, "Recovered; durable receipt replay approved."
    )
    try:
        assert record_diagnostics(
            failure_event["id"],
            1,
            Diagnostics(FailureCategory.TRANSIENT, "Reviewed outage"),
            using="operations",
        )
        retry_task_once(
            task_id,
            event_id=failure_event["id"],
            attempt=1,
            actor="test-operator",
            reason="Replay safe after recovery",
            using="operations",
        )
    finally:
        connections["operations"].close()
        del connections["operations"]
        del connections.databases["operations"]
    newer = store("claim", 1)[0]
    next_invocation = store("start", newer)
    assert next_invocation["attempts"] == 2
    assert store("finish", newer, next_invocation["execution_token"])
    row = snapshot(engine, task_id)
    assert (row["status"], row["attempts"], row["max_attempts"]) == ("completed", 2, 2)
    assert [e["kind"] for e in events(engine, task_id)] == ["dead", "retry_requested", "completed"]


def test_direct_bulk_transition_rollbacks_and_same_state_updates_do_not_invent_events(engine):
    from dewey.sqlalchemy.models import TaskEntryModel as Task

    ids = [create(engine) for _ in range(3)]
    with Session(engine) as session:
        session.execute(update(Task).where(Task.id.in_(ids)).values(status="dead"))
        session.rollback()
    assert all(events(engine, task_id) == [] for task_id in ids)
    with Session(engine) as session, session.begin():
        session.execute(update(Task).where(Task.id.in_(ids)).values(status="dead"))
        session.execute(
            update(Task).where(Task.id.in_(ids)).values(status="dead", error="TEST-PRIVATE")
        )
    assert all(len(events(engine, task_id)) == 1 for task_id in ids)
