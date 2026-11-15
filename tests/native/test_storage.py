"""Real-Postgres native ledger contract parity, with no handler execution yet."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dewey._worker.contracts import Reconciliation
from dewey._worker.pool import Assignment
from dewey.sqlalchemy.models import TaskEntryModel as Task


def create(engine, **values):
    with Session(engine) as session, session.begin():
        task = Task(task_type=values.pop("task_type", "native.synthetic"), **values)
        session.add(task)
        session.flush()
        return task.id


def snapshot(engine, task_id):
    with engine.connect() as connection:
        return dict(
            connection.execute(select(Task.__table__).where(Task.id == task_id)).mappings().one()
        )


def expire_lease(engine, task_id, *, deadline=False):
    with engine.begin() as connection:
        changes = {"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
        if deadline:
            changes["execution_deadline_at"] = changes["lease_expires_at"]
        connection.execute(update(Task).where(Task.id == task_id).values(**changes))


def test_native_claim_cannot_see_uncommitted_or_rolled_back_business_task(store, engine):
    from dewey.sqlalchemy.executor import create_task

    with Session(engine) as producer:
        task = create_task(producer, task_type="native.synthetic")
        task_id = task.id
        assert store("claim", 1) == []
        producer.rollback()
    with engine.connect() as connection:
        assert connection.execute(select(Task.id).where(Task.id == task_id)).first() is None
    with Session(engine) as producer:
        task = create_task(producer, task_type="native.synthetic")
        committed_id = task.id
        assert store("claim", 1) == []
        producer.commit()
    assert store("claim", 1)[0].task_id == committed_id


def test_failed_without_retry_schedule_is_due_without_resetting_attempts(store, engine):
    now = datetime.now(UTC)
    due_id = create(engine, status="failed", attempts=1, scheduled_for=None)
    future_id = create(engine, status="failed", attempts=1, scheduled_for=now + timedelta(days=1))
    exhausted_id = create(engine, status="failed", attempts=3, max_attempts=3)
    before = {task_id: snapshot(engine, task_id) for task_id in (future_id, exhausted_id)}
    assignments = store("claim", 100)
    assert [ticket.task_id for ticket in assignments] == [due_id]
    assert snapshot(engine, due_id)["attempts"] == 1
    assert store("start", assignments[0])["attempts"] == 2
    for task_id, row in before.items():
        assert snapshot(engine, task_id) == row


def test_committed_dispatch_start_and_completion(store, engine):
    store("readiness")
    task_id = create(engine)
    assignment = store("claim", 1)[0]
    assert assignment.task_id == task_id
    assert snapshot(engine, task_id)["attempts"] == 0
    invocation = store("start", assignment)
    assert invocation["task_type"] == "native.synthetic"
    assert snapshot(engine, task_id)["attempts"] == 1
    assert store("renew", assignment)
    assert store("finish", assignment, invocation["execution_token"])
    row = snapshot(engine, task_id)
    assert row["status"] == "completed"
    assert row["latest_outcome"] == "completed"
    assert row["execution_token"] is None
    assert row["dispatch_token"] is None
    assert row["lease_expires_at"] is None
    assert store("reconcile", assignment) == Reconciliation.INVALIDATED


def test_stale_start_abandon_and_outcome_cannot_mutate_new_generation(store, engine):
    task_id = create(engine)
    old = store("claim", 1)[0]
    assert store("abandon", old).durable_release
    new = store("claim", 1)[0]
    assert new.dispatch_token != old.dispatch_token
    invocation = store("start", new)
    before = snapshot(engine, task_id)
    assert store("start", old) is None
    assert store("abandon", old) == Reconciliation.INVALIDATED
    assert not store("renew", old)
    assert not store("finish", old, invocation["execution_token"])
    assert snapshot(engine, task_id) == before
    assert store("finish", new, invocation["execution_token"])


def test_expired_lease_cannot_renew_or_finish(store, engine):
    task_id = create(engine)
    assignment = store("claim", 1)[0]
    invocation = store("start", assignment)
    expire_lease(engine, task_id)
    before = snapshot(engine, task_id)
    assert not store("renew", assignment)
    assert not store("finish", assignment, invocation["execution_token"])
    assert snapshot(engine, task_id) == before
    assert store("recover", 1) == [task_id]
    row = snapshot(engine, task_id)
    assert row["attempts"] == 1
    assert row["status"] == "failed"
    assert row["latest_outcome"] == "interrupted"
    assert row["scheduled_for"] is not None


def test_assignment_recovery_consumes_no_attempt(store, engine):
    task_id = create(engine)
    assignment = store("claim", 1)[0]
    expire_lease(engine, task_id)
    assert store("start", assignment) is None
    assert store("recover", 1) == [task_id]
    row = snapshot(engine, task_id)
    assert row["status"] == "pending"
    assert row["attempts"] == 0
    assert row["latest_outcome"] == "abandoned"


def test_repeated_death_exhausts_budget_without_extra_attempt(store, engine):
    task_id = create(engine, max_attempts=2)
    for expected in (1, 2):
        assignment = store("claim", 1)[0]
        assert store("start", assignment) is not None
        expire_lease(engine, task_id, deadline=True)
        assert store("recover", 1) == [task_id]
        row = snapshot(engine, task_id)
        assert row["attempts"] == expected
        assert row["latest_outcome"] == "timed_out"
        if expected < 2:
            assert row["status"] == "failed"
            with engine.begin() as connection:
                connection.execute(
                    update(Task)
                    .where(Task.id == task_id)
                    .values(scheduled_for=datetime.now(UTC) - timedelta(seconds=1))
                )
        else:
            assert row["status"] == "dead"
    assert store("claim", 1) == []


def test_wrong_execution_token_is_full_noop(store, engine):
    task_id = create(engine)
    assignment = store("claim", 1)[0]
    store("start", assignment)
    before = snapshot(engine, task_id)
    assert not store("finish", assignment, str(uuid4()), ValueError("must not be saved"))
    assert snapshot(engine, task_id) == before


def test_schedule_queue_priority_and_start_expiry(store, engine):
    create(engine, queue="unserved", priority=100)
    create(engine, scheduled_for=datetime.now(UTC) + timedelta(days=1), priority=100)
    expired = create(engine, expires_at=datetime.now(UTC) - timedelta(seconds=1), priority=200)
    high = create(engine, priority=10)
    low = create(engine, priority=1)
    assignments = store("claim", 3)
    assert [a.task_id for a in assignments] == [high, low]
    assert snapshot(engine, expired)["status"] == "expired"
    assert snapshot(engine, expired)["attempts"] == 0


def test_missing_or_forged_assignment_does_not_create_work(store, engine):
    assignment = Assignment(str(uuid4()), str(uuid4()))
    assert store("start", assignment) is None
    assert store("reconcile", assignment) == Reconciliation.INVALIDATED


def test_operator_reset_invalidates_all_native_ownership(store, engine):
    from dewey.sqlalchemy.queries import retry_task

    task_id = create(engine)
    assignment = store("claim", 1)[0]
    invocation = store("start", assignment)
    with Session(engine) as session, session.begin():
        retry_task(session, task_id)
    row = snapshot(engine, task_id)
    assert row["status"] == "pending"
    assert all(
        row[field] is None
        for field in (
            "dispatch_token",
            "worker_id",
            "lease_expires_at",
            "execution_deadline_at",
            "execution_token",
        )
    )
    newer = store("claim", 1)[0]
    before = snapshot(engine, task_id)
    assert not store("finish", assignment, invocation["execution_token"])
    assert snapshot(engine, task_id) == before
    assert newer.dispatch_token != assignment.dispatch_token


def test_partial_tuple_is_not_admitted_or_recovered_as_legacy(store, engine):
    task_id = create(engine)
    assignment = store("claim", 1)[0]
    with engine.begin() as connection:
        connection.execute(update(Task).where(Task.id == task_id).values(worker_id=None))
    before = snapshot(engine, task_id)
    store("readiness")
    with pytest.raises(RuntimeError, match="Incomplete native"):
        store("start", assignment)
    assert snapshot(engine, task_id) == before
