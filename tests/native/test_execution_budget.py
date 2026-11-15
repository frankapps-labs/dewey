"""Per-row attempt budgets and cooperative cancellation on real PostgreSQL."""

import pytest
from sqlalchemy import update
from sqlalchemy.orm import Session

from dewey.errors import IdempotencyConflictError
from dewey.runner import FailureReport, RunnerAssignment, RunnerPolicy, RunnerTiming
from dewey.sqlalchemy.models import TaskEntryModel as Task
from tests.native.test_remote import runner as runner
from tests.native.test_storage import create, expire_lease, snapshot


@pytest.fixture
def producer(store, engine):
    """Use each public producer, including its idempotent creation path."""

    def enqueue(**values):
        idempotent = "idempotency_key" in values
        values = {"task_type": "native.synthetic", **values}
        if store.mode == "django":
            from dewey.django import create_or_get_task, create_task

            fn = create_or_get_task if idempotent else create_task
            return fn(using="native", **values).id
        if store.mode == "async":
            from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

            from dewey._worker.loop import ChildLoop
            from dewey.sqlalchemy import create_or_get_task_async, create_task_async

            async def run():
                async_engine = create_async_engine(engine.url.set(drivername="postgresql+asyncpg"))
                try:
                    async with AsyncSession(async_engine) as session, session.begin():
                        fn = create_or_get_task_async if idempotent else create_task_async
                        return (await fn(session, **values)).id
                finally:
                    await async_engine.dispose()

            loop = ChildLoop()
            try:
                return loop.invoke(run)
            finally:
                loop.close()
        from dewey.sqlalchemy import create_or_get_task, create_task

        with Session(engine) as session, session.begin():
            fn = create_or_get_task if idempotent else create_task
            return fn(session, **values).id

    return enqueue


@pytest.mark.parametrize("budget,expected", [(600, 600), (1200, 900), (None, 900), (60, 60)])
def test_claim_stamps_row_budget_bounded_by_server_ceiling(
    runner, producer, engine, budget, expected
):
    task_id = producer(execution_timeout_s=budget)
    policy = RunnerPolicy(runner.policy.owner, ["default"], RunnerTiming(30, 30, 2, 1, 0.1, 1, 900))
    assignment = runner("claim", custom_policy=policy)[0]
    row = snapshot(engine, task_id)
    assert row["execution_timeout_s"] == budget
    assert (row["execution_deadline_at"] - row["dispatching_at"]).total_seconds() == expected
    invocation = runner("start", assignment, custom_policy=policy)
    assert invocation["execution_timeout_s"] == budget
    assert invocation["execution_deadline_at"] == row["execution_deadline_at"]


@pytest.mark.parametrize("budget", [0, -1, float("inf"), float("-inf"), float("nan"), True, "60"])
def test_invalid_budget_never_creates_work(producer, engine, budget):
    with pytest.raises(ValueError, match="execution_timeout_s"):
        producer(execution_timeout_s=budget)
    with engine.connect() as connection:
        assert connection.execute(Task.__table__.select()).all() == []


def test_budget_is_part_of_idempotent_creation_contract(producer, engine):
    task_id = producer(execution_timeout_s=60, idempotency_key="synthetic-budget")
    before = snapshot(engine, task_id)
    assert producer(execution_timeout_s=60, idempotency_key="synthetic-budget") == task_id
    with pytest.raises(IdempotencyConflictError):
        producer(execution_timeout_s=600, idempotency_key="synthetic-budget")
    assert snapshot(engine, task_id) == before


def test_retry_keeps_budget_and_gets_a_new_attempt_deadline(runner, producer, engine):
    task_id = producer(execution_timeout_s=15)
    first = runner("claim")[0]
    token = runner("start", first)["execution_token"]
    assert runner("fail", first, token, FailureReport())
    with engine.begin() as connection:
        connection.execute(update(Task).where(Task.id == task_id).values(scheduled_for=None))
    second = runner("claim")[0]
    assert second.dispatch_token != first.dispatch_token
    row = snapshot(engine, task_id)
    assert row["attempts"] == 1
    assert row["execution_timeout_s"] == 15
    assert (row["execution_deadline_at"] - row["dispatching_at"]).total_seconds() == 15
    assert runner("start", second)["attempts"] == 2


def test_renewal_never_extends_execution_budget(runner, producer, engine):
    task_id = producer(execution_timeout_s=10)
    assignment = runner("claim")[0]
    token = runner("start", assignment)["execution_token"]
    deadline = snapshot(engine, task_id)["execution_deadline_at"]
    for _ in range(3):
        result = runner("renew", assignment, token)
        assert result and result.cancel_requested is False
        row = snapshot(engine, task_id)
        assert row["execution_deadline_at"] == deadline
        assert row["lease_expires_at"] <= deadline


def test_cancel_request_is_fenced_and_delivered_by_renewal(runner, producer, engine):
    task_id = producer(execution_timeout_s=60)
    assignment = runner("claim")[0]
    token = runner("start", assignment)["execution_token"]
    before = snapshot(engine, task_id)
    assert not runner("request_cancel", assignment, "wrong-generation")
    assert not runner("request_cancel", RunnerAssignment(task_id, "wrong-assignment"), token)
    assert snapshot(engine, task_id) == before
    assert runner("request_cancel", assignment, token)
    assert runner("renew", assignment, token).cancel_requested is True
    # No winner after cancellation; the failure settles the obligation, not physical stop.
    assert not runner("complete", assignment, token)
    assert runner("fail", assignment, token, FailureReport())
    row = snapshot(engine, task_id)
    assert row["status"] == "dead"
    assert row["latest_outcome"] == "cancelled"
    assert runner("claim") == []


def test_cancelled_worker_death_does_not_retry(runner, engine):
    task_id = create(engine, execution_timeout_s=60)
    assignment = runner("claim")[0]
    token = runner("start", assignment)["execution_token"]
    assert runner("request_cancel", assignment, token)
    expire_lease(engine, task_id)
    assert runner("renew", assignment, token) is None
    assert runner("recover") == [task_id]
    row = snapshot(engine, task_id)
    assert row["status"] == "dead"
    assert row["latest_outcome"] == "cancelled"
