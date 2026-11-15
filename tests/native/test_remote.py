"""The real three-adapter server boundary; no tool, provider or live host calls."""

import threading
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import Column, MetaData, String, Table, func, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session

from dewey._worker.loop import ChildLoop
from dewey.runner import FailureReport, LeaseExpired, RunnerAssignment, RunnerPolicy, RunnerTiming
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.models import TaskEventModel
from tests.native.test_storage import create, expire_lease, snapshot

OUTPUT = Table(
    "synthetic_runner_outputs",
    MetaData(),
    Column("task_id", String(36), primary_key=True),
    Column("execution_token", String(36), nullable=False),
)


class RollbackProbe(Exception):
    pass


@pytest.fixture
def runner(store, engine):
    """One real caller root per request, with explicit commit before returning."""
    policy = RunnerPolicy(store.owner, ["default"], RunnerTiming(30, 30, 2, 1, 0.1, 1, 60))
    OUTPUT.create(engine)
    loop = ChildLoop()
    async_engine = create_async_engine(engine.url.set(drivername="postgresql+asyncpg"))

    def call(operation, *args, rollback=False, winner=None, custom_policy=None):
        selected = custom_policy or policy
        if store.mode == "django":
            from django.db import connections, transaction

            from dewey.django.runner import DjangoRunner

            with transaction.atomic(using="native"):
                result = getattr(DjangoRunner(selected, using="native"), operation)(*args)
                if winner is not None:
                    assert result
                    with connections["native"].cursor() as cursor:
                        cursor.execute(
                            "INSERT INTO synthetic_runner_outputs VALUES (%s,%s)", winner
                        )
                if rollback:
                    raise RollbackProbe
                return result
        if store.mode == "sync":
            from dewey.sqlalchemy.runner import SQLAlchemyRunner

            with engine.connect() as connection, connection.begin() as root:
                with Session(bind=connection, join_transaction_mode="rollback_only") as session:
                    result = getattr(SQLAlchemyRunner(session, selected), operation)(*args)
                    if winner is not None:
                        assert result
                        connection.execute(
                            insert(OUTPUT).values(task_id=winner[0], execution_token=winner[1])
                        )
                    assert root.is_active and not connection.closed
                    if rollback:
                        raise RollbackProbe
                    return result

        async def run():
            from dewey.sqlalchemy.runner import AsyncSQLAlchemyRunner

            async with async_engine.connect() as connection, connection.begin() as root:
                async with AsyncSession(
                    bind=connection, join_transaction_mode="rollback_only"
                ) as session:
                    result = await getattr(AsyncSQLAlchemyRunner(session, selected), operation)(
                        *args
                    )
                    if winner is not None:
                        assert result
                        await connection.execute(
                            insert(OUTPUT).values(task_id=winner[0], execution_token=winner[1])
                        )
                    assert root.is_active and not connection.closed
                    if rollback:
                        raise RollbackProbe
                    return result

        return loop.invoke(run)

    call.policy, call.mode = policy, store.mode
    try:
        yield call
    finally:
        loop.invoke(async_engine.dispose)
        loop.close()
        OUTPUT.drop(engine)


def history(engine, task_id):
    with engine.connect() as connection:
        return (
            connection.execute(
                select(TaskEventModel.__table__)
                .where(TaskEventModel.task_id == task_id)
                .order_by(TaskEventModel.id)
            )
            .mappings()
            .all()
        )


def observed(engine, task_id):
    return snapshot(engine, task_id), history(engine, task_id)


def test_sql_error_preserves_original_failure_and_caller_rollback(runner, engine, monkeypatch):
    from django.db import DatabaseError, connections
    from sqlalchemy.exc import DBAPIError

    from dewey.django.runner import _Operations as DjangoOperations
    from dewey.sqlalchemy.runner import _Operations

    task_id = create(engine)
    before = observed(engine, task_id)

    def reject_sqlalchemy(self, session, limit):
        session.execute(text("SELECT 1 / 0"))

    def reject_django(self, limit):
        with connections[self.using].cursor() as cursor:
            cursor.execute("SELECT 1 / 0")

    with monkeypatch.context() as patch:
        patch.setattr(_Operations, "claim", reject_sqlalchemy)
        patch.setattr(DjangoOperations, "claim", reject_django)
        with pytest.raises((DBAPIError, DatabaseError)) as failure:
            runner("claim")
        original = getattr(failure.value, "orig", failure.value.__cause__)
        assert original.pgcode == "22012"  # Not a secondary aborted-root SET error.
    assert observed(engine, task_id) == before
    assert runner("claim")[0].task_id == task_id


def test_full_lifecycle_and_atomic_winner_receipt(runner, engine):
    runner("check_readiness")
    task_id = create(engine, kwargs={"input_ref": "synthetic://fixture"})
    assignment = runner("claim")[0]
    assert snapshot(engine, task_id)["attempts"] == 0
    invocation = runner("start", assignment)
    token = invocation["execution_token"]
    assert invocation["kwargs"] == {"input_ref": "synthetic://fixture"}
    assert invocation["attempts"] == 1
    assert runner("renew", assignment, token)
    before = observed(engine, task_id)
    with pytest.raises(RollbackProbe):
        runner("complete", assignment, token, winner=(task_id, token), rollback=True)
    assert observed(engine, task_id) == before
    with engine.connect() as connection:
        assert connection.execute(select(OUTPUT)).all() == []
    assert runner("complete", assignment, token, winner=(task_id, token))
    assert snapshot(engine, task_id)["status"] == "completed"
    with engine.connect() as connection:
        assert connection.execute(select(OUTPUT)).one() == (task_id, token)
    settled = observed(engine, task_id)
    assert not runner("complete", assignment, token)
    assert observed(engine, task_id) == settled


@pytest.mark.parametrize("method", ["renew", "complete", "fail"])
def test_wrong_execution_generation_is_a_total_noop(runner, engine, method):
    task_id = create(engine)
    assignment = runner("claim")[0]
    runner("start", assignment)
    before = observed(engine, task_id)
    args = (FailureReport(),) if method == "fail" else ()
    assert not runner(method, assignment, str(uuid4()), *args)
    assert observed(engine, task_id) == before


@pytest.mark.parametrize("scope", ["owner", "queue"])
def test_known_ticket_does_not_bypass_server_owner_or_queue_scope(runner, engine, scope):
    task_id = create(engine)
    assignment = runner("claim")[0]
    token = runner("start", assignment)["execution_token"]
    policy = RunnerPolicy(
        str(uuid4()) if scope == "owner" else runner.policy.owner,
        ["foreign"] if scope == "queue" else ["default"],
        runner.policy.timing,
    )
    before = observed(engine, task_id)
    assert runner("start", assignment, custom_policy=policy) is None
    assert not runner("renew", assignment, token, custom_policy=policy)
    assert not runner("complete", assignment, token, custom_policy=policy)
    assert not runner("fail", assignment, token, FailureReport(), custom_policy=policy)
    assert not runner("release", assignment, custom_policy=policy)
    assert observed(engine, task_id) == before


@pytest.mark.parametrize("part", ["task_id", "dispatch_token"])
def test_forged_ticket_cannot_mutate_an_execution(runner, engine, part):
    task_id = create(engine)
    assignment = runner("claim")[0]
    token = runner("start", assignment)["execution_token"]
    forged = RunnerAssignment(
        str(uuid4()) if part == "task_id" else assignment.task_id,
        str(uuid4()) if part == "dispatch_token" else assignment.dispatch_token,
    )
    before = observed(engine, task_id)
    assert runner("start", forged) is None
    assert not runner("renew", forged, token)
    assert not runner("complete", forged, token)
    assert not runner("fail", forged, token, FailureReport())
    assert not runner("release", forged)
    assert observed(engine, task_id) == before


def test_assignment_release_never_acts_as_a_running_worker_stop_ack(runner, engine):
    task_id = create(engine)
    assignment = runner("claim")[0]
    assert runner("release", assignment)
    assert snapshot(engine, task_id)["attempts"] == 0
    fresh = runner("claim")[0]
    assert fresh.dispatch_token != assignment.dispatch_token
    token = runner("start", fresh)["execution_token"]
    before = observed(engine, task_id)
    assert not runner("release", fresh)
    assert not runner("renew", assignment, token)
    assert not runner("complete", assignment, token)
    assert observed(engine, task_id) == before


def test_capacity_reservation_failure_rolls_back_claim_without_an_attempt(runner, engine):
    task_id = create(engine)
    before = observed(engine, task_id)
    with pytest.raises(RollbackProbe):
        runner("claim", rollback=True)
    assert observed(engine, task_id) == before
    assignment = runner("claim")[0]
    with pytest.raises(RollbackProbe):
        runner("start", assignment, rollback=True)
    assert snapshot(engine, task_id)["attempts"] == 0
    assert runner("start", assignment)["attempts"] == 1


@pytest.mark.parametrize(
    "report,expected",
    [
        (FailureReport("transient", "synthetic"), "failed"),
        (FailureReport("non_retryable", "synthetic"), "dead"),
        (FailureReport("transient", "synthetic", 120), "failed"),
    ],
)
def test_data_failure_preserves_server_classification(runner, engine, report, expected):
    task_id = create(engine)
    assignment = runner("claim")[0]
    token = runner("start", assignment)["execution_token"]
    with engine.connect() as connection:
        now = connection.execute(select(func.clock_timestamp())).scalar_one()
    assert runner("fail", assignment, token, report)
    row = snapshot(engine, task_id)
    assert row["status"] == expected
    assert row["attempts"] == 1 and row["execution_token"] is None
    if report.retry_after_seconds:
        assert row["scheduled_for"] >= now + timedelta(seconds=report.retry_after_seconds)


@pytest.mark.parametrize("deadline", [False, True])
def test_lost_runner_and_deadline_recovery_keep_stale_reports_fenced(runner, engine, deadline):
    task_id = create(engine)
    assignment = runner("claim")[0]
    token = runner("start", assignment)["execution_token"]
    expire_lease(engine, task_id, deadline=deadline)
    before = observed(engine, task_id)
    assert not runner("renew", assignment, token)
    assert not runner("complete", assignment, token)
    assert observed(engine, task_id) == before
    assert runner("recover") == [task_id]
    row = snapshot(engine, task_id)
    assert row["attempts"] == 1 and row["status"] == "failed"
    assert row["latest_outcome"] == ("timed_out" if deadline else "interrupted")
    with engine.begin() as connection:
        connection.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(scheduled_for=datetime.now(UTC) - timedelta(seconds=1))
        )
    fresh = runner("claim")[0]
    new_token = runner("start", fresh)["execution_token"]
    assert token != new_token
    fresh_before = observed(engine, task_id)
    assert not runner("renew", assignment, token)
    assert not runner("complete", assignment, token)
    assert observed(engine, task_id) == fresh_before


def test_host_ownership_invalidation_is_not_a_worker_stop_ack(runner, engine):
    task_id = create(engine)
    assignment = runner("claim")[0]
    token = runner("start", assignment)["execution_token"]
    # Represents the task side of a host cancellation; no process stop is claimed.
    with engine.begin() as connection:
        connection.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(
                status="dead",
                dispatch_token=None,
                worker_id=None,
                execution_token=None,
                lease_expires_at=None,
                execution_deadline_at=None,
            )
        )
    before = observed(engine, task_id)
    assert not runner("renew", assignment, token)
    assert not runner("complete", assignment, token)
    assert not runner("fail", assignment, token, FailureReport())
    assert not runner("release", assignment)
    assert observed(engine, task_id) == before


def test_assignment_expiry_and_unstarted_recovery_do_not_consume_an_attempt(runner, engine):
    task_id = create(engine)
    assignment = runner("claim")[0]
    expire_lease(engine, task_id)
    assert runner("start", assignment) is None
    assert runner("recover") == [task_id]
    assert snapshot(engine, task_id)["attempts"] == 0


def test_expired_at_claim_and_queue_selection(runner, engine):
    foreign = create(engine, queue="other", priority=999)
    expired = create(engine, expires_at=datetime.now(UTC) - timedelta(seconds=1))
    valid = create(engine)
    before = observed(engine, foreign)
    assignments = runner("claim", 100)
    assert [a.task_id for a in assignments] == [valid]
    assert observed(engine, foreign) == before
    assert snapshot(engine, expired)["status"] == "expired"


@pytest.mark.parametrize("limit", [0, -1, True, 101])
def test_bounded_limits_reject_before_mutation(runner, engine, limit):
    task_id = create(engine)
    before = observed(engine, task_id)
    with pytest.raises(ValueError):
        runner("claim", limit)
    with pytest.raises(ValueError):
        runner("recover", limit)
    assert observed(engine, task_id) == before


@pytest.mark.parametrize("method", ["start", "renew", "complete", "fail"])
def test_expiry_after_writes_requires_full_caller_rollback(runner, engine, monkeypatch, method):
    """Actual DB time advances after the write; provisional results cannot escape."""
    from dewey.django.runner import _Operations as DjangoOperations
    from dewey.sqlalchemy.runner import _Operations as SQLAlchemyOperations

    task_id = create(engine)
    assignment = runner("claim")[0]
    token = None if method == "start" else runner("start", assignment)["execution_token"]
    with engine.begin() as connection:
        connection.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(lease_expires_at=datetime.now(UTC) + timedelta(seconds=0.5))
        )
    before = observed(engine, task_id)
    target = DjangoOperations if runner.mode == "django" else SQLAlchemyOperations
    original = target.now
    calls = []

    def now(self, *args):
        calls.append(True)
        if len(calls) == 2:
            time.sleep(0.7)
        return original(self, *args)

    monkeypatch.setattr(target, "now", now)
    args = () if method == "start" else (token,)
    if method == "fail":
        args += (FailureReport(),)
    policy = RunnerPolicy(
        runner.policy.owner, runner.policy.queues, RunnerTiming(30, 30, 2, 3, 0.1, 1, 60)
    )
    with pytest.raises(LeaseExpired):
        runner(method, assignment, *args, custom_policy=policy)
    assert len(calls) == 2
    assert observed(engine, task_id) == before


def test_expiry_during_observed_row_lock_wait_is_a_noop(runner, engine):
    """An independent backend witnesses the wait; DB time is refreshed after it."""
    task_id = create(engine)
    assignment = runner("claim")[0]
    deadline = datetime.now(UTC) + timedelta(seconds=0.5)
    with engine.begin() as connection:
        connection.execute(update(Task).where(Task.id == task_id).values(lease_expires_at=deadline))
    before = observed(engine, task_id)
    locked = threading.Event()
    witnesses, errors = [], []

    def hold():
        try:
            # Fresh statistics per probe: a transaction-cached activity snapshot
            # can omit a waiter whose backend connected after the first probe.
            with (
                engine.begin() as locker,
                engine.connect().execution_options(isolation_level="AUTOCOMMIT") as observer,
            ):
                pid = locker.execute(select(func.pg_backend_pid())).scalar_one()
                locker.execute(select(Task.id).where(Task.id == task_id).with_for_update())
                locked.set()
                stop = time.monotonic() + 1
                while time.monotonic() < stop:
                    blocked = observer.execute(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                            "WHERE :locker = ANY(pg_blocking_pids(pid)))"
                        ),
                        {"locker": pid},
                    ).scalar_one()
                    if blocked:
                        witnesses.append(True)
                        break
                    time.sleep(0.01)
                remaining = (deadline - datetime.now(UTC)).total_seconds() + 0.05
                if remaining > 0:
                    time.sleep(remaining)
        except BaseException as exc:
            errors.append(exc)
            locked.set()

    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert locked.wait(2)
        policy = RunnerPolicy(
            runner.policy.owner, runner.policy.queues, RunnerTiming(30, 30, 2, 3, 0.1, 1, 60)
        )
        assert runner("start", assignment, custom_policy=policy) is None
    finally:
        thread.join(4)
    assert not thread.is_alive() and not errors
    assert witnesses == [True]
    assert observed(engine, task_id) == before


@pytest.mark.parametrize("queues", [[], "default", [""], [None]])
def test_queue_authority_fails_closed(runner, queues):
    with pytest.raises(ValueError):
        RunnerPolicy(runner.policy.owner, queues, runner.policy.timing)


@pytest.mark.parametrize("delay", [True, -1, float("nan"), float("inf"), 365 * 86400 + 1])
def test_untrusted_failure_delay_is_bounded(runner, delay):
    with pytest.raises(ValueError):
        FailureReport(retry_after_seconds=delay)


def test_hosts_create_schedule_ticks_through_the_facade(runner, store, engine):
    """Nothing schedule-related is Python-worker-only: a gateway host calls tick()."""
    import dewey

    dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
    store("ops", "schedules.apply")
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE dewey_schedules SET cursor_at = clock_timestamp() - interval '3 min',"
                " next_due_at = clock_timestamp() - interval '3 min'"
            )
        )

    def ticks():
        with engine.connect() as connection:
            return connection.execute(
                text("SELECT count(*) FROM task_entries WHERE task_type = 'inventory.poll'")
            ).scalar_one()

    # Provisional like every facade result: the caller's rollback takes it back.
    with pytest.raises(RollbackProbe):
        runner("tick", rollback=True)
    assert ticks() == 0
    (report,) = runner("tick")
    assert report["schedule"] == "poll" and len(report["created"]) == 1 and ticks() == 1
    assert runner("tick") == []


def test_a_runner_with_an_empty_catalog_claims_nothing(runner, engine):
    waiting = create(engine, priority=100)
    before = observed(engine, waiting)
    timing = RunnerTiming(30, 30, 2, 1, 0.1, 1, 60)
    assert runner("claim", 10, custom_policy=RunnerPolicy("c" * 36, ["default"], timing, [])) == []
    assert observed(engine, waiting) == before
    assert [a.task_id for a in runner("claim", 10)] == [waiting]  # no catalog: every type


@pytest.mark.parametrize("task_types", ["render.frame", [""], [None], ["t" * 101], 7])
def test_a_catalog_is_a_list_of_task_type_names(task_types):
    with pytest.raises(ValueError, match="Task types"):
        RunnerPolicy("c" * 36, ["default"], RunnerTiming(30, 30, 2, 1, 0.1, 1, 60), task_types)


def test_a_host_receives_the_creators_trace_with_the_invocation(runner, engine):
    from dewey.core.logging import reset_trace_context, set_trace_context
    from dewey.sqlalchemy.executor import create_task

    parent = "00-" + "a" * 32 + "-" + "b" * 16 + "-01"
    token = set_trace_context({"traceparent": parent})
    try:
        with Session(engine) as session, session.begin():
            create_task(session, task_type="render.frame", metadata={"private": "kept-out"})
    finally:
        reset_trace_context(token)
    (assignment,) = runner("claim")
    invocation = runner("start", assignment)
    assert invocation["trace"] == {"traceparent": parent}
    assert "kept-out" not in repr(invocation)


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_a_tick_pass_needs_a_positive_whole_limit(runner, limit):
    with pytest.raises(ValueError):
        runner("tick", limit)


def test_a_runner_policy_can_narrow_claims_to_its_catalog(runner, engine):
    known, unknown = create(engine, task_type="render.frame"), create(engine, task_type="other")
    scoped = RunnerPolicy(
        "c" * 36, ["default"], RunnerTiming(30, 30, 2, 1, 0.1, 1, 60), task_types=["render.frame"]
    )
    assert [a.task_id for a in runner("claim", 10, custom_policy=scoped)] == [known]
    assert snapshot(engine, unknown)["status"] == "pending"
    with pytest.raises(ValueError):
        RunnerPolicy("c" * 36, ["default"], RunnerTiming(30, 30, 2, 1, 0.1, 1, 60), task_types="x")
