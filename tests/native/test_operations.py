"""Operations, heartbeats, alerts and claim scope: one contract on every storage."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.orm import Session, sessionmaker

import dewey
from dewey import alerts
from dewey._worker.contracts import Timing
from dewey.context import TaskContext
from dewey.core.logging import reset_trace_context, set_trace_context
from dewey.sqlalchemy.executor import create_task
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.models import WorkerHeartbeatModel as Heartbeat
from dewey.sqlalchemy.native import NativeSQLAlchemyStorage

TIMING = Timing(30, 30, 2, 1, 0.1, 1, 60)


@pytest.fixture(autouse=True)
def clean_registry():
    yield
    dewey.registry.clear()
    alerts.on_alert(None)


@pytest.fixture
def heard():
    events = []
    alerts.on_alert(events.append)
    return events


def create(engine, task_type="ops.work", **values) -> str:
    with Session(engine) as session, session.begin():
        return create_task(session, task_type=task_type, **values).id


def set_fields(engine, task_id: str, **values) -> None:
    with engine.begin() as connection:
        connection.execute(update(Task).where(Task.id == task_id).values(**values))


def status(engine, task_id: str) -> str:
    with engine.connect() as connection:
        return connection.execute(select(Task.status).where(Task.id == task_id)).scalar_one()


class TestOperations:
    def test_queues_and_task_listing(self, store, engine):
        first = create(engine, kwargs={"secret": "s3"})
        set_fields(engine, create(engine, queue="reports"), status="dead", error="boom")
        queues = {row["queue"]: row for row in store("ops", "queues")}
        assert (queues["default"]["pending"], queues["reports"]["dead"]) == (1, 1)
        (listed,) = store("ops", "tasks.list", {"queue": "default"})
        assert listed["id"] == first and "kwargs" not in listed
        (dead,) = store("ops", "tasks.list", {"status": "dead", "reveal": True})
        assert (dead["error"], dead["has_error"]) == ("boom", True)
        with pytest.raises(ValueError):
            store("ops", "tasks.list", {"limit": 0})
        with pytest.raises(ValueError, match="Unknown operation"):
            store("ops", "tasks.delete")

    def test_every_queue_row_counts_every_status(self, store, engine):
        create(engine, queue="slow")
        set_fields(engine, create(engine, queue="fast"), status="failed")
        create(engine, queue="fast")
        fast, slow = store("ops", "queues")
        statuses = {
            "pending",
            "dispatching",
            "processing",
            "completed",
            "failed",
            "dead",
            "expired",
        }
        assert set(fast) == set(slow) == {"queue"} | statuses
        assert (fast["queue"], fast["pending"], fast["failed"], fast["dead"]) == ("fast", 1, 1, 0)
        assert sum(slow[status] for status in statuses) == 1

    def test_filters_combine_and_nothing_private_leaves_unless_revealed(self, store, engine):
        private = {
            "args": ["private-args"],
            "kwargs": {"secret": "private-kwargs"},
            "metadata": {"note": "private-metadata"},
        }
        wanted = create(engine, queue="fast", **private)
        create(engine, queue="slow", **private)
        create(engine, queue="fast", task_type="ops.other")
        set_fields(engine, create(engine, queue="fast"), status="pending")
        for task_id in (wanted,):
            set_fields(engine, task_id, status="dead", error="private-error")
        filters = {"status": "dead", "queue": "fast", "task_type": "ops.work", "limit": 1}
        listed = store("ops", "tasks.list", filters)
        assert [row["id"] for row in listed] == [wanted]
        shown = store("ops", "tasks.show", {"task_id": wanted})
        assert "private-" not in json.dumps([listed, shown])
        (revealed,) = store("ops", "tasks.list", {**filters, "reveal": True})
        assert (revealed["args"], revealed["kwargs"], revealed["error"]) == (
            ["private-args"],
            {"secret": "private-kwargs"},
            "private-error",
        )
        assert set(revealed) - set(listed[0]) == {"args", "kwargs", "error"}
        assert "private-metadata" not in json.dumps(revealed)  # metadata is never part of it

    @pytest.mark.parametrize(
        "arguments",
        [
            {"status": "sleeping"},
            {"limit": 0},
            {"limit": 501},
            {"limit": True},
            {"reveal": "false"},  # a transport's string must not pass for consent
        ],
    )
    def test_listing_rejects_what_it_cannot_honour(self, store, arguments):
        with pytest.raises(ValueError):
            store("ops", "tasks.list", arguments)

    def test_killing_is_recorded_once_and_history_carries_no_payload(self, store, engine):
        task_id = create(engine, args=["private-args"])
        set_fields(engine, task_id, error="private-error")
        assert store("ops", "tasks.kill", {"task_id": task_id})["status"] == "dead"
        store("ops", "tasks.kill", {"task_id": task_id})  # already dead: nothing more to record
        shown = store("ops", "tasks.show", {"task_id": task_id})
        (event,) = shown["history"]
        assert (event["kind"], event["attempt"], set(event)) == (
            "dead",
            0,
            {"kind", "attempt", "at"},
        )
        assert "private-" not in json.dumps(shown)
        revealed = store("ops", "tasks.show", {"task_id": task_id, "reveal": True})
        assert revealed["error"] == "private-error" and revealed["history"] == shown["history"]

    @pytest.mark.parametrize(
        "before", ["pending", "dispatching", "processing", "completed", "failed", "dead", "expired"]
    )
    def test_retry_and_kill_follow_the_state_machine(self, store, engine, before):
        retried, killed = (create(engine, args=["private-args"], kwargs={"n": 1}) for _ in range(2))
        for task_id in (retried, killed):
            set_fields(engine, task_id, status=before, attempts=2, error="private-error")
        result = store("ops", "tasks.retry", {"task_id": retried})
        requeued = before in ("dispatching", "processing", "failed", "dead")
        if before == "expired":
            assert result is None  # its moment has passed; create a new task instead
        else:
            assert (result["status"], result["attempts"]) == (
                ("pending", 0) if requeued else (before, 2)
            )
        result = store("ops", "tasks.kill", {"task_id": killed})
        assert result["status"] == (before if before in ("completed", "expired") else "dead")
        assert result["attempts"] == 2 and "private-" not in json.dumps(result)

    def test_schedule_listings_never_carry_arguments(self, store):
        dewey.schedule(
            "poll",
            task="ops.work",
            when=dewey.every(minutes=1),
            args=("private-args",),
            kwargs={"secret": "private-kwargs"},
        )
        plan = store("ops", "schedules.plan")
        applied = store("ops", "schedules.apply")
        (row,) = store("ops", "schedules.list")
        assert set(row) == {
            "name",
            "task_type",
            "when",
            "catch_up",
            "overlap",
            "paused",
            "paused_until",
            "next_due_at",
            "late",
            "last_fired_at",
            "missed_count",
            "skipped_count",
        }
        assert "private-" not in json.dumps([plan, applied, row, store("ops", "doctor")])

    def test_show_retry_and_kill(self, store, engine):
        task_id = create(engine)
        assert store("ops", "tasks.show", {"task_id": task_id})["status"] == "pending"
        assert store("ops", "tasks.kill", {"task_id": task_id})["status"] == "dead"
        assert store("ops", "tasks.retry", {"task_id": task_id})["status"] == "pending"
        assert store("ops", "tasks.show", {"task_id": "missing"}) is None
        assert store("ops", "tasks.retry", {"task_id": "missing"}) is None

    def test_heartbeat_makes_a_worker_visible(self, store, engine):
        assert store("ops", "workers") == []
        store("heartbeat")
        store("heartbeat")  # refreshed in place, not duplicated
        (worker,) = store("ops", "workers")
        assert worker["instance_id"] == store.owner and worker["queues"] == ["default"]
        assert set(worker) == {
            "instance_id",
            "dewey_version",
            "backend",
            "database",
            "queues",
            "started_at",
            "last_seen_at",
        }
        assert json.loads(json.dumps(worker)) == worker
        assert worker["backend"] == ("django" if store.mode == "django" else "sqlalchemy")
        with engine.begin() as connection:  # long gone
            connection.execute(
                update(Heartbeat).values(last_seen_at=datetime.now(UTC) - timedelta(days=8))
            )
        assert store("ops", "workers") == []
        store("heartbeat")
        with engine.connect() as connection:  # the stale row was swept, ours re-created
            assert connection.execute(select(Heartbeat.instance_id)).scalars().all() == [
                store.owner
            ]


def test_an_operation_reads_the_database_not_the_callers_session_cache(engine):
    from dewey.sqlalchemy.ops import run

    task_id = create(engine)
    with Session(engine) as session:
        cached = session.get(Task, task_id)  # the caller's own object, read earlier
        assert cached.status == "pending"
        set_fields(engine, task_id, status="failed")
        (listed,) = run(session, "tasks.list", status="failed")
        assert listed["status"] == "failed"
        assert run(session, "tasks.show", task_id=task_id)["status"] == "failed"


class TestDoctor:
    def found(self, store, **arguments) -> set[str]:
        report = store("ops", "doctor", arguments)
        errors = {f["id"].rsplit(".", 1)[-1] for f in report["findings"] if f["level"] == "error"}
        assert report["ok"] is (not errors)
        return errors

    def test_a_running_worker_with_nothing_wrong_is_ready(self, store, engine):
        dewey.task("ops.work")(lambda: None)
        create(engine)
        assert self.found(store) == {"no_worker"}
        store("heartbeat")
        assert self.found(store) == set()

    def test_unserved_queues_unknown_types_and_stuck_work(self, store, engine):
        store("heartbeat")
        create(engine, queue="reports")
        assert self.found(store) == {"queue_unserved", "unknown_task_type"}
        assert self.found(store, queues=["default"]) == {"unknown_task_type"}
        dewey.task("ops.work")(lambda: None)
        stuck = create(engine)
        past = datetime.now(UTC) - timedelta(minutes=10)
        set_fields(
            engine,
            stuck,
            status="processing",
            dispatch_token="d" * 36,
            worker_id="w" * 36,
            execution_token="e" * 36,
            lease_expires_at=past,
            execution_deadline_at=past,
        )
        assert self.found(store, queues=["default"]) == {"unrecovered_work"}

    def test_a_worker_that_stopped_beating_is_no_worker(self, store, engine):
        dewey.task("ops.work")(lambda: None)
        create(engine)
        store("heartbeat")
        with engine.begin() as connection:  # two minutes of silence: well within retention
            connection.execute(
                text(
                    "UPDATE dewey_dispatcher_heartbeats"
                    " SET last_seen_at = clock_timestamp() - interval '2 min'"
                )
            )
        assert self.found(store) == {"no_worker"}
        assert store("ops", "workers") == []

    def test_each_problem_is_reported_once_and_only_for_work_still_waiting(self, store, engine):
        store("heartbeat")
        for status in ("pending", "failed"):
            set_fields(engine, create(engine, queue="slow", kwargs={"k": "private"}), status=status)
        set_fields(engine, create(engine, task_type="ops.finished"), status="dead")
        set_fields(engine, create(engine, task_type="ops.done"), status="completed")
        report = store("ops", "doctor")
        assert sorted(finding["id"].rsplit(".", 1)[-1] for finding in report["findings"]) == [
            "queue_unserved",
            "unknown_task_type",
        ]
        assert "ops.finished" not in json.dumps(report) and "private" not in json.dumps(report)
        assert all(set(f) == {"id", "level", "message"} for f in report["findings"])

    def test_only_a_lease_long_past_its_expiry_is_unrecovered_work(self, store, engine):
        dewey.task("ops.work")(lambda: None)
        store("heartbeat")
        lease = {
            "status": "processing",
            "dispatch_token": "d" * 36,
            "worker_id": "w" * 36,
            "execution_token": "e" * 36,
        }
        now = datetime.now(UTC)
        recent, live = now - timedelta(seconds=10), now + timedelta(minutes=5)
        set_fields(
            engine, create(engine), lease_expires_at=recent, execution_deadline_at=live, **lease
        )
        set_fields(
            engine, create(engine), lease_expires_at=live, execution_deadline_at=live, **lease
        )
        assert self.found(store) == set()  # recovery is due any moment: not yet a finding

    def test_a_timed_pause_that_ran_out_no_longer_excuses_a_late_schedule(self, store, engine):
        dewey.task("ops.work")(lambda: None)
        store("heartbeat")
        dewey.schedule("poll", task="ops.work", when=dewey.every(minutes=1))
        store("ops", "schedules.apply")
        store("ops", "schedules.pause", {"seconds": 600})
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE dewey_schedules SET next_due_at = clock_timestamp() - interval '10 min'"
                )
            )
        assert self.found(store) == set()
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE dewey_schedules SET paused_until = clock_timestamp() - interval '1 sec'"
                )
            )
        assert self.found(store) == {"schedule_late"}

    def test_drift_names_what_would_change(self, store, engine):
        dewey.task("ops.work")(lambda: None)
        store("heartbeat")
        for name in ("kept", "changed", "removed"):
            dewey.schedule(name, task="ops.work", when=dewey.every(minutes=1))
        store("ops", "schedules.apply")
        dewey.schedules.clear()
        dewey.schedule("kept", task="ops.work", when=dewey.every(minutes=1))
        dewey.schedule("changed", task="ops.work", when=dewey.every(minutes=2))
        dewey.schedule("added", task="ops.work", when=dewey.every(minutes=1), args=("private",))
        (finding,) = store("ops", "doctor")["findings"]
        assert finding["id"] == "dewey.doctor.schedule_drift"
        assert "to add: added; to change: changed; to remove: removed" in finding["message"]
        assert "kept" not in finding["message"] and "private" not in finding["message"]

    def test_a_ledger_missing_a_migration_is_a_finding_not_a_crash(self, store, engine):
        with engine.begin() as connection:
            connection.execute(text("ALTER TABLE dewey_schedules RENAME TO dewey_schedules_later"))
        try:
            report = store("ops", "doctor")
        finally:
            with engine.begin() as connection:
                connection.execute(
                    text("ALTER TABLE dewey_schedules_later RENAME TO dewey_schedules")
                )
        assert report["ok"] is False
        assert any("dewey_schedules" in finding["message"] for finding in report["findings"])

    def test_schedule_drift_late_ticks_and_unknown_tasks(self, store, engine):
        dewey.task("ops.work")(lambda: None)
        store("heartbeat")
        dewey.schedule("poll", task="ops.work", when=dewey.every(minutes=1))
        assert self.found(store) == {"schedule_drift"}
        store("ops", "schedules.apply")
        assert self.found(store) == set()
        with engine.begin() as connection:  # nothing has created ticks for ten minutes
            connection.execute(
                text(
                    "UPDATE dewey_schedules SET next_due_at = clock_timestamp() - interval '10 min'"
                )
            )
        assert self.found(store) == {"schedule_late"}
        store("ops", "schedules.pause")
        report = store("ops", "doctor")
        assert report["ok"] and [f["level"] for f in report["findings"]] == ["warning"]
        dewey.registry.clear()
        assert self.found(store) == {"schedule_unknown_task"}


def test_a_worker_claims_only_task_types_it_has_handlers_for(engine, django_db_blocker):
    known, unknown = create(engine, task_type="ops.known"), create(engine, task_type="ops.unknown")
    dewey.task("ops.known")(lambda: None)
    sync = NativeSQLAlchemyStorage(
        sessionmaker(engine),
        owner="a" * 36,
        timing=TIMING,
        queues=["default"],
        task_types=dewey.registry.task_types,
    )
    assert [assignment.task_id for assignment in sync.call("claim", 10)] == [known]
    assert status(engine, unknown) == "pending"  # left for a worker that knows it
    dewey.registry.clear()
    assert sync.call("claim", 10) == []
    # Without a filter (a pull-runner host with its own catalog) everything is claimable.
    unscoped = NativeSQLAlchemyStorage(
        sessionmaker(engine), owner="b" * 36, timing=TIMING, queues=["default"]
    )
    assert [assignment.task_id for assignment in unscoped.call("claim", 10)] == [unknown]


class TestAlerts:
    def run(self, store, engine, task_id: str, failure: BaseException | None):
        (assignment,) = store("claim", 1)
        invocation = store("start", assignment)
        assert store("finish", assignment, invocation["execution_token"], failure)
        return assignment

    def test_a_dead_letter_alerts_after_commit_and_a_retry_does_not(self, store, engine, heard):
        dewey.task("ops.work", max_attempts=2)(lambda: None)
        task_id = create(engine)
        self.run(store, engine, task_id, ValueError("first"))
        assert status(engine, task_id) == "failed" and heard == []
        set_fields(engine, task_id, scheduled_for=None)
        self.run(store, engine, task_id, ValueError("second"))
        assert heard == [
            {
                "event": "task_dead",
                "task_id": task_id,
                "task_type": "ops.work",
                "queue": "default",
                "attempts": 2,
                "reason": "handler_failure",
            }
        ]

    def test_a_recovered_execution_alerts(self, store, engine, heard):
        dewey.task("ops.work")(lambda: None)
        task_id = create(engine)
        (assignment,) = store("claim", 1)
        store("start", assignment)
        past = datetime.now(UTC) - timedelta(seconds=5)
        set_fields(engine, task_id, lease_expires_at=past)
        assert store("recover", 10) == [task_id]
        assert [(e["event"], e["reason"], e["status"]) for e in heard] == [
            ("execution_recovered", "interrupted", "failed")
        ]

    def test_missed_ticks_alert(self, store, engine, heard):
        dewey.schedule("poll", task="ops.work", when=dewey.every(minutes=1))
        store("ops", "schedules.apply")
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE dewey_schedules SET cursor_at = clock_timestamp() - interval '1 hour',"
                    " next_due_at = clock_timestamp() - interval '1 hour'"
                )
            )
        store("tick")
        (event,) = heard
        assert event["event"] == "schedule_tick_missed" and event["schedule"] == "poll"
        assert event["missed"] >= 58

    def test_an_operator_kill_alerts_once(self, store, engine, heard):
        task_id = create(engine)
        store("ops", "tasks.kill", {"task_id": task_id})
        store("ops", "tasks.kill", {"task_id": task_id})  # already dead: nothing new
        assert [(e["event"], e["reason"], e["task_id"]) for e in heard] == [
            ("task_dead", "killed", task_id)
        ]

    def test_a_failing_hook_never_affects_the_ledger(self, store, engine):
        def broken(event):
            raise RuntimeError("alerting is down")

        alerts.on_alert(broken)
        dewey.task("ops.work", max_attempts=1)(lambda: None)
        task_id = create(engine)
        self.run(store, engine, task_id, ValueError("only attempt"))
        assert status(engine, task_id) == "dead"


def test_the_creators_trace_reaches_the_invocation(store, engine):
    dewey.task("ops.work")(lambda: None)
    token = set_trace_context(
        {"request_id": "req-1", "traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01"}
    )
    try:
        create(engine, metadata={"source": "api"})
    finally:
        reset_trace_context(token)
    create(engine)  # created with no ambient trace
    (first, second) = sorted(
        (store("start", assignment) for assignment in store("claim", 2)),
        key=lambda invocation: len(invocation["trace"]),
        reverse=True,
    )
    assert first["trace"] == {
        "request_id": "req-1",
        "traceparent": "00-" + "a" * 32 + "-" + "b" * 16 + "-01",
    }
    assert second["trace"] == {} and first["execution_deadline_at"] is not None


def test_django_applies_task_database_budgets_around_the_handler(store):
    if store.mode != "django":
        pytest.skip("Django applies budgets to its own connection; SQLAlchemy exposes them")
    from django.db import connections

    from dewey.django.native import NativeDjangoStorage

    storage = NativeDjangoStorage(owner="c" * 36, timing=TIMING, queues=["default"], using="native")

    def setting(name: str) -> str:
        with connections["native"].cursor() as cursor:
            cursor.execute(f"SHOW {name}")
            return cursor.fetchone()[0]

    before = (setting("statement_timeout"), setting("lock_timeout"))
    context = TaskContext("t", "ops.work", 1, statement_timeout_s=2.5, lock_timeout_s=1)
    with storage.handler_scope(context):
        assert (setting("statement_timeout"), setting("lock_timeout")) == ("2500ms", "1s")
    assert (setting("statement_timeout"), setting("lock_timeout")) == before
    with storage.handler_scope(TaskContext("t", "ops.work", 1)):  # no budgets: untouched
        assert setting("statement_timeout") == before[0]


def test_doctor_alerts_whoever_calls_it_not_only_the_command_line(engine, heard):
    from dewey.sqlalchemy.ops import run

    with Session(engine) as session, session.begin():  # e.g. a health endpoint
        report = run(session, "doctor")
    assert report["ok"] is False
    assert [event["event"] for event in heard] == ["worker_heartbeat_stale"]


def test_an_operator_question_gets_longer_on_the_database_than_a_worker_round():
    from dewey._worker.budget import OPERATOR_WAIT_SECONDS, wait_seconds

    timing = Timing(30, 30, 2, 1, 0.1, 1, 60)
    assert wait_seconds("claim", timing) == timing.db_wait_seconds < OPERATOR_WAIT_SECONDS
    assert wait_seconds("ops", timing) == OPERATOR_WAIT_SECONDS
