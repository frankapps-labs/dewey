"""Schedules against real Postgres, identically on SQLAlchemy sync, async and Django."""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

import dewey
from dewey._worker.contracts import Timing
from dewey._worker.loop import ChildLoop
from dewey.scheduling import CATCH_UP_LIMIT, SCAN_LIMIT, tick_key
from dewey.sqlalchemy.executor import create_task
from dewey.sqlalchemy.models import ScheduleEntryModel as Schedule
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.native import AsyncNativeSQLAlchemyStorage, NativeSQLAlchemyStorage
from dewey.sqlalchemy.schedules import SQLAlchemyScheduleStore

MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)


def apply(store, **arguments):
    return store("ops", "schedules.apply", arguments)


def db_now(engine) -> datetime:
    with engine.connect() as connection:
        return connection.execute(text("SELECT clock_timestamp()")).scalar_one()


def rewind(engine, name: str, cursor: datetime) -> None:
    """Pretend nothing has created ticks for ``name`` since ``cursor``."""
    with engine.begin() as connection:
        connection.execute(
            update(Schedule)
            .where(Schedule.name == name)
            .values(cursor_at=cursor, next_due_at=cursor)
        )


def schedule_row(engine, name: str) -> dict:
    with engine.connect() as connection:
        return dict(
            connection.execute(select(Schedule.__table__).where(Schedule.name == name))
            .mappings()
            .one()
        )


def tasks(engine, task_type: str) -> list[dict]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                select(Task.__table__)
                .where(Task.task_type == task_type)
                .order_by(Task.idempotency_key)
            ).mappings()
        ]


def set_status(engine, task_id: str, status: str) -> None:
    with engine.begin() as connection:
        connection.execute(update(Task).where(Task.id == task_id).values(status=status))


def top_of_hour(moment: datetime) -> datetime:
    return moment.replace(minute=0, second=0, microsecond=0)


def top_of_minute(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0)


def store_class(store):
    """The schedule store behind the storage under test."""
    if store.mode == "django":
        from dewey.django.schedules import DjangoScheduleStore

        return DjangoScheduleStore
    return SQLAlchemyScheduleStore


def allow(store, monkeypatch, *, seconds: float) -> None:
    """Give the store's operations a different total database budget."""
    target = store.storage if store.mode == "django" else store.storage.operations
    monkeypatch.setattr(target, "timing", replace(target.timing, db_wait_seconds=seconds))


def in_parallel(store, engine, operation: str, *args, callers: int = 4) -> list:
    """Run one operation from several independent storages of the store's kind at once."""
    barrier = Barrier(callers)
    timing = Timing(30, 30, 2, 5, 0.1, 1, 60)

    def call(number: int):
        owner, bridge, async_engine = f"caller-{number}", ChildLoop(), None
        if store.mode == "django":
            from django.db import connections

            from dewey.django.native import NativeDjangoStorage

            storage = NativeDjangoStorage(
                owner=owner, timing=timing, queues=["default"], using="native"
            )
        elif store.mode == "async":
            async_engine = create_async_engine(os.environ["DEWEY_TEST_DATABASE_URL_ASYNC"])
            storage = AsyncNativeSQLAlchemyStorage(
                async_sessionmaker(async_engine), owner=owner, timing=timing, queues=["default"]
            )
        else:
            storage = NativeSQLAlchemyStorage(
                sessionmaker(engine), owner=owner, timing=timing, queues=["default"]
            )
        try:
            bridge.invoke(storage.call, "readiness")  # connected before the race starts
            barrier.wait(timeout=10)
            return bridge.invoke(storage.call, operation, *args)
        finally:
            if async_engine is not None:
                bridge.invoke(async_engine.dispose)
            bridge.close()
            if store.mode == "django":
                connections["native"].close()  # this thread's connection

    with ThreadPoolExecutor(callers) as pool:
        return list(pool.map(call, range(callers)))


class TestPlanAndApply:
    def test_apply_adds_then_reports_nothing_to_do(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1), queue="polls")
        assert store("ops", "schedules.plan") == {
            "added": ["poll"],
            "changed": [],
            "removed": [],
            "unchanged": [],
        }
        assert apply(store)["added"] == ["poll"]
        assert store("ops", "schedules.plan")["unchanged"] == ["poll"]
        row = schedule_row(engine, "poll")
        # A new schedule starts from now: nothing is backfilled.
        assert row["next_due_at"] - row["cursor_at"] <= MINUTE
        assert row["definition"]["queue"] == "polls"
        assert store("tick") == []

    def test_a_changed_definition_keeps_cursor_and_pause(self, store, engine):
        dewey.schedule("nightly", task="clean.up", when=dewey.cron("0 2 * * *"))
        apply(store)
        store("ops", "schedules.pause", {"names": ["nightly"]})
        before = schedule_row(engine, "nightly")
        dewey.schedules.clear()
        dewey.schedule("nightly", task="clean.up", when=dewey.cron("0 3 * * *"))
        assert apply(store)["changed"] == ["nightly"]
        after = schedule_row(engine, "nightly")
        assert after["cursor_at"] == before["cursor_at"] and after["paused"] is True
        assert after["definition"]["when"]["expression"] == "0 3 * * *"
        assert after["next_due_at"].astimezone(UTC).hour == 3

    @pytest.mark.parametrize("keep_pending", [False, True])
    def test_removal_expires_unstarted_ticks_unless_kept(self, store, engine, keep_pending):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        assert len(store("tick")[0]["created"]) == 1
        dewey.schedules.clear()
        assert store("ops", "schedules.plan")["removed"] == [{"name": "poll", "unstarted_ticks": 1}]
        apply(store, keep_pending=keep_pending)
        (tick,) = tasks(engine, "inventory.poll")
        assert tick["status"] == ("pending" if keep_pending else "expired")
        assert store("ops", "schedules.list") == []

    def test_ticks_after_a_change_carry_the_new_definition(self, store, engine):
        dewey.schedule(
            "poll", task="inventory.poll", when=dewey.every(minutes=1), kwargs={"version": 1}
        )
        apply(store)
        dewey.schedules.clear()
        dewey.schedule(
            "poll", task="inventory.poll", when=dewey.every(minutes=1), kwargs={"version": 2}
        )
        assert store("ops", "schedules.plan")["changed"] == ["poll"]  # reading changes nothing
        assert schedule_row(engine, "poll")["definition"]["kwargs"] == {"version": 1}
        apply(store)
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        store("tick")
        assert [row["kwargs"] for row in tasks(engine, "inventory.poll")] == [{"version": 2}]

    def test_a_tick_uses_the_policy_resolved_when_it_was_applied(self, store, engine):
        try:
            dewey.task("inventory.poll", queue="polls", priority=8, max_attempts=3)(lambda: None)
            dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
            apply(store)
        finally:
            dewey.registry.clear()  # the process creating ticks may not have the handler's code
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        store("tick")
        (row,) = tasks(engine, "inventory.poll")
        assert (row["queue"], row["priority"], row["max_attempts"]) == ("polls", 8, 3)

    def test_removal_touches_only_that_schedules_waiting_ticks(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1), catch_up="all")
        dewey.schedule("poll:eu", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        rewind(engine, "poll", top_of_minute(db_now(engine)) - 4 * MINUTE)
        rewind(engine, "poll:eu", db_now(engine) - 2 * MINUTE)
        created = {report["schedule"]: report["created"] for report in store("tick")}
        running, finished, *waiting = created["poll"]
        assert len(waiting) >= 2
        set_status(engine, running, "processing")
        set_status(engine, finished, "completed")
        with Session(engine) as session, session.begin():
            manual = create_task(session, task_type="inventory.poll").id
        dewey.schedules.unregister("poll")
        assert store("ops", "schedules.plan")["removed"] == [
            {"name": "poll", "unstarted_ticks": len(waiting)}
        ]
        apply(store)
        statuses = {row["id"]: row["status"] for row in tasks(engine, "inventory.poll")}
        assert {statuses[task_id] for task_id in waiting} == {"expired"}
        # Started or finished ticks, another schedule's ticks and ordinary tasks are not its to end.
        assert (statuses[running], statuses[finished]) == ("processing", "completed")
        assert (statuses[created["poll:eu"][0]], statuses[manual]) == ("pending", "pending")
        assert [row["name"] for row in store("ops", "schedules.list")] == ["poll:eu"]

    def test_removal_ends_a_tick_a_worker_has_claimed_but_not_started(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        rewind(engine, "poll", db_now(engine) - 2 * MINUTE)
        ((tick,),) = [report["created"] for report in store("tick")]
        (assignment,) = store("claim", 1)
        dewey.schedules.unregister("poll")
        assert store("ops", "schedules.plan")["removed"] == [{"name": "poll", "unstarted_ticks": 1}]
        apply(store)
        (row,) = tasks(engine, "inventory.poll")
        assert (row["id"], row["status"], row["dispatch_token"]) == (tick, "expired", None)
        # The worker that held it finds it is no longer its to run.
        assert store("start", assignment) is None
        assert tasks(engine, "inventory.poll")[0]["status"] == "expired"

    def test_removal_also_ends_ticks_created_before_the_task_type_changed(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        store("tick")
        dewey.schedules.clear()
        dewey.schedule("poll", task="inventory.sync", when=dewey.every(minutes=1))
        apply(store)
        dewey.schedules.clear()
        assert store("ops", "schedules.plan")["removed"] == [{"name": "poll", "unstarted_ticks": 1}]
        apply(store)
        assert [row["status"] for row in tasks(engine, "inventory.poll")] == ["expired"]


class TestConcurrency:
    def test_concurrent_callers_create_exactly_one_row_per_tick(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        reports = [report for result in in_parallel(store, engine, "tick") for report in result]
        assert [len(report["created"]) for report in reports] == [1]
        assert len(tasks(engine, "inventory.poll")) == 1

    def test_concurrent_applies_add_a_schedule_once(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        plans = in_parallel(store, engine, "ops", "schedules.apply")
        assert sorted(len(plan["added"]) for plan in plans) == [0, 0, 0, 1]
        assert sorted(len(plan["unchanged"]) for plan in plans) == [0, 1, 1, 1]
        assert [row["name"] for row in store("ops", "schedules.list")] == ["poll"]


class TestTicks:
    @pytest.mark.parametrize(("catch_up", "kept"), [("once", 1), ("all", 100)])
    def test_a_two_hour_outage_is_accounted_for_exactly(
        self, store, engine, monkeypatch, catch_up, kept
    ):
        dewey.schedule(
            "poll", task="inventory.poll", when=dewey.every(minutes=1), catch_up=catch_up
        )
        apply(store)
        allow(store, monkeypatch, seconds=10)  # a hundred rows, whatever the machine's speed
        down = top_of_minute(db_now(engine)) - 120 * MINUTE
        rewind(engine, "poll", down)
        reports = []
        while batch := store("tick"):  # a large catch-up takes several bounded passes
            reports += batch
        assert json.loads(json.dumps(reports)) == reports
        report = {
            "created": [task_id for each in reports for task_id in each["created"]],
            "missed": sum(each["missed"] for each in reports),
        }
        state = schedule_row(engine, "poll")
        owed = (state["cursor_at"] - down) // MINUTE
        assert owed in (120, 121)  # 121 when a minute turned between the rewind and the tick
        # A tick that comes due while the backlog is worked off is not part of it.
        arrived = len(report["created"]) - kept
        assert arrived in (0, 1)
        kept += arrived
        assert report["missed"] == state["missed_count"] == owed - kept
        rows = tasks(engine, "inventory.poll")
        # What is kept is the most recent: the latest tick down to the cap.
        assert [row["idempotency_key"] for row in rows] == [
            tick_key("poll", state["cursor_at"] - n * MINUTE) for n in reversed(range(kept))
        ]
        if catch_up == "all":
            assert {row["expires_at"] for row in rows} == {None}
        assert store("tick") == []

    def test_a_backlog_longer_than_one_pass_is_worked_off_over_several(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        down = top_of_minute(db_now(engine)) - 2 * SCAN_LIMIT * MINUTE - 30 * MINUTE
        rewind(engine, "poll", down)
        step = SCAN_LIMIT - CATCH_UP_LIMIT  # each pass holds back the newest it may still run
        passes = [store("tick")[0] for _ in range(3)]
        assert [(len(report["created"]), report["missed"]) for report in passes[:2]] == [
            (0, step),
            (0, step),
        ]
        owed = 2 * SCAN_LIMIT + 30
        assert len(passes[2]["created"]) == 1 and passes[2]["missed"] in (
            owed - 2 * step - 2,
            owed - 2 * step - 1,
        )
        state = schedule_row(engine, "poll")
        assert state["missed_count"] == (state["cursor_at"] - down) // MINUTE - 1
        assert store("tick") == []

    def test_a_catch_up_too_slow_for_one_pass_is_continued_not_repeated(
        self, store, engine, monkeypatch
    ):
        dewey.schedule("billing", task="billing.run", when=dewey.every(minutes=1), catch_up="all")
        apply(store)
        rewind(engine, "billing", top_of_minute(db_now(engine)) - 100 * MINUTE)
        create_tick = store_class(store).create_tick

        def slow(self, *args):
            time.sleep(0.01)  # a database a few milliseconds away, several statements a row
            return create_tick(self, *args)

        monkeypatch.setattr(store_class(store), "create_tick", slow)
        created = 0
        for _ in range(10):
            created += sum(len(report["created"]) for report in store("tick"))
        assert created >= 100

    def test_a_failed_pass_leaves_no_rows_and_no_progress(self, store, engine, monkeypatch):
        dewey.schedule("billing", task="billing.run", when=dewey.every(minutes=1), catch_up="all")
        apply(store)
        rewind(engine, "billing", top_of_minute(db_now(engine)) - 3 * MINUTE)
        before = schedule_row(engine, "billing")
        create_tick, calls = store_class(store).create_tick, []

        def interrupted(self, *args):
            calls.append(args)
            if len(calls) == 2:
                raise RuntimeError("insertion interrupted")
            return create_tick(self, *args)

        with monkeypatch.context() as patch:
            patch.setattr(store_class(store), "create_tick", interrupted)
            with pytest.raises(RuntimeError, match="interrupted"):
                store("tick")
        assert tasks(engine, "billing.run") == [] and schedule_row(engine, "billing") == before
        assert len(store("tick")[0]["created"]) >= 3

    def test_a_pass_handles_a_bounded_number_of_schedules_most_overdue_first(self, store, engine):
        for name in ("first", "second"):
            dewey.schedule(name, task=f"work.{name}", when=dewey.every(minutes=1))
        apply(store)
        rewind(engine, "second", db_now(engine) - 9 * MINUTE)
        rewind(engine, "first", db_now(engine) - 3 * MINUTE)
        assert [report["schedule"] for report in store("tick", 1)] == ["second"]
        assert [report["schedule"] for report in store("tick", 1)] == ["first"]
        assert store("tick", 1) == []

    def test_an_unstarted_tick_expires_instead_of_running_late(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        (late,) = store("tick")[0]["created"]
        with engine.begin() as connection:  # nobody claimed it before the next tick came due
            connection.execute(
                update(Task).where(Task.id == late).values(expires_at=db_now(engine) - MINUTE)
            )
        assert store("claim", 5) == []
        (row,) = tasks(engine, "inventory.poll")
        assert (row["status"], row["latest_outcome"], row["attempts"]) == ("expired", "expired", 0)

    def test_a_tick_is_an_ordinary_task_row_created_once(self, store, engine):
        dewey.schedule(
            "poll",
            task="inventory.poll",
            when=dewey.every(minutes=1),
            args=("eu",),
            kwargs={"full": False},
            queue="polls",
            priority=3,
        )
        apply(store)
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        (report,) = store("tick")
        assert report["schedule"] == "poll" and len(report["created"]) == 1
        (row,) = tasks(engine, "inventory.poll")
        assert row["id"] == report["created"][0]
        assert (row["args"], row["kwargs"], row["queue"], row["priority"]) == (
            ["eu"],
            {"full": False},
            "polls",
            3,
        )
        tick = datetime.fromisoformat(row["metadata"]["tick"])
        assert row["idempotency_key"] == f"schedule:poll:{tick:%Y-%m-%dT%H:%M:%SZ}"
        assert row["expires_at"] == tick + MINUTE  # an unstarted poll is not worth running late
        state = schedule_row(engine, "poll")
        assert state["cursor_at"] == tick == state["last_fired_at"]
        assert state["last_task_id"] == row["id"]
        # Asking again creates nothing: the cursor moved in the same transaction.
        assert store("tick") == []
        assert len(tasks(engine, "inventory.poll")) == 1

    def test_a_two_hour_outage_under_each_policy(self, store, engine):
        dewey.schedule("poll", task="t.once", when=dewey.every(minutes=1), catch_up="once")
        dewey.schedule("weekly", task="t.skip", when=dewey.every(days=7), catch_up="skip")
        dewey.schedule("billing", task="t.all", when=dewey.cron("0 * * * *"), catch_up="all")
        apply(store)
        now = db_now(engine)
        rewind(engine, "poll", now - 2 * HOUR)
        rewind(engine, "weekly", now - timedelta(days=15))
        rewind(engine, "billing", top_of_hour(now) - 2 * HOUR)
        reports = {report["schedule"]: report for report in store("tick")}
        # once: the latest tick runs, the rest are on record as missed.
        assert len(reports["poll"]["created"]) == 1 and reports["poll"]["missed"] >= 118
        # skip: a stale tick is recorded, never run.
        assert reports["weekly"]["created"] == [] and reports["weekly"]["missed"] in (2, 3)
        assert tasks(engine, "t.skip") == []
        # all: every owed period runs, oldest first, and none of them expires.
        owed = tasks(engine, "t.all")
        assert len(owed) == len(reports["billing"]["created"]) == 2
        assert [row["expires_at"] for row in owed] == [None, None]
        assert schedule_row(engine, "weekly")["missed_count"] == reports["weekly"]["missed"]
        assert store("tick") == []

    def test_overlap_skip_waits_for_the_previous_tick(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        (first,) = store("tick")[0]["created"]
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        (report,) = store("tick")
        assert (report["created"], report["skipped"]) == ([], 1)
        assert schedule_row(engine, "poll")["skipped_count"] == 1
        set_status(engine, first, "completed")
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        # The same instant again: its unique key already holds that tick.
        assert store("tick")[0]["created"] == [] and len(tasks(engine, "inventory.poll")) == 1
        with engine.begin() as connection:  # as if the finished one were an earlier tick
            connection.execute(
                update(Task).where(Task.id == first).values(idempotency_key="schedule:poll:earlier")
            )
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        assert len(store("tick")[0]["created"]) == 1
        assert len(tasks(engine, "inventory.poll")) == 2

    def test_a_tick_waiting_past_its_expiry_does_not_block_the_next(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        (stale,) = store("tick")[0]["created"]
        with engine.begin() as connection:  # it failed, and its retry is past the expiry
            connection.execute(
                update(Task)
                .where(Task.id == stale)
                .values(
                    status="failed",
                    idempotency_key="schedule:poll:earlier",
                    expires_at=db_now(engine) - MINUTE,
                )
            )
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        (report,) = store("tick")
        assert (len(report["created"]), report["skipped"]) == (1, 0)

    def test_a_locked_schedule_is_skipped_not_waited_for(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        with Session(engine) as other, other.begin():
            other.execute(select(Schedule).with_for_update())
            assert store("tick") == []  # another process is on it
        assert len(store("tick")[0]["created"]) == 1


def test_concurrent_processes_create_exactly_one_row_per_tick(engine):
    dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
    storages = [
        NativeSQLAlchemyStorage(
            sessionmaker(engine),
            owner=f"owner-{n}",
            timing=Timing(30, 30, 2, 5, 0.1, 1, 60),
            queues=["default"],
        )
        for n in range(8)
    ]
    storages[0].call("ops", "schedules.apply")
    for _ in range(5):
        rewind(engine, "poll", db_now(engine) - 3 * MINUTE)
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM task_entries"))
        barrier = Barrier(len(storages))

        def tick(storage, barrier=barrier):
            barrier.wait()
            return storage.call("tick")

        with ThreadPoolExecutor(len(storages)) as pool:
            reports = [report for result in pool.map(tick, storages) for report in result]
        assert len(reports) == 1 and len(reports[0]["created"]) == 1
        assert len(tasks(engine, "inventory.poll")) == 1


class TestPause:
    def test_a_paused_schedule_creates_nothing_until_resumed(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        assert store("ops", "schedules.pause") == ["poll"]
        rewind(engine, "poll", db_now(engine) - 5 * MINUTE)
        assert store("tick") == []
        assert store("ops", "schedules.list")[0]["paused"] is True
        assert store("ops", "schedules.resume", {"names": ["poll"]}) == ["poll"]
        # Ticks that came due meanwhile follow the catch-up policy.
        (report,) = store("tick")
        assert len(report["created"]) == 1 and report["missed"] >= 3

    def test_a_timed_pause_ends_by_itself(self, store, engine):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        apply(store)
        store("ops", "schedules.pause", {"names": ["poll"], "seconds": 600})
        row = schedule_row(engine, "poll")
        assert row["paused"] and row["paused_until"] > db_now(engine) + 9 * MINUTE
        with engine.begin() as connection:  # ten minutes later
            connection.execute(
                update(Schedule).values(
                    paused_until=db_now(engine) - MINUTE,
                    cursor_at=db_now(engine) - 3 * MINUTE,
                    next_due_at=db_now(engine) - 3 * MINUTE,
                )
            )
        assert store("ops", "schedules.list")[0]["paused"] is False
        assert len(store("tick")[0]["created"]) == 1
        assert schedule_row(engine, "poll")["paused"] is False

    def test_naming_an_unknown_schedule_pauses_none_of_them(self, store, engine):
        for name in ("first", "second"):
            dewey.schedule(name, task=f"work.{name}", when=dewey.every(minutes=1))
        apply(store)
        with pytest.raises(ValueError, match="No applied schedule named nope"):
            store("ops", "schedules.pause", {"names": ["first", "nope"]})
        assert [row["paused"] for row in store("ops", "schedules.list")] == [False, False]
        assert store("ops", "schedules.pause", {"names": ["first", "second"]}) == [
            "first",
            "second",
        ]
        assert store("ops", "schedules.resume", {"names": ["second"]}) == ["second"]
        assert [row["paused"] for row in store("ops", "schedules.list")] == [True, False]

    def test_unknown_names_and_bad_durations_are_rejected(self, store):
        with pytest.raises(ValueError, match="No applied schedule"):
            store("ops", "schedules.pause", {"names": ["nope"]})
        with pytest.raises(ValueError, match="positive"):
            store("ops", "schedules.pause", {"seconds": 0})
