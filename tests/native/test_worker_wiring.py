"""What the worker parent does around tasks: heartbeat, claim scope, schedules."""

import logging
from types import SimpleNamespace

import pytest
from sqlalchemy import select, text

import dewey
from dewey import alerts
from dewey._worker.runner import NativeRunner
from dewey.sqlalchemy.models import ScheduleEntryModel as Schedule
from dewey.sqlalchemy.models import WorkerHeartbeatModel as Heartbeat
from tests._helpers.native_runtime import TIMING
from tests.native.test_storage import create, snapshot


@pytest.fixture(autouse=True)
def clean():
    yield
    dewey.registry.clear()
    alerts.on_alert(None)


def heartbeat(engine, owner: str) -> dict:
    with engine.connect() as connection:
        return dict(
            connection.execute(select(Heartbeat.__table__).where(Heartbeat.instance_id == owner))
            .mappings()
            .one()
        )


def schedule_names(engine) -> list[str]:
    with engine.connect() as connection:
        return list(connection.execute(select(Schedule.name)).scalars())


def make_due(engine, minutes: int = 3) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE dewey_schedules SET cursor_at = clock_timestamp() - :age * interval"
                " '1 min', next_due_at = clock_timestamp() - :age * interval '1 min'"
            ),
            {"age": minutes},
        )


@pytest.fixture
def parent(store):
    """The worker parent's loop over the store, with no task processes behind it."""
    runners = []

    def build(**options) -> NativeRunner:
        pool = SimpleNamespace(
            concurrency=1,
            capacity=0,
            task_timeout_seconds=TIMING.task_timeout_seconds,
            termination_timeout_seconds=0.2,
            start=lambda: None,
            poll=lambda: [],
            shutdown=lambda **kwargs: None,
        )
        runners.append(NativeRunner(pool, SimpleNamespace(call=store), timing=TIMING, **options))
        return runners[-1]

    yield build
    for runner in runners:
        runner.close()


def test_a_heartbeat_is_refreshed_in_place(store, engine):
    store("heartbeat")
    first = heartbeat(engine, store.owner)
    store("heartbeat")
    second = heartbeat(engine, store.owner)
    assert second["started_at"] == first["started_at"]
    assert second["last_seen_at"] > first["last_seen_at"]
    assert second["queues"] == ["default"]
    assert second["backend"] == ("django" if store.mode == "django" else "sqlalchemy")


def test_a_type_without_a_handler_here_is_left_exactly_as_it_was(store, engine, monkeypatch):
    unknown = create(engine, task_type="work.unknown", priority=100)  # first in line
    known = create(engine, task_type="work.known")
    before = snapshot(engine, unknown)
    target = store.storage if store.mode == "django" else store.storage.operations
    monkeypatch.setattr(target, "task_types", dewey.registry.task_types)
    assert store("claim", 10) == []  # nothing registered: nothing claimed
    dewey.task("work.known")(lambda: None)
    assert [assignment.task_id for assignment in store("claim", 10)] == [known]
    assert snapshot(engine, unknown) == before


class TestSchedulesAtStartup:
    def test_drift_is_logged_and_nothing_is_written(self, store, engine, parent, caplog):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        with caplog.at_level(logging.INFO, logger="dewey.worker"):
            parent().start()
        (record,) = caplog.records
        assert record.levelno == logging.WARNING and "dewey schedules apply" in record.message
        assert '"added": ["poll"]' in record.message
        assert schedule_names(engine) == []

    def test_auto_apply_writes_the_declared_schedules(self, store, engine, parent, caplog):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        with caplog.at_level(logging.INFO, logger="dewey.worker"):
            parent(auto_apply_schedules=True).start()
        (record,) = caplog.records
        assert record.levelno == logging.INFO and "Applied schedules" in record.message
        assert schedule_names(engine) == ["poll"]

    def test_schedules_in_step_with_the_code_are_not_mentioned(self, store, parent, caplog):
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
        store("ops", "schedules.apply")
        with caplog.at_level(logging.INFO, logger="dewey.worker"):
            parent().start()
        assert caplog.records == []


def test_maintenance_beats_and_creates_due_ticks_at_its_own_cadence(store, engine, parent):
    heard = []
    alerts.on_alert(heard.append)
    dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1))
    store("ops", "schedules.apply")
    make_due(engine)
    runner = parent()
    runner.start()
    assert runner.step() == 0
    beat = heartbeat(engine, store.owner)
    with engine.connect() as connection:
        keys = connection.execute(
            text("SELECT idempotency_key FROM task_entries WHERE task_type = 'inventory.poll'")
        ).scalars()
        assert [key.startswith("schedule:poll:") for key in keys] == [True]
    assert [event["event"] for event in heard] == ["schedule_tick_missed"]
    # Maintenance runs once per renewal interval, not on every poll.
    make_due(engine)
    assert runner.step() == 0
    assert heartbeat(engine, store.owner) == beat and len(heard) == 1
    runner._renew_at = 0  # the interval has passed
    assert runner.step() == 0
    assert heartbeat(engine, store.owner)["last_seen_at"] > beat["last_seen_at"]
    assert len(heard) == 2


def test_one_maintenance_round_creates_ticks_for_more_schedules_than_one_pass_takes(
    store, engine, parent
):
    # A pass locks at most 20 due schedules. A worker with many must not fall behind.
    for number in range(70):
        dewey.schedule(f"poll-{number:02}", task="inventory.poll", when=dewey.every(minutes=1))
    store("ops", "schedules.apply")
    make_due(engine)
    runner = parent()
    runner.start()
    runner.step()
    with engine.connect() as connection:
        created = connection.execute(
            text("SELECT count(*) FROM task_entries WHERE task_type = 'inventory.poll'")
        ).scalar_one()
        still_due = connection.execute(
            text("SELECT count(*) FROM dewey_schedules WHERE next_due_at <= clock_timestamp()")
        ).scalar_one()
    assert (created, still_due) == (70, 0)
