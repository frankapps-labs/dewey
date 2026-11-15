"""Alerts describe what committed: never a provisional write, a rollback or a retry."""

import json
import logging

import pytest
from sqlalchemy import select, text, update

import dewey
from dewey import alerts
from dewey.sqlalchemy.models import ScheduleEntryModel as Schedule
from dewey.sqlalchemy.models import TaskEntryModel as Task
from tests.native.test_storage import create, expire_lease, snapshot


@pytest.fixture(autouse=True)
def clean():
    yield
    dewey.registry.clear()
    alerts.on_alert(None)


@pytest.fixture
def heard():
    events = []
    alerts.on_alert(events.append)
    return events


def started(store, engine, **values):
    """A task admitted for execution: its id, assignment and execution token."""
    task_id = create(engine, **values)
    (assignment,) = store("claim", 1)
    return task_id, assignment, store("start", assignment)["execution_token"]


def names(events) -> list[str]:
    return sorted(event["event"] for event in events)


class TestDeadLetters:
    def test_the_hook_sees_the_committed_row_and_hears_of_it_once(self, store, engine, heard):
        task_id, assignment, token = started(store, engine, max_attempts=1)
        seen = []
        alerts.on_alert(lambda event: seen.append(snapshot(engine, event["task_id"])["status"]))
        assert store("finish", assignment, token, ValueError("synthetic"))
        assert seen == ["dead"]  # read through another connection: it was committed
        alerts.on_alert(heard.append)
        assert not store("finish", assignment, token, ValueError("again"))
        assert heard == []

    def test_a_task_out_of_attempts_at_admission_is_a_dead_letter(self, store, engine, heard):
        task_id = create(engine, max_attempts=1)
        (assignment,) = store("claim", 1)
        with engine.begin() as connection:  # its budget was spent while it waited to start
            connection.execute(update(Task).where(Task.id == task_id).values(attempts=1))
        assert store("start", assignment) is None
        assert snapshot(engine, task_id)["status"] == "dead"
        assert [(e["event"], e["task_id"], e["reason"]) for e in heard] == [
            ("task_dead", task_id, "exhausted")
        ]

    @pytest.mark.parametrize("exhausted", [False, True])
    def test_an_execution_its_own_worker_had_to_stop_alerts_like_a_recovered_one(
        self, store, engine, heard, exhausted
    ):
        task_id, assignment, _ = started(store, engine, max_attempts=1 if exhausted else 2)
        assert store("abandon", assignment).durable_release  # the handler was stopped
        assert snapshot(engine, task_id)["status"] == ("dead" if exhausted else "failed")
        assert names(heard) == ["execution_recovered"] + ["task_dead"] * exhausted
        assert {event["task_id"] for event in heard} == {task_id}
        assert json.loads(json.dumps(heard)) == heard

    def test_giving_back_an_unstarted_assignment_alerts_nothing(self, store, engine, heard):
        task_id = create(engine)
        (assignment,) = store("claim", 1)
        assert store("abandon", assignment).durable_release
        assert snapshot(engine, task_id)["status"] == "pending" and heard == []


class TestRecovery:
    @pytest.mark.parametrize("exhausted", [False, True])
    def test_a_recovered_execution_alerts_once_it_is_committed(
        self, store, engine, heard, exhausted
    ):
        task_id, _, _ = started(store, engine, max_attempts=1 if exhausted else 2)
        expire_lease(engine, task_id)
        seen = []

        def committed(event):
            row = snapshot(engine, task_id)
            seen.append((event["event"], row["status"], row["dispatch_token"]))

        alerts.on_alert(committed)
        assert store("recover", 10) == [task_id]
        status = "dead" if exhausted else "failed"
        assert sorted(seen) == sorted(
            [("execution_recovered", status, None)] + [("task_dead", status, None)] * exhausted
        )
        assert store("recover", 10) == [] and len(seen) == 1 + exhausted

    def test_handing_back_an_unstarted_assignment_is_not_an_incident(self, store, engine, heard):
        task_id = create(engine)
        store("claim", 1)
        expire_lease(engine, task_id)
        assert store("recover", 10) == [task_id]
        assert snapshot(engine, task_id)["status"] == "pending" and heard == []


@pytest.mark.parametrize("operation", ["finish", "recover"])
def test_a_rolled_back_change_never_alerts_not_even_later(
    store, engine, heard, monkeypatch, operation
):
    task_id, assignment, token = started(store, engine, max_attempts=1)
    if operation == "recover":
        expire_lease(engine, task_id)
    before = snapshot(engine, task_id)
    target = store.storage if store.mode == "django" else store.storage.operations
    original = getattr(target, operation)

    def fails_after_writing(*args):
        assert original(*args)
        raise RuntimeError("the transaction is lost after the write")

    arguments = (assignment, token, ValueError("synthetic")) if operation == "finish" else (10,)
    with monkeypatch.context() as patch:
        patch.setattr(target, operation, fails_after_writing)
        with pytest.raises(RuntimeError, match="lost after the write"):
            store(operation, *arguments)
    assert snapshot(engine, task_id) == before and heard == []
    store("heartbeat")  # a later, unrelated commit must not deliver the lost one
    assert heard == []
    assert store(operation, *arguments)
    assert names(heard) == (
        ["task_dead"] if operation == "finish" else ["execution_recovered", "task_dead"]
    )


def test_a_failing_hook_is_logged_and_changes_nothing(store, engine, caplog):
    calls = []

    def broken(event):
        calls.append(event["event"])
        raise RuntimeError("alerting is down")

    alerts.on_alert(broken)
    task_id, assignment, token = started(store, engine, max_attempts=1)
    with caplog.at_level(logging.ERROR, logger="dewey.alerts"):
        assert store("finish", assignment, token, ValueError("synthetic"))
    assert snapshot(engine, task_id)["status"] == "dead" and calls == ["task_dead"]
    assert "Alert hook failed for task_dead" in caplog.text


def test_a_coroutine_function_cannot_be_the_hook():
    async def hook(event):
        pass

    with pytest.raises(TypeError):  # nothing would await it: the alert would vanish
        alerts.on_alert(hook)


class TestMissedTicks:
    def overdue(self, store, engine, minutes: int = 5, **declaration) -> None:
        dewey.schedule("poll", task="inventory.poll", when=dewey.every(minutes=1), **declaration)
        store("ops", "schedules.apply")
        self.rewind(engine, minutes)

    def rewind(self, engine, minutes: int) -> None:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE dewey_schedules SET cursor_at = clock_timestamp() - :age * interval"
                    " '1 min', next_due_at = clock_timestamp() - :age * interval '1 min'"
                ),
                {"age": minutes},
            )

    def missed_count(self, engine) -> int:
        with engine.connect() as connection:
            return connection.execute(select(Schedule.missed_count)).scalar_one()

    def test_missed_ticks_alert_once_their_count_is_committed(self, store, engine):
        self.overdue(store, engine)
        seen = []
        alerts.on_alert(lambda event: seen.append((event, self.missed_count(engine))))
        (report,) = store("tick")
        ((event, committed),) = seen
        assert event == {"event": "schedule_tick_missed", "schedule": "poll", "missed": committed}
        assert committed == report["missed"] >= 3
        assert store("tick") == [] and len(seen) == 1

    def test_a_pass_that_rolls_back_alerts_nothing(self, store, engine, heard, monkeypatch):
        self.overdue(store, engine)
        target = store.storage if store.mode == "django" else store.storage.operations
        original = target.tick

        def fails_after_writing(*args):
            assert original(*args)[0]["missed"] > 0
            raise RuntimeError("the transaction is lost after the write")

        with monkeypatch.context() as patch:
            patch.setattr(target, "tick", fails_after_writing)
            with pytest.raises(RuntimeError, match="lost after the write"):
                store("tick")
        assert heard == [] and self.missed_count(engine) == 0
        store("tick")
        assert names(heard) == ["schedule_tick_missed"]

    def test_a_tick_skipped_for_overlap_is_not_a_missed_tick(self, store, engine, heard):
        self.overdue(store, engine, minutes=1)
        assert len(store("tick")[0]["created"]) == 1
        with engine.begin() as connection:  # one tick falls due while that one is unfinished
            connection.execute(
                text(
                    "UPDATE dewey_schedules SET next_due_at = clock_timestamp(), cursor_at ="
                    " date_trunc('minute', clock_timestamp()) - interval '1 second'"
                )
            )
        (report,) = store("tick")
        assert (report["skipped"], report["missed"]) == (1, 0) and heard == []
