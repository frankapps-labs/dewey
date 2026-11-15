"""Every failed attempt is a log record: WARNING while it retries, ERROR once it is dead.

The worker already recorded failures on the row and told the alert hook about dead
letters; nothing reached the log pipeline, so an error tracker fed by logging never
saw a failing handler. These tests hold the records' level, exception and fields on
every storage, in the task process and in the worker parent that recovers a lost
execution, and across the spawn and fork process boundaries.
"""

import asyncio
import importlib
import json
import logging
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import update

import dewey
from dewey._worker.runtime import LedgerRuntime
from dewey.core.execution import configure_failure_details
from dewey.errors import NonRetryableError
from dewey.sqlalchemy.models import TaskEntryModel as Task
from tests._helpers import native_runtime
from tests.native.test_execution_budget import producer as producer
from tests.native.test_runner import drive, runner_for
from tests.native.test_storage import create, expire_lease, snapshot

FIELDS = ("task_id", "task_type", "queue", "attempt", "max_attempts", "outcome", "reason")
JSON_NATIVE = (str, int, float, bool, type(None))


def assert_json_native(record):
    """A JSON log handler serialises the arguments and the fields as they are.

    One that meets an object it has no encoder for may go looking for one
    (Logfire imports pandas and numpy to find a match), which on a small CPU
    takes longer than the worker waits for a task process to report back.
    """
    for value in record.args:
        assert isinstance(value, JSON_NATIVE), f"argument {value!r} is not JSON-native"
    for name in (*FIELDS, "retry_at"):
        if hasattr(record, name):
            assert isinstance(getattr(record, name), JSON_NATIVE), name
    assert record.exc_info is None or isinstance(record.exc_info[1], BaseException)


@pytest.fixture(autouse=True)
def clean():
    yield
    dewey.registry.clear()


@pytest.fixture
def full_details():
    """The opt-in: message and traceback in the record, for a scrubbing pipeline."""
    configure_failure_details("full")
    yield
    configure_failure_details("type")


@pytest.fixture
def runtime(store):
    storage = SimpleNamespace(call=store)
    if hasattr(store.storage, "handler_scope"):
        storage.handler_scope = store.storage.handler_scope
    runtime = LedgerRuntime(storage, close_resources=lambda: None)
    yield runtime
    runtime.close()


@pytest.fixture
def worker_log(caplog):
    caplog.set_level(logging.INFO, logger="dewey.worker")
    return caplog


def failures(caplog):
    return [r for r in caplog.records if r.name == "dewey.worker" and r.levelno >= logging.WARNING]


def fields(record):
    return {name: getattr(record, name) for name in FIELDS}


def make_due(engine, task_id):
    with engine.begin() as connection:
        connection.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(scheduled_for=datetime.now(UTC) - timedelta(seconds=1))
        )


def register(store, name, body, **policy):
    def handler():
        body()

    async def async_handler():
        await asyncio.sleep(0)
        body()

    dewey.task(name, **policy)(async_handler if store.mode == "async" else handler)


class TestHandlerFailures:
    def test_by_default_the_record_names_the_exceptions_type_only(
        self, store, producer, engine, runtime, worker_log
    ):
        """An exception's message may quote handler input; the log pipeline is not
        the ledger's reveal boundary. The type is enough to count and group by."""

        def fail():
            raise ValueError("customer 42 at sql 'SELECT ... WHERE email = %s'")

        register(store, "work.fails", fail, max_attempts=1)
        task_id = producer(task_type="work.fails")
        runtime.execute(store("claim", 1)[0])
        (record,) = failures(worker_log)
        assert record.levelno == logging.ERROR and record.task_id == task_id
        assert record.exc_info is None
        assert "error=ValueError" in record.getMessage()
        assert "customer 42" not in record.getMessage() and "SELECT" not in record.getMessage()
        assert_json_native(record)
        assert snapshot(engine, task_id)["status"] == "dead"  # the row is unchanged by the setting

    def test_a_retried_attempt_warns_and_the_last_one_errors_with_the_exception(
        self, store, producer, engine, runtime, worker_log, full_details
    ):
        def fail():
            raise ValueError("synthetic handler failure")

        register(store, "work.fails", fail, max_attempts=2, backoff=dewey.Constant(0))
        task_id = producer(task_type="work.fails")
        runtime.execute(store("claim", 1)[0])
        (first,) = failures(worker_log)
        assert first.levelno == logging.WARNING
        assert isinstance(first.exc_info[1], ValueError) and "synthetic" in first.getMessage()
        assert_json_native(first)
        assert fields(first) == {
            "task_id": task_id,
            "task_type": "work.fails",
            "queue": "default",
            "attempt": 1,
            "max_attempts": 2,
            "outcome": "failed",
            "reason": "retry",
        }
        assert first.retry_at == snapshot(engine, task_id)["scheduled_for"].isoformat()

        make_due(engine, task_id)
        runtime.execute(store("claim", 1)[0])
        _, last = failures(worker_log)
        assert last.levelno == logging.ERROR
        assert isinstance(last.exc_info[1], ValueError)
        assert_json_native(last)
        assert fields(last) == {
            "task_id": task_id,
            "task_type": "work.fails",
            "queue": "default",
            "attempt": 2,
            "max_attempts": 2,
            "outcome": "dead",
            "reason": "attempts exhausted",
        }
        assert snapshot(engine, task_id)["status"] == "dead"

    def test_a_non_retryable_failure_is_an_error_at_once(
        self, store, producer, engine, runtime, worker_log, full_details
    ):
        def fail():
            raise NonRetryableError("synthetic malformed input")

        register(store, "work.malformed", fail, max_attempts=5)
        task_id = producer(task_type="work.malformed")
        runtime.execute(store("claim", 1)[0])
        (record,) = failures(worker_log)
        assert record.levelno == logging.ERROR
        assert isinstance(record.exc_info[1], NonRetryableError)
        assert (record.outcome, record.reason, record.attempt) == ("dead", "non-retryable", 1)
        assert snapshot(engine, task_id)["status"] == "dead"

    def test_a_completed_attempt_logs_no_failure(self, store, producer, runtime, worker_log):
        register(store, "work.fine", lambda: None)
        producer(task_type="work.fine")
        runtime.execute(store("claim", 1)[0])
        assert failures(worker_log) == []


class TestRecoveredExecutions:
    """The worker parent has no exception, only the row: it still logs the attempt."""

    @pytest.mark.parametrize(
        "max_attempts,deadline,level,reason",
        [
            (2, False, logging.WARNING, "interrupted"),
            (1, False, logging.ERROR, "interrupted"),
            (2, True, logging.WARNING, "timed_out"),
            (1, True, logging.ERROR, "timed_out"),
        ],
    )
    def test_recovery_logs_the_lost_attempt(
        self, store, engine, worker_log, max_attempts, deadline, level, reason
    ):
        task_id = create(engine, task_type="work.lost", max_attempts=max_attempts)
        (assignment,) = store("claim", 1)
        assert store("start", assignment) is not None
        expire_lease(engine, task_id, deadline=deadline)
        assert store("recover", 1) == [task_id]
        (record,) = failures(worker_log)
        assert record.levelno == level and record.exc_info is None
        assert_json_native(record)
        assert fields(record) == {
            "task_id": task_id,
            "task_type": "work.lost",
            "queue": "default",
            "attempt": 1,
            "max_attempts": max_attempts,
            "outcome": "dead" if level == logging.ERROR else "failed",
            "reason": reason,
        }

    def test_a_stopped_execution_logs_like_a_recovered_one(self, store, engine, worker_log):
        task_id = create(engine, task_type="work.stopped", max_attempts=3)
        (assignment,) = store("claim", 1)
        assert store("start", assignment) is not None
        store("abandon", assignment)
        (record,) = failures(worker_log)
        assert (record.levelno, record.reason, record.task_id) == (
            logging.WARNING,
            "interrupted",
            task_id,
        )

    def test_an_unstarted_assignment_handed_back_is_not_a_failure(self, store, engine, worker_log):
        task_id = create(engine, task_type="work.unstarted")
        store("claim", 1)
        expire_lease(engine, task_id)
        assert store("recover", 1) == [task_id]
        assert failures(worker_log) == []

    def test_an_operator_kill_is_not_logged_as_a_failure(self, store, engine, worker_log):
        task_id = create(engine, task_type="work.killed")
        (assignment,) = store("claim", 1)
        invocation = store("start", assignment)
        with engine.begin() as connection:
            connection.execute(update(Task).where(Task.id == task_id).values(cancel_requested=True))
        assert store("finish", assignment, invocation["execution_token"], ValueError("late"))
        assert snapshot(engine, task_id)["status"] == "dead"
        assert failures(worker_log) == []


class TestOneRecordPerSettlement:
    """A settlement the database budget rolls back is logged by the retry that commits.

    The budget is wall-clock over the whole ledger call and checked once more
    after the operation. A log handler that is slow on a small CPU (an error
    tracker formatting a traceback) used to run inside that window: the record
    went out, the budget check then rolled the transaction back, and the task
    process's retry logged the same outcome again. The alert outbox was already
    discarded on rollback; the log now goes through it too.
    """

    @pytest.fixture
    def slow_handler(self):
        class Slow(logging.Handler):
            def emit(self, record):
                time.sleep(0.3)

        handler = Slow()
        logging.getLogger("dewey.worker").addHandler(handler)
        yield
        logging.getLogger("dewey.worker").removeHandler(handler)

    @pytest.fixture
    def tight_finish_budget(self, monkeypatch):
        from dewey.django import native as django_native
        from dewey.sqlalchemy import native as sqlalchemy_native

        def budget(operation, timing):
            return 0.2 if operation == "finish" else timing.db_wait_seconds

        monkeypatch.setattr(sqlalchemy_native, "wait_seconds", budget)
        monkeypatch.setattr(django_native, "wait_seconds", budget)

    @pytest.fixture
    def alerts(self):
        heard = []
        dewey.on_alert(heard.append)
        yield heard
        dewey.on_alert(None)

    def test_a_rolled_back_settlement_is_logged_once_by_the_retry(
        self,
        store,
        producer,
        engine,
        runtime,
        worker_log,
        slow_handler,
        tight_finish_budget,
        alerts,
    ):
        def fail():
            raise ValueError("synthetic handler failure")

        register(store, "work.fails", fail, max_attempts=1)
        task_id = producer(task_type="work.fails")
        runtime.execute(store("claim", 1)[0])
        assert snapshot(engine, task_id)["status"] == "dead"
        records = failures(worker_log)
        assert [(r.levelno, r.task_id) for r in records] == [(logging.ERROR, task_id)]
        assert [a["event"] for a in alerts] == ["task_dead"]


@pytest.mark.parametrize("start_method", ["spawn", "fork"])
def test_task_processes_log_failures_with_the_exception(
    store, engine, tmp_path, monkeypatch, start_method
):
    """The record is made in the task process, whichever way it was started."""
    if "native.synthetic.fail" not in dewey.registry.task_types():
        importlib.reload(native_runtime)
    if store.mode == "django" and start_method == "fork":
        monkeypatch.setenv("DEWEY_NATIVE_TEST_ALIAS", "native")  # the parent's alias
    monkeypatch.setenv("DEWEY_NATIVE_TEST_FAILURE_DETAILS", "full")
    log = tmp_path / "worker.log"
    monkeypatch.setenv("DEWEY_NATIVE_TEST_LOG", str(log))
    task_id = create(engine, task_type="native.synthetic.fail", max_attempts=2)
    runner = runner_for(store, monkeypatch, start_method=start_method)
    pids = set()
    try:
        runner.start()
        pids.update(runner.pool.worker_pids)
        drive(runner, engine, task_id, "failed")
        pids.update(runner.pool.worker_pids)
        make_due(engine, task_id)
        drive(runner, engine, task_id, "dead")
        pids.update(runner.pool.worker_pids)
        until = time.monotonic() + 5
        while runner.assignments and time.monotonic() < until:
            runner.step()
            time.sleep(0.005)
    finally:
        runner.close()
    records = [json.loads(line) for line in log.read_text().splitlines()]
    assert [(r["level"], r["attempt"]) for r in records] == [("WARNING", 1), ("ERROR", 2)]
    for record in records:
        assert record["task_id"] == task_id and record["task_type"] == "native.synthetic.fail"
        assert record["queue"] == "default" and record["max_attempts"] == 2
        assert "ValueError: synthetic handler failure" in record["exception"]
        assert record["pid"] in pids  # made in a task process, not the parent
