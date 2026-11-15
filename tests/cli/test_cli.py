"""The ``dewey`` command against a real ledger, and one worker run end to end."""

import json
import os
import signal
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.orm import Session

import dewey
from dewey.cli import CHANGES, NOT_OK, main
from dewey.sqlalchemy.executor import create_task
from dewey.sqlalchemy.models import ScheduleEntryModel as Schedule
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.models import WorkerHeartbeatModel as Heartbeat
from tests._helpers import cli_app

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def clean_state(engine):
    yield
    dewey.schedules.clear()
    dewey.registry.clear()
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM dewey_schedules"))
        connection.execute(text("DELETE FROM dewey_dispatcher_heartbeats"))


def run(capsys, *argv: str) -> tuple[int, object]:
    """Run the command in-process; return its exit status and parsed JSON output."""
    code = main(["--app", cli_app.APP, "--json", *argv])
    out = capsys.readouterr().out
    return code, json.loads(out) if out.strip() else None


def create(engine, task_type="cli.touch", **values) -> str:
    with Session(engine) as session, session.begin():
        return create_task(session, task_type=task_type, **values).id


def set_status(engine, task_id: str, status: str) -> None:
    with engine.begin() as connection:
        connection.execute(update(Task).where(Task.id == task_id).values(status=status))


class TestTasksAndQueues:
    def test_queues_counts_every_status(self, engine, capsys):
        create(engine)
        set_status(engine, create(engine, queue="reports"), "dead")
        code, queues = run(capsys, "queues")
        assert code == 0
        by_name = {row["queue"]: row for row in queues}
        assert by_name["default"]["pending"] == 1 and by_name["default"]["dead"] == 0
        assert by_name["reports"]["dead"] == 1

    def test_listing_hides_arguments_and_errors_unless_revealed(self, engine, capsys):
        task_id = create(engine, kwargs={"marker": "/private/detail"})
        _, listed = run(capsys, "tasks", "list", "--type", "cli.touch")
        assert [row["id"] for row in listed] == [task_id]
        assert "kwargs" not in listed[0] and "error" not in listed[0]
        _, shown = run(capsys, "tasks", "show", task_id, "--reveal")
        assert shown["kwargs"] == {"marker": "/private/detail"} and shown["history"] == []

    def test_dead_letters_can_be_listed_and_retried(self, engine, capsys):
        dead, pending = create(engine, task_type="cli.fail"), create(engine)
        set_status(engine, dead, "dead")
        _, letters = run(capsys, "dlq", "list")
        assert [row["id"] for row in letters] == [dead]
        code, retried = run(capsys, "dlq", "retry", dead)
        assert (code, retried["status"], retried["attempts"]) == (0, "pending", 0)
        with pytest.raises(SystemExit):  # only dead letters go through the dlq
            run(capsys, "dlq", "retry", pending)

    def test_kill_and_unknown_ids(self, engine, capsys):
        task_id = create(engine)
        code, killed = run(capsys, "tasks", "kill", task_id)
        assert (code, killed["status"]) == (0, "dead")
        assert run(capsys, "tasks", "show", "no-such-task")[0] == NOT_OK

    def test_human_output_is_a_table(self, engine, capsys):
        create(engine)
        assert main(["--app", cli_app.APP, "queues"]) == 0
        header, row = capsys.readouterr().out.strip().splitlines()
        assert header.split()[:2] == ["QUEUE", "PENDING"] and row.split()[:2] == ["default", "1"]


class TestSchedules:
    def test_plan_gates_and_apply_converges(self, capsys):
        code, plan = run(capsys, "schedules", "plan")
        assert (code, plan["added"]) == (CHANGES, ["cli-poll"])  # a CI gate can fail on this
        assert run(capsys, "schedules", "apply")[1]["added"] == ["cli-poll"]
        code, plan = run(capsys, "schedules", "plan")
        assert (code, plan["unchanged"]) == (0, ["cli-poll"])
        _, listed = run(capsys, "schedules", "list")
        assert listed[0]["name"] == "cli-poll" and listed[0]["when"] == "every 0:01:00"

    def test_pause_for_a_while_and_resume(self, capsys):
        run(capsys, "schedules", "apply")
        assert run(capsys, "schedules", "pause", "--all", "--for", "10m") == (0, ["cli-poll"])
        paused = run(capsys, "schedules", "list")[1][0]
        assert paused["paused"] is True and paused["paused_until"] is not None
        assert run(capsys, "schedules", "resume", "cli-poll") == (0, ["cli-poll"])
        assert run(capsys, "schedules", "list")[1][0]["paused"] is False
        with pytest.raises(SystemExit):  # neither names nor --all
            run(capsys, "schedules", "pause")

    @pytest.mark.parametrize("duration", ["0m", "-1m", "10", "1.5h", "10 m", "forever"])
    def test_a_pause_length_that_is_not_a_duration_is_a_usage_error(self, capsys, duration):
        with pytest.raises(SystemExit) as usage:
            run(capsys, "schedules", "pause", "--all", "--for", duration)
        assert usage.value.code == 2 and "--for" in capsys.readouterr().err


def test_flags_are_accepted_after_the_command_too(capsys, monkeypatch):
    # The documented spelling: `dewey run --app ...`, `dewey schedules plan --json`.
    code = main(["schedules", "plan", "--app", cli_app.APP, "--json"])
    assert code == CHANGES and json.loads(capsys.readouterr().out)["added"] == ["cli-poll"]
    assert main(["queues", "--json", "--app", cli_app.APP]) == 0
    assert json.loads(capsys.readouterr().out) == []
    launched = []
    monkeypatch.setattr("dewey.worker.run_worker", lambda config: launched.append(config) or 0)
    assert main(["run", "--app", "app.after:create", "--concurrency", "2"]) == 0
    assert main(["--app", "app.before:create", "run"]) == 0  # and not reset by the command
    assert [config.factory for config in launched] == ["app.after:create", "app.before:create"]


def test_a_database_failure_is_one_line_not_a_traceback(capsys, monkeypatch):
    def unreachable(*args):
        raise RuntimeError("connection refused\nlong driver detail")

    monkeypatch.setattr("dewey.cli.operate", unreachable)
    assert main(["--app", cli_app.APP, "queues"]) == NOT_OK
    assert capsys.readouterr().err == "dewey: RuntimeError: connection refused\n"


class TestWorkerSettings:
    """``dewey run`` builds its configuration from a file, the environment and flags."""

    @pytest.fixture
    def launched(self, monkeypatch):
        configs = []
        monkeypatch.setattr("dewey.worker.run_worker", lambda config: configs.append(config) or 0)
        return configs

    def test_flags_override_the_file_and_the_environment_names_the_app(
        self, tmp_path, monkeypatch, launched
    ):
        settings = tmp_path / "worker.json"
        settings.write_text(
            json.dumps({"factory": "app.from_file:create", "concurrency": 2, "queues": ["files"]})
        )
        flags = ["--app", "app.from_flag:create", "--config", str(settings)]
        assert main([*flags, "run", "--queues", "fast, slow", "--concurrency", "3"]) == 0
        assert main(["--config", str(settings), "run"]) == 0
        monkeypatch.setenv("DEWEY_APP", "app.from_env:create")
        assert main(["run", "--auto-apply-schedules"]) == 0
        by_flag, by_file, by_env = launched
        assert (by_flag.factory, by_flag.queues, by_flag.concurrency) == (
            "app.from_flag:create",
            ("fast", "slow"),
            3,
        )
        assert (by_file.factory, by_file.queues, by_file.concurrency) == (
            "app.from_file:create",
            ("files",),
            2,
        )
        assert (by_env.factory, by_env.queues, by_env.auto_apply_schedules) == (
            "app.from_env:create",
            ("default",),
            True,
        )
        assert by_file.auto_apply_schedules is False

    @pytest.mark.parametrize(
        "argv",
        [
            [],
            ["run"],  # no application named anywhere
            ["--app", "not-a-factory", "run"],
            ["--app", "app:create", "run", "--concurrency", "0"],
            ["--app", "app:create", "run", "--queues", ","],
            ["--app", "app:create", "--config", "/nonexistent/worker.json", "run"],
            ["--app", "app:create", "launch"],
        ],
    )
    def test_a_worker_that_cannot_be_configured_is_never_launched(
        self, monkeypatch, capsys, launched, argv
    ):
        monkeypatch.delenv("DEWEY_APP", raising=False)
        with pytest.raises(SystemExit) as usage:
            main(argv)
        assert usage.value.code == 2 and launched == []
        assert capsys.readouterr().err.startswith("usage: dewey")

    def test_a_typo_in_the_settings_file_is_rejected(self, tmp_path, capsys, launched):
        settings = tmp_path / "worker.json"
        settings.write_text(json.dumps({"concurency": 4}))
        with pytest.raises(SystemExit) as usage:
            main(["--app", "app:create", "--config", str(settings), "run"])
        assert usage.value.code == 2 and launched == []


class TestDoctor:
    def test_not_ready_without_a_worker_or_applied_schedules(self, engine, capsys, monkeypatch):
        alerts = []
        monkeypatch.setattr("dewey.alerts._hook", alerts.append)
        create(engine, task_type="cli.unknown")
        code, report = run(capsys, "doctor")
        assert code == NOT_OK and report["ok"] is False
        found = {finding["id"].rsplit(".", 1)[-1] for finding in report["findings"]}
        assert {"no_worker", "unknown_task_type", "schedule_drift"} <= found
        assert [alert["event"] for alert in alerts] == ["worker_heartbeat_stale"]


def wait_for(condition, seconds: float = 30.0):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        value = condition()
        if value:
            return value
        time.sleep(0.1)
    pytest.fail("Timed out waiting for the worker")


def test_one_worker_process_runs_tasks_ticks_and_alerts(engine, tmp_path, capsys):
    marker, tick_marker, alerts = tmp_path / "ran", tmp_path / "ticked", tmp_path / "alerts"
    settings = tmp_path / "worker.json"
    settings.write_text(
        json.dumps(
            {
                "auto_apply_schedules": True,
                "alert_hook": "tests._helpers.cli_app:record_alert",
                "timing": {
                    "renewal_interval_seconds": 1,
                    "poll_interval_seconds": 0.2,
                    "db_wait_seconds": 1,
                    "termination_seconds": 2,
                },
            }
        )
    )
    env = dict(
        os.environ,
        DEWEY_CLI_TEST_ALERTS=str(alerts),
        DEWEY_CLI_TEST_TICK_MARKER=str(tick_marker),
        DEWEY_APP=cli_app.APP,
        # The worker is another process: it needs the source tree whether or not
        # the package is installed in this environment.
        PYTHONPATH=os.pathsep.join([str(ROOT / "src"), str(ROOT)]),
    )
    done = create(engine, kwargs={"marker": str(marker)})
    doomed = create(engine, task_type="cli.fail")
    stranger = create(engine, task_type="cli.unknown")
    worker = subprocess.Popen(
        [sys.executable, "-m", "dewey.cli", "--config", str(settings), "run"], cwd=ROOT, env=env
    )
    try:

        def status(task_id):
            with engine.connect() as connection:
                return connection.execute(select(Task.status).where(Task.id == task_id)).scalar()

        wait_for(lambda: status(done) == "completed" and status(doomed) == "dead")
        assert marker.read_text() == "cli.touch"
        # A dead letter reaches the alert hook, from the process that settled it.
        dead = wait_for(lambda: alerts.exists() and alerts.read_text())
        assert json.loads(dead.splitlines()[0])["event"] == "task_dead"
        # A type this worker has no handler for is left alone, not failed.
        assert status(stranger) == "pending"

        with engine.connect() as connection:
            beats = connection.execute(select(Heartbeat.queues, Heartbeat.backend)).all()
        assert beats == [(["default"], "sqlalchemy")]

        # The worker applied the declared schedule; make its next tick due now.
        with engine.begin() as connection:
            now = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            connection.execute(
                update(Schedule).values(
                    cursor_at=now - timedelta(minutes=3), next_due_at=now - timedelta(minutes=3)
                )
            )
        wait_for(tick_marker.exists)
        with engine.connect() as connection:
            keys = connection.execute(
                select(Task.idempotency_key).where(Task.idempotency_key.is_not(None))
            ).scalars()
            assert [key.startswith("schedule:cli-poll:") for key in keys] == [True]

        os.environ.update(DEWEY_CLI_TEST_TICK_MARKER=str(tick_marker))
        try:
            code, report = run(capsys, "doctor")
        finally:
            del os.environ["DEWEY_CLI_TEST_TICK_MARKER"]
        found = [f["id"].rsplit(".", 1)[-1] for f in report["findings"]]
        assert found == ["unknown_task_type"] and code == NOT_OK

        worker.send_signal(signal.SIGTERM)
        assert worker.wait(timeout=30) == 0
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait()
