"""The ``dewey`` command over each kind of application: SQLAlchemy, async SQLAlchemy, Django."""

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

import dewey
from dewey.cli import CHANGES, NOT_OK, main
from dewey.sqlalchemy.models import ScheduleEntryModel as Schedule
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.models import WorkerHeartbeatModel as Heartbeat
from tests._helpers import cli_app
from tests.native.test_storage import create, snapshot

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def clean():
    yield
    dewey.registry.clear()
    dewey.alerts.on_alert(None)


@pytest.fixture
def cli(store, monkeypatch, capsys):
    """Run the command in-process against the store's kind of application."""
    monkeypatch.setenv("DEWEY_CLI_TEST_BACKEND", store.mode)

    def run(*argv: str, json_output: bool = True):
        flags = ["--app", cli_app.APP, *(["--json"] if json_output else [])]
        code = main([*flags, *argv])
        out = capsys.readouterr().out
        return code, (json.loads(out) if out.strip() else None) if json_output else out

    return run


def set_fields(engine, task_id: str, **values) -> None:
    with engine.begin() as connection:
        connection.execute(update(Task).where(Task.id == task_id).values(**values))


def test_tasks_queues_and_dead_letters_round_trip(cli, engine):
    task_id = create(engine, task_type="cli.fail", args=["hidden-args"])
    set_fields(engine, task_id, status="dead", error="hidden-error")
    code, queues = cli("queues")
    assert (code, [(row["queue"], row["dead"]) for row in queues]) == (0, [("default", 1)])
    for group in ("tasks", "dlq"):
        code, listed = cli(group, "list", "--queue", "default", "--limit", "1")
        assert (code, [row["id"] for row in listed]) == (0, [task_id])
        assert "hidden-" not in json.dumps(listed)
    shown = cli("tasks", "show", task_id, "--reveal")[1]
    assert (shown["args"], shown["error"]) == (["hidden-args"], "hidden-error")
    assert cli("dlq", "retry", task_id)[1]["status"] == "pending"
    assert cli("tasks", "kill", task_id)[1]["status"] == "dead"
    code, retried = cli("tasks", "retry", task_id)
    assert (code, retried["status"], retried["attempts"], retried["has_error"]) == (
        0,
        "pending",
        0,
        False,
    )
    assert "dead" in [event["kind"] for event in cli("tasks", "show", task_id)[1]["history"]]


def test_schedules_are_planned_applied_paused_and_resumed(cli, engine):
    assert cli("schedules", "plan") == (
        CHANGES,
        {"added": ["cli-poll"], "changed": [], "removed": [], "unchanged": []},
    )
    assert cli("schedules", "apply", "--keep-pending")[1]["added"] == ["cli-poll"]
    assert cli("schedules", "plan") == (
        0,
        {"added": [], "changed": [], "removed": [], "unchanged": ["cli-poll"]},
    )
    assert cli("schedules", "pause", "cli-poll", "--for", "10m") == (0, ["cli-poll"])
    with engine.connect() as connection:
        remaining = connection.execute(
            select(Schedule.paused_until - text("clock_timestamp()"))
        ).scalar_one()
    assert timedelta(seconds=590) < remaining <= timedelta(minutes=10)
    code, (listed,) = cli("schedules", "list")
    assert (code, listed["paused"]) == (0, True)
    assert "unused" not in json.dumps(listed)  # the schedule's arguments are not listed
    assert cli("schedules", "resume", "--all") == (0, ["cli-poll"])
    assert cli("schedules", "pause", "--all") == (0, ["cli-poll"])
    assert cli("schedules", "resume", "cli-poll") == (0, ["cli-poll"])
    assert cli("schedules", "list")[1][0]["paused"] is False


def test_doctor_passes_once_a_worker_beats_and_schedules_are_applied(cli, store):
    cli("schedules", "apply")
    code, report = cli("doctor", "--queues", "default")
    assert (code, report["ok"]) == (NOT_OK, False)
    assert [finding["id"] for finding in report["findings"]] == ["dewey.doctor.no_worker"]
    assert cli("workers") == (0, [])
    store("heartbeat")
    assert cli("doctor") == (0, {"ok": True, "findings": []})
    code, (worker,) = cli("workers")
    assert (code, worker["instance_id"], worker["queues"]) == (0, store.owner, ["default"])


def test_tables_for_people_show_no_payloads(cli, engine):
    assert cli("queues", json_output=False) == (0, "(none)\n")
    task_id = create(engine, task_type="cli.touch", kwargs={"marker": "hidden-kwargs"})
    set_fields(engine, task_id, error="hidden-error")
    code, listing = cli("tasks", "list", json_output=False)
    assert code == 0 and listing.splitlines()[0].split()[:2] == ["ID", "TASK_TYPE"]
    assert task_id in listing and "hidden-" not in listing
    assert "hidden-" not in cli("tasks", "show", task_id, json_output=False)[1]
    code, plan = cli("schedules", "plan", json_output=False)
    assert (code, plan.splitlines()[0]) == (CHANGES, "add: cli-poll")
    cli("schedules", "apply")
    code, schedules = cli("schedules", "list", json_output=False)
    assert code == 0 and schedules.splitlines()[0].split()[:2] == ["NAME", "TASK_TYPE"]
    code, doctor = cli("doctor", json_output=False)
    assert code == NOT_OK and "ERROR dewey.doctor.no_worker" in doctor
    assert doctor.splitlines()[-1] == "Dewey doctor: not ready"


class TestDoctorAlerts:
    @pytest.fixture
    def heard(self):
        events = []
        dewey.on_alert(events.append)
        return events

    def test_a_missing_worker_alerts_and_the_report_stays_json(self, cli, heard):
        cli("schedules", "apply")
        code, report = cli("doctor")
        assert code == NOT_OK and report["ok"] is False
        (event,) = heard
        assert event == {
            "event": "worker_heartbeat_stale",
            "message": report["findings"][0]["message"],
        }

    def test_an_unserved_queue_alerts_only_when_it_is_asked_about(self, cli, store, engine, heard):
        cli("schedules", "apply")
        store("heartbeat")
        create(engine, task_type="cli.touch", queue="slow")
        assert cli("doctor", "--queues", "default")[0] == 0 and heard == []
        code, report = cli("doctor")
        assert code == NOT_OK
        assert [finding["id"] for finding in report["findings"]] == ["dewey.doctor.queue_unserved"]
        assert [event["event"] for event in heard] == ["worker_heartbeat_stale"]
        assert "'slow'" in heard[0]["message"]

    def test_findings_about_anything_else_do_not_alert(self, cli, store, engine, heard):
        store("heartbeat")
        create(engine, task_type="cli.unknown")
        code, report = cli("doctor")
        assert code == NOT_OK and heard == []
        assert sorted(finding["id"].rsplit(".", 1)[-1] for finding in report["findings"]) == [
            "schedule_drift",
            "unknown_task_type",
        ]

    def test_a_failing_hook_does_not_change_the_verdict(self, cli):
        calls = []

        def broken(event):
            calls.append(event["event"])
            raise RuntimeError("alerting is down")

        dewey.on_alert(broken)
        cli("schedules", "apply")
        code, report = cli("doctor")
        assert (code, report["ok"], calls) == (NOT_OK, False, ["worker_heartbeat_stale"])


def test_operator_commands_use_the_configured_alert_hook(cli, engine, tmp_path, monkeypatch):
    alerts, settings = tmp_path / "alerts", tmp_path / "worker.json"
    settings.write_text(json.dumps({"alert_hook": "tests._helpers.cli_app:record_alert"}))
    monkeypatch.setenv("DEWEY_CLI_TEST_ALERTS", str(alerts))
    task_id = create(engine, task_type="cli.touch")
    assert cli("--config", str(settings), "tasks", "kill", task_id)[1]["status"] == "dead"
    (event,) = map(json.loads, alerts.read_text().splitlines())
    assert (event["event"], event["task_id"], event["reason"]) == ("task_dead", task_id, "killed")


def wait_for(condition, worker, seconds: float = 40.0):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        assert worker.poll() is None, "the worker exited early"
        value = condition()
        if value:
            return value
        time.sleep(0.1)
    pytest.fail("Timed out waiting for the worker")


@pytest.mark.parametrize("mode", ["async", "django"])
def test_a_worker_of_each_kind_runs_tasks_ticks_and_alerts(engine, tmp_path, mode):
    marker, tick_marker, alerts = tmp_path / "ran", tmp_path / "ticked", tmp_path / "alerts"
    timing = {
        "renewal_interval_seconds": 1,
        "poll_interval_seconds": 0.2,
        "db_wait_seconds": 1,
        "termination_seconds": 2,
    }
    hook = "tests._helpers.cli_app:record_alert"
    env = dict(
        os.environ,
        DEWEY_CLI_TEST_ALERTS=str(alerts),
        DEWEY_CLI_TEST_TICK_MARKER=str(tick_marker),
        DEWEY_CLI_TEST_BACKEND=mode,
        PYTHONPATH=os.pathsep.join([str(tmp_path), str(ROOT)]),
    )
    if mode == "django":  # settings-driven: no factory, handlers found by autodiscovery
        (tmp_path / "worker_settings.py").write_text(
            "from tests.django_settings import *\n"
            "INSTALLED_APPS = [*INSTALLED_APPS, 'tests._helpers.cli_django_app']\n"
            f"DEWEY = {{'AUTO_APPLY_SCHEDULES': True, 'ALERT_HOOK': {hook!r}, "
            f"'TIMING': {timing!r}}}\n"
        )
        env["DJANGO_SETTINGS_MODULE"] = "worker_settings"
        command = [sys.executable, "-m", "django", "dewey", "run"]
    else:
        settings = tmp_path / "worker.json"
        settings.write_text(
            json.dumps({"auto_apply_schedules": True, "alert_hook": hook, "timing": timing})
        )
        command = [sys.executable, "-m", "dewey.cli", "--app", cli_app.APP]
        command += ["--config", str(settings), "run"]
    done = create(engine, task_type="cli.touch", kwargs={"marker": str(marker)})
    doomed = create(engine, task_type="cli.fail")
    stranger = create(engine, task_type="cli.unknown", priority=100)
    untouched = snapshot(engine, stranger)
    worker = subprocess.Popen(command, cwd=ROOT, env=env)
    try:

        def status(task_id):
            return snapshot(engine, task_id)["status"]

        def heard() -> list[dict]:
            return [json.loads(line) for line in alerts.read_text().splitlines()]

        wait_for(lambda: status(done) == "completed" and status(doomed) == "dead", worker)
        assert marker.read_text() == "cli.touch"
        wait_for(alerts.exists, worker)
        assert [(e["event"], e["task_id"]) for e in heard()] == [("task_dead", doomed)]
        assert snapshot(engine, stranger) == untouched  # no handler here: not even looked at

        with engine.connect() as connection:
            beats = connection.execute(select(Heartbeat.queues, Heartbeat.backend)).all()
        assert beats == [(["default"], "django" if mode == "django" else "sqlalchemy")]

        # The worker applied the declared schedule at start; let three ticks fall due.
        with engine.begin() as connection:
            now = connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            connection.execute(
                update(Schedule).values(
                    cursor_at=now - timedelta(minutes=3), next_due_at=now - timedelta(minutes=3)
                )
            )
        wait_for(tick_marker.exists, worker)
        missed = wait_for(lambda: [e for e in heard() if e["event"] != "task_dead"], worker)
        assert [(e["event"], e["schedule"]) for e in missed] == [
            ("schedule_tick_missed", "cli-poll")
        ]
        assert missed[0]["missed"] >= 2

        worker.send_signal(signal.SIGTERM)
        assert worker.wait(timeout=30) == 0
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait()
