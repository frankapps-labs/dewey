"""``manage.py dewey`` and the settings-driven Django worker application."""

# ruff: noqa: E402 — django.setup() must run before model imports
import json
import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings

import dewey
from dewey.django import create_task
from dewey.django.checks import check_dewey_configuration
from dewey.django.conf import get_settings
from dewey.django.management.commands.dewey import Command
from dewey.django.worker import FACTORY, worker_settings


@pytest.fixture(autouse=True)
def clean_registries():
    yield
    dewey.schedules.clear()
    dewey.registry.clear()


def manage(capsys, *argv: str) -> tuple[int, object]:
    with pytest.raises(SystemExit) as exit_info:
        Command().run_from_argv(["manage.py", "dewey", "--json", *argv])
    out = capsys.readouterr().out
    return exit_info.value.code, json.loads(out) if out.strip() else None


@pytest.mark.django_db(transaction=True)
class TestManageCommand:
    def test_operator_commands_need_no_factory(self, capsys):
        task = create_task(task_type="reports.daily", queue="reports")
        code, queues = manage(capsys, "queues")
        assert code == 0 and [(row["queue"], row["pending"]) for row in queues] == [("reports", 1)]
        code, shown = manage(capsys, "tasks", "show", task.id)
        assert (code, shown["task_type"]) == (0, "reports.daily")

    def test_schedules_are_planned_and_applied_from_project_code(self, capsys):
        dewey.schedule("nightly", task="clean.up", when=dewey.cron("0 2 * * *"))
        assert manage(capsys, "schedules", "plan")[0] == 2
        assert manage(capsys, "schedules", "apply")[1]["added"] == ["nightly"]
        code, listed = manage(capsys, "schedules", "list")
        assert code == 0 and listed[0]["when"] == "0 2 * * *"

    def test_doctor_reports_a_missing_worker(self, capsys):
        code, report = manage(capsys, "doctor")
        assert code == 1 and report["findings"][0]["id"] == "dewey.doctor.no_worker"


class TestRun:
    """``manage.py dewey run`` needs no factory: the project's settings are the configuration."""

    @pytest.fixture
    def launched(self, monkeypatch):
        configs = []
        monkeypatch.setattr("dewey.worker.run_worker", lambda config: configs.append(config) or 0)
        return configs

    def run(self, *argv: str) -> int:
        with pytest.raises(SystemExit) as exit_info:
            Command().run_from_argv(["manage.py", "dewey", "run", *argv])
        return exit_info.value.code

    def test_the_worker_is_configured_from_settings(self, launched):
        with override_settings(
            DEWEY={
                "QUEUES": ["fast"],
                "CONCURRENCY": 3,
                "AUTO_APPLY_SCHEDULES": True,
                "ALERT_HOOK": "ops.alerts:page",
            }
        ):
            assert self.run() == 0
            assert self.run("--queues", "slow", "--concurrency", "1") == 0  # flags still win
        by_settings, by_flags = launched
        assert by_settings.factory == FACTORY
        assert (by_settings.queues, by_settings.concurrency) == (("fast",), 3)
        assert (by_settings.auto_apply_schedules, by_settings.alert_hook) == (
            True,
            "ops.alerts:page",
        )
        assert (by_flags.queues, by_flags.concurrency) == (("slow",), 1)

    @pytest.mark.parametrize(
        "settings",
        [
            {"CONCURRENCY": 0},
            {"QUEUES": []},
            {"AUTO_APPLY_SCHEDULES": "yes"},
            {"ALERT_HOOK": "no-colon"},
            {"TIMING": {"execution_lease_seconds": 1}},
            {"TIMING": {"patience": 1}},
        ],
    )
    def test_settings_a_worker_cannot_run_with_stop_it_before_launch(
        self, launched, capsys, settings
    ):
        with override_settings(DEWEY=settings):
            assert self.run() == 2
        assert launched == [] and "manage.py dewey: error" in capsys.readouterr().err

    @pytest.mark.parametrize("autodiscover", ["tasks", "jobs", None])
    def test_the_factory_sets_django_up_and_imports_each_apps_task_module(
        self, monkeypatch, autodiscover
    ):
        from dewey.django.worker import create_application
        from dewey.worker import WorkerConfig, WorkerContext

        calls = []
        monkeypatch.setattr("django.setup", lambda: calls.append("setup"))
        monkeypatch.setattr(
            "django.utils.module_loading.autodiscover_modules",
            lambda name: calls.append(("discover", name)),
        )
        context = WorkerContext(WorkerConfig(FACTORY, queues=("fast",)), "o" * 36, "parent")
        with override_settings(DEWEY={"AUTODISCOVER": autodiscover}):
            application = create_application(context)
        assert calls == ["setup"] + ([("discover", autodiscover)] if autodiscover else [])
        assert (application.storage.using, application.storage.queues) == ("default", ("fast",))
        assert application.storage.task_types == dewey.registry.task_types


@pytest.mark.django_db(transaction=True)
def test_the_command_can_be_called_from_code():
    from io import StringIO

    from django.core.management import call_command

    output = StringIO()
    call_command("dewey", "queues", "--json", stdout=output)
    assert json.loads(output.getvalue()) == []


class TestSettings:
    def test_worker_settings_come_from_the_project(self):
        with override_settings(
            DEWEY={
                "QUEUES": ["default", "reports"],
                "CONCURRENCY": 4,
                "ALERT_HOOK": "ops.alerts:page",
                "TIMING": {"task_timeout_seconds": 600},
            }
        ):
            assert worker_settings() == {
                "factory": FACTORY,
                "queues": ("default", "reports"),
                "concurrency": 4,
                "alert_hook": "ops.alerts:page",
                "timing": {"task_timeout_seconds": 600},
            }

    def test_only_configured_keys_are_forwarded_so_defaults_cannot_drift(self):
        from dataclasses import fields

        from dewey.django.conf import DEFAULTS, WORKER_SETTINGS
        from dewey.worker import WorkerConfig

        with override_settings(DEWEY={}):
            assert worker_settings() == {"factory": FACTORY}  # the worker's defaults apply
        with override_settings(DEWEY={"START_METHOD": "fork", "STARTUP_SECONDS": 90}):
            assert worker_settings() == {
                "factory": FACTORY,
                "start_method": "fork",
                "startup_seconds": 90,
            }
        worker_defaults = {field.name: field.default for field in fields(WorkerConfig)}
        for key, name in WORKER_SETTINGS.items():
            if name != "timing":
                assert DEFAULTS[key] == worker_defaults[name], key

    def test_defaults_and_unknown_keys(self):
        assert get_settings()["AUTODISCOVER"] == "tasks"
        with override_settings(DEWEY={"DISPATCH": "a.b.c"}), pytest.raises(ImproperlyConfigured):
            get_settings()  # a setting from before 0.6 is a typo now, not silently ignored


class TestSystemChecks:
    def ids(self) -> list[str]:
        return [finding.id for finding in check_dewey_configuration()]

    def test_a_default_project_is_clean(self):
        assert self.ids() == []

    def test_unknown_alias_bad_queues_and_split_databases(self):
        with override_settings(DEWEY={"DATABASE": "missing"}):
            assert "dewey.E002" in self.ids()
        with override_settings(DEWEY={"QUEUES": "default"}):
            assert self.ids() == ["dewey.E004"]
        with override_settings(DEWEY={"DATABASE": "second"}):
            # Producers write to "default": the worker would never see their tasks.
            assert self.ids() == ["dewey.W002"]
        with override_settings(DEWEY=["not", "a", "dict"]):
            assert self.ids() == ["dewey.E001"]
