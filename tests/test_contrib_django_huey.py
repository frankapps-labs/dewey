"""Tests for dewey.contrib.django_huey — first-class Django/Huey wiring.

Uses ``MemoryHuey(immediate=True)`` injected as ``settings.HUEY`` before the
first import of ``huey.contrib.djhuey``: no Redis and no consumer process is
needed to prove the contract. The contrib module does its work at import time,
so most tests exercise it through ``importlib.reload`` under
``override_settings``.
"""

# ruff: noqa: E402 — django.setup() and settings.HUEY must precede djhuey imports

import importlib
import os
import subprocess
import sys
from pathlib import Path

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings
from django.utils.module_loading import import_string
from huey import MemoryHuey

# huey.contrib.djhuey builds its HUEY singleton from settings at import time.
# The test settings module defines no HUEY, and djhuey's fallback demands Redis,
# so inject an in-memory immediate-mode instance before that first import.
settings.HUEY = MemoryHuey("dewey-contrib-tests", immediate=True)

import huey.contrib.djhuey as djhuey

from dewey.contrib import django_huey


@pytest.fixture
def restore_module():
    """Re-execute the contrib module under the original settings after a test reloads it."""
    yield
    importlib.reload(django_huey)


@pytest.fixture
def recorded_process(monkeypatch):
    """Stub the Django executor and record what the worker forwards to it."""
    calls: list[tuple[str, str | None]] = []

    def fake_process_task(task_id, using=None):
        calls.append((task_id, using))
        return True

    monkeypatch.setattr("dewey.django.executor.process_task", fake_process_task)
    return calls


def _dewey_registrations() -> list[str]:
    registry = djhuey.HUEY._registry._registry
    return [name for name in registry if name.endswith(".dewey_process_task")]


class TestImportString:
    def test_dispatch_resolves_as_a_module_level_callable(self):
        """DEWEY["DISPATCH"] must name something import_string can reach."""
        assert import_string("dewey.contrib.django_huey.dispatch") is django_huey.dispatch

    def test_adapter_is_module_level_and_uses_the_djhuey_singleton(self):
        assert import_string("dewey.contrib.django_huey.adapter") is django_huey.adapter
        assert django_huey.adapter._huey is djhuey.HUEY


class TestRegistration:
    def test_import_registered_exactly_one_huey_task(self):
        assert len(_dewey_registrations()) == 1

    def test_reload_adopts_the_existing_registration(self, restore_module):
        """Django's autoreloader re-executes modules; a second registration would raise."""
        adapter_before = django_huey.adapter
        module = importlib.reload(django_huey)
        assert len(_dewey_registrations()) == 1
        assert module.adapter is adapter_before

    def test_dispatch_still_works_after_reload(self, restore_module, recorded_process):
        importlib.reload(django_huey)
        django_huey.dispatch("task-after-reload")
        assert recorded_process == [("task-after-reload", None)]

    def test_huey_retries_are_disabled(self):
        """Dewey owns retry scheduling — Huey retrying underneath would re-run work."""
        [name] = _dewey_registrations()
        assert djhuey.HUEY._registry._registry[name].default_retries == 0
        assert django_huey.adapter._process_task.settings["default_retries"] == 0


class TestWorkerAlias:
    def test_dispatch_hands_the_id_to_the_worker_with_the_default_alias(self, recorded_process):
        django_huey.dispatch("task-123")
        assert recorded_process == [("task-123", None)]

    def test_worker_database_is_forwarded_to_process_task(self, restore_module, recorded_process):
        with override_settings(DEWEY={"WORKER_DATABASE": "second"}):
            module = importlib.reload(django_huey)
            assert module.WORKER_DATABASE == "second"
            module.dispatch("task-456")
        assert recorded_process == [("task-456", "second")]

    def test_dispatcher_database_does_not_leak_into_the_worker(
        self, restore_module, recorded_process
    ):
        with override_settings(DEWEY={"DATABASE": "second"}):
            module = importlib.reload(django_huey)
            assert module.WORKER_DATABASE is None
            module.dispatch("task-789")
        assert recorded_process == [("task-789", None)]


class TestCloseDb:
    def test_the_worker_is_wrapped_around_the_process_function(self):
        """djhuey.close_db uses functools.wraps, so the wrapping is observable."""
        assert django_huey._worker.__wrapped__ is django_huey._process

    def test_close_db_manages_connections_around_the_worker(self, monkeypatch):
        events: list[str] = []
        monkeypatch.setattr(djhuey, "close_old_connections", lambda: events.append("close"))
        monkeypatch.setattr(
            "dewey.django.executor.process_task",
            lambda task_id, using=None: events.append("work") or True,
        )

        # close_db deliberately skips connection management in immediate mode; a
        # real consumer is not immediate, so flip the supported toggle briefly.
        djhuey.HUEY.immediate = False
        try:
            result = django_huey._worker("task-close-db")
        finally:
            djhuey.HUEY.immediate = True

        assert result is True
        assert events == ["close", "work", "close"]


class TestSettingsErrors:
    def test_missing_huey_setting_is_actionable(self, restore_module):
        with override_settings(HUEY=None):
            with pytest.raises(ImproperlyConfigured, match="needs a HUEY setting"):
                importlib.reload(django_huey)

    def test_unknown_worker_database_alias_names_the_alias_and_the_options(self, restore_module):
        with override_settings(DEWEY={"WORKER_DATABASE": "reporting"}):
            with pytest.raises(ImproperlyConfigured) as excinfo:
                importlib.reload(django_huey)
        message = str(excinfo.value)
        assert "WORKER_DATABASE" in message
        assert "'reporting'" in message
        # The fix is in the message: the aliases that actually exist.
        assert "default" in message
        assert "second" in message

    def test_non_string_worker_database_is_rejected(self, restore_module):
        with override_settings(DEWEY={"WORKER_DATABASE": 5}):
            with pytest.raises(ImproperlyConfigured, match="alias string or None"):
                importlib.reload(django_huey)

    def test_a_typoed_dewey_key_fails_at_import(self, restore_module):
        with override_settings(DEWEY={"WORKER_DATABSE": "default"}):  # typo on purpose
            with pytest.raises(ImproperlyConfigured, match="WORKER_DATABSE"):
                importlib.reload(django_huey)

    def test_missing_huey_points_at_the_install_extra(self, restore_module):
        hidden = {
            name: sys.modules.pop(name)
            for name in list(sys.modules)
            if name == "huey" or name.startswith("huey.")
        }
        sys.modules["huey"] = None  # any import of huey now raises ModuleNotFoundError
        try:
            with pytest.raises(ImproperlyConfigured, match=r"dewey\[huey\]"):
                importlib.reload(django_huey)
        finally:
            del sys.modules["huey"]
            sys.modules.update(hidden)

    def test_broken_djhuey_configuration_reports_the_setting_to_fix(self, restore_module):
        # Force djhuey to rebuild its singleton from settings naming a bogus backend.
        saved = sys.modules.pop("huey.contrib.djhuey")
        try:
            with override_settings(HUEY={"huey_class": "not_a_real.Backend"}):
                with pytest.raises(ImproperlyConfigured, match="HUEY setting"):
                    importlib.reload(django_huey)
        finally:
            sys.modules["huey.contrib.djhuey"] = saved


class TestOptionalImports:
    def test_core_dewey_and_contrib_import_without_django_or_huey(self):
        """Core dewey must stay framework-free; contrib must not wire itself."""
        src = Path(django_huey.__file__).resolve().parents[2]
        code = (
            "import sys; "
            "import dewey, dewey.contrib; "
            "assert 'django' not in sys.modules, 'dewey imported Django'; "
            "assert 'huey' not in sys.modules, 'dewey imported Huey'; "
            "assert 'dewey.contrib.django_huey' not in sys.modules, 'contrib wired itself'"
        )
        result = subprocess.run(
            [sys.executable, "-c", code],
            env={**os.environ, "PYTHONPATH": str(src)},
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
