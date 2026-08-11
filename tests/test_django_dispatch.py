"""Django dispatch backend, settings contract, and the dispatcher command."""

from __future__ import annotations

# ruff: noqa: E402 — django.setup() must run before model imports
import os
from datetime import UTC, datetime, timedelta

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.core.exceptions import ImproperlyConfigured
from django.core.management import call_command
from django.test import override_settings

import dewey
from dewey.core.states import TaskStatus
from dewey.dispatcher import Dispatcher
from dewey.django.conf import get_dispatch_fn, get_settings
from dewey.django.dispatch import DjangoDispatchBackend
from dewey.django.executor import create_task, process_task
from dewey.django.models import TaskEntry


@pytest.fixture(autouse=True)
def clean_registry():
    """pytest-django truncates tables between transactional tests; the process-local
    policy registry is ours to reset."""
    yield
    dewey.registry.clear()


@pytest.fixture
def backend():
    return DjangoDispatchBackend()


@pytest.mark.django_db(transaction=True)
class TestClaim:
    def test_claims_a_pending_task_as_dispatching(self, backend):
        task = create_task(task_type="t")

        assert backend.claim(10) == [task.id]

        row = TaskEntry.objects.get(id=task.id)
        assert row.status == TaskStatus.DISPATCHING.value
        assert row.dispatching_at is not None

    def test_a_claimed_task_is_not_claimed_again(self, backend):
        create_task(task_type="t")
        assert len(backend.claim(10)) == 1
        assert backend.claim(10) == []

    def test_batch_size_is_respected(self, backend):
        for _ in range(5):
            create_task(task_type="t")
        assert len(backend.claim(2)) == 2

    def test_scheduled_work_is_invisible_until_due(self, backend):
        create_task(task_type="t", scheduled_for=datetime.now(UTC) + timedelta(minutes=5))
        assert backend.claim(10) == []

    def test_higher_priority_goes_first(self, backend):
        low = create_task(task_type="t", priority=0)
        high = create_task(task_type="t", priority=100)
        assert backend.claim(2) == [high.id, low.id]

    def test_a_due_retry_is_not_starved_by_newer_immediate_work(self, backend):
        """Immediate and due scheduled work share one effective due-time order.

        A retry that came due a minute ago must beat a task created just now.
        The old NULLs-first ordering put every fresh immediate task ahead of the
        whole retry backlog, so steady producer traffic starved retries forever.
        """
        retry = create_task(task_type="t", scheduled_for=datetime.now(UTC) - timedelta(minutes=1))
        fresh = create_task(task_type="t")

        assert backend.claim(2) == [retry.id, fresh.id]

    def test_queue_scoping(self):
        create_task(task_type="t", queue="bulk")
        critical = create_task(task_type="t", queue="critical")

        assert DjangoDispatchBackend(queues=["critical"]).claim(10) == [critical.id]

    def test_release_returns_work_without_burning_an_attempt(self, backend):
        task = create_task(task_type="t")
        backend.claim(10)

        backend.release([task.id])

        row = TaskEntry.objects.get(id=task.id)
        assert row.status == TaskStatus.PENDING.value
        assert row.dispatching_at is None
        assert row.attempts == 0


@pytest.mark.django_db(transaction=True)
class TestSweepAndDispatch:
    def test_the_dispatcher_runs_the_sweep(self, backend):
        task = create_task(task_type="t", max_attempts=5)
        TaskEntry.objects.filter(id=task.id).update(
            status=TaskStatus.FAILED.value,
            attempts=1,
            scheduled_for=datetime.now(UTC) - timedelta(minutes=1),
        )

        result = Dispatcher(backend, lambda task_id: None).maybe_sweep()

        assert result is not None
        assert result["failed"] == [task.id]
        assert TaskEntry.objects.get(id=task.id).status == TaskStatus.PENDING.value

    def test_end_to_end_through_the_dispatcher(self, backend):
        seen = []

        @dewey.task("orders.confirm")
        def confirm(order_id: int) -> None:
            seen.append(order_id)

        task = create_task(task_type="orders.confirm", args=[7])

        dispatcher = Dispatcher(backend, process_task, sweep_interval_seconds=None)
        assert dispatcher.dispatch_batch() == 1

        assert seen == [7]
        assert TaskEntry.objects.get(id=task.id).status == TaskStatus.COMPLETED.value

    def test_transport_failure_returns_work_to_pending(self, backend):
        task = create_task(task_type="t")

        def broken(task_id: str) -> None:
            raise ConnectionError("redis is down")

        Dispatcher(backend, broken, sweep_interval_seconds=None).dispatch_batch()

        row = TaskEntry.objects.get(id=task.id)
        assert row.status == TaskStatus.PENDING.value
        assert row.attempts == 0


class TestSettingsContract:
    def test_defaults_apply_when_unconfigured(self):
        config = get_settings()
        assert config["BATCH_SIZE"] == 100
        assert config["IDLE_POLL_SECONDS"] == 5.0

    @override_settings(DEWEY={"BATCH_SIZE": 5})
    def test_configured_values_win(self):
        assert get_settings()["BATCH_SIZE"] == 5

    @override_settings(DEWEY={"BATCH_SIZ": 5})
    def test_a_misspelled_key_is_refused_rather_than_ignored(self):
        with pytest.raises(ImproperlyConfigured, match="Unknown DEWEY setting"):
            get_settings()

    @override_settings(DEWEY=["not", "a", "dict"])
    def test_a_non_dict_setting_is_refused(self):
        with pytest.raises(ImproperlyConfigured, match="must be a dict"):
            get_settings()

    def test_missing_dispatch_explains_what_to_set(self):
        with pytest.raises(ImproperlyConfigured, match="DISPATCH"):
            get_dispatch_fn()

    @override_settings(DEWEY={"DISPATCH": "tests.test_django_dispatch.does_not_exist"})
    def test_an_unimportable_dispatch_path_is_reported(self):
        with pytest.raises(ImproperlyConfigured, match="could not be imported"):
            get_dispatch_fn()

    @override_settings(DEWEY={"DISPATCH": "dewey.django.conf.DEFAULTS"})
    def test_a_non_callable_dispatch_target_is_refused(self):
        with pytest.raises(ImproperlyConfigured, match="not callable"):
            get_dispatch_fn()

    @override_settings(DEWEY={"DISPATCH": "tests.test_django_dispatch.record_dispatch"})
    def test_a_dotted_path_resolves_to_the_callable(self):
        assert get_dispatch_fn() is record_dispatch


DISPATCHED: list[str] = []


def record_dispatch(task_id: str) -> None:
    """Stand-in transport for the management-command test."""
    DISPATCHED.append(task_id)


@pytest.mark.django_db(transaction=True)
class TestManagementCommand:
    @override_settings(DEWEY={"DISPATCH": "tests.test_django_dispatch.record_dispatch"})
    def test_once_dispatches_a_single_batch_and_exits(self):
        DISPATCHED.clear()
        task = create_task(task_type="t")

        call_command("dewey_dispatcher", "--once")

        assert DISPATCHED == [task.id]
        assert TaskEntry.objects.get(id=task.id).status == TaskStatus.DISPATCHING.value

    @override_settings(DEWEY={"DISPATCH": "tests.test_django_dispatch.record_dispatch"})
    def test_queue_scoping_from_the_command_line(self):
        DISPATCHED.clear()
        create_task(task_type="t", queue="bulk")
        critical = create_task(task_type="t", queue="critical")

        call_command("dewey_dispatcher", "--once", "--queues", "critical")

        assert DISPATCHED == [critical.id]

    def test_running_without_dispatch_configured_fails_loudly(self):
        with pytest.raises(ImproperlyConfigured, match="DISPATCH"):
            call_command("dewey_dispatcher", "--once")


@pytest.mark.django_db(transaction=True)
class TestIdempotencyKey:
    def test_a_duplicate_key_is_rejected_by_the_database(self):
        from django.db import IntegrityError, transaction

        create_task(task_type="t", idempotency_key="command-1")

        with pytest.raises(IntegrityError), transaction.atomic():
            create_task(task_type="t", idempotency_key="command-1")

    def test_the_same_key_under_a_different_task_type_is_allowed(self):
        create_task(task_type="a", idempotency_key="command-1")
        create_task(task_type="b", idempotency_key="command-1")
        assert TaskEntry.objects.count() == 2

    def test_rows_without_a_key_never_collide(self):
        """Postgres treats NULLs as distinct — unkeyed tasks must stay independent."""
        for _ in range(3):
            create_task(task_type="t")
        assert TaskEntry.objects.filter(idempotency_key__isnull=True).count() == 3


@pytest.mark.django_db
class TestMigrationsShip:
    def test_the_initial_migration_is_part_of_the_package(self):
        """The wheel has to carry migrations, or a consumer cannot migrate at all."""
        from importlib import resources

        files = {path.name for path in resources.files("dewey.django.migrations").iterdir()}
        assert "0001_initial.py" in files

    def test_models_and_migrations_do_not_drift(self):
        """makemigrations --check catches a model edit that never got a migration."""
        from django.core.management import call_command

        call_command("makemigrations", "dewey", check=True, dry_run=True, verbosity=0)


class TestMissingDjangoGuidance:
    """`import dewey.django` works without Django; using it must say what to install.

    The package imports nothing at module level (so Django's app registry is never
    touched too early), which means a missing Django only surfaces on first attribute
    access — where a bare "No module named 'django'" is not actionable.
    """

    def test_the_error_names_the_extra_to_install(self, monkeypatch):
        import dewey.django as dewey_django

        def missing_django(path: str):
            raise ModuleNotFoundError("No module named 'django'", name="django")

        monkeypatch.setattr(dewey_django, "import_module", missing_django)
        monkeypatch.delitem(dewey_django.__dict__, "get_stats", raising=False)

        with pytest.raises(ModuleNotFoundError) as excinfo:
            _ = dewey_django.get_stats  # the attribute access is what fails

        message = str(excinfo.value)
        assert "dewey[django]" in message
        assert "get_stats" in message  # says which name was being reached for

    def test_an_unrelated_import_error_is_not_disguised(self, monkeypatch):
        """A broken dependency inside our own module must not be reported as Django."""
        import dewey.django as dewey_django

        def missing_other(path: str):
            raise ModuleNotFoundError("No module named 'psycopg2'", name="psycopg2")

        monkeypatch.setattr(dewey_django, "import_module", missing_other)
        monkeypatch.delitem(dewey_django.__dict__, "get_stats", raising=False)

        with pytest.raises(ModuleNotFoundError, match="psycopg2"):
            _ = dewey_django.get_stats

    def test_an_unknown_name_is_still_an_attribute_error(self):
        import dewey.django as dewey_django

        with pytest.raises(AttributeError, match="no attribute"):
            _ = dewey_django.definitely_not_a_dewey_function
