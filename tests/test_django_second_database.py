"""The documented non-default alias deployment, end to end.

docs/getting-started.md tells multi-database projects to give the dispatcher and
worker their own DATABASES alias. Where a project has routers, that is only
correct if every transaction, SELECT FOR UPDATE, write and NOTIFY runs on the
same resolved alias — ``transaction.atomic()`` does not consult routers, so an
atomic block opened on "default" around a query routed elsewhere locks nothing
and sends the commit-gated NOTIFY on a connection that never wrote the row.
These tests pin the resolved-alias contract for create, process and sweep, and
the producer-atomicity contract the docs lead with: one alias is one
transaction; two aliases are two, and never roll back together.
"""

from __future__ import annotations

# ruff: noqa: E402 — django.setup() must run before model imports
import os
from datetime import UTC, datetime, timedelta

import django
import pytest
from django.db import transaction

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

import dewey
from dewey.core.states import TaskStatus
from dewey.django.dispatch import DjangoDispatchBackend
from dewey.django.executor import create_task, process_task
from dewey.django.models import TaskEntry, resolve_db_alias
from dewey.django.sweep import sweep


@pytest.fixture(autouse=True)
def clean_registry():
    """pytest-django truncates tables between transactional tests; the process-local
    policy registry is ours to reset."""
    yield
    dewey.registry.clear()


@pytest.fixture
def captured_notify(monkeypatch):
    """Record the alias each NOTIFY is sent on, instead of sending it."""
    calls: list[dict] = []

    def capture(**kwargs):
        calls.append(kwargs)
        return True

    monkeypatch.setattr("dewey.django.executor.notify_work_available", capture)
    return calls


class DeweyRouter:
    """Send Dewey's models to the second alias — the router the docs describe."""

    def db_for_read(self, model, **hints):
        return "second" if model._meta.app_label == "dewey" else None

    def db_for_write(self, model, **hints):
        return "second" if model._meta.app_label == "dewey" else None

    def allow_relation(self, obj1, obj2, **hints):
        return None

    def allow_migrate(self, db, app_label, **hints):
        return None


@pytest.fixture
def dewey_router(settings):
    settings.DATABASE_ROUTERS = [DeweyRouter()]


def _fail_row(task_id: str, alias: str) -> None:
    """Turn a task into a FAILED row whose retry is already due."""
    TaskEntry.objects.using(alias).filter(id=task_id).update(
        status=TaskStatus.FAILED.value,
        attempts=1,
        scheduled_for=datetime.now(UTC) - timedelta(minutes=1),
    )


@pytest.mark.django_db(transaction=True, databases=["default", "second"])
class TestExplicitAlias:
    """`using="second"` pins every statement to that alias."""

    def test_create_task_writes_the_row_and_the_notify_to_the_alias(self, captured_notify):
        task = create_task(task_type="t", using="second")

        assert TaskEntry.objects.using("second").filter(id=task.id).exists()
        assert not TaskEntry.objects.using("default").filter(id=task.id).exists()
        # The NOTIFY must ride the transaction that wrote the row.
        assert [call["using"] for call in captured_notify] == ["second"]

    def test_process_task_claims_and_completes_on_the_alias(self):
        seen = []

        @dewey.task("orders.confirm")
        def confirm(order_id: int) -> None:
            seen.append(order_id)

        task = create_task(task_type="orders.confirm", args=[7], using="second")

        assert process_task(task.id, using="second") is True

        assert seen == [7]
        row = TaskEntry.objects.using("second").get(id=task.id)
        assert row.status == TaskStatus.COMPLETED.value
        assert not TaskEntry.objects.using("default").filter(id=task.id).exists()

    def test_process_task_failure_path_writes_the_retry_on_the_alias(self):
        @dewey.task("flaky", max_attempts=3)
        def flaky() -> None:
            raise ValueError("boom")

        task = create_task(task_type="flaky", using="second")

        assert process_task(task.id, using="second") is False

        row = TaskEntry.objects.using("second").get(id=task.id)
        assert row.status == TaskStatus.FAILED.value
        assert row.attempts == 1

    def test_sweep_re_enqueues_only_on_the_given_alias(self):
        on_second = create_task(task_type="t", max_attempts=5, using="second")
        on_default = create_task(task_type="t", max_attempts=5)
        _fail_row(on_second.id, "second")
        _fail_row(on_default.id, "default")

        result = sweep(using="second")

        assert result["failed"] == [on_second.id]
        assert (
            TaskEntry.objects.using("second").get(id=on_second.id).status
            == TaskStatus.PENDING.value
        )
        # The default database was not swept — the pass stayed on its alias.
        assert (
            TaskEntry.objects.using("default").get(id=on_default.id).status
            == TaskStatus.FAILED.value
        )

    def test_the_dispatch_backend_sweeps_on_its_own_alias(self):
        task = create_task(task_type="t", max_attempts=5, using="second")
        _fail_row(task.id, "second")

        result = DjangoDispatchBackend(using="second").run_sweep()

        assert result["failed"] == [task.id]
        assert TaskEntry.objects.using("second").get(id=task.id).status == TaskStatus.PENDING.value


@pytest.mark.django_db(transaction=True, databases=["default", "second"])
class TestRouterResolution:
    """With no explicit ``using``, the project's routers pick the alias — and the
    transactions follow it rather than opening on "default"."""

    def test_the_alias_resolves_through_the_router(self, dewey_router):
        assert resolve_db_alias() == "second"
        assert resolve_db_alias("default") == "default"  # explicit still wins

    def test_create_task_follows_the_router(self, dewey_router, captured_notify):
        task = create_task(task_type="t")

        assert TaskEntry.objects.using("second").filter(id=task.id).exists()
        assert not TaskEntry.objects.using("default").filter(id=task.id).exists()
        assert [call["using"] for call in captured_notify] == ["second"]

    def test_process_task_transacts_on_the_routed_alias(self, dewey_router):
        # Before the alias was resolved up front, this raised
        # TransactionManagementError: atomic() opened on "default" while the
        # router sent SELECT FOR UPDATE to "second".
        seen = []

        @dewey.task("orders.confirm")
        def confirm(order_id: int) -> None:
            seen.append(order_id)

        task = create_task(task_type="orders.confirm", args=[7])

        assert process_task(task.id) is True

        assert seen == [7]
        row = TaskEntry.objects.using("second").get(id=task.id)
        assert row.status == TaskStatus.COMPLETED.value

    def test_sweep_follows_the_router(self, dewey_router):
        task = create_task(task_type="t", max_attempts=5)
        _fail_row(task.id, "second")

        result = sweep()

        assert result["failed"] == [task.id]
        assert TaskEntry.objects.using("second").get(id=task.id).status == TaskStatus.PENDING.value


class _RollBack(Exception):
    """Raised to abort the producer's transaction deliberately."""


@pytest.mark.django_db(transaction=True, databases=["default", "second"])
class TestProducerAtomicity:
    """The producer contract docs/getting-started.md leads with: create_task on the
    business write's alias shares its connection and transaction, so the pair
    commits or rolls back as one. A different alias never shares it — a second
    alias is a second connection and a second transaction, and that is equally
    true when two aliases point at one physical database."""

    def test_business_row_and_task_roll_back_together_on_one_alias(self):
        # No business app is installed in the test project, so a directly created
        # TaskEntry stands in for the business row: any ORM write on the
        # producer's alias behaves identically, and it needs no extra migration.
        with pytest.raises(_RollBack):
            with transaction.atomic(using="second"):
                business = TaskEntry.objects.using("second").create(task_type="business.row")
                task = create_task(task_type="orders.confirm", using="second")
                # Both rows are visible inside the transaction that wrote them.
                assert TaskEntry.objects.using("second").filter(id=business.id).exists()
                assert TaskEntry.objects.using("second").filter(id=task.id).exists()
                raise _RollBack

        # One alias, one connection, one transaction: the rollback took both.
        assert not TaskEntry.objects.using("second").filter(id=business.id).exists()
        assert not TaskEntry.objects.using("second").filter(id=task.id).exists()

    def test_split_aliases_do_not_share_atomicity(self):
        # The business write runs in a transaction on "default" while the task is
        # written through "second" — exactly what a catch-all router sending
        # TaskEntry writes to a background alias would do silently.
        with pytest.raises(_RollBack):
            with transaction.atomic(using="default"):
                business = TaskEntry.objects.using("default").create(task_type="business.row")
                task = create_task(task_type="orders.confirm", using="second")
                raise _RollBack

        # The business row rolled back...
        assert not TaskEntry.objects.using("default").filter(id=business.id).exists()
        # ...but the task committed anyway. "second" never joined the producer's
        # transaction: it is its own connection in autocommit, so the write was
        # durable the moment it happened, rollback next door notwithstanding.
        orphan = TaskEntry.objects.using("second").get(id=task.id)
        assert orphan.status == TaskStatus.PENDING.value
        assert not TaskEntry.objects.using("default").filter(id=task.id).exists()
