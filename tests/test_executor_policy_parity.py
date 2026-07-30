"""Policy behaviour must be identical on SQLAlchemy async and Django.

The failure decision lives in ``dewey.core.execution`` precisely so the three
backends cannot drift. These tests hold that line from the outside.
"""

from __future__ import annotations

# ruff: noqa: E402 — django.setup() must run before model imports
import os

import django
import pytest

import dewey
from dewey.core.states import TaskStatus
from dewey.errors import NonRetryableError, RetryAfter
from dewey.policy import Constant, clear_project_policies, registry
from dewey.sqlalchemy.async_executor import create_task_async, process_task_async
from dewey.sqlalchemy.models import TaskEntryModel

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from dewey.django.executor import create_task as django_create_task
from dewey.django.executor import process_task as django_process_task


@pytest.fixture(autouse=True)
def _clean_registry():
    registry.clear()
    clear_project_policies()
    yield
    registry.clear()
    clear_project_policies()


# --- SQLAlchemy async ---


@pytest.mark.asyncio
async def test_async_resolves_the_registered_handler(async_session):
    seen = []

    @dewey.task("orders.confirm")
    async def confirm(order_id: int) -> None:
        seen.append(order_id)

    task = await create_task_async(async_session, task_type="orders.confirm", args=[7])
    await async_session.commit()

    assert await process_task_async(async_session, task.id) is True
    assert seen == [7]


@pytest.mark.asyncio
async def test_async_non_retryable_error_dead_letters_immediately(async_session):
    @dewey.task("malformed", max_attempts=5)
    async def malformed() -> None:
        raise NonRetryableError("never going to parse")

    task = await create_task_async(async_session, task_type="malformed")
    await async_session.commit()

    assert await process_task_async(async_session, task.id) is False

    row = await async_session.get(TaskEntryModel, task.id)
    assert row is not None
    assert row.status == TaskStatus.DEAD.value
    assert row.attempts == 1


@pytest.mark.asyncio
async def test_async_retry_after_is_floored_by_the_policy(async_session):
    @dewey.task("rate.limited", max_attempts=5, backoff=Constant(60))
    async def limited() -> None:
        raise RetryAfter(1)

    task = await create_task_async(async_session, task_type="rate.limited")
    await async_session.commit()

    await process_task_async(async_session, task.id)

    row = await async_session.get(TaskEntryModel, task.id)
    assert row is not None
    assert row.status == TaskStatus.FAILED.value
    assert row.scheduled_for is not None


@pytest.mark.asyncio
async def test_async_producer_defaults_come_from_policy(async_session):
    @dewey.task("orders.confirm", queue="critical", priority=7, max_attempts=2)
    async def confirm() -> None: ...

    task = await create_task_async(async_session, task_type="orders.confirm")
    assert (task.queue, task.priority, task.max_attempts) == ("critical", 7, 2)


# --- Django ---


@pytest.mark.django_db(transaction=True)
def test_django_resolves_the_registered_handler():
    from dewey.django.models import TaskEntry

    seen = []

    @dewey.task("orders.confirm")
    def confirm(order_id: int) -> None:
        seen.append(order_id)

    task = django_create_task(task_type="orders.confirm", args=[7])
    assert django_process_task(task.id) is True
    assert seen == [7]
    assert TaskEntry.objects.get(id=task.id).status == TaskStatus.COMPLETED.value


@pytest.mark.django_db(transaction=True)
def test_django_non_retryable_error_dead_letters_immediately():
    from dewey.django.models import TaskEntry

    @dewey.task("malformed", max_attempts=5)
    def malformed() -> None:
        raise NonRetryableError("never going to parse")

    task = django_create_task(task_type="malformed")
    assert django_process_task(task.id) is False

    row = TaskEntry.objects.get(id=task.id)
    assert row.status == TaskStatus.DEAD.value
    assert row.attempts == 1


@pytest.mark.django_db(transaction=True)
def test_django_retry_after_is_floored_by_the_policy():
    from dewey.django.models import TaskEntry

    @dewey.task("rate.limited", max_attempts=5, backoff=Constant(60))
    def limited() -> None:
        raise RetryAfter(1)

    task = django_create_task(task_type="rate.limited")
    django_process_task(task.id)

    row = TaskEntry.objects.get(id=task.id)
    assert row.status == TaskStatus.FAILED.value
    assert row.scheduled_for is not None


@pytest.mark.django_db(transaction=True)
def test_django_producer_defaults_come_from_policy():
    @dewey.task("orders.confirm", queue="critical", priority=7, max_attempts=2)
    def confirm() -> None: ...

    task = django_create_task(task_type="orders.confirm")
    assert (task.queue, task.priority, task.max_attempts) == ("critical", 7, 2)


@pytest.mark.django_db(transaction=True)
def test_django_unknown_task_type_fails_the_attempt():
    from dewey.django.models import TaskEntry

    task = django_create_task(task_type="not.declared", max_attempts=3)
    assert django_process_task(task.id) is False

    row = TaskEntry.objects.get(id=task.id)
    assert row.status == TaskStatus.FAILED.value
    assert "No handler registered" in row.error
