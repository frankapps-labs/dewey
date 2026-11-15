"""The inline process_task helpers run a handler the way the worker does.

They bind ``dewey.current_task()`` with the policy's database budgets and, on
Django, apply those budgets to the connection, so a handler tested inline
exercises the production path.
"""

from __future__ import annotations

# ruff: noqa: E402 - django.setup() must run before model imports
import os

import django
import pytest
from sqlalchemy import text

import dewey
from dewey.policy import clear_project_policies, registry
from dewey.sqlalchemy import apply_task_timeouts, apply_task_timeouts_async
from dewey.sqlalchemy.async_executor import create_task_async, process_task_async
from dewey.sqlalchemy.executor import create_task, process_task

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.db import connection

from dewey.django.executor import create_task as django_create_task
from dewey.django.executor import process_task as django_process_task


@pytest.fixture(autouse=True)
def _clean_registry():
    registry.clear()
    clear_project_policies()
    yield
    registry.clear()
    clear_project_policies()


def test_current_task_or_none_is_none_outside_a_handler():
    assert dewey.current_task_or_none() is None
    with pytest.raises(RuntimeError):
        dewey.current_task()


def test_sqlalchemy_inline_handler_sees_its_budgets_and_can_apply_them(session):
    seen = []

    @dewey.task("photos.moderate", statement_timeout_s=1.5, lock_timeout_s=0.25)
    def moderate() -> None:
        task = dewey.current_task_or_none()
        assert task is dewey.current_task()
        with session.begin():
            apply_task_timeouts(session)
            shown = session.execute(text("SHOW statement_timeout")).scalar_one()
        seen.append((task.statement_timeout_s, task.lock_timeout_s, shown))

    task = create_task(session, task_type="photos.moderate")
    session.commit()
    assert process_task(session, task.id) is True
    assert seen == [(1.5, 0.25, "1500ms")]
    assert dewey.current_task_or_none() is None


@pytest.mark.asyncio
async def test_async_inline_handler_sees_its_budgets_and_can_apply_them(async_session):
    seen = []

    @dewey.task("photos.moderate", statement_timeout_s=1.5, lock_timeout_s=0.25)
    async def moderate() -> None:
        task = dewey.current_task()
        async with async_session.begin():
            await apply_task_timeouts_async(async_session)
            shown = (await async_session.execute(text("SHOW statement_timeout"))).scalar_one()
        seen.append((task.statement_timeout_s, task.lock_timeout_s, shown))

    task = await create_task_async(async_session, task_type="photos.moderate")
    await async_session.commit()
    assert await process_task_async(async_session, task.id) is True
    assert seen == [(1.5, 0.25, "1500ms")]


@pytest.mark.django_db(transaction=True)
def test_django_inline_handler_runs_under_the_policy_budgets():
    def settings():
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')"
            )
            return cursor.fetchone()

    before = settings()
    seen = []

    @dewey.task("photos.moderate", statement_timeout_s=1.5, lock_timeout_s=0.25)
    def moderate() -> None:
        task = dewey.current_task()
        seen.append((task.statement_timeout_s, task.lock_timeout_s, settings()))

    task = django_create_task(task_type="photos.moderate")
    assert django_process_task(task.id) is True
    assert seen == [(1.5, 0.25, ("1500ms", "250ms"))]
    assert settings() == before  # the application's own settings are back
