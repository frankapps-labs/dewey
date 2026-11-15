"""The inline executors log a failed attempt the way the worker does.

Sync, async and Django share ``dewey.core.execution.log_outcome`` so the level,
the exception and the structured fields cannot drift between them.
"""

from __future__ import annotations

# ruff: noqa: E402 - django.setup() must run before model imports
import logging
import os
from datetime import timedelta

import django
import pytest

import dewey
from dewey.core.execution import configure_failure_details
from dewey.policy import clear_project_policies, registry
from dewey.sqlalchemy.async_executor import create_task_async, process_task_async
from dewey.sqlalchemy.executor import create_task, process_task

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from dewey.django.executor import create_task as django_create_task
from dewey.django.executor import process_task as django_process_task

FIELDS = ("task_id", "task_type", "queue", "attempt", "max_attempts", "outcome", "reason")


@pytest.fixture(autouse=True)
def _clean_registry():
    registry.clear()
    clear_project_policies()
    configure_failure_details("full")  # these tests look at the traceback
    yield
    configure_failure_details("type")
    registry.clear()
    clear_project_policies()


def test_the_default_record_names_the_type_and_carries_no_traceback(session, caplog):
    configure_failure_details("type")

    @dewey.task("inline.fails", max_attempts=1)
    def fails() -> None:
        raise ValueError("secret input")

    task = create_task(session, task_type="inline.fails")
    session.commit()
    with caplog.at_level(logging.WARNING, logger="dewey.sqlalchemy.executor"):
        assert process_task(session, task.id, backoff=no_backoff) is False
    (record,) = failures(caplog, "dewey.sqlalchemy.executor")
    assert record.exc_info is None and "error=ValueError" in record.getMessage()
    assert "secret input" not in record.getMessage()


def no_backoff(attempt: int) -> timedelta:
    return timedelta(0)


def failures(caplog, logger_name):
    return [r for r in caplog.records if r.name == logger_name and r.levelno >= logging.WARNING]


def check(records, retried_id, dead_id):
    """One task had budget left (WARNING), the other did not (ERROR); both carry the exception."""
    first, last = records
    assert first.levelno == logging.WARNING and last.levelno == logging.ERROR
    for record, task_id, max_attempts, outcome, reason in (
        (first, retried_id, 2, "failed", "retry"),
        (last, dead_id, 1, "dead", "attempts exhausted"),
    ):
        assert isinstance(record.exc_info[1], ValueError), record.getMessage()
        for value in record.args:  # a JSON handler serialises these as they are
            assert isinstance(value, (str, int, float, bool, type(None))), repr(value)
        assert {name: getattr(record, name) for name in FIELDS} == {
            "task_id": task_id,
            "task_type": "inline.fails",
            "queue": "default",
            "attempt": 1,
            "max_attempts": max_attempts,
            "outcome": outcome,
            "reason": reason,
        }


def test_sqlalchemy_logs_each_failed_attempt(session, caplog):
    @dewey.task("inline.fails")
    def fails() -> None:
        raise ValueError("synthetic failure")

    retried = create_task(session, task_type="inline.fails", max_attempts=2)
    dead = create_task(session, task_type="inline.fails", max_attempts=1)
    session.commit()
    with caplog.at_level(logging.WARNING, logger="dewey.sqlalchemy.executor"):
        assert process_task(session, retried.id, backoff=no_backoff) is False
        assert process_task(session, dead.id, backoff=no_backoff) is False
    check(failures(caplog, "dewey.sqlalchemy.executor"), retried.id, dead.id)


@pytest.mark.asyncio
async def test_async_sqlalchemy_logs_each_failed_attempt(async_session, caplog):
    @dewey.task("inline.fails")
    async def fails() -> None:
        raise ValueError("synthetic failure")

    retried = await create_task_async(async_session, task_type="inline.fails", max_attempts=2)
    dead = await create_task_async(async_session, task_type="inline.fails", max_attempts=1)
    await async_session.commit()
    with caplog.at_level(logging.WARNING, logger="dewey.sqlalchemy.async_executor"):
        assert await process_task_async(async_session, retried.id, backoff=no_backoff) is False
        assert await process_task_async(async_session, dead.id, backoff=no_backoff) is False
    check(failures(caplog, "dewey.sqlalchemy.async_executor"), retried.id, dead.id)


@pytest.mark.django_db(transaction=True)
def test_django_logs_each_failed_attempt(caplog):
    @dewey.task("inline.fails")
    def fails() -> None:
        raise ValueError("synthetic failure")

    retried = django_create_task(task_type="inline.fails", max_attempts=2)
    dead = django_create_task(task_type="inline.fails", max_attempts=1)
    with caplog.at_level(logging.WARNING, logger="dewey.django.executor"):
        assert django_process_task(retried.id, backoff=no_backoff) is False
        assert django_process_task(dead.id, backoff=no_backoff) is False
    check(failures(caplog, "dewey.django.executor"), retried.id, dead.id)
