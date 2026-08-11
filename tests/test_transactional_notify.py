"""A rolled-back producer must never wake a dispatcher.

This is the property that makes the producer API safe to call inside business
logic: the task row and the wake-up are committed together or not at all, so
there is no window in which a dispatcher can see work that never happened.
"""

from __future__ import annotations

# ruff: noqa: E402 — django.setup() must run before model imports
import os

import django
import pytest
from sqlalchemy import select

from dewey.core.states import TaskStatus
from dewey.listen_sync import SyncWorkListener
from dewey.sqlalchemy.dispatch import SQLAlchemyDispatchBackend
from dewey.sqlalchemy.executor import create_task
from dewey.sqlalchemy.models import TaskEntryModel

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.db import transaction

from dewey.django.dispatch import DjangoDispatchBackend
from dewey.django.executor import create_task as django_create_task
from dewey.django.models import TaskEntry

WAIT = 1.0  # generous enough for a local notification, short enough to keep tests quick


@pytest.fixture
def listener(engine):
    def connect():
        fairy = engine.raw_connection()
        raw = fairy.driver_connection
        fairy.detach()
        return raw

    with SyncWorkListener(connect) as listening:
        if not listening.supported:
            pytest.skip("driver does not support LISTEN")
        yield listening


class TestSQLAlchemyRollback:
    def test_a_committed_task_wakes_the_dispatcher(self, session, listener):
        create_task(session, task_type="t")
        session.commit()

        assert listener.wait(WAIT) is True

    def test_a_rolled_back_task_never_wakes_the_dispatcher(self, session, listener):
        create_task(session, task_type="t")
        session.rollback()

        assert listener.wait(WAIT) is False

    def test_a_rolled_back_task_leaves_nothing_to_claim(self, session, engine):
        create_task(session, task_type="t")
        session.rollback()

        assert session.execute(select(TaskEntryModel)).scalars().all() == []
        assert SQLAlchemyDispatchBackend(engine).claim(10) == []

    def test_a_rollback_after_a_commit_keeps_the_committed_task(self, session, engine):
        """Only the failed unit of work disappears."""
        kept = create_task(session, task_type="kept")
        session.commit()
        create_task(session, task_type="discarded")
        session.rollback()

        assert SQLAlchemyDispatchBackend(engine).claim(10) == [kept.id]


@pytest.mark.django_db(transaction=True)
class TestDjangoRollback:
    def test_a_rolled_back_atomic_block_leaves_nothing_to_claim(self):
        with pytest.raises(RuntimeError), transaction.atomic():
            django_create_task(task_type="t")
            raise RuntimeError("business logic failed after creating the task")

        assert TaskEntry.objects.count() == 0
        assert DjangoDispatchBackend().claim(10) == []

    def test_a_committed_atomic_block_leaves_claimable_work(self):
        with transaction.atomic():
            task = django_create_task(task_type="t")

        assert DjangoDispatchBackend().claim(10) == [task.id]

    def test_autocommit_creation_is_immediately_claimable(self):
        """No enclosing transaction: the row commits on its own and is ready at once."""
        task = django_create_task(task_type="t")

        assert DjangoDispatchBackend().claim(10) == [task.id]
        assert TaskEntry.objects.get(id=task.id).status == TaskStatus.DISPATCHING.value

    def test_a_nested_rollback_discards_only_the_inner_task(self):
        with transaction.atomic():
            outer = django_create_task(task_type="outer")
            try:
                with transaction.atomic():
                    django_create_task(task_type="inner")
                    raise RuntimeError("inner failed")
            except RuntimeError:
                pass

        assert list(TaskEntry.objects.values_list("task_type", flat=True)) == ["outer"]
        assert DjangoDispatchBackend().claim(10) == [outer.id]
