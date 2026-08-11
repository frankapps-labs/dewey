"""Race-safe idempotent creation across SQLAlchemy sync/async and Django."""

from __future__ import annotations

# ruff: noqa: E402 — django.setup() must run before model imports
import asyncio
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import django
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.orm import Session

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.db import transaction

from dewey.core.states import TaskStatus
from dewey.django.executor import create_or_get_task as django_create_or_get_task
from dewey.django.executor import create_task as django_create_task
from dewey.django.models import TaskEntry
from dewey.errors import IdempotencyConflictError
from dewey.sqlalchemy.async_executor import create_or_get_task_async
from dewey.sqlalchemy.executor import create_or_get_task, create_task
from dewey.sqlalchemy.models import TaskEntryModel


class TestSQLAlchemyCreateOrGet:
    def test_identical_contract_returns_the_same_row_without_mutation(self, session):
        scheduled = datetime.now(UTC) + timedelta(minutes=5)
        expires = scheduled + timedelta(minutes=10)
        first = create_or_get_task(
            session,
            task_type="report.build",
            idempotency_key="report-001",
            args=[1],
            kwargs={"format": "csv"},
            queue="bulk",
            priority=4,
            max_attempts=3,
            scheduled_for=scheduled,
            expires_at=expires,
            metadata={"trace": "first"},
        )
        first.status = TaskStatus.COMPLETED.value
        first.scheduled_for = scheduled + timedelta(hours=1)  # retry/lifecycle mutation
        session.flush()

        second = create_or_get_task(
            session,
            task_type="report.build",
            idempotency_key="report-001",
            args=[1],
            kwargs={"format": "csv"},
            queue="bulk",
            priority=4,
            max_attempts=3,
            scheduled_for=scheduled,
            expires_at=expires,
            metadata={"trace": "ignored"},  # metadata is not execution-defining
        )

        assert second.id == first.id
        assert second.status == TaskStatus.COMPLETED.value
        assert second.task_metadata == {"trace": "first"}
        count = session.scalar(select(func.count()).select_from(TaskEntryModel))
        assert count == 1

    def test_conflict_names_fields_without_values(self, session):
        create_or_get_task(
            session,
            task_type="report.build",
            idempotency_key="report-002",
            args=["private-first"],
            priority=1,
        )

        with pytest.raises(IdempotencyConflictError) as excinfo:
            create_or_get_task(
                session,
                task_type="report.build",
                idempotency_key="report-002",
                args=["private-second"],
                priority=9,
            )

        assert excinfo.value.differing_fields == ("args", "priority")
        assert "private-first" not in str(excinfo.value)
        assert "private-second" not in str(excinfo.value)

    def test_conflict_savepoint_preserves_the_surrounding_transaction(self, session):
        create_or_get_task(
            session,
            task_type="report.build",
            idempotency_key="report-003",
            args=[1],
        )
        marker = create_task(session, task_type="transaction.marker")

        with pytest.raises(IdempotencyConflictError):
            create_or_get_task(
                session,
                task_type="report.build",
                idempotency_key="report-003",
                args=[2],
            )

        session.flush()
        assert session.get(TaskEntryModel, marker.id) is not None

    def test_requires_a_non_empty_key(self, session):
        with pytest.raises(ValueError, match="non-empty"):
            create_or_get_task(session, task_type="report.build", idempotency_key="")

    def test_concurrent_creators_converge_on_one_row(self, engine):
        """The unique constraint and savepoint make the race loser return the winner."""
        barrier = threading.Barrier(2)

        def create() -> str:
            with Session(engine) as worker_session, worker_session.begin():
                barrier.wait(timeout=10)
                task = create_or_get_task(
                    worker_session,
                    task_type="report.concurrent",
                    idempotency_key="report-concurrent-001",
                    kwargs={"page": 1},
                )
                return task.id

        with ThreadPoolExecutor(max_workers=2) as pool:
            ids = list(pool.map(lambda _index: create(), range(2)))

        assert ids[0] == ids[1]
        with Session(engine) as verification_session:
            count = verification_session.scalar(
                select(func.count())
                .select_from(TaskEntryModel)
                .where(
                    TaskEntryModel.task_type == "report.concurrent",
                    TaskEntryModel.idempotency_key == "report-concurrent-001",
                )
            )
        assert count == 1


@pytest.mark.asyncio
class TestAsyncCreateOrGet:
    async def test_identical_and_conflicting_contracts(self, async_session):
        first = await create_or_get_task_async(
            async_session,
            task_type="async.report",
            idempotency_key="async-001",
            kwargs={"page": 1},
        )
        second = await create_or_get_task_async(
            async_session,
            task_type="async.report",
            idempotency_key="async-001",
            kwargs={"page": 1},
        )
        assert second.id == first.id

        with pytest.raises(IdempotencyConflictError) as excinfo:
            await create_or_get_task_async(
                async_session,
                task_type="async.report",
                idempotency_key="async-001",
                kwargs={"page": 2},
            )
        assert excinfo.value.differing_fields == ("kwargs",)

    async def test_concurrent_creators_converge(self, async_engine):
        factory = async_sessionmaker(async_engine, expire_on_commit=False)
        ready = asyncio.Event()
        arrivals = 0
        lock = asyncio.Lock()

        async def create() -> str:
            nonlocal arrivals
            async with lock:
                arrivals += 1
                if arrivals == 2:
                    ready.set()
            await ready.wait()
            async with factory() as worker_session, worker_session.begin():
                task = await create_or_get_task_async(
                    worker_session,
                    task_type="async.concurrent",
                    idempotency_key="async-concurrent-001",
                    args=[1],
                )
                return task.id

        ids = await asyncio.gather(create(), create())
        assert ids[0] == ids[1]


@pytest.mark.django_db(transaction=True)
class TestDjangoCreateOrGet:
    def test_identical_contract_returns_the_same_task_in_any_lifecycle_state(self):
        first = django_create_or_get_task(
            task_type="django.report",
            idempotency_key="django-001",
            kwargs={"page": 1},
            metadata={"source": "first"},
        )
        TaskEntry.objects.filter(id=first.id).update(status=TaskStatus.DEAD.value)

        second = django_create_or_get_task(
            task_type="django.report",
            idempotency_key="django-001",
            kwargs={"page": 1},
            metadata={"source": "ignored"},
        )

        assert second.id == first.id
        assert second.status == TaskStatus.DEAD
        assert TaskEntry.objects.count() == 1
        assert TaskEntry.objects.get(id=first.id).metadata == {"source": "first"}

    def test_conflict_preserves_surrounding_atomic_block(self):
        with transaction.atomic():
            django_create_or_get_task(
                task_type="django.report",
                idempotency_key="django-002",
                args=[1],
            )
            marker = django_create_task(task_type="transaction.marker")
            with pytest.raises(IdempotencyConflictError):
                django_create_or_get_task(
                    task_type="django.report",
                    idempotency_key="django-002",
                    args=[2],
                )
            assert TaskEntry.objects.filter(id=marker.id).exists()

        assert TaskEntry.objects.filter(id=marker.id).exists()

    def test_requires_a_non_empty_key(self):
        with pytest.raises(ValueError, match="non-empty"):
            django_create_or_get_task(task_type="django.report", idempotency_key="")
