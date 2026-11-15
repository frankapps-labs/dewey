"""Fail closed before autoflush; the caller, not the API, owns every root."""

from contextlib import AsyncExitStack, ExitStack
from uuid import uuid4

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, event
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, registry

from dewey.runner import RunnerPolicy, RunnerTiming
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.runner import AsyncSQLAlchemyRunner, SQLAlchemyRunner


class SyntheticDomain:
    pass


MAPPERS = registry()
MAPPERS.map_imperatively(
    SyntheticDomain,
    Table("synthetic_foreign_pending", MetaData(), Column("id", Integer, primary_key=True)),
)
POLICY = RunnerPolicy("synthetic-instance", ["default"], RunnerTiming(30, 30, 2, 1, 0.1, 1, 60))
MODES = [
    "engine_bound",
    "conditional_savepoint",
    "control_fully",
    "create_savepoint",
    "foreign_task",
    "foreign_pending",
    "nested_connection",
    "nested_session",
    "inactive_root",
]


def kwargs(connection, engine, mode):
    values = {
        "bind": engine if mode == "engine_bound" else connection,
        "join_transaction_mode": mode
        if mode in ("conditional_savepoint", "control_fully", "create_savepoint")
        else "rollback_only",
    }
    if mode in ("foreign_task", "foreign_pending"):
        values["binds"] = {Task if mode == "foreign_task" else SyntheticDomain: engine}
    return values


def pending(session, flushed):
    session.add(Task(id=str(uuid4()), task_type="synthetic.pending"))
    session.add(SyntheticDomain())
    event.listen(session, "before_flush", lambda *args: flushed.append(True))


@pytest.mark.parametrize("mode", MODES)
def test_sync_binding_rejection_precedes_any_flush(engine, mode):
    flushed = []
    with engine.connect() as connection, connection.begin() as root, ExitStack() as stack:
        if mode == "nested_connection":
            stack.enter_context(connection.begin_nested())
        if mode == "inactive_root":
            root.rollback()
        session = stack.enter_context(Session(**kwargs(connection, engine, mode)))
        if mode == "nested_session":
            stack.enter_context(session.begin_nested())
        pending(session, flushed)
        with pytest.raises(ValueError):
            SQLAlchemyRunner(session, POLICY).claim()
        assert flushed == []
        if mode != "inactive_root":
            assert root.is_active and not connection.closed
        session.expunge_all()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
async def test_async_binding_rejection_precedes_any_flush(engine, mode):
    async_engine = create_async_engine(engine.url.set(drivername="postgresql+asyncpg"))
    flushed = []
    try:
        async with (
            async_engine.connect() as connection,
            connection.begin() as root,
            AsyncExitStack() as stack,
        ):
            if mode == "nested_connection":
                await stack.enter_async_context(connection.begin_nested())
            if mode == "inactive_root":
                await root.rollback()
            session = await stack.enter_async_context(
                AsyncSession(**kwargs(connection, async_engine, mode))
            )
            if mode == "nested_session":
                await stack.enter_async_context(session.begin_nested())
            pending(session.sync_session, flushed)
            with pytest.raises(ValueError):
                await AsyncSQLAlchemyRunner(session, POLICY).claim()
            assert flushed == []
            if mode != "inactive_root":
                assert root.is_active and not connection.closed
            session.expunge_all()
    finally:
        await async_engine.dispose()


def test_sync_pending_host_invalidation_is_not_overwritten_by_refresh(engine):
    with engine.connect() as connection, connection.begin():
        with Session(bind=connection, join_transaction_mode="rollback_only") as session:
            task = Task(task_type="synthetic.pending-cancellation")
            session.add(task)
            session.flush()
            api = SQLAlchemyRunner(session, POLICY)
            assignment = api.claim()[0]
            token = api.start(assignment)["execution_token"]
            task.status = "dead"
            for field in (
                "dispatch_token",
                "worker_id",
                "execution_token",
                "lease_expires_at",
                "execution_deadline_at",
            ):
                setattr(task, field, None)
            assert not api.renew(assignment, token)
            assert task.status == "dead"
            assert not api.complete(assignment, token)


@pytest.mark.asyncio
async def test_async_pending_host_invalidation_is_not_overwritten_by_refresh(engine):
    async_engine = create_async_engine(engine.url.set(drivername="postgresql+asyncpg"))
    try:
        async with async_engine.connect() as connection, connection.begin():
            async with AsyncSession(
                bind=connection, join_transaction_mode="rollback_only"
            ) as session:
                task = Task(task_type="synthetic.pending-cancellation")
                session.add(task)
                await session.flush()
                api = AsyncSQLAlchemyRunner(session, POLICY)
                assignment = (await api.claim())[0]
                token = (await api.start(assignment))["execution_token"]
                task.status = "dead"
                for field in (
                    "dispatch_token",
                    "worker_id",
                    "execution_token",
                    "lease_expires_at",
                    "execution_deadline_at",
                ):
                    setattr(task, field, None)
                assert not await api.renew(assignment, token)
                assert task.status == "dead"
                assert not await api.complete(assignment, token)
    finally:
        await async_engine.dispose()


@pytest.mark.parametrize("store", ["django"], indirect=True)
def test_django_rejects_autocommit_and_nested_blocks_before_writes(store):
    from django.db import connections, transaction

    from dewey.django.runner import DjangoRunner

    api = DjangoRunner(POLICY, using="native")
    with pytest.raises(ValueError):
        api.claim()
    with transaction.atomic(using="native"):
        connection = connections["native"]
        for savepoint in (True, False):
            with transaction.atomic(using="native", savepoint=savepoint):
                with pytest.raises(ValueError):
                    api.claim()
                assert connection.in_atomic_block
        assert api.claim() == []
