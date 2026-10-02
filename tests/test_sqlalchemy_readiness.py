"""Preflight checks are read-only and fail before serving incompatible databases."""

import asyncio

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session

from dewey.sqlalchemy import (
    ReadinessError,
    TaskEntryModel,
    check_readiness,
    check_readiness_async,
    create_task,
    create_task_async,
)

CASES = [
    ("ready", None),
    ("legacy", "schema is outdated"),
    ("missing", "table is missing"),
    ("wrong-type", "nullable varchar"),
    ("not-null", "nullable varchar"),
    ("repeatable-read", "READ COMMITTED"),
]
DDL = {
    "legacy": "ALTER TABLE task_entries DROP COLUMN execution_token",
    "missing": "DROP TABLE task_entries",
    "wrong-type": "ALTER TABLE task_entries ALTER COLUMN execution_token TYPE varchar(10)",
    "not-null": "ALTER TABLE task_entries ALTER COLUMN execution_token SET NOT NULL",
}


@pytest.mark.parametrize("case,message", CASES)
@pytest.mark.parametrize("backend", ["sync", "async"])
def test_readiness_checks_schema_and_isolation_without_committing(engine, backend, case, message):
    """DDL is transactional fault injection in the owned DB and always rolled back."""
    isolation = "REPEATABLE READ" if case == "repeatable-read" else "READ COMMITTED"
    if backend == "sync":
        with engine.connect().execution_options(isolation_level=isolation) as connection:
            transaction = connection.begin()
            try:
                if case in DDL:
                    connection.execute(text(DDL[case]))
                with Session(bind=connection) as session:
                    if message:
                        with pytest.raises(ReadinessError, match=message):
                            check_readiness(session)
                    else:
                        task = create_task(session, task_type="synthetic.readiness")
                        check_readiness(session)
                        row = session.get(TaskEntryModel, task.id, populate_existing=True)
                        assert (row.status, row.attempts, row.execution_token) == (
                            "pending",
                            0,
                            None,
                        )
                        assert (
                            session.execute(text("SELECT count(*) FROM task_entries")).scalar() == 1
                        )
                    assert transaction.is_active, (
                        "Preflight must not commit the caller's transaction"
                    )
            finally:
                transaction.rollback()
        return

    async def run():
        async_engine = create_async_engine(engine.url.set(drivername="postgresql+asyncpg"))
        try:
            async with async_engine.connect() as connection:
                connection = await connection.execution_options(isolation_level=isolation)
                transaction = await connection.begin()
                try:
                    if case in DDL:
                        await connection.execute(text(DDL[case]))
                    async with AsyncSession(bind=connection) as session:
                        if message:
                            with pytest.raises(ReadinessError, match=message):
                                await check_readiness_async(session)
                        else:
                            task = await create_task_async(session, task_type="synthetic.readiness")
                            await check_readiness_async(session)
                            row = await session.get(TaskEntryModel, task.id, populate_existing=True)
                            assert (row.status, row.attempts, row.execution_token) == (
                                "pending",
                                0,
                                None,
                            )
                            result = await session.execute(
                                text("SELECT count(*) FROM task_entries")
                            )
                            assert result.scalar() == 1
                        assert transaction.is_active
                finally:
                    await transaction.rollback()
        finally:
            await async_engine.dispose()

    asyncio.run(run())


@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
def test_snapshot_isolation_conflict_is_an_error_not_a_stale_ownership_write(engine, isolation):
    """Postgres rejects a conflicting locked read; cache refresh cannot change its snapshot."""
    with Session(engine) as producer:
        task = create_task(producer, task_type="synthetic.isolation")
        task_id = task.id
        producer.commit()
    with Session(engine.execution_options(isolation_level=isolation)) as stale:
        assert stale.get(TaskEntryModel, task_id).status == "pending"
        with Session(engine) as newer:
            row = newer.get(TaskEntryModel, task_id)
            row.status = "completed"
            newer.commit()
        with pytest.raises(OperationalError) as error:
            stale.execute(
                select(TaskEntryModel)
                .where(TaskEntryModel.id == task_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            ).scalar_one()
        assert error.value.orig.pgcode == "40001"
        stale.rollback()
    with Session(engine) as fresh:
        row = fresh.get(TaskEntryModel, task_id)
        assert (row.status, row.attempts, row.execution_token) == ("completed", 0, None)
