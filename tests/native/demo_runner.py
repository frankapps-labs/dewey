"""Run an installed-wheel async gateway lifecycle in an owned scratch schema.

Set DEWEY_TEST_DATABASE_URL_ASYNC to a disposable local PostgreSQL database.
Run this file directly, not through pytest (which adds the source tree to sys.path).
The UUID schema is removed on exit; no existing application tables are changed.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from importlib.metadata import version
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from dewey.runner import (
    PROTOCOL_VERSION,
    FailureReport,
    LeaseExpired,
    RunnerAssignment,
    RunnerPolicy,
    RunnerTiming,
)
from dewey.sqlalchemy.models import Base
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.runner import AsyncSQLAlchemyRunner


async def main():
    assert RunnerAssignment.__module__ == RunnerTiming.__module__ == "dewey.runner"
    assert FailureReport().kind == "transient" and issubclass(LeaseExpired, Exception)
    schema = f"runner_demo_{uuid4().hex}"
    engine = create_async_engine(
        os.environ["DEWEY_TEST_DATABASE_URL_ASYNC"],
        connect_args={"server_settings": {"search_path": schema}},
    )
    policy = RunnerPolicy("synthetic-demo", ["default"], RunnerTiming(30, 30, 2, 0.2, 0.1, 1, 60))

    @asynccontextmanager
    async def request():
        async with engine.connect() as connection, connection.begin():
            async with AsyncSession(
                bind=connection, join_transaction_mode="rollback_only"
            ) as session:
                yield AsyncSQLAlchemyRunner(session, policy), session, connection

    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.run_sync(Base.metadata.create_all)
        async with request() as (_, session, _):
            healthy = Task(task_type="synthetic.demo", status="failed", attempts=1)
            malformed = Task(task_type="synthetic.demo", worker_id="synthetic-quarantined")
            session.add_all([healthy, malformed])
            await session.flush()
            task_id, bad_id = healthy.id, malformed.id
        async with request() as (runner, _, connection):
            await connection.execute(text("SET LOCAL statement_timeout = '2s'"))
            await connection.execute(text("SET LOCAL lock_timeout = '900ms'"))
            await runner.check_readiness()
            (assignment,) = await runner.claim()
            assert assignment.task_id == task_id
            assert (await connection.execute(text("SHOW statement_timeout"))).scalar_one() == "2s"
            assert (await connection.execute(text("SHOW lock_timeout"))).scalar_one() == "900ms"
            await connection.execute(
                text(
                    "UPDATE task_entries SET error = 'host-write' FROM pg_sleep(0.3) WHERE id = :id"
                ),
                {"id": task_id},
            )
        print(f"wheel={version('dewey')} protocol={PROTOCOL_VERSION} public_types=dewey.runner")
        print("host_write=ok statement_timeout=2s lock_timeout=900ms")
        async with request() as (runner, _, _):
            invocation = await runner.start(assignment)
            assert invocation["attempts"] == 2
            token = invocation["execution_token"]
        async with request() as (runner, _, _):
            assert await runner.renew(assignment, token)
        async with request() as (runner, _, _):
            assert await runner.complete(assignment, token)
        async with request() as (runner, session, _):
            assert await runner.recover() == []
            result = await session.get(Task, task_id)
            bad = await session.get(Task, bad_id)
            assert result.status == "completed" and result.attempts == 2
            assert bad.status == "pending" and bad.worker_id == "synthetic-quarantined"
            assert bad.dispatch_token is None
        print("failed_null_schedule=claimed start_attempts=2 renew=ok complete=ok status=completed")
        print("quarantine=retained recovery=healthy")
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            print("scratch_schema=removed")
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
