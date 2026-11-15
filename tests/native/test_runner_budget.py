"""Runner SQL budgets must not govern subsequent host writes in the same root."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session

from dewey._worker.loop import ChildLoop
from dewey.runner import RunnerPolicy, RunnerTiming
from tests.native.test_storage import create

POLICY = RunnerPolicy("synthetic-budget", ["default"], RunnerTiming(30, 30, 2, 0.2, 0.1, 1, 60))


@pytest.mark.parametrize("raises", [False, True])
def test_runner_restores_host_timeouts_before_return_or_python_error(
    store, engine, monkeypatch, raises
):
    from dewey.django.runner import DjangoRunner
    from dewey.django.runner import _Operations as DjangoOperations
    from dewey.sqlalchemy.runner import AsyncSQLAlchemyRunner, SQLAlchemyRunner, _Operations

    if raises:

        def reject(*args):
            raise ValueError("synthetic operation failure")

        monkeypatch.setattr(_Operations, "claim", reject)
        monkeypatch.setattr(DjangoOperations, "claim", reject)

    task_id = create(engine)

    def exercise(api, execute):
        execute("SET LOCAL statement_timeout = '2s'")
        execute("SET LOCAL lock_timeout = '900ms'")
        if raises:
            with pytest.raises(ValueError, match="synthetic operation failure"):
                api()
        else:
            assert len(api()) == 1
        assert execute("SHOW statement_timeout").scalar_one() == "2s"
        assert execute("SHOW lock_timeout").scalar_one() == "900ms"
        # A real host write takes longer than the runner's entire SQL budget.
        execute(
            "UPDATE task_entries SET error = 'synthetic-host-write' "
            "FROM pg_sleep(0.3) WHERE id = :task_id",
            {"task_id": task_id},
        )
        assert (
            execute(
                "SELECT error FROM task_entries WHERE id = :task_id", {"task_id": task_id}
            ).scalar_one()
            == "synthetic-host-write"
        )

    if store.mode == "django":
        from django.db import connections, transaction

        class Result:
            def __init__(self, value):
                self.value = value

            def scalar_one(self):
                return self.value[0]

        with transaction.atomic(using="native"):

            def execute(sql, params=None):
                with connections["native"].cursor() as cursor:
                    cursor.execute(sql.replace(":task_id", "%(task_id)s"), params)
                    return Result(cursor.fetchone() if cursor.description else None)

            exercise(DjangoRunner(POLICY, using="native").claim, execute)
            transaction.set_rollback(True, using="native")
    elif store.mode == "sync":
        with engine.connect() as connection, connection.begin() as root:
            with Session(bind=connection, join_transaction_mode="rollback_only") as session:
                exercise(
                    SQLAlchemyRunner(session, POLICY).claim,
                    lambda sql, params=None: connection.execute(text(sql), params or {}),
                )
                root.rollback()
    else:
        async_engine = create_async_engine(engine.url.set(drivername="postgresql+asyncpg"))

        # Async execution stays on one loop, including all host SQL.
        async def scenario():
            try:
                async with async_engine.connect() as connection, connection.begin() as root:
                    async with AsyncSession(
                        bind=connection, join_transaction_mode="rollback_only"
                    ) as session:
                        await connection.execute(text("SET LOCAL statement_timeout = '2s'"))
                        await connection.execute(text("SET LOCAL lock_timeout = '900ms'"))
                        api = AsyncSQLAlchemyRunner(session, POLICY)
                        if raises:
                            with pytest.raises(ValueError, match="synthetic operation failure"):
                                await api.claim()
                        else:
                            assert len(await api.claim()) == 1
                        assert (
                            await connection.execute(text("SHOW statement_timeout"))
                        ).scalar_one() == "2s"
                        assert (
                            await connection.execute(text("SHOW lock_timeout"))
                        ).scalar_one() == "900ms"
                        await connection.execute(
                            text(
                                "UPDATE task_entries SET error = 'synthetic-host-write' FROM pg_sleep(0.3) WHERE id = :task_id"
                            ),
                            {"task_id": task_id},
                        )
                        assert (
                            await connection.execute(
                                text("SELECT error FROM task_entries WHERE id = :task_id"),
                                {"task_id": task_id},
                            )
                        ).scalar_one() == "synthetic-host-write"
                        await root.rollback()
            finally:
                await async_engine.dispose()

        loop = ChildLoop()
        try:
            loop.invoke(scenario)
        finally:
            loop.close()
