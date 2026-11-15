"""A pull-runner host is told what its calls changed once it commits them, never before."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session

from dewey import alerts
from dewey.runner import FailureReport, RunnerPolicy, RunnerTiming
from dewey.sqlalchemy.executor import create_task
from dewey.sqlalchemy.runner import AsyncSQLAlchemyRunner, SQLAlchemyRunner

POLICY = RunnerPolicy("synthetic-host", ["default"], RunnerTiming(30, 30, 2, 1, 0.1, 1, 60))


@pytest.fixture
def heard():
    events = []
    alerts.on_alert(events.append)
    yield events
    alerts.on_alert(None)


FAILED = FailureReport("non_retryable", "synthetic failure")


@pytest.fixture
def doomed(engine):
    """A task whose one attempt is about to fail: a dead letter."""
    with Session(engine) as session, session.begin():
        return create_task(session, task_type="work.doomed", max_attempts=1).id


def dead_letter(api) -> None:
    (assignment,) = api.claim()
    assert api.fail(assignment, api.start(assignment)["execution_token"], FAILED)


@pytest.mark.parametrize("commits", [True, False])
def test_a_sync_host_delivers_alerts_after_its_commit_and_none_after_a_rollback(
    engine, heard, doomed, commits
):
    with engine.connect() as connection:
        root = connection.begin()
        with Session(bind=connection, join_transaction_mode="rollback_only") as session:
            api = SQLAlchemyRunner(session, POLICY)
            dead_letter(api)
        root.commit() if commits else root.rollback()
        assert heard == []  # nothing is sent until the host says its commit succeeded
        api.deliver_alerts()
        api.deliver_alerts()  # and nothing twice
    assert [(event["event"], event["task_id"]) for event in heard] == (
        [("task_dead", doomed)] if commits else []
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("commits", [True, False])
async def test_an_async_host_delivers_alerts_after_its_commit_and_none_after_a_rollback(
    engine, heard, doomed, commits
):
    async_engine = create_async_engine(engine.url.set(drivername="postgresql+asyncpg"))
    try:
        async with async_engine.connect() as connection:
            root = await connection.begin()
            async with AsyncSession(
                bind=connection, join_transaction_mode="rollback_only"
            ) as session:
                api = AsyncSQLAlchemyRunner(session, POLICY)
                (assignment,) = await api.claim()
                token = (await api.start(assignment))["execution_token"]
                assert await api.fail(assignment, token, FAILED)
            await (root.commit() if commits else root.rollback())
            assert heard == []
            api.deliver_alerts()
    finally:
        await async_engine.dispose()
    assert [event["event"] for event in heard] == (["task_dead"] if commits else [])


@pytest.mark.parametrize("store", ["django"], indirect=True)
@pytest.mark.parametrize("commits", [True, False])
def test_a_django_host_hears_once_its_atomic_block_commits(store, heard, doomed, commits):
    from django.db import transaction

    from dewey.django.runner import DjangoRunner

    class ChangedItsMind(Exception):
        pass

    try:
        with transaction.atomic(using="native"):
            dead_letter(DjangoRunner(POLICY, using="native"))
            assert heard == []
            if not commits:
                raise ChangedItsMind
    except ChangedItsMind:
        pass
    assert [event["event"] for event in heard] == (["task_dead"] if commits else [])
