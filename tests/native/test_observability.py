"""Legacy sweep-candidate queries exclude complete and partial native ownership."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session

from dewey._worker.loop import ChildLoop
from dewey.sqlalchemy.async_queries import get_stuck_async
from dewey.sqlalchemy.models import TaskEntryModel as Task
from dewey.sqlalchemy.queries import get_stuck
from tests.native.test_storage import create, snapshot


@pytest.mark.parametrize(
    "tag",
    ["complete", "dispatch_token", "worker_id", "lease_expires_at", "execution_deadline_at"],
)
def test_get_stuck_preserves_legacy_candidates_and_excludes_any_native_tuple(
    store, engine, monkeypatch, tag
):
    """Operators must not see leased or malformed native rows as legacy sweep work."""
    old = datetime.now(UTC) - timedelta(hours=1)
    legacy = create(engine, status="processing", started_at=old, attempts=1)
    if tag == "complete":
        native = create(engine)
        assignment = store("claim", 1)[0]
        store("start", assignment)
        with engine.begin() as connection:
            connection.execute(update(Task).where(Task.id == native).values(started_at=old))
    else:
        value = datetime.now(UTC) + timedelta(minutes=10) if tag.endswith("_at") else str(uuid4())
        native = create(engine, status="processing", started_at=old, attempts=1, **{tag: value})
    before = {task_id: snapshot(engine, task_id) for task_id in (legacy, native)}
    if store.mode == "django":
        from django.db import router

        from dewey.django.queries import get_stuck as django_get_stuck

        # The native fixture's alias targets the owned SQLAlchemy physical DB;
        # do not accidentally query pytest-django's separate test-default DB.
        monkeypatch.setattr(router, "db_for_read", lambda model, **hints: "native")
        rows = django_get_stuck()
    elif store.mode == "async":

        async def query():
            async_engine = create_async_engine(os.environ["DEWEY_TEST_DATABASE_URL_ASYNC"])
            try:
                async with async_sessionmaker(async_engine)() as session:
                    return await get_stuck_async(session)
            finally:
                await async_engine.dispose()

        loop = ChildLoop()
        try:
            rows = loop.invoke(query)
        finally:
            loop.close()
    else:
        with Session(engine) as session:
            rows = get_stuck(session)
    assert [row.id for row in rows] == [legacy]
    assert {task_id: snapshot(engine, task_id) for task_id in before} == before
