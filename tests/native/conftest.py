"""Owned-Postgres native storage parity fixtures."""

import os
from copy import deepcopy
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

import dewey
from dewey._worker.contracts import Timing
from dewey._worker.loop import ChildLoop
from dewey.sqlalchemy.native import AsyncNativeSQLAlchemyStorage, NativeSQLAlchemyStorage


@pytest.fixture(scope="session")
def engine(engine):
    # Extend the parent's freshly created disposable schema, not a shared DB.
    # Django requires JSONB; SQLAlchemy supports both JSON and JSONB.
    with engine.begin() as connection:
        for column in ("args", "kwargs", "metadata"):
            connection.execute(
                text(
                    f'ALTER TABLE task_entries ALTER COLUMN "{column}" TYPE jsonb '
                    f'USING "{column}"::jsonb'
                )
            )
        connection.execute(
            text(
                "ALTER TABLE dewey_dispatcher_heartbeats ALTER COLUMN queues TYPE jsonb "
                "USING queues::jsonb"
            )
        )
    yield engine


@pytest.fixture(autouse=True)
def clean_schedule_state(engine):
    yield
    dewey.schedules.clear()
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM dewey_schedules"))
        connection.execute(text("DELETE FROM dewey_dispatcher_heartbeats"))


@pytest.fixture(params=["sync", "async", "django"])
def store(request, engine, django_db_blocker):
    owner = str(uuid4())
    timing = Timing(30, 30, 2, 1, 0.1, 1, 60)
    bridge = ChildLoop()
    async_engine = None
    with django_db_blocker.unblock():
        if request.param == "django":
            from django.db import connections

            from dewey.django.native import NativeDjangoStorage

            # pytest-django may point default at test_dewey_test. Match the owned
            # SQLAlchemy fixture's physical DB explicitly, never a shared alias.
            config = deepcopy(connections.databases["default"])
            config["NAME"] = engine.url.database
            connections.databases["native"] = config
            storage = NativeDjangoStorage(
                owner=owner, timing=timing, queues=["default"], using="native"
            )
            close_resources = connections.close_all
        elif request.param == "async":
            async_engine = create_async_engine(os.environ["DEWEY_TEST_DATABASE_URL_ASYNC"])
            storage = AsyncNativeSQLAlchemyStorage(
                async_sessionmaker(async_engine), owner=owner, timing=timing, queues=["default"]
            )
            close_resources = lambda: bridge.invoke(async_engine.dispose)  # noqa: E731
        else:
            storage = NativeSQLAlchemyStorage(
                sessionmaker(engine), owner=owner, timing=timing, queues=["default"]
            )
            close_resources = engine.dispose
        try:

            def call(operation, *args):
                return bridge.invoke(storage.call, operation, *args)

            call.storage = storage
            call.mode = request.param
            call.owner = owner
            call.close_resources = close_resources  # what a factory would hand the worker
            yield call
        finally:
            if async_engine is not None:
                bridge.invoke(async_engine.dispose)
            bridge.close()
            if request.param == "django":
                connections["native"].close()
                del connections["native"]
                del connections.databases["native"]
