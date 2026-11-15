"""Synthetic application bootstrap for real spawned native runner acceptance."""

import asyncio
import inspect
import json
import logging
import os
from pathlib import Path

import dewey
from dewey._worker.contracts import Timing
from dewey._worker.runtime import LedgerRuntime

TIMING = Timing(30, 30, 2, 1, 0.1, 1, 60)


@dewey.task("native.synthetic.sync")
def sync_effect(path):
    Path(path).touch(exist_ok=True)


@dewey.task("native.synthetic.async")
async def async_effect(path):
    await asyncio.sleep(0)
    Path(path).touch(exist_ok=True)


@dewey.task("native.synthetic.crash")
def crash():
    os._exit(23)


@dewey.task("native.synthetic.fail", backoff=dewey.Constant(3600))
def fail():
    raise ValueError("synthetic handler failure")


class _JsonLines(logging.Formatter):
    """What a JSON log handler would ship: level, message, exception and the fields."""

    def format(self, record):
        return json.dumps(
            {
                "level": record.levelname,
                "message": record.getMessage(),
                "exception": self.formatException(record.exc_info) if record.exc_info else None,
                "pid": os.getpid(),
                **{
                    name: getattr(record, name, None)
                    for name in ("task_id", "task_type", "queue", "attempt", "max_attempts")
                },
            }
        )


def capture_worker_logs():
    """Append the task process's ``dewey.worker`` records to the file the test named."""
    path = os.environ.get("DEWEY_NATIVE_TEST_LOG")
    if path is None:
        return
    handler = logging.FileHandler(path)
    handler.setFormatter(_JsonLines())
    logger = logging.getLogger("dewey.worker")
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)


@dewey.task("native.synthetic.backend_pid")
def backend_pid(path):
    """Record which Postgres backend this process's Django worker connection talks to."""
    from django.db import connections

    with connections[os.environ.get("DEWEY_NATIVE_TEST_ALIAS", "default")].cursor() as cursor:
        cursor.execute("SELECT pg_backend_pid()")
        Path(path).write_text(str(cursor.fetchone()[0]))


def record_context(path):
    context = dewey.current_task()
    with Path(path).open("a") as output:
        output.write(
            json.dumps(
                {
                    "task_id": context.task_id,
                    "attempt": context.attempt,
                    "effect_key": context.effect_key("capture"),
                }
            )
            + "\n"
        )
    if context.attempt == 1:
        raise ValueError("Synthetic provider response lost after effect")


@dewey.task("native.synthetic.context.sync", backoff=dewey.Constant(0), max_attempts=2)
def context_sync(path):
    record_context(path)


@dewey.task("native.synthetic.context.async", backoff=dewey.Constant(0), max_attempts=2)
async def context_async(path):
    await asyncio.sleep(0)
    record_context(path)


class CompletionFault:
    """Synthetic completion uncertainty; never modifies real connection settings."""

    def __init__(self, storage):
        self.storage = storage

    def call(self, operation, *args):
        fault = os.environ.get("DEWEY_NATIVE_TEST_COMPLETION_FAULT")
        if operation == "finish" and fault == "before":
            raise ConnectionError("Synthetic pre-commit uncertainty")
        result = self.storage.call(operation, *args)
        if inspect.isawaitable(result):
            return self.await_result(result, operation, fault)
        return self.check_result(result, operation, fault)

    @staticmethod
    def check_result(result, operation, fault):
        if operation == "finish" and fault == "after":
            raise ConnectionError("Synthetic post-commit uncertainty")
        return result

    async def await_result(self, result, operation, fault):
        return self.check_result(await result, operation, fault)


def capture_spans():
    """Trace this task process into the file the test named, through the real SDK."""
    if os.environ.get("DEWEY_NATIVE_TEST_SPANS") is None:
        return
    from dewey.core.tracing import configure_tracing
    from tests._helpers.tracing import install_provider

    install_provider()  # a no-op in a forked child, which inherited the parent's
    configure_tracing(True)


def create_runtime():
    from dewey.core.execution import configure_failure_details

    mode = os.environ["DEWEY_NATIVE_TEST_MODE"]
    owner = os.environ["DEWEY_NATIVE_TEST_OWNER"]
    configure_failure_details(os.environ.get("DEWEY_NATIVE_TEST_FAILURE_DETAILS", "type"))
    capture_worker_logs()
    capture_spans()
    if mode == "django":
        import django

        django.setup()
        from django.db import connections

        from dewey.django.native import NativeDjangoStorage

        alias = os.environ.get("DEWEY_NATIVE_TEST_ALIAS", "default")
        connections[alias].settings_dict.setdefault("OPTIONS", {})["connect_timeout"] = 1
        storage = NativeDjangoStorage(owner=owner, timing=TIMING, queues=["default"], using=alias)
        return LedgerRuntime(CompletionFault(storage), close_resources=connections.close_all)
    if mode == "async":
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from dewey.sqlalchemy.native import AsyncNativeSQLAlchemyStorage

        engine = create_async_engine(
            os.environ["DEWEY_TEST_DATABASE_URL_ASYNC"],
            connect_args={"timeout": 1, "command_timeout": 1},
        )
        storage = AsyncNativeSQLAlchemyStorage(
            async_sessionmaker(engine), owner=owner, timing=TIMING, queues=["default"]
        )
    else:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        from dewey.sqlalchemy.native import NativeSQLAlchemyStorage

        engine = create_engine(
            os.environ["DEWEY_TEST_DATABASE_URL"], connect_args={"connect_timeout": 1}
        )
        storage = NativeSQLAlchemyStorage(
            sessionmaker(engine), owner=owner, timing=TIMING, queues=["default"]
        )
    return LedgerRuntime(CompletionFault(storage), close_resources=engine.dispose)
