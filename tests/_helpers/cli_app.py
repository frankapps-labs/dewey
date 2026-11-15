"""A small real application for exercising the ``dewey`` command end to end."""

import json
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import dewey

APP = "tests._helpers.cli_app:create_application"


def touch(marker: str) -> None:
    Path(marker).write_text(dewey.current_task().task_type)


def fail() -> None:
    raise dewey.NonRetryableError("synthetic permanent failure")


def record_alert(event: dict) -> None:
    with Path(os.environ["DEWEY_CLI_TEST_ALERTS"]).open("a") as stream:
        stream.write(json.dumps(event) + "\n")


def register() -> None:
    """Idempotent, so every process (and every test) sees the same declarations."""
    dewey.task("cli.touch")(touch)
    dewey.task("cli.fail")(fail)
    dewey.schedule(
        "cli-poll",
        task="cli.touch",
        when=dewey.every(minutes=1),
        kwargs={"marker": os.environ.get("DEWEY_CLI_TEST_TICK_MARKER", "unused")},
    )


def create_application(context):
    """SQLAlchemy by default; DEWEY_CLI_TEST_BACKEND selects ``async`` or ``django``."""
    register()
    backend = os.environ.get("DEWEY_CLI_TEST_BACKEND", "sync")
    if backend == "django":
        return context.django(using=os.environ.get("DEWEY_CLI_TEST_ALIAS", "native"))
    if backend == "async":
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        async_engine = create_async_engine(
            os.environ["DEWEY_TEST_DATABASE_URL_ASYNC"], pool_size=1, max_overflow=0
        )
        return context.async_sqlalchemy(
            async_sessionmaker(async_engine), close_resources=async_engine.dispose
        )
    engine = create_engine(os.environ["DEWEY_TEST_DATABASE_URL"], pool_size=1, max_overflow=0)
    return context.sqlalchemy(sessionmaker(engine), close_resources=engine.dispose)
