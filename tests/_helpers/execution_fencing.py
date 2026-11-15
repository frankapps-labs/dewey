"""Spawned stale-writer probes: explicit isolated DB settings, no default connections."""

from __future__ import annotations

import asyncio


def execute_probe(backend, database, task_id, started, release, results, outcome):
    """Claim one task in a fresh process, gate its handler, then return/raise."""
    from dewey.errors import NonRetryableError

    def handler():
        started.set()
        if not release.wait(10):
            raise RuntimeError("synthetic-probe-barrier-timeout")
        if outcome == "retryable":
            raise RuntimeError("synthetic-probe-failure")
        if outcome == "nonretryable":
            raise NonRetryableError("synthetic-probe-failure")

    if backend == "django":
        import django
        from django.conf import settings

        settings.DATABASES["default"] = database
        django.setup()
        from django.db import connections

        from dewey.django.executor import process_task

        try:
            results.put(process_task(task_id, handler))
        finally:
            connections.close_all()
    elif backend == "sqlalchemy":
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session

        from dewey.sqlalchemy.executor import process_task

        engine = create_engine(database, pool_size=1, max_overflow=0)
        try:
            # Must refresh ownership even when the claimed object stays in the identity map.
            with Session(engine, expire_on_commit=False) as session:
                results.put(process_task(session, task_id, handler))
        finally:
            engine.dispose()
    else:
        from sqlalchemy.engine import make_url
        from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

        from dewey.sqlalchemy.async_executor import process_task_async

        async def run():
            engine = create_async_engine(
                make_url(database).set(drivername="postgresql+asyncpg"),
                pool_size=1,
                max_overflow=0,
            )

            async def async_handler():
                handler()

            try:
                async with AsyncSession(engine, expire_on_commit=False) as session:
                    results.put(await process_task_async(session, task_id, async_handler))
            finally:
                await engine.dispose()

        asyncio.run(run())
