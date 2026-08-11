"""Tests for the optional ``backoff`` override on process_task.

The policy's backoff is the normal path; this argument exists so a caller can pin the
timing deterministically, which tests rely on.

Covers:
- Default behavior unchanged when ``backoff`` is not passed.
- A custom backoff is honoured on failure.
- Sync and async parity.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from dewey.core.states import TaskStatus
from dewey.sqlalchemy.async_executor import create_task_async, process_task_async
from dewey.sqlalchemy.executor import create_task, process_task
from dewey.sqlalchemy.models import TaskEntryModel

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def always_fail(**kwargs):
    raise RuntimeError("boom")


async def always_fail_async(**kwargs):
    raise RuntimeError("boom")


class TestSyncTaskBackoff:
    def test_custom_backoff_used_on_failure(self, session):
        task = create_task(session, task_type="t", max_attempts=5)
        session.commit()
        task_id = task.id

        before = datetime.now(UTC)
        process_task(session, task_id, always_fail, backoff=lambda a: timedelta(seconds=7))

        row = session.execute(
            TaskEntryModel.__table__.select().where(TaskEntryModel.id == task_id)
        ).one()
        assert row.status == TaskStatus.FAILED.value
        # scheduled_for should be ~7s in the future (within a generous window).
        delta = (row.scheduled_for - before).total_seconds()
        assert 6 <= delta <= 12, f"expected ~7s, got {delta}s"

    def test_default_backoff_when_omitted(self, session):
        task = create_task(session, task_type="t", max_attempts=5)
        session.commit()
        task_id = task.id

        before = datetime.now(UTC)
        process_task(session, task_id, always_fail)

        row = session.execute(
            TaskEntryModel.__table__.select().where(TaskEntryModel.id == task_id)
        ).one()
        # Default task backoff is ~120s base (±25% jitter): expect well over 7s.
        delta = (row.scheduled_for - before).total_seconds()
        assert delta > 30, f"expected default backoff > 30s, got {delta}s"


# ---------------------------------------------------------------------------
# Async task
# ---------------------------------------------------------------------------


class TestAsyncTaskBackoff:
    @pytest.mark.asyncio
    async def test_custom_backoff_used_on_failure(self, async_session):
        task = await create_task_async(async_session, task_type="t", max_attempts=5)
        await async_session.commit()
        task_id = task.id

        before = datetime.now(UTC)
        await process_task_async(
            async_session, task_id, always_fail_async, backoff=lambda a: timedelta(seconds=3)
        )

        result = await async_session.execute(
            TaskEntryModel.__table__.select().where(TaskEntryModel.id == task_id)
        )
        row = result.one()
        delta = (row.scheduled_for - before).total_seconds()
        assert 2 <= delta <= 8, f"expected ~3s, got {delta}s"
