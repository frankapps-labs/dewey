"""0.5 → 0.6: rows claimed by the removed broker path become claimable again."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from dewey.core.states import TaskStatus
from dewey.core.upgrade import RESET_LEGACY_CLAIMS_SQL
from dewey.sqlalchemy.models import TaskEntryModel
from dewey.sqlalchemy.upgrade import reset_legacy_claims, reset_legacy_claims_async


def _rows(session: Session) -> dict[str, str]:
    now = datetime.now(UTC)
    lease = {
        "dispatch_token": "d" * 36,
        "worker_id": "w" * 36,
        "lease_expires_at": now + timedelta(minutes=5),
        "execution_deadline_at": now + timedelta(minutes=5),
    }
    rows = {
        "legacy_dispatching": TaskEntryModel(
            task_type="t", status=TaskStatus.DISPATCHING.value, dispatching_at=now, attempts=1
        ),
        "legacy_processing": TaskEntryModel(
            task_type="t",
            status=TaskStatus.PROCESSING.value,
            started_at=now,
            attempts=2,
            execution_token="e" * 36,
        ),
        "native_processing": TaskEntryModel(
            task_type="t",
            status=TaskStatus.PROCESSING.value,
            attempts=1,
            execution_token="n" * 36,
            **lease,
        ),
        "failed": TaskEntryModel(task_type="t", status=TaskStatus.FAILED.value, attempts=1),
    }
    session.add_all(rows.values())
    session.flush()
    return {name: row.id for name, row in rows.items()}


def _assert_reset(session: Session, ids: dict[str, str]) -> None:
    session.expire_all()
    get = lambda name: session.get(TaskEntryModel, ids[name])  # noqa: E731
    dispatching, processing = get("legacy_dispatching"), get("legacy_processing")
    assert (dispatching.status, dispatching.attempts, dispatching.dispatching_at) == (
        "pending",
        1,
        None,
    )
    # The interrupted attempt is handed back with the row.
    assert (processing.status, processing.attempts) == ("pending", 1)
    assert processing.execution_token is None and processing.started_at is None
    assert get("native_processing").status == "processing"
    assert get("failed").status == "failed"


def test_legacy_claims_are_released_and_native_rows_untouched(session):
    ids = _rows(session)
    assert reset_legacy_claims(session) == 2
    _assert_reset(session, ids)
    assert reset_legacy_claims(session) == 0


@pytest.mark.asyncio
async def test_async_helper_runs_the_same_step(async_session):
    ids = await async_session.run_sync(_rows)
    assert await reset_legacy_claims_async(async_session) == 2
    await async_session.run_sync(_assert_reset, ids)


def test_django_migration_runs_the_shared_step():
    from importlib import import_module

    migration = import_module("dewey.django.migrations.0008_native_only").Migration
    assert migration.operations[0].sql == RESET_LEGACY_CLAIMS_SQL


OWNERSHIP = ("dispatch_token", "worker_id", "lease_expires_at", "execution_deadline_at")


def _matrix() -> dict[str, dict]:
    """Every kind of row the step can meet, by name."""
    now = datetime.now(UTC)
    owned = {
        "dispatch_token": "d" * 36,
        "worker_id": "w" * 36,
        "lease_expires_at": now + timedelta(minutes=1),
        "execution_deadline_at": now + timedelta(minutes=2),
    }
    rows: dict[str, dict] = {"legacy_first_attempt": {"status": "processing", "attempts": 0}}
    for status in ("dispatching", "processing"):
        rows[f"legacy_{status}"] = {"status": status}
        rows[f"native_{status}"] = {"status": status, **owned}
        for field in OWNERSHIP:  # damaged ownership is for the worker's quarantine, not for us
            rows[f"partly_owned_{status}_{field}"] = {"status": status, field: owned[field]}
    for status in ("pending", "failed", "dead", "completed", "expired"):
        rows[status] = {"status": status}
    return {
        name: {
            "task_type": "upgrade.work",
            "attempts": 2,
            "max_attempts": 3,
            "error": "the previous error",
            "execution_token": "e" * 36,
            "scheduled_for": now,
            "started_at": now - timedelta(minutes=10),
            "dispatching_at": now - timedelta(minutes=11),
            **values,
        }
        for name, values in rows.items()
    }


def _seed(session: Session) -> dict[str, str]:
    rows = {name: TaskEntryModel(**values) for name, values in _matrix().items()}
    session.add_all(rows.values())
    session.flush()
    return {name: row.id for name, row in rows.items()}


def _snapshot(session: Session, ids: dict[str, str]) -> dict[str, dict]:
    by_id = {
        row["id"]: dict(row) for row in session.execute(select(TaskEntryModel.__table__)).mappings()
    }
    return {name: by_id[task_id] for name, task_id in ids.items()}


def _assert_only_legacy_claims_changed(before: dict[str, dict], after: dict[str, dict]) -> None:
    cleared = {"execution_token", "dispatching_at", "started_at"}
    for name, original in before.items():
        row = after[name]
        if not name.startswith("legacy_"):
            assert row == original, name
            continue
        handed_back = 1 if original["status"] == "processing" and original["attempts"] else 0
        assert (row["status"], row["attempts"]) == ("pending", original["attempts"] - handed_back)
        assert {row[field] for field in cleared} == {None}
        assert row["updated_at"] >= original["updated_at"]
        untouched = set(row) - cleared - {"status", "attempts", "updated_at"}
        assert {field: row[field] for field in untouched} == {
            field: original[field] for field in untouched
        }


@pytest.mark.parametrize("documented_sql", [False, True], ids=["helper", "documented-sql"])
def test_only_ownerless_claims_are_released_and_a_rollback_undoes_it(session, documented_sql):
    ids = _seed(session)
    session.commit()
    before = _snapshot(session, ids)
    if documented_sql:
        assert session.execute(text(RESET_LEGACY_CLAIMS_SQL)).rowcount == 3
    else:
        assert reset_legacy_claims(session) == 3
    _assert_only_legacy_claims_changed(before, _snapshot(session, ids))
    session.rollback()  # the step commits nothing itself
    assert _snapshot(session, ids) == before
    assert reset_legacy_claims(session) == 3
    session.commit()
    after = _snapshot(session, ids)
    assert reset_legacy_claims(session) == 0 and _snapshot(session, ids) == after


@pytest.mark.asyncio
async def test_the_async_helper_selects_the_same_rows(async_session):
    ids = await async_session.run_sync(_seed)
    await async_session.commit()
    before = await async_session.run_sync(_snapshot, ids)
    assert await reset_legacy_claims_async(async_session) == 3
    _assert_only_legacy_claims_changed(before, await async_session.run_sync(_snapshot, ids))
    await async_session.rollback()
    assert await async_session.run_sync(_snapshot, ids) == before


@pytest.mark.django_db(transaction=True)
def test_the_django_migration_releases_the_same_rows_and_reversing_it_keeps_them():
    from django.db import connection
    from django.db.migrations.executor import MigrationExecutor

    before_step = [("dewey", "0007_task_execution_budget")]
    with_step = [("dewey", "0008_native_only")]
    latest = MigrationExecutor(connection).loader.graph.leaf_nodes("dewey")
    executor = MigrationExecutor(connection)
    executor.migrate(before_step)
    try:
        tasks = executor.loader.project_state(before_step).apps.get_model("dewey", "TaskEntry")
        ids = {name: tasks.objects.create(**values).pk for name, values in _matrix().items()}

        def read() -> dict[str, dict]:
            by_id = {row["id"]: row for row in tasks.objects.values()}
            return {name: by_id[task_id] for name, task_id in ids.items()}

        before = read()
        MigrationExecutor(connection).migrate(with_step)
        after = read()
        _assert_only_legacy_claims_changed(before, after)
        MigrationExecutor(connection).migrate(before_step)  # backwards: nothing is re-claimed
        assert read() == after
        MigrationExecutor(connection).migrate(with_step)
        assert read() == after
    finally:
        MigrationExecutor(connection).migrate(latest)
