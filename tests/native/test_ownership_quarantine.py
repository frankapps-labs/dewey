"""Malformed ownership remains fenced without starving healthy queue maintenance."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import update

from dewey._worker.pool import Assignment
from dewey.sqlalchemy.models import TaskEntryModel as Task
from tests.native.test_remote import runner as runner
from tests.native.test_storage import create, expire_lease, snapshot


@pytest.fixture(params=["native", "pull"])
def maintenance(request, store, runner):
    def call(operation, *args):
        if request.param == "native":
            return store(operation, *args)
        return runner("check_readiness" if operation == "readiness" else operation, *args)

    return call


@pytest.mark.parametrize(
    "corruption,reason",
    [
        ({"worker_id": None}, "Incomplete native ownership tuple"),
        ({"dispatch_token": None}, "Incomplete native ownership tuple"),
        ({"lease_expires_at": None}, "Incomplete native ownership tuple"),
        ({"execution_deadline_at": None}, "Incomplete native ownership tuple"),
        ({"status": "pending"}, "Native ownership on an inactive task"),
        (
            {"status": "processing", "execution_token": None},
            "Native processing task has no execution token",
        ),
    ],
)
def test_malformed_row_is_reported_unchanged_while_healthy_work_recovers(
    maintenance, engine, caplog, corruption, reason
):
    bad_id = create(engine)
    bad_assignment = maintenance("claim", 1)[0]
    healthy_id = create(engine)
    maintenance("claim", 1)
    expire_lease(engine, bad_id, deadline=True)
    expire_lease(engine, healthy_id)
    with engine.begin() as connection:
        connection.execute(update(Task).where(Task.id == bad_id).values(**corruption))
    before = snapshot(engine, bad_id)
    maintenance("readiness")
    assert maintenance("recover", 1) == [healthy_id]
    assert bad_id in caplog.text and reason in caplog.text
    assert bad_assignment.dispatch_token not in caplog.text
    assert snapshot(engine, bad_id) == before
    fresh = maintenance("claim", 1)
    assert [ticket.task_id for ticket in fresh] == [healthy_id]
    assert snapshot(engine, bad_id) == before
    assert maintenance("recover", 1) == []


def test_quarantine_diagnostics_are_bounded_and_do_not_consume_recovery_capacity(
    maintenance, engine, caplog
):
    old = datetime.now(UTC) - timedelta(seconds=1)
    for _ in range(105):
        create(engine, dispatch_token=str(uuid4()), lease_expires_at=old)
    healthy_id = create(engine)
    maintenance("claim", 1)
    expire_lease(engine, healthy_id)
    maintenance("readiness")
    reports = [r for r in caplog.records if "quarantined" in r.message.lower()]
    assert len(reports) == 100
    caplog.clear()
    assert maintenance("recover", 1) == [healthy_id]
    reports = [r for r in caplog.records if "quarantined" in r.message.lower()]
    assert len(reports) == 100


def test_pull_runner_never_releases_malformed_ownership(runner, engine):
    task_id = create(engine)
    assignment = runner("claim")[0]
    with engine.begin() as connection:
        connection.execute(update(Task).where(Task.id == task_id).values(worker_id=None))
    before = snapshot(engine, task_id)
    with pytest.raises(RuntimeError, match="Incomplete native ownership tuple"):
        runner("release", Assignment(task_id, assignment.dispatch_token))
    assert snapshot(engine, task_id) == before
