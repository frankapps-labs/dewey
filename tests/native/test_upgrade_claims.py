"""0.5 to 0.6 on a live ledger: what the worker does with claims the removed path left."""

import pytest
from sqlalchemy.orm import Session

from dewey.sqlalchemy.upgrade import reset_legacy_claims
from tests.native.test_storage import create, snapshot


@pytest.mark.parametrize("status", ["dispatching", "processing"])
def test_a_legacy_claim_is_stranded_until_the_upgrade_step_releases_it(store, engine, status):
    task_id = create(engine, status=status, attempts=1, max_attempts=3)
    stranded = snapshot(engine, task_id)
    # No lease to expire and no owner: neither recovery nor a claim will ever reach it.
    assert store("recover", 100) == [] and store("claim", 10) == []
    assert snapshot(engine, task_id) == stranded
    with Session(engine) as session, session.begin():
        assert reset_legacy_claims(session) == 1
    (assignment,) = store("claim", 10)
    assert assignment.task_id == task_id
    invocation = store("start", assignment)
    # The upgrade consumed no attempt: an interrupted one was handed back.
    assert invocation["attempts"] == (1 if status == "processing" else 2)
    assert store("finish", assignment, invocation["execution_token"])
    assert snapshot(engine, task_id)["status"] == "completed"
