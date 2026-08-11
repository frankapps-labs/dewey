"""Tests for the Huey adapter — transport only, no scheduling authority.

Uses ``SqliteHuey(immediate=True)`` so tasks execute synchronously: no Redis and
no consumer process needed to prove the contract.
"""

import logging
import uuid
from pathlib import Path

import pytest
from huey import SqliteHuey
from sqlalchemy import select
from sqlalchemy.orm import Session

from dewey.adapters.base import DispatcherAdapter
from dewey.adapters.huey import HueyAdapter
from dewey.core.states import TaskStatus
from dewey.sqlalchemy.executor import create_task, process_task
from dewey.sqlalchemy.models import TaskEntryModel


@pytest.fixture
def huey(tmp_path: Path):
    """Immediate-mode Huey. Unique file per test so registrations cannot leak."""
    return SqliteHuey(filename=str(tmp_path / f"huey-{uuid.uuid4().hex}.db"), immediate=True)


@pytest.fixture
def adapter(huey):
    return HueyAdapter(huey)


class TestContract:
    def test_satisfies_the_dispatcher_adapter_protocol(self, adapter):
        assert isinstance(adapter, DispatcherAdapter)

    def test_registers_under_a_stable_task_name(self, huey):
        """The name is the contract between the dispatcher and the worker pool."""
        adapter = HueyAdapter(huey, task_name="custom_dewey_name")
        adapter.register(lambda task_id: None)
        registered = list(huey._registry._registry)
        assert any(name.endswith(".custom_dewey_name") for name in registered), registered

    def test_dispatch_hands_the_task_id_to_the_worker(self, adapter):
        seen = []
        adapter.register(lambda task_id: seen.append(task_id))

        adapter.dispatch("task-abc")

        assert seen == ["task-abc"]

    def test_dispatch_before_register_explains_the_fix(self, adapter):
        with pytest.raises(RuntimeError, match="register"):
            adapter.dispatch("task-abc")

    def test_registering_twice_is_refused(self, adapter):
        adapter.register(lambda task_id: None)
        with pytest.raises(RuntimeError, match="twice"):
            adapter.register(lambda task_id: None)

    def test_dewey_keeps_retry_authority(self, adapter, huey):
        """Huey must not retry underneath Dewey, or a task runs twice."""
        adapter.register(lambda task_id: None)
        assert adapter._process_task.settings["default_retries"] == 0
        task_class = huey._registry._registry[
            next(n for n in huey._registry._registry if n.endswith(".dewey_process_task"))
        ]
        assert task_class.default_retries == 0

    def test_no_legacy_producer_api_remains(self, adapter):
        assert not hasattr(adapter, "enqueue")
        assert not hasattr(adapter, "enqueue_sweep")
        assert not hasattr(adapter, "setup")


class TestDuplicateDelivery:
    def test_a_second_delivery_of_a_finished_task_is_harmless(self, adapter, session, engine):
        """Redis re-delivery, or a sweep racing a slow worker, must not re-run work."""
        runs = []

        def _process(task_id: str) -> bool:
            with Session(engine) as worker_session:
                return process_task(worker_session, task_id, lambda: runs.append(1))

        adapter.register(_process)

        task = create_task(session, task_type="test.duplicate")
        session.commit()

        adapter.dispatch(task.id)
        adapter.dispatch(task.id)

        assert runs == [1]
        updated = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == task.id)
        ).scalar_one()
        session.refresh(updated)
        assert updated.status == TaskStatus.COMPLETED.value
        assert updated.attempts == 1


class TestFullLifecycle:
    def test_dispatch_runs_the_registered_handler_and_completes_the_row(
        self, adapter, session, engine
    ):
        handler_args = []

        def _process(task_id: str) -> bool:
            with Session(engine) as worker_session:
                return process_task(
                    worker_session, task_id, lambda **kwargs: handler_args.append(kwargs)
                )

        adapter.register(_process)

        task = create_task(session, task_type="test.huey", kwargs={"key": "val"})
        session.commit()

        adapter.dispatch(task.id)

        updated = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == task.id)
        ).scalar_one()
        session.refresh(updated)
        assert updated.status == TaskStatus.COMPLETED.value
        assert updated.attempts == 1
        assert handler_args == [{"key": "val"}]

    def test_a_failing_handler_leaves_dewey_in_charge_of_the_retry(
        self, adapter, session, engine, caplog
    ):
        def _process(task_id: str) -> bool:
            def boom() -> None:
                raise ValueError("handler exploded")

            with Session(engine) as worker_session:
                return process_task(worker_session, task_id, boom)

        adapter.register(_process)

        task = create_task(session, task_type="test.huey", max_attempts=3)
        session.commit()

        with caplog.at_level(logging.WARNING):
            adapter.dispatch(task.id)

        updated = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == task.id)
        ).scalar_one()
        session.refresh(updated)
        assert updated.status == TaskStatus.FAILED.value
        assert updated.scheduled_for is not None  # Dewey scheduled the retry, not Huey
        assert "handler exploded" in updated.error
