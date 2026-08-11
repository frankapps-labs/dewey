"""Policy-driven execution: typed errors, registry handlers, RetryAfter timing."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select

import dewey
from dewey.core.states import TaskStatus
from dewey.errors import NonRetryableError, RetryAfter, TransientError
from dewey.policy import Constant, clear_project_policies, configure_policies, registry
from dewey.sqlalchemy.executor import create_task, process_task
from dewey.sqlalchemy.models import TaskEntryModel


@pytest.fixture(autouse=True)
def _clean_registry():
    registry.clear()
    clear_project_policies()
    yield
    registry.clear()
    clear_project_policies()


def _row(session, task_id) -> TaskEntryModel:
    return session.execute(select(TaskEntryModel).where(TaskEntryModel.id == task_id)).scalar_one()


class TestProducerDefaultsFromPolicy:
    def test_queue_priority_and_budget_come_from_the_policy(self, session):
        @dewey.task("orders.confirm", queue="critical", priority=7, max_attempts=2)
        def confirm() -> None: ...

        task = create_task(session, task_type="orders.confirm")
        assert task.queue == "critical"
        assert task.priority == 7
        assert task.max_attempts == 2

    def test_explicit_arguments_still_win(self, session):
        @dewey.task("orders.confirm", queue="critical", max_attempts=2)
        def confirm() -> None: ...

        task = create_task(session, task_type="orders.confirm", queue="bulk", max_attempts=9)
        assert task.queue == "bulk"
        assert task.max_attempts == 9

    def test_project_config_reaches_the_producer(self, session):
        @dewey.task("orders.confirm", queue="critical")
        def confirm() -> None: ...

        configure_policies({"orders.confirm": {"queue": "overridden"}})
        assert create_task(session, task_type="orders.confirm").queue == "overridden"


class TestHandlerResolution:
    def test_handler_comes_from_the_registry(self, session):
        seen = []

        @dewey.task("orders.confirm")
        def confirm(order_id: int) -> None:
            seen.append(order_id)

        task = create_task(session, task_type="orders.confirm", args=[7])
        session.commit()

        assert process_task(session, task.id) is True
        assert seen == [7]
        assert _row(session, task.id).status == TaskStatus.COMPLETED.value

    def test_explicit_handler_overrides_the_registry(self, session):
        registered, explicit = [], []

        @dewey.task("orders.confirm")
        def confirm() -> None:
            registered.append(1)

        task = create_task(session, task_type="orders.confirm")
        session.commit()

        process_task(session, task.id, lambda: explicit.append(1))
        assert registered == [] and explicit == [1]

    def test_unknown_task_type_fails_the_attempt_instead_of_crashing(self, session):
        """A worker deployed before the handler must not destroy the work."""
        task = create_task(session, task_type="not.declared", max_attempts=3)
        session.commit()

        assert process_task(session, task.id) is False

        row = _row(session, task.id)
        assert row.status == TaskStatus.FAILED.value  # retries, does not dead-letter yet
        assert row.attempts == 1
        assert "No handler registered" in row.error
        assert row.scheduled_for is not None

    def test_unknown_task_type_eventually_dead_letters(self, session):
        task = create_task(session, task_type="not.declared", max_attempts=1)
        session.commit()

        process_task(session, task.id)
        assert _row(session, task.id).status == TaskStatus.DEAD.value


class TestTypedErrors:
    def test_transient_error_retries(self, session):
        @dewey.task("flaky", max_attempts=3, backoff=Constant(1))
        def flaky() -> None:
            raise TransientError("recipient offline")

        task = create_task(session, task_type="flaky")
        session.commit()

        assert process_task(session, task.id) is False
        row = _row(session, task.id)
        assert row.status == TaskStatus.FAILED.value
        assert row.attempts == 1

    def test_non_retryable_error_dead_letters_on_the_first_attempt(self, session):
        @dewey.task("malformed", max_attempts=5)
        def malformed() -> None:
            raise NonRetryableError("payload will never parse")

        task = create_task(session, task_type="malformed")
        session.commit()

        assert process_task(session, task.id) is False
        row = _row(session, task.id)
        assert row.status == TaskStatus.DEAD.value
        assert row.attempts == 1  # the rest of the budget is not burned
        assert "never parse" in row.error

    def test_exception_outside_retry_on_dead_letters_immediately(self, session):
        @dewey.task("narrow", max_attempts=5, retry_on=(ConnectionError,))
        def narrow() -> None:
            raise ValueError("programming error")

        task = create_task(session, task_type="narrow")
        session.commit()

        process_task(session, task.id)
        assert _row(session, task.id).status == TaskStatus.DEAD.value

    def test_exception_inside_retry_on_retries(self, session):
        @dewey.task("narrow", max_attempts=5, retry_on=(ConnectionError,), backoff=Constant(1))
        def narrow() -> None:
            raise ConnectionError("broker blinked")

        task = create_task(session, task_type="narrow")
        session.commit()

        process_task(session, task.id)
        assert _row(session, task.id).status == TaskStatus.FAILED.value


class TestRetryAfter:
    def test_handler_can_ask_for_a_longer_delay_than_the_policy(self, session):
        @dewey.task("rate.limited", max_attempts=5, backoff=Constant(3))
        def limited() -> None:
            raise RetryAfter(300, "provider said 300s")

        task = create_task(session, task_type="rate.limited")
        session.commit()
        before = datetime.now(UTC)

        process_task(session, task.id)

        row = _row(session, task.id)
        assert row.status == TaskStatus.FAILED.value
        waited = (row.scheduled_for - before).total_seconds()
        assert 295 <= waited <= 320

    def test_handler_cannot_retry_earlier_than_the_policy_allows(self, session):
        """A misbehaving provider must not pull retries inside our rate budget."""

        @dewey.task("rate.limited", max_attempts=5, backoff=Constant(60))
        def limited() -> None:
            raise RetryAfter(1)

        task = create_task(session, task_type="rate.limited")
        session.commit()
        before = datetime.now(UTC)

        process_task(session, task.id)

        waited = (_row(session, task.id).scheduled_for - before).total_seconds()
        assert 55 <= waited <= 70

    def test_retry_after_still_respects_the_attempt_budget(self, session):
        @dewey.task("rate.limited", max_attempts=1, backoff=Constant(1))
        def limited() -> None:
            raise RetryAfter(30)

        task = create_task(session, task_type="rate.limited")
        session.commit()

        process_task(session, task.id)
        assert _row(session, task.id).status == TaskStatus.DEAD.value

    def test_negative_retry_after_is_refused_at_construction(self):
        with pytest.raises(ValueError, match="must not be negative"):
            RetryAfter(-1)


class TestProducerSerialization:
    """create_task encodes arguments, and refuses what it cannot store honestly."""

    def test_rich_types_are_encoded_on_the_row(self, session):
        from datetime import date
        from decimal import Decimal
        from uuid import UUID

        task = create_task(
            session,
            task_type="invoice.send",
            args=[UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")],
            kwargs={"due": date(2026, 1, 31), "amount": Decimal("19.99")},
        )
        assert task.args == ["6ba7b810-9dad-11d1-80b4-00c04fd430c8"]
        assert task.kwargs == {"due": "2026-01-31", "amount": "19.99"}

    def test_bytes_are_refused_before_the_row_is_written(self, session):
        from dewey.errors import SerializationError

        with pytest.raises(SerializationError):
            create_task(session, task_type="invoice.send", kwargs={"blob": b"nope"})

        session.rollback()
        assert session.execute(select(TaskEntryModel)).scalars().all() == []

    def test_an_orm_instance_is_refused(self, session):
        from dewey.errors import SerializationError

        other = create_task(session, task_type="other")
        session.commit()

        with pytest.raises(SerializationError, match="TaskEntryModel"):
            create_task(session, task_type="invoice.send", args=[other])
