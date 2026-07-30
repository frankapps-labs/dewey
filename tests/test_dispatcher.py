"""Dispatcher behaviour: claiming, transport failure, sweep ticks, and the loop.

These run against real Postgres — ``SKIP LOCKED`` and committed claims are the
whole point, and neither can be proven against a fake.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

from dewey.core.states import TaskStatus
from dewey.dispatcher import Dispatcher
from dewey.sqlalchemy.dispatch import SQLAlchemyDispatchBackend
from dewey.sqlalchemy.executor import create_task, process_task
from dewey.sqlalchemy.models import TaskEntryModel


@pytest.fixture
def backend(engine):
    return SQLAlchemyDispatchBackend(engine)


@pytest.fixture
def collected():
    return []


@pytest.fixture
def dispatcher(backend, collected):
    return Dispatcher(
        backend,
        collected.append,
        batch_size=10,
        idle_poll_seconds=0.05,
        sweep_interval_seconds=None,
    )


def _status(session, task_id: str) -> str:
    return session.execute(
        select(TaskEntryModel.status).where(TaskEntryModel.id == task_id)
    ).scalar_one()


class TestClaim:
    def test_claims_a_pending_task_and_commits_it_as_dispatching(self, backend, session):
        task = create_task(session, task_type="t")
        session.commit()

        claimed = backend.claim(10)

        assert claimed == [task.id]
        # Committed, not just flushed: a crash here must not lose the claim.
        assert _status(session, task.id) == TaskStatus.DISPATCHING.value

    def test_a_claimed_task_is_not_claimed_again(self, backend, session):
        create_task(session, task_type="t")
        session.commit()

        assert len(backend.claim(10)) == 1
        assert backend.claim(10) == []

    def test_scheduled_work_is_invisible_until_it_is_due(self, backend, session):
        create_task(session, task_type="t", scheduled_for=datetime.now(UTC) + timedelta(minutes=5))
        session.commit()

        assert backend.claim(10) == []

    def test_due_scheduled_work_is_claimed(self, backend, session):
        task = create_task(
            session, task_type="t", scheduled_for=datetime.now(UTC) - timedelta(minutes=5)
        )
        session.commit()

        assert backend.claim(10) == [task.id]

    def test_higher_priority_goes_first(self, backend, session):
        low = create_task(session, task_type="t", priority=0)
        high = create_task(session, task_type="t", priority=100)
        session.commit()

        assert backend.claim(2) == [high.id, low.id]

    def test_equal_priority_is_oldest_first(self, backend, session):
        first = create_task(session, task_type="t")
        session.commit()
        second = create_task(session, task_type="t")
        session.commit()

        assert backend.claim(2) == [first.id, second.id]

    def test_batch_size_is_respected(self, backend, session):
        for _ in range(5):
            create_task(session, task_type="t")
        session.commit()

        assert len(backend.claim(2)) == 2

    def test_terminal_rows_are_never_claimed(self, backend, session):
        for status in (TaskStatus.COMPLETED, TaskStatus.DEAD, TaskStatus.PROCESSING):
            task = create_task(session, task_type="t")
            session.execute(
                update(TaskEntryModel)
                .where(TaskEntryModel.id == task.id)
                .values(status=status.value)
            )
        session.commit()

        assert backend.claim(10) == []

    def test_queue_scoping(self, engine, session):
        create_task(session, task_type="t", queue="bulk")
        critical = create_task(session, task_type="t", queue="critical")
        session.commit()

        scoped = SQLAlchemyDispatchBackend(engine, queues=["critical"])
        assert scoped.claim(10) == [critical.id]


class TestRelease:
    def test_release_returns_a_row_to_pending_without_burning_an_attempt(self, backend, session):
        task = create_task(session, task_type="t")
        session.commit()
        backend.claim(10)

        backend.release([task.id])

        row = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == task.id)
        ).scalar_one()
        session.refresh(row)
        assert row.status == TaskStatus.PENDING.value
        assert row.dispatching_at is None
        assert row.attempts == 0

    def test_release_ignores_rows_a_worker_already_started(self, backend, session, engine):
        """Releasing must never yank a task out from under a running worker."""
        task = create_task(session, task_type="t")
        session.commit()
        backend.claim(10)
        session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == task.id)
            .values(status=TaskStatus.PROCESSING.value)
        )
        session.commit()

        backend.release([task.id])

        assert _status(session, task.id) == TaskStatus.PROCESSING.value


class TestDispatchBatch:
    def test_dispatches_every_claimed_task(self, dispatcher, collected, session):
        ids = [create_task(session, task_type="t").id for _ in range(3)]
        session.commit()

        assert dispatcher.dispatch_batch() == 3
        assert sorted(collected) == sorted(ids)

    def test_an_empty_backlog_is_not_an_error(self, dispatcher):
        assert dispatcher.dispatch_batch() == 0

    def test_a_transport_failure_returns_work_to_pending_immediately(self, backend, session):
        """Broker down: rows go back to PENDING now, not after the dispatch timeout."""
        task = create_task(session, task_type="t")
        session.commit()

        def broken(task_id: str) -> None:
            raise ConnectionError("redis is down")

        dispatcher = Dispatcher(backend, broken, sweep_interval_seconds=None)
        assert dispatcher.dispatch_batch() == 0

        row = session.execute(
            select(TaskEntryModel).where(TaskEntryModel.id == task.id)
        ).scalar_one()
        session.refresh(row)
        assert row.status == TaskStatus.PENDING.value
        assert row.attempts == 0  # a transport failure is not a handler attempt

    def test_the_rest_of_the_batch_is_released_after_the_first_failure(self, backend, session):
        """One transport error means the broker is down; stop pushing at it."""
        for _ in range(5):
            create_task(session, task_type="t")
        session.commit()

        attempts: list[str] = []

        def fail_on_third(task_id: str) -> None:
            attempts.append(task_id)
            if len(attempts) == 3:
                raise ConnectionError("redis is down")

        dispatcher = Dispatcher(backend, fail_on_third, batch_size=5, sweep_interval_seconds=None)
        dispatched = dispatcher.dispatch_batch()

        assert dispatched == 2
        assert len(attempts) == 3  # stopped at the failure instead of trying all five
        statuses = session.execute(select(TaskEntryModel.status)).scalars().all()
        assert sorted(statuses) == sorted(
            [TaskStatus.DISPATCHING.value] * 2 + [TaskStatus.PENDING.value] * 3
        )

    def test_transport_failure_backs_off_and_recovers(self, backend, session):
        create_task(session, task_type="t")
        session.commit()
        failing = True

        def flaky(task_id: str) -> None:
            if failing:
                raise ConnectionError("redis is down")

        dispatcher = Dispatcher(backend, flaky, sweep_interval_seconds=None)

        dispatcher.dispatch_batch()
        assert dispatcher._retry_delay == pytest.approx(1.0)
        dispatcher.dispatch_batch()
        assert dispatcher._retry_delay == pytest.approx(2.0)  # doubles, does not spin

        failing = False
        dispatcher.dispatch_batch()
        assert dispatcher._retry_delay == 0.0  # reset once the broker answers

    def test_backoff_is_capped(self, backend):
        dispatcher = Dispatcher(
            backend,
            lambda task_id: None,
            dispatch_retry_base_seconds=1.0,
            dispatch_retry_cap_seconds=4.0,
        )
        for _ in range(10):
            dispatcher._note_dispatch_failure()
        assert dispatcher._retry_delay == 4.0


class TestSweepTick:
    def test_the_dispatcher_runs_the_sweep(self, backend, session):
        """Retries only become eligible when something runs the sweep."""
        task = create_task(session, task_type="t", max_attempts=5)
        session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == task.id)
            .values(
                status=TaskStatus.FAILED.value,
                attempts=1,
                scheduled_for=datetime.now(UTC) - timedelta(minutes=1),
            )
        )
        session.commit()

        dispatcher = Dispatcher(backend, lambda task_id: None, sweep_interval_seconds=60)
        result = dispatcher.maybe_sweep()

        assert result is not None
        assert result["failed"] == [task.id]
        assert _status(session, task.id) == TaskStatus.PENDING.value

    def test_the_sweep_does_not_run_again_inside_its_interval(self, backend):
        clock = [1000.0]
        dispatcher = Dispatcher(
            backend,
            lambda task_id: None,
            sweep_interval_seconds=60,
            monotonic=lambda: clock[0],
        )

        assert dispatcher.maybe_sweep() is not None  # first tick runs immediately
        clock[0] += 30
        assert dispatcher.maybe_sweep() is None
        clock[0] += 31
        assert dispatcher.maybe_sweep() is not None

    def test_sweep_can_be_disabled(self, backend):
        dispatcher = Dispatcher(backend, lambda task_id: None, sweep_interval_seconds=None)
        assert dispatcher.maybe_sweep() is None

    def test_a_failing_sweep_does_not_stop_dispatch(self, backend, session, collected):
        task = create_task(session, task_type="t")
        session.commit()

        def boom() -> dict[str, list[str]]:
            raise RuntimeError("sweep exploded")

        backend.run_sweep = boom  # type: ignore[method-assign]
        dispatcher = Dispatcher(
            backend, collected.append, idle_poll_seconds=0.05, sweep_interval_seconds=60
        )

        dispatcher.run(max_iterations=1)

        assert collected == [task.id]


class TestRunLoop:
    def test_run_stops_after_max_iterations(self, dispatcher):
        dispatcher.run(max_iterations=2)
        assert not dispatcher.stopped  # bounded, not asked to stop

    def test_stop_ends_the_loop(self, backend, session):
        create_task(session, task_type="t")
        session.commit()

        dispatcher = Dispatcher(
            backend, lambda task_id: None, idle_poll_seconds=0.05, sweep_interval_seconds=None
        )

        def stop_after_first(task_id: str) -> None:
            dispatcher.stop()

        dispatcher.dispatch_fn = stop_after_first
        dispatcher.run()

        assert dispatcher.stopped

    def test_a_full_batch_loops_without_waiting(self, backend, session, collected):
        """Draining a backlog must not pause for the poll interval between batches."""
        ids = [create_task(session, task_type="t").id for _ in range(4)]
        session.commit()

        waits: list[float] = []
        backend.wait_for_work = lambda timeout: waits.append(timeout) or False  # type: ignore[assignment]

        dispatcher = Dispatcher(
            backend, collected.append, batch_size=2, sweep_interval_seconds=None
        )
        dispatcher.run(max_iterations=2)

        assert sorted(collected) == sorted(ids)
        assert waits == []  # both batches were full, so neither waited

    def test_an_empty_claim_waits_for_work(self, backend, collected):
        waits: list[float] = []
        backend.wait_for_work = lambda timeout: waits.append(timeout) or False  # type: ignore[assignment]

        dispatcher = Dispatcher(
            backend,
            collected.append,
            idle_poll_seconds=0.25,
            sweep_interval_seconds=None,
        )
        dispatcher.run(max_iterations=1)

        assert waits == [0.25]

    def test_invalid_batch_size_is_refused(self, backend):
        with pytest.raises(ValueError, match="batch_size"):
            Dispatcher(backend, lambda task_id: None, batch_size=0)


class TestEndToEnd:
    def test_pending_to_completed_through_the_dispatcher(self, backend, session, engine):
        """The full path: create, claim, dispatch, process, complete."""
        import dewey

        seen = []

        @dewey.task("orders.confirm")
        def confirm(order_id: int) -> None:
            seen.append(order_id)

        try:
            task = create_task(session, task_type="orders.confirm", args=[7])
            session.commit()

            def transport(task_id: str) -> None:
                # Stand-in for a worker picking the ID off the broker.
                from sqlalchemy.orm import Session

                with Session(engine) as worker_session:
                    process_task(worker_session, task_id)

            dispatcher = Dispatcher(backend, transport, sweep_interval_seconds=None)
            assert dispatcher.dispatch_batch() == 1

            assert seen == [7]
            assert _status(session, task.id) == TaskStatus.COMPLETED.value
        finally:
            dewey.registry.clear()


class TestConcurrentDispatchers:
    """Several dispatchers must cooperate without handing the same row out twice."""

    def test_no_task_is_claimed_by_two_dispatchers(self, engine, session):
        import threading

        task_count = 60
        for _ in range(task_count):
            create_task(session, task_type="t")
        session.commit()

        claims: list[list[str]] = []
        errors: list[BaseException] = []
        lock = threading.Lock()
        start = threading.Barrier(4)

        def drain() -> None:
            backend = SQLAlchemyDispatchBackend(engine)
            mine: list[str] = []
            try:
                start.wait(timeout=10)
                while True:
                    batch = backend.claim(7)
                    if not batch:
                        break
                    mine.extend(batch)
            except BaseException as exc:  # surfaced in the parent, not swallowed
                with lock:
                    errors.append(exc)
            finally:
                backend.close()
                with lock:
                    claims.append(mine)

        threads = [threading.Thread(target=drain, name=f"dispatcher-{i}") for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert not errors, errors
        claimed = [task_id for batch in claims for task_id in batch]
        assert len(claimed) == task_count  # every task claimed
        assert len(set(claimed)) == task_count  # exactly once each
        assert sum(1 for batch in claims if batch) > 1  # work really was shared

        statuses = set(session.execute(select(TaskEntryModel.status)).scalars().all())
        assert statuses == {TaskStatus.DISPATCHING.value}
