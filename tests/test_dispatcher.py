"""Dispatcher behaviour: claiming, transport failure, sweep ticks, and the loop.

These run against real Postgres — ``SKIP LOCKED`` and committed claims are the
whole point, and neither can be proven against a fake.
"""

from __future__ import annotations

import logging
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

    def test_due_failed_retry_is_claimed_without_a_sweep(self, backend, session):
        task = create_task(session, task_type="t", max_attempts=3)
        session.flush()
        session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == task.id)
            .values(
                status=TaskStatus.FAILED.value,
                attempts=1,
                scheduled_for=datetime.now(UTC) - timedelta(seconds=1),
            )
        )
        session.commit()

        assert backend.claim(10) == [task.id]
        assert _status(session, task.id) == TaskStatus.DISPATCHING.value

    def test_future_and_exhausted_failed_rows_are_not_claimed(self, backend, session):
        future = create_task(session, task_type="t", max_attempts=3)
        exhausted = create_task(session, task_type="t", max_attempts=1)
        session.flush()
        session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == future.id)
            .values(
                status=TaskStatus.FAILED.value,
                attempts=1,
                scheduled_for=datetime.now(UTC) + timedelta(minutes=1),
            )
        )
        session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == exhausted.id)
            .values(
                status=TaskStatus.FAILED.value,
                attempts=1,
                scheduled_for=datetime.now(UTC) - timedelta(seconds=1),
            )
        )
        session.commit()

        assert backend.claim(10) == []

    def test_expired_ready_row_is_terminalized_not_dispatched(self, backend, session):
        task = create_task(session, task_type="t")
        session.flush()
        session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == task.id)
            .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
        session.commit()

        assert backend.claim(10) == []
        row = session.get(TaskEntryModel, task.id)
        session.refresh(row)
        assert row.status == TaskStatus.EXPIRED.value
        assert row.expired_at is not None
        assert row.attempts == 0

    def test_next_due_uses_an_earlier_expiry(self, backend, session):
        now = datetime.now(UTC)
        task = create_task(session, task_type="t", scheduled_for=now + timedelta(minutes=5))
        session.flush()
        session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == task.id)
            .values(expires_at=now + timedelta(seconds=10))
        )
        session.commit()

        due = backend.next_due()
        assert due is not None
        assert abs((due - (now + timedelta(seconds=10))).total_seconds()) < 1

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

    def test_due_scheduled_work_competes_with_immediate_work_on_due_time(self, backend, session):
        """A due scheduled row must not queue behind every immediate row.

        Sorting NULL ``scheduled_for`` first meant a sustained stream of immediate
        work outranked scheduled and retried rows that were already due, starving
        them indefinitely. Ordering by coalesce(scheduled_for, created_at) makes
        them compete on how long each has actually been waiting.
        """
        older_immediate = create_task(session, task_type="t")
        overdue = create_task(
            session, task_type="t", scheduled_for=datetime.now(UTC) - timedelta(minutes=5)
        )
        newer_immediate = create_task(session, task_type="t")
        session.commit()
        session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == older_immediate.id)
            .values(created_at=datetime.now(UTC) - timedelta(minutes=10))
        )
        session.commit()

        # Waiting longest goes first: the overdue row beats immediate work created
        # after its due time, without jumping immediate work that was there before.
        assert backend.claim(3) == [older_immediate.id, overdue.id, newer_immediate.id]

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
        assert dispatcher._health.retry_delay == pytest.approx(1.0)
        dispatcher.dispatch_batch()
        assert dispatcher._health.retry_delay == pytest.approx(2.0)  # doubles, does not spin

        failing = False
        dispatcher.dispatch_batch()
        assert dispatcher._health.retry_delay == 0.0  # reset once the broker answers

    def test_backoff_is_capped(self, backend):
        dispatcher = Dispatcher(
            backend,
            lambda task_id: None,
            dispatch_retry_base_seconds=1.0,
            dispatch_retry_cap_seconds=4.0,
        )
        for _ in range(10):
            dispatcher._health.note_failure()
        assert dispatcher._health.retry_delay == 4.0


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

    def test_wait_is_shortened_to_the_next_due_row(self, backend, session, collected):
        now = datetime.now(UTC)
        create_task(session, task_type="t", scheduled_for=now + timedelta(seconds=2))
        session.commit()
        waits: list[float] = []
        backend.wait_for_work = lambda timeout: waits.append(timeout) or False  # type: ignore[assignment]

        Dispatcher(
            backend,
            collected.append,
            idle_poll_seconds=60,
            sweep_interval_seconds=None,
            wall_clock=lambda: now,
        ).run(max_iterations=1)

        assert waits == [pytest.approx(2.0, abs=0.5)]

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


class TestWakeUp:
    def test_the_backend_really_gets_a_listener(self, backend):
        """Guards against silently regressing to poll-only wake-ups."""
        backend.wait_for_work(0.01)
        assert backend._listener.supported is True, (
            "LISTEN did not start; the dispatcher would fall back to polling"
        )

    def test_a_committed_task_wakes_a_waiting_dispatcher(self, backend, engine):
        """The wake-up path, measured: a notification must beat the poll interval."""
        import threading

        from sqlalchemy.orm import Session

        backend.wait_for_work(0.01)  # open the listen connection first

        def produce() -> None:
            with Session(engine) as producer_session:
                create_task(producer_session, task_type="t")
                producer_session.commit()

        timer = threading.Timer(0.1, produce)
        timer.start()
        try:
            assert backend.wait_for_work(10.0) is True
        finally:
            timer.cancel()

        assert len(backend.claim(10)) == 1


class TestSurvivesDatabaseOutage:
    """A database blip must not end the dispatcher process.

    Found by the resilience lab: when Postgres went away mid-run, `claim()` raised,
    the exception escaped `run()`, and the dispatcher exited. Claimed work then sat
    until the dispatch timeout and nothing swept at all — a blip became an outage.
    """

    def test_a_failing_claim_does_not_end_the_loop(self, backend, collected):
        calls = []

        def flaky_claim(limit: int) -> list[str]:
            calls.append(limit)
            raise ConnectionError("connection is closed")

        backend.claim = flaky_claim  # type: ignore[method-assign]
        dispatcher = Dispatcher(
            backend,
            collected.append,
            idle_poll_seconds=0.01,
            sweep_interval_seconds=None,
            dispatch_retry_base_seconds=0.01,
        )

        dispatcher.run(max_iterations=3)

        assert len(calls) == 3  # kept trying instead of exiting
        assert dispatcher._db_health.retry_delay > 0  # and backed off while broken

    def test_the_loop_recovers_when_the_database_comes_back(self, backend, session, collected):
        task = create_task(session, task_type="t")
        session.commit()

        real_claim = backend.claim
        failures = [True]

        def flaky_claim(limit: int) -> list[str]:
            if failures[0]:
                failures[0] = False
                raise ConnectionError("connection is closed")
            return real_claim(limit)

        backend.claim = flaky_claim  # type: ignore[method-assign]
        dispatcher = Dispatcher(
            backend,
            collected.append,
            idle_poll_seconds=0.01,
            sweep_interval_seconds=None,
            dispatch_retry_base_seconds=0.01,
        )

        dispatcher.run(max_iterations=2)

        assert collected == [task.id]
        assert dispatcher._db_health.retry_delay == 0.0  # cleared once the database answered

    def test_a_failing_release_is_retried_not_abandoned(self, backend, session, caplog):
        """A stranded claim must not wait out the dispatch timeout.

        Found by the resilience lab: when the database died mid-dispatch the release
        could not be written, rows sat in DISPATCHING, and only the timeout sweep
        (300s by default) recovered them — so a 4-second blip stranded work for
        minutes. The dispatcher knows which IDs it holds, so it retries them.
        """
        task = create_task(session, task_type="t")
        session.commit()

        real_release = backend.release
        release_broken = [True]

        def flaky_release(task_ids) -> None:
            if release_broken[0]:
                raise ConnectionError("connection is closed")
            real_release(task_ids)

        backend.release = flaky_release  # type: ignore[method-assign]

        def broken_transport(task_id: str) -> None:
            raise ConnectionError("redis is down")

        dispatcher = Dispatcher(backend, broken_transport, sweep_interval_seconds=None)

        with caplog.at_level(logging.WARNING):
            assert dispatcher.dispatch_batch() == 0  # did not raise

        assert dispatcher._pending_release == [task.id]  # remembered
        assert _status(session, task.id) == TaskStatus.DISPATCHING.value

        # Database recovers; the next iteration returns the row without any sweep.
        release_broken[0] = False
        dispatcher._retry_pending_release()

        assert dispatcher._pending_release == []
        assert _status(session, task.id) == TaskStatus.PENDING.value

    def test_a_still_broken_release_stays_queued_for_the_next_pass(self, backend, session):
        create_task(session, task_type="t")
        session.commit()

        def broken_release(task_ids) -> None:
            raise ConnectionError("connection is closed")

        backend.release = broken_release  # type: ignore[method-assign]
        dispatcher = Dispatcher(
            backend,
            lambda task_id: (_ for _ in ()).throw(ConnectionError("redis is down")),
            sweep_interval_seconds=None,
        )

        dispatcher.dispatch_batch()
        held = list(dispatcher._pending_release)
        assert held

        dispatcher._retry_pending_release()
        assert dispatcher._pending_release == held  # not dropped on the floor

    def test_the_sweep_still_runs_while_a_release_stays_stranded(self, backend, session):
        """Recovery must not stop just because claiming is paused.

        A persistently unwritable release pauses new claims so the stranded list
        stays bounded — but the sweep is what makes retries eligible and reclaims
        timed-out DISPATCHING rows, so it has to keep ticking through the pause.
        """
        task = create_task(session, task_type="t")
        session.commit()

        def broken_release(task_ids) -> None:
            raise ConnectionError("release unavailable")

        def broken_transport(task_id: str) -> None:
            raise ConnectionError("redis is down")

        backend.release = broken_release  # type: ignore[method-assign]
        dispatcher = Dispatcher(
            backend,
            broken_transport,
            batch_size=1,
            idle_poll_seconds=0.001,
            sweep_interval_seconds=0.0,
            dispatch_retry_base_seconds=0.001,
            database_retry_cap_seconds=0.005,
        )
        dispatcher.dispatch_batch()
        assert dispatcher._pending_release == [task.id]

        real_sweep = backend.run_sweep
        sweeps: list[dict[str, list[str]]] = []

        def counted_sweep() -> dict[str, list[str]]:
            result = real_sweep()
            sweeps.append(result)
            return result

        real_claim = backend.claim
        claim_calls: list[int] = []

        def counted_claim(limit: int) -> list[str]:
            claim_calls.append(limit)
            return real_claim(limit)

        backend.run_sweep = counted_sweep  # type: ignore[method-assign]
        backend.claim = counted_claim  # type: ignore[method-assign]
        dispatcher.run(max_iterations=3)

        assert len(sweeps) == 3  # recovery kept running
        assert claim_calls == []  # claiming stayed paused
        assert dispatcher._pending_release == [task.id]  # still held for the next retry
        assert _status(session, task.id) == TaskStatus.DISPATCHING.value
        assert dispatcher._health.retry_delay > 0  # transport backoff kept its own deadline
        assert dispatcher._db_health.retry_delay > 0  # database backoff kept climbing

    def test_a_failing_sweep_is_already_tolerated(self, backend, collected):
        def boom() -> dict[str, list[str]]:
            raise ConnectionError("connection is closed")

        backend.run_sweep = boom  # type: ignore[method-assign]
        dispatcher = Dispatcher(
            backend, collected.append, idle_poll_seconds=0.01, sweep_interval_seconds=0.0
        )

        dispatcher.run(max_iterations=2)  # must not raise


class TestDatabaseBackoffIsShorterThanTransport:
    """A database that comes back should be noticed quickly.

    Found by the resilience lab: with one shared 1s->30s curve, the dispatcher could
    still be asleep for 30s after Postgres had already recovered, which pushed the
    db-outage scenario past its drain gate. The database is what we are already
    connected to and re-probing costs one query, so it gets its own shorter cap.
    """

    def test_the_database_cap_is_lower_than_the_transport_cap(self, backend):
        dispatcher = Dispatcher(backend, lambda task_id: None)
        for _ in range(20):
            dispatcher._health.note_failure()
            dispatcher._db_health.note_failure()

        assert dispatcher._db_health.retry_delay == 5.0
        assert dispatcher._health.retry_delay == 30.0
        assert dispatcher._db_health.retry_delay < dispatcher._health.retry_delay

    def test_a_recovered_database_clears_its_backoff_on_the_next_pass(self, backend, collected):
        failures = [3]

        real_claim = backend.claim

        def flaky_claim(limit: int) -> list[str]:
            if failures[0] > 0:
                failures[0] -= 1
                raise ConnectionError("connection is closed")
            return real_claim(limit)

        backend.claim = flaky_claim  # type: ignore[method-assign]
        dispatcher = Dispatcher(
            backend,
            collected.append,
            idle_poll_seconds=0.01,
            sweep_interval_seconds=None,
            dispatch_retry_base_seconds=0.01,
        )

        dispatcher.run(max_iterations=3)
        assert dispatcher._db_health.retry_delay > 0  # still failing

        dispatcher.run(max_iterations=1)
        assert dispatcher._db_health.retry_delay == 0.0  # recovered, no lingering sleep

    def test_transport_and_database_backoff_are_tracked_separately(self, backend, session):
        """A broker outage must not slow database recovery, or vice versa."""
        create_task(session, task_type="t")
        session.commit()

        def broken_transport(task_id: str) -> None:
            raise ConnectionError("redis is down")

        dispatcher = Dispatcher(backend, broken_transport, sweep_interval_seconds=None)
        dispatcher.dispatch_batch()

        assert dispatcher._health.retry_delay > 0  # transport is unhealthy
        assert dispatcher._db_health.retry_delay == 0.0  # the database is fine

    def test_an_elapsed_transport_delay_is_not_charged_again_to_the_database(self, backend):
        """Backoff steps are not the same thing as remaining sleep time."""
        clock = [0.0]
        dispatcher = Dispatcher(backend, lambda task_id: None, monotonic=lambda: clock[0])
        for _ in range(20):
            dispatcher._health.note_failure()

        assert dispatcher._health.retry_delay == 30.0
        clock[0] = 30.0  # the broker's wait has already elapsed
        dispatcher._db_health.note_failure()

        assert dispatcher._health.remaining_delay == 0.0
        assert dispatcher._db_health.remaining_delay == 1.0
        assert (
            max(
                dispatcher._health.remaining_delay,
                dispatcher._db_health.remaining_delay,
            )
            == 1.0
        )

    def test_a_stranded_release_blocks_new_claims_until_it_is_written(self, backend, session):
        """Selective release failure must not accumulate more DISPATCHING rows."""
        first = create_task(session, task_type="t")
        second = create_task(session, task_type="t")
        session.commit()

        def broken_release(task_ids) -> None:
            raise ConnectionError("release unavailable")

        def broken_transport(task_id: str) -> None:
            raise ConnectionError("redis is down")

        backend.release = broken_release  # type: ignore[method-assign]
        dispatcher = Dispatcher(
            backend,
            broken_transport,
            batch_size=1,
            idle_poll_seconds=0.001,
            sweep_interval_seconds=None,
            dispatch_retry_base_seconds=0.001,
            database_retry_cap_seconds=0.005,
        )
        dispatcher.dispatch_batch()
        assert dispatcher._pending_release == [first.id]

        real_claim = backend.claim
        claim_calls = []

        def counted_claim(limit: int) -> list[str]:
            claim_calls.append(limit)
            return real_claim(limit)

        backend.claim = counted_claim  # type: ignore[method-assign]
        dispatcher.run(max_iterations=1)

        assert claim_calls == []
        assert dispatcher._pending_release == [first.id]
        assert _status(session, first.id) == TaskStatus.DISPATCHING.value
        assert _status(session, second.id) == TaskStatus.PENDING.value
