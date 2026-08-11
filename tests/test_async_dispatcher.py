"""The async dispatcher must behave exactly like the sync one.

An asyncpg deployment should not have to add a synchronous driver to get a
dispatcher, and it should not get subtly different semantics for having avoided it.
These mirror the sync tests case for case.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

import dewey
from dewey.core.states import TaskStatus
from dewey.dispatcher import AsyncDispatcher
from dewey.sqlalchemy.async_executor import create_task_async, process_task_async
from dewey.sqlalchemy.dispatch import AsyncSQLAlchemyDispatchBackend
from dewey.sqlalchemy.models import TaskEntryModel

pytestmark = pytest.mark.asyncio


@pytest.fixture
def backend(async_engine):
    return AsyncSQLAlchemyDispatchBackend(async_engine)


async def _status(session, task_id: str) -> str:
    result = await session.execute(
        select(TaskEntryModel.status).where(TaskEntryModel.id == task_id)
    )
    return result.scalar_one()


class TestClaim:
    async def test_claims_and_commits_as_dispatching(self, backend, async_session):
        task = await create_task_async(async_session, task_type="t")
        await async_session.commit()

        assert await backend.claim(10) == [task.id]
        assert await _status(async_session, task.id) == TaskStatus.DISPATCHING.value

    async def test_a_claimed_task_is_not_claimed_again(self, backend, async_session):
        await create_task_async(async_session, task_type="t")
        await async_session.commit()

        assert len(await backend.claim(10)) == 1
        assert await backend.claim(10) == []

    async def test_batch_size_is_respected(self, backend, async_session):
        for _ in range(5):
            await create_task_async(async_session, task_type="t")
        await async_session.commit()

        assert len(await backend.claim(2)) == 2

    async def test_higher_priority_goes_first(self, backend, async_session):
        low = await create_task_async(async_session, task_type="t", priority=0)
        high = await create_task_async(async_session, task_type="t", priority=100)
        await async_session.commit()

        assert await backend.claim(2) == [high.id, low.id]

    async def test_scheduled_work_is_invisible_until_due(self, backend, async_session):
        await create_task_async(
            async_session, task_type="t", scheduled_for=datetime.now(UTC) + timedelta(minutes=5)
        )
        await async_session.commit()

        assert await backend.claim(10) == []

    async def test_due_failed_retry_is_claimed_without_a_sweep(self, backend, async_session):
        task = await create_task_async(async_session, task_type="t", max_attempts=3)
        await async_session.flush()
        await async_session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == task.id)
            .values(
                status=TaskStatus.FAILED.value,
                attempts=1,
                scheduled_for=datetime.now(UTC) - timedelta(seconds=1),
            )
        )
        await async_session.commit()

        assert await backend.claim(10) == [task.id]

    async def test_expired_ready_row_is_terminalized_not_dispatched(self, backend, async_session):
        task = await create_task_async(async_session, task_type="t")
        await async_session.flush()
        await async_session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == task.id)
            .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
        )
        await async_session.commit()

        assert await backend.claim(10) == []
        row = await async_session.get(TaskEntryModel, task.id)
        assert row is not None
        await async_session.refresh(row)
        assert row.status == TaskStatus.EXPIRED.value
        assert row.expired_at is not None
        assert row.attempts == 0

    async def test_due_scheduled_work_competes_with_immediate_work_on_due_time(
        self, backend, async_session
    ):
        """Same guarantee as the sync backend: ordering by effective due time —
        coalesce(scheduled_for, created_at) — so a sustained stream of immediate
        work cannot starve scheduled and retried rows that are already due."""
        older_immediate = await create_task_async(async_session, task_type="t")
        overdue = await create_task_async(
            async_session, task_type="t", scheduled_for=datetime.now(UTC) - timedelta(minutes=5)
        )
        newer_immediate = await create_task_async(async_session, task_type="t")
        await async_session.commit()
        await async_session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == older_immediate.id)
            .values(created_at=datetime.now(UTC) - timedelta(minutes=10))
        )
        await async_session.commit()

        # Waiting longest goes first: the overdue row beats immediate work created
        # after its due time, without jumping immediate work that was there before.
        assert await backend.claim(3) == [
            older_immediate.id,
            overdue.id,
            newer_immediate.id,
        ]

    async def test_queue_scoping(self, async_engine, async_session):
        await create_task_async(async_session, task_type="t", queue="bulk")
        critical = await create_task_async(async_session, task_type="t", queue="critical")
        await async_session.commit()

        scoped = AsyncSQLAlchemyDispatchBackend(async_engine, queues=["critical"])
        assert await scoped.claim(10) == [critical.id]

    async def test_release_does_not_burn_an_attempt(self, backend, async_session):
        task = await create_task_async(async_session, task_type="t")
        await async_session.commit()
        await backend.claim(10)

        await backend.release([task.id])

        row = await async_session.get(TaskEntryModel, task.id)
        assert row is not None
        await async_session.refresh(row)
        assert row.status == TaskStatus.PENDING.value
        assert row.dispatching_at is None
        assert row.attempts == 0


class TestDispatchBatch:
    async def test_dispatches_every_claimed_task(self, backend, async_session):
        ids = [(await create_task_async(async_session, task_type="t")).id for _ in range(3)]
        await async_session.commit()

        seen: list[str] = []
        dispatcher = AsyncDispatcher(backend, seen.append, sweep_interval_seconds=None)

        assert await dispatcher.dispatch_batch() == 3
        assert sorted(seen) == sorted(ids)

    async def test_an_async_dispatch_fn_is_awaited(self, backend, async_session):
        task = await create_task_async(async_session, task_type="t")
        await async_session.commit()

        seen: list[str] = []

        async def transport(task_id: str) -> None:
            await asyncio.sleep(0)
            seen.append(task_id)

        dispatcher = AsyncDispatcher(backend, transport, sweep_interval_seconds=None)
        assert await dispatcher.dispatch_batch() == 1
        assert seen == [task.id]

    async def test_transport_failure_returns_work_to_pending_immediately(
        self, backend, async_session
    ):
        task = await create_task_async(async_session, task_type="t")
        await async_session.commit()

        async def broken(task_id: str) -> None:
            raise ConnectionError("redis is down")

        dispatcher = AsyncDispatcher(backend, broken, sweep_interval_seconds=None)
        assert await dispatcher.dispatch_batch() == 0

        row = await async_session.get(TaskEntryModel, task.id)
        assert row is not None
        await async_session.refresh(row)
        assert row.status == TaskStatus.PENDING.value
        assert row.attempts == 0

    async def test_the_rest_of_the_batch_is_released_after_the_first_failure(
        self, backend, async_session
    ):
        for _ in range(5):
            await create_task_async(async_session, task_type="t")
        await async_session.commit()

        attempts: list[str] = []

        async def fail_on_third(task_id: str) -> None:
            attempts.append(task_id)
            if len(attempts) == 3:
                raise ConnectionError("redis is down")

        dispatcher = AsyncDispatcher(
            backend, fail_on_third, batch_size=5, sweep_interval_seconds=None
        )
        assert await dispatcher.dispatch_batch() == 2
        assert len(attempts) == 3

        result = await async_session.execute(select(TaskEntryModel.status))
        statuses = sorted(result.scalars().all())
        assert statuses == sorted(
            [TaskStatus.DISPATCHING.value] * 2 + [TaskStatus.PENDING.value] * 3
        )

    async def test_backoff_matches_the_sync_dispatcher(self, backend, async_session):
        await create_task_async(async_session, task_type="t")
        await async_session.commit()
        failing = True

        async def flaky(task_id: str) -> None:
            if failing:
                raise ConnectionError("redis is down")

        dispatcher = AsyncDispatcher(backend, flaky, sweep_interval_seconds=None)

        await dispatcher.dispatch_batch()
        assert dispatcher._health.retry_delay == pytest.approx(1.0)
        await dispatcher.dispatch_batch()
        assert dispatcher._health.retry_delay == pytest.approx(2.0)

        failing = False
        await dispatcher.dispatch_batch()
        assert dispatcher._health.retry_delay == 0.0


class TestSweepTick:
    async def test_the_dispatcher_runs_the_sweep(self, backend, async_session):
        task = await create_task_async(async_session, task_type="t", max_attempts=5)
        await async_session.execute(
            update(TaskEntryModel)
            .where(TaskEntryModel.id == task.id)
            .values(
                status=TaskStatus.FAILED.value,
                attempts=1,
                scheduled_for=datetime.now(UTC) - timedelta(minutes=1),
            )
        )
        await async_session.commit()

        dispatcher = AsyncDispatcher(backend, lambda task_id: None, sweep_interval_seconds=60)
        result = await dispatcher.maybe_sweep()

        assert result is not None
        assert result["failed"] == [task.id]
        assert await _status(async_session, task.id) == TaskStatus.PENDING.value

    async def test_the_sweep_respects_its_interval(self, backend):
        clock = [1000.0]
        dispatcher = AsyncDispatcher(
            backend,
            lambda task_id: None,
            sweep_interval_seconds=60,
            monotonic=lambda: clock[0],
        )

        assert await dispatcher.maybe_sweep() is not None
        clock[0] += 30
        assert await dispatcher.maybe_sweep() is None
        clock[0] += 31
        assert await dispatcher.maybe_sweep() is not None

    async def test_a_failing_sweep_does_not_stop_dispatch(self, backend, async_session):
        task = await create_task_async(async_session, task_type="t")
        await async_session.commit()

        async def boom() -> dict[str, list[str]]:
            raise RuntimeError("sweep exploded")

        backend.run_sweep = boom  # type: ignore[method-assign]
        seen: list[str] = []
        dispatcher = AsyncDispatcher(
            backend, seen.append, idle_poll_seconds=0.05, sweep_interval_seconds=60
        )

        await dispatcher.run(max_iterations=1)
        assert seen == [task.id]


class TestRunLoop:
    async def test_stop_ends_the_loop(self, backend, async_session):
        await create_task_async(async_session, task_type="t")
        await async_session.commit()

        dispatcher = AsyncDispatcher(
            backend, lambda task_id: None, idle_poll_seconds=0.05, sweep_interval_seconds=None
        )

        def stop_after_first(task_id: str) -> None:
            dispatcher.stop()

        dispatcher.dispatch_fn = stop_after_first
        await dispatcher.run()
        assert dispatcher.stopped

    async def test_cancellation_closes_the_backend(self, backend):
        dispatcher = AsyncDispatcher(
            backend, lambda task_id: None, idle_poll_seconds=30, sweep_interval_seconds=None
        )
        task = asyncio.create_task(dispatcher.run())
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_a_full_batch_loops_without_waiting(self, backend, async_session):
        ids = [(await create_task_async(async_session, task_type="t")).id for _ in range(4)]
        await async_session.commit()

        waits: list[float] = []

        async def record_wait(timeout: float) -> bool:
            waits.append(timeout)
            return False

        backend.wait_for_work = record_wait  # type: ignore[method-assign]
        seen: list[str] = []
        dispatcher = AsyncDispatcher(
            backend, seen.append, batch_size=2, sweep_interval_seconds=None
        )

        await dispatcher.run(max_iterations=2)

        assert sorted(seen) == sorted(ids)
        assert waits == []

    async def test_invalid_batch_size_is_refused(self, backend):
        with pytest.raises(ValueError, match="batch_size"):
            AsyncDispatcher(backend, lambda task_id: None, batch_size=0)


class TestEndToEnd:
    async def test_pending_to_completed_through_the_async_dispatcher(
        self, backend, async_engine, async_session
    ):
        from sqlalchemy.ext.asyncio import AsyncSession

        seen = []

        @dewey.task("orders.confirm")
        async def confirm(order_id: int) -> None:
            seen.append(order_id)

        try:
            task = await create_task_async(async_session, task_type="orders.confirm", args=[7])
            await async_session.commit()

            async def transport(task_id: str) -> None:
                async with AsyncSession(async_engine) as worker_session:
                    await process_task_async(worker_session, task_id)

            dispatcher = AsyncDispatcher(backend, transport, sweep_interval_seconds=None)
            assert await dispatcher.dispatch_batch() == 1

            assert seen == [7]
            assert await _status(async_session, task.id) == TaskStatus.COMPLETED.value
        finally:
            dewey.registry.clear()


class TestWakeUp:
    async def test_a_committed_task_wakes_a_waiting_dispatcher(self, backend, async_engine):
        """LISTEN on the asyncpg listener, not a blocking thread."""
        from sqlalchemy.ext.asyncio import AsyncSession

        await backend.wait_for_work(0.01)  # start the listener

        async def produce() -> None:
            await asyncio.sleep(0.1)
            async with AsyncSession(async_engine) as producer_session:
                await create_task_async(producer_session, task_type="t")
                await producer_session.commit()

        producer = asyncio.create_task(produce())
        try:
            assert await backend.wait_for_work(10.0) is True
        finally:
            await producer
            await backend.close()

        assert len(await backend.claim(10)) == 1


class TestSurvivesDatabaseOutage:
    """Same guarantee as the sync dispatcher: a database blip is survivable."""

    async def test_a_failing_claim_does_not_end_the_loop(self, backend):
        calls = []

        async def flaky_claim(limit: int) -> list[str]:
            calls.append(limit)
            raise ConnectionError("connection is closed")

        backend.claim = flaky_claim  # type: ignore[method-assign]
        dispatcher = AsyncDispatcher(
            backend,
            lambda task_id: None,
            idle_poll_seconds=0.01,
            sweep_interval_seconds=None,
            dispatch_retry_base_seconds=0.01,
        )

        await dispatcher.run(max_iterations=3)

        assert len(calls) == 3
        assert dispatcher._db_health.retry_delay > 0

    async def test_the_loop_recovers_when_the_database_comes_back(self, backend, async_session):
        task = await create_task_async(async_session, task_type="t")
        await async_session.commit()

        real_claim = backend.claim
        failures = [True]

        async def flaky_claim(limit: int) -> list[str]:
            if failures[0]:
                failures[0] = False
                raise ConnectionError("connection is closed")
            return await real_claim(limit)

        backend.claim = flaky_claim  # type: ignore[method-assign]
        seen: list[str] = []
        dispatcher = AsyncDispatcher(
            backend,
            seen.append,
            idle_poll_seconds=0.01,
            sweep_interval_seconds=None,
            dispatch_retry_base_seconds=0.01,
        )

        await dispatcher.run(max_iterations=2)

        assert seen == [task.id]
        assert dispatcher._db_health.retry_delay == 0.0

    async def test_a_failing_release_is_remembered_for_retry(self, backend, async_session):
        task = await create_task_async(async_session, task_type="t")
        await async_session.commit()

        async def broken_release(task_ids) -> None:
            raise ConnectionError("connection is closed")

        async def broken_transport(task_id: str) -> None:
            raise ConnectionError("redis is down")

        backend.release = broken_release  # type: ignore[method-assign]
        dispatcher = AsyncDispatcher(backend, broken_transport, sweep_interval_seconds=None)

        assert await dispatcher.dispatch_batch() == 0  # did not raise
        assert dispatcher._pending_release == [task.id]
        assert await _status(async_session, task.id) == TaskStatus.DISPATCHING.value

    async def test_a_stranded_release_blocks_new_claims_until_it_is_written(
        self, backend, async_session
    ):
        first = await create_task_async(async_session, task_type="t")
        second = await create_task_async(async_session, task_type="t")
        await async_session.commit()

        async def broken_release(task_ids) -> None:
            raise ConnectionError("release unavailable")

        async def broken_transport(task_id: str) -> None:
            raise ConnectionError("redis is down")

        backend.release = broken_release  # type: ignore[method-assign]
        dispatcher = AsyncDispatcher(
            backend,
            broken_transport,
            batch_size=1,
            idle_poll_seconds=0.001,
            sweep_interval_seconds=None,
            dispatch_retry_base_seconds=0.001,
            database_retry_cap_seconds=0.005,
        )
        await dispatcher.dispatch_batch()
        assert dispatcher._pending_release == [first.id]

        real_claim = backend.claim
        claim_calls = []

        async def counted_claim(limit: int) -> list[str]:
            claim_calls.append(limit)
            return await real_claim(limit)

        backend.claim = counted_claim  # type: ignore[method-assign]
        await dispatcher.run(max_iterations=1)

        assert claim_calls == []
        assert dispatcher._pending_release == [first.id]
        assert await _status(async_session, first.id) == TaskStatus.DISPATCHING.value
        assert await _status(async_session, second.id) == TaskStatus.PENDING.value

    async def test_the_sweep_still_runs_while_a_release_stays_stranded(
        self, backend, async_session
    ):
        """Same guarantee as the sync dispatcher: a persistently unwritable release
        pauses new claims, but the sweep — retry eligibility and dispatch-timeout
        recovery — keeps ticking through the pause."""
        task = await create_task_async(async_session, task_type="t")
        await async_session.commit()

        async def broken_release(task_ids) -> None:
            raise ConnectionError("release unavailable")

        async def broken_transport(task_id: str) -> None:
            raise ConnectionError("redis is down")

        backend.release = broken_release  # type: ignore[method-assign]
        dispatcher = AsyncDispatcher(
            backend,
            broken_transport,
            batch_size=1,
            idle_poll_seconds=0.001,
            sweep_interval_seconds=0.0,
            dispatch_retry_base_seconds=0.001,
            database_retry_cap_seconds=0.005,
        )
        await dispatcher.dispatch_batch()
        assert dispatcher._pending_release == [task.id]

        real_sweep = backend.run_sweep
        sweeps: list[dict[str, list[str]]] = []

        async def counted_sweep() -> dict[str, list[str]]:
            result = await real_sweep()
            sweeps.append(result)
            return result

        real_claim = backend.claim
        claim_calls: list[int] = []

        async def counted_claim(limit: int) -> list[str]:
            claim_calls.append(limit)
            return await real_claim(limit)

        backend.run_sweep = counted_sweep  # type: ignore[method-assign]
        backend.claim = counted_claim  # type: ignore[method-assign]
        await dispatcher.run(max_iterations=3)

        assert len(sweeps) == 3  # recovery kept running
        assert claim_calls == []  # claiming stayed paused
        assert dispatcher._pending_release == [task.id]  # still held for the next retry
        assert await _status(async_session, task.id) == TaskStatus.DISPATCHING.value
        assert dispatcher._health.retry_delay > 0  # transport backoff kept its own deadline
        assert dispatcher._db_health.retry_delay > 0  # database backoff kept climbing

    async def test_elapsed_transport_delay_does_not_extend_database_delay(self, backend):
        clock = [0.0]
        dispatcher = AsyncDispatcher(backend, lambda task_id: None, monotonic=lambda: clock[0])
        for _ in range(20):
            dispatcher._health.note_failure()

        clock[0] = 30.0
        dispatcher._db_health.note_failure()

        assert dispatcher._health.remaining_delay == 0.0
        assert dispatcher._db_health.remaining_delay == 1.0

    async def test_cancellation_still_wins_over_the_retry_loop(self, backend):
        """Surviving errors must not make the dispatcher unkillable."""

        async def flaky_claim(limit: int) -> list[str]:
            raise ConnectionError("connection is closed")

        backend.claim = flaky_claim  # type: ignore[method-assign]
        dispatcher = AsyncDispatcher(
            backend,
            lambda task_id: None,
            idle_poll_seconds=0.01,
            sweep_interval_seconds=None,
            dispatch_retry_base_seconds=5.0,
        )

        task = asyncio.create_task(dispatcher.run())
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
