"""The dispatcher — Dewey's half of the deal with your broker.

Postgres is the backlog. The dispatcher is the only thing that hands work to a
transport, and it does so in one direction only:

1. claim a batch of ready rows with ``FOR UPDATE SKIP LOCKED`` and commit them as
   ``DISPATCHING``, so the claim survives a crash;
2. call ``adapter.dispatch(task_id)`` for each claimed row;
3. wait for a notification, or poll.

Producers never talk to the broker. Workers only ever receive a task ID. That
keeps the transport swappable and the backlog queryable in SQL.

Two properties are worth stating plainly because they are what makes an outage
boring:

- **A transport failure does not lose work.** The claimed rows go straight back to
  ``PENDING`` — not left to time out — so they are dispatched again as soon as the
  broker returns. A failed dispatch does not consume an attempt; it was never a
  handler attempt.
- **The sweep runs here.** Retries become eligible when the sweep moves ``FAILED``
  rows back to ``PENDING``, so a deployment without a running dispatcher does not
  retry anything. The dispatcher owning that tick means one process to run, not
  two.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from dewey.core.heartbeat import DEFAULT_HEARTBEAT_INTERVAL_SECONDS

logger = logging.getLogger(__name__)

#: Claim this many rows per round trip.
DEFAULT_BATCH_SIZE = 100
#: How long to wait for a notification before polling anyway. The poll is the
#: correctness path: it picks up missed notifications and newly-due scheduled work.
DEFAULT_IDLE_POLL_SECONDS = 5.0
#: How often to run the recovery sweep.
DEFAULT_SWEEP_INTERVAL_SECONDS = 60.0
#: Transport failure backoff, so a dead broker is not hammered.
DEFAULT_DISPATCH_RETRY_BASE_SECONDS = 1.0
DEFAULT_DISPATCH_RETRY_CAP_SECONDS = 30.0
#: Database failure backoff. Deliberately much shorter than the transport cap: the
#: database is what we are already connected to, re-probing it costs one cheap query, and
#: a long sleep here means work sits idle after Postgres has already come back.
DEFAULT_DATABASE_RETRY_CAP_SECONDS = 5.0

DispatchFn = Callable[[str], Any]


@runtime_checkable
class DispatchBackend(Protocol):
    """Database operations the dispatcher needs, for one ORM.

    Implementations live in ``dewey.sqlalchemy.dispatch`` and
    ``dewey.django.dispatch``. Every method must commit before returning: a claim
    that is not committed is a claim that a crash can lose.
    """

    def claim(self, limit: int) -> list[str]:
        """Move up to ``limit`` ready rows to DISPATCHING and return their IDs."""
        ...

    def release(self, task_ids: Sequence[str]) -> None:
        """Return claimed rows to PENDING without consuming an attempt."""
        ...

    def run_sweep(self) -> dict[str, list[str]]:
        """Run the recovery passes and return what each one touched."""
        ...

    def wait_for_work(self, timeout: float) -> bool:
        """Block until notified or ``timeout`` elapses. True if notified."""
        ...

    def close(self) -> None:
        """Release any resources held (listen connections, sessions)."""
        ...


class _BackoffState:
    """Independent retry timing for one upstream dependency.

    ``retry_delay`` is the exponential step to use after the next failure;
    ``remaining_delay`` is what the loop must still wait now. Keeping those separate
    prevents an already-served 30-second broker delay from making a newly recovered
    database sleep for another 30 seconds.
    """

    def __init__(
        self,
        label: str,
        base_seconds: float,
        cap_seconds: float,
        monotonic: Callable[[], float],
    ) -> None:
        self.label = label
        self.base_seconds = base_seconds
        self.cap_seconds = cap_seconds
        self.retry_delay = 0.0
        self._retry_at = 0.0
        self._monotonic = monotonic

    def note_failure(self) -> None:
        if self.retry_delay:
            self.retry_delay = min(self.retry_delay * 2, self.cap_seconds)
        else:
            self.retry_delay = self.base_seconds
        self._retry_at = self._monotonic() + self.retry_delay

    def note_success(self) -> None:
        if self.retry_delay:
            logger.info("%s recovered; resuming normal dispatch", self.label)
        self.retry_delay = 0.0
        self._retry_at = 0.0

    @property
    def remaining_delay(self) -> float:
        if not self.retry_delay:
            return 0.0
        return max(0.0, self._retry_at - self._monotonic())


class _SweepClock:
    """Tracks whether the recovery sweep is due."""

    def __init__(self, interval_seconds: float | None, monotonic: Callable[[], float]) -> None:
        self.interval_seconds = interval_seconds
        self._monotonic = monotonic
        self._last: float | None = None

    def due(self) -> bool:
        if self.interval_seconds is None:
            return False
        now = self._monotonic()
        if self._last is not None and now - self._last < self.interval_seconds:
            return False
        self._last = now
        return True


def _log_sweep(result: dict[str, list[str]]) -> None:
    touched = {key: len(ids) for key, ids in result.items() if ids}
    if touched:
        logger.info("Sweep recovered %s", touched)


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _bounded_wait(idle_poll_seconds: float, due_at: datetime | None, now: datetime) -> float:
    if due_at is None:
        return idle_poll_seconds
    if due_at.tzinfo is None:
        due_at = due_at.replace(tzinfo=UTC)
    remaining = max(0.0, (due_at - now).total_seconds())
    # A due row may be SKIP LOCKED by another dispatcher. A zero-second wait would
    # hot-loop on that visible pre-commit row until its owner commits.
    if remaining == 0:
        return min(idle_poll_seconds, 0.05)
    return min(idle_poll_seconds, remaining)


class Dispatcher:
    """Claim ready work from Postgres and hand task IDs to a transport.

    Runs several instances safely: ``SKIP LOCKED`` makes them cooperate without a
    leader election, and the worst case of contention is an empty claim.

    Args:
        backend: Database operations for your ORM.
        dispatch_fn: Usually ``adapter.dispatch``. Receives one task ID.
        batch_size: Rows to claim per round trip.
        idle_poll_seconds: Maximum wait before polling anyway.
        sweep_interval_seconds: How often to run recovery. ``None`` disables it,
            which is only correct if something else runs the sweep.
    """

    def __init__(
        self,
        backend: DispatchBackend,
        dispatch_fn: DispatchFn,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        idle_poll_seconds: float = DEFAULT_IDLE_POLL_SECONDS,
        sweep_interval_seconds: float | None = DEFAULT_SWEEP_INTERVAL_SECONDS,
        dispatch_retry_base_seconds: float = DEFAULT_DISPATCH_RETRY_BASE_SECONDS,
        dispatch_retry_cap_seconds: float = DEFAULT_DISPATCH_RETRY_CAP_SECONDS,
        database_retry_cap_seconds: float = DEFAULT_DATABASE_RETRY_CAP_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], datetime] = _utcnow,
        heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size!r}")
        self.backend = backend
        self.dispatch_fn = dispatch_fn
        self.batch_size = batch_size
        self.idle_poll_seconds = idle_poll_seconds
        self.sweep_interval_seconds = sweep_interval_seconds
        self._health = _BackoffState(
            "Transport", dispatch_retry_base_seconds, dispatch_retry_cap_seconds, monotonic
        )
        # A failing claim or sweep is the database, not the transport. Recovering from it
        # quickly matters more, so it gets its own shorter cap.
        self._db_health = _BackoffState(
            "Database", dispatch_retry_base_seconds, database_retry_cap_seconds, monotonic
        )
        self._sweep_clock = _SweepClock(sweep_interval_seconds, monotonic)
        self._heartbeat_clock = _SweepClock(heartbeat_interval_seconds, monotonic)
        self._wall_clock = wall_clock
        self._stop = threading.Event()
        # Claims we could not return to PENDING because the database was unreachable.
        # Retried every iteration: the dispatch-timeout sweep is the backstop for a
        # dispatcher that dies, not the primary path for one that is still running.
        self._pending_release: list[str] = []

    # --- one unit of work ---

    def dispatch_batch(self) -> int:
        """Claim one batch and dispatch it. Returns how many reached the transport.

        The first transport failure abandons the rest of the batch and returns
        every undispatched row to PENDING. A transport error is nearly always
        broker-wide, so pushing the remaining IDs at it would only delay their
        release and slow the recovery.
        """
        task_ids = self.backend.claim(self.batch_size)
        if not task_ids:
            self._health.note_success()
            return 0

        for index, task_id in enumerate(task_ids):
            try:
                self.dispatch_fn(task_id)
            except Exception:
                unsent = task_ids[index:]
                logger.warning(
                    "Dispatch failed for task_id=%s; returning %d claimed task(s) to "
                    "pending and backing off",
                    task_id,
                    len(unsent),
                    exc_info=True,
                )
                self._release_quietly(unsent)
                self._health.note_failure()
                return index
            logger.debug("Dispatched task_id=%s", task_id)

        self._health.note_success()
        logger.info("Dispatched %d task(s)", len(task_ids))
        return len(task_ids)

    def maybe_sweep(self) -> dict[str, list[str]] | None:
        """Run the sweep if its interval has elapsed. Returns what it touched."""
        if not self._sweep_clock.due():
            return None
        try:
            result = self.backend.run_sweep()
        except Exception:
            # Recovery failing must not stop dispatch; the next tick tries again.
            logger.warning("Dewey sweep failed; will retry next tick", exc_info=True)
            return None
        _log_sweep(result)
        return result

    def maybe_heartbeat(self) -> bool:
        """Write readiness on a bounded cadence without making dispatch depend on it."""
        if not self._heartbeat_clock.due():
            return False
        heartbeat = getattr(self.backend, "heartbeat", None)
        if heartbeat is None:
            return False
        try:
            heartbeat()
        except Exception:
            logger.warning("Dewey dispatcher heartbeat failed; dispatch continues", exc_info=True)
            return False
        return True

    # --- the loop ---

    def run(self, *, max_iterations: int | None = None) -> None:
        """Dispatch until :meth:`stop` is called.

        ``max_iterations`` bounds the loop, which is what tests use instead of
        racing a thread.
        """
        logger.info(
            "Dewey dispatcher starting (instance=%s database=%s queues=%s batch_size=%d "
            "idle_poll=%.1fs recovery_sweep=%s retry_scheduling=direct)",
            getattr(self.backend, "instance_id", "unreported"),
            getattr(
                self.backend,
                "database_identity",
                getattr(self.backend, "using", "unreported"),
            ),
            getattr(self.backend, "queues", None) or "all",
            self.batch_size,
            self.idle_poll_seconds,
            f"{self.sweep_interval_seconds}s" if self.sweep_interval_seconds else "disabled",
        )
        iterations = 0
        try:
            while not self._stop.is_set():
                if max_iterations is not None and iterations >= max_iterations:
                    break
                iterations += 1

                release_ready = self._retry_pending_release()
                wait_timeout = self.idle_poll_seconds
                try:
                    # The sweep runs even while a stranded release pauses claiming:
                    # it is recovery, and a broken release path must not also stop
                    # retries becoming eligible or timed-out claims being reclaimed.
                    self.maybe_heartbeat()
                    self.maybe_sweep()
                    if not release_ready:
                        # Do not claim more work while rows we already own remain
                        # stranded. Otherwise a selective release failure can grow
                        # this list without bound even when claim queries still
                        # succeed.
                        dispatched = 0
                    else:
                        dispatched = self.dispatch_batch()
                    next_due = getattr(self.backend, "next_due", lambda: None)()
                    wait_timeout = _bounded_wait(
                        self.idle_poll_seconds, next_due, self._wall_clock()
                    )
                except Exception:
                    # Almost always the database going away underneath us. A
                    # dispatcher that exits here is worse than useless: claimed work
                    # waits for the dispatch timeout and nothing sweeps at all, so a
                    # blip becomes an outage. Back off and try again.
                    logger.warning(
                        "Dispatcher iteration failed; backing off and retrying",
                        exc_info=True,
                    )
                    self._db_health.note_failure()
                    dispatched = 0
                else:
                    if not self._pending_release:
                        self._db_health.note_success()

                delay = max(
                    self._health.remaining_delay,
                    self._db_health.remaining_delay,
                )
                if delay:
                    # Something upstream is unhealthy. Wait only the time still owed;
                    # an elapsed broker delay must not be charged again to the database.
                    self._stop.wait(delay)
                    continue
                if dispatched >= self.batch_size:
                    # A full batch usually means more is waiting; skip the wait.
                    continue
                self.backend.wait_for_work(wait_timeout)
        finally:
            logger.info("Dewey dispatcher stopped after %d iteration(s)", iterations)
            self.backend.close()

    def _release_quietly(self, task_ids: Sequence[str]) -> None:
        """Release claims, tolerating a database that is itself unavailable.

        A release that cannot be written is remembered and retried on the next
        iteration. Leaving it to the dispatch-timeout sweep instead would strand the
        work for ``dispatch_timeout_seconds`` — minutes, by default — after a blip the
        database had already recovered from.
        """
        try:
            self.backend.release(task_ids)
        except Exception:
            self._pending_release = list(dict.fromkeys([*self._pending_release, *task_ids]))
            self._db_health.note_failure()
            logger.warning(
                "Could not return %d claimed task(s) to pending; will retry",
                len(task_ids),
                exc_info=True,
            )

    def _retry_pending_release(self) -> bool:
        """Re-attempt stranded releases before claiming more work.

        Returns ``False`` while the database still refuses the release, which tells
        the loop to skip claiming for this iteration. The sweep is unaffected:
        recovery keeps running while claiming is paused.
        """
        if not self._pending_release:
            return True
        task_ids, self._pending_release = self._pending_release, []
        try:
            self.backend.release(task_ids)
        except Exception:
            self._pending_release = task_ids
            self._db_health.note_failure()
            logger.warning("Still cannot return %d claimed task(s) to pending", len(task_ids))
            return False
        self._db_health.note_success()
        logger.info("Returned %d previously stranded task(s) to pending", len(task_ids))
        return True

    def stop(self) -> None:
        """Ask the loop to finish. Safe to call from a signal handler."""
        self._stop.set()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_DATABASE_RETRY_CAP_SECONDS",
    "AsyncDispatchBackend",
    "AsyncDispatcher",
    "DEFAULT_DISPATCH_RETRY_CAP_SECONDS",
    "DEFAULT_IDLE_POLL_SECONDS",
    "DEFAULT_SWEEP_INTERVAL_SECONDS",
    "DispatchBackend",
    "DispatchFn",
    "Dispatcher",
]


@runtime_checkable
class AsyncDispatchBackend(Protocol):
    """The async twin of :class:`DispatchBackend`.

    Implemented by ``dewey.sqlalchemy.dispatch.AsyncSQLAlchemyDispatchBackend``. Same
    contract, same guarantees: every method commits before returning.
    """

    async def claim(self, limit: int) -> list[str]:
        """Move up to ``limit`` ready rows to DISPATCHING and return their IDs."""
        ...

    async def release(self, task_ids: Sequence[str]) -> None:
        """Return claimed rows to PENDING without consuming an attempt."""
        ...

    async def run_sweep(self) -> dict[str, list[str]]:
        """Run the recovery passes and return what each one touched."""
        ...

    async def wait_for_work(self, timeout: float) -> bool:
        """Wait until notified or ``timeout`` elapses. True if notified."""
        ...

    async def close(self) -> None:
        """Release any resources held (listen connections, sessions)."""
        ...


class AsyncDispatcher:
    """Dispatcher for asyncio consumers.

    Behaviourally identical to :class:`Dispatcher` — same claim-commit-dispatch order,
    same immediate release and backoff on transport failure, same sweep tick, same
    "a full batch skips the wait" pacing. The pacing decisions themselves are shared
    code (:class:`_BackoffState`, :class:`_SweepClock`), so the two loops cannot
    drift apart on the parts that matter.

    Exists because an asyncpg-only deployment should not have to add a synchronous
    driver and a second engine just to run a dispatcher.

    Args:
        backend: Async database operations.
        dispatch_fn: Receives one task ID. May be sync or a coroutine function.
    """

    def __init__(
        self,
        backend: AsyncDispatchBackend,
        dispatch_fn: Callable[[str], Any],
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        idle_poll_seconds: float = DEFAULT_IDLE_POLL_SECONDS,
        sweep_interval_seconds: float | None = DEFAULT_SWEEP_INTERVAL_SECONDS,
        dispatch_retry_base_seconds: float = DEFAULT_DISPATCH_RETRY_BASE_SECONDS,
        dispatch_retry_cap_seconds: float = DEFAULT_DISPATCH_RETRY_CAP_SECONDS,
        database_retry_cap_seconds: float = DEFAULT_DATABASE_RETRY_CAP_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], datetime] = _utcnow,
        heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size!r}")
        self.backend = backend
        self.dispatch_fn = dispatch_fn
        self.batch_size = batch_size
        self.idle_poll_seconds = idle_poll_seconds
        self.sweep_interval_seconds = sweep_interval_seconds
        self._health = _BackoffState(
            "Transport", dispatch_retry_base_seconds, dispatch_retry_cap_seconds, monotonic
        )
        # See the sync dispatcher: database failures recover on a shorter leash.
        self._db_health = _BackoffState(
            "Database", dispatch_retry_base_seconds, database_retry_cap_seconds, monotonic
        )
        self._sweep_clock = _SweepClock(sweep_interval_seconds, monotonic)
        self._heartbeat_clock = _SweepClock(heartbeat_interval_seconds, monotonic)
        self._wall_clock = wall_clock
        self._stop = asyncio.Event()
        # See the sync dispatcher: stranded claims are retried, not left to the sweep.
        self._pending_release: list[str] = []

    # --- one unit of work ---

    async def dispatch_batch(self) -> int:
        """Claim one batch and dispatch it. Returns how many reached the transport."""
        task_ids = await self.backend.claim(self.batch_size)
        if not task_ids:
            self._health.note_success()
            return 0

        for index, task_id in enumerate(task_ids):
            try:
                result = self.dispatch_fn(task_id)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                unsent = task_ids[index:]
                logger.warning(
                    "Dispatch failed for task_id=%s; returning %d claimed task(s) to "
                    "pending and backing off",
                    task_id,
                    len(unsent),
                    exc_info=True,
                )
                await self._release_quietly(unsent)
                self._health.note_failure()
                return index
            logger.debug("Dispatched task_id=%s", task_id)

        self._health.note_success()
        logger.info("Dispatched %d task(s)", len(task_ids))
        return len(task_ids)

    async def maybe_sweep(self) -> dict[str, list[str]] | None:
        """Run the sweep if its interval has elapsed. Returns what it touched."""
        if not self._sweep_clock.due():
            return None
        try:
            result = await self.backend.run_sweep()
        except Exception:
            logger.warning("Dewey sweep failed; will retry next tick", exc_info=True)
            return None
        _log_sweep(result)
        return result

    async def maybe_heartbeat(self) -> bool:
        """Write readiness on a bounded cadence without making dispatch depend on it."""
        if not self._heartbeat_clock.due():
            return False
        heartbeat = getattr(self.backend, "heartbeat", None)
        if heartbeat is None:
            return False
        try:
            await heartbeat()
        except Exception:
            logger.warning(
                "Dewey async dispatcher heartbeat failed; dispatch continues",
                exc_info=True,
            )
            return False
        return True

    # --- the loop ---

    async def run(self, *, max_iterations: int | None = None) -> None:
        """Dispatch until :meth:`stop` is called, or the task is cancelled."""
        logger.info(
            "Dewey async dispatcher starting (instance=%s database=%s queues=%s "
            "batch_size=%d idle_poll=%.1fs recovery_sweep=%s retry_scheduling=direct)",
            getattr(self.backend, "instance_id", "unreported"),
            getattr(self.backend, "database_identity", "unreported"),
            getattr(self.backend, "queues", None) or "all",
            self.batch_size,
            self.idle_poll_seconds,
            f"{self.sweep_interval_seconds}s" if self.sweep_interval_seconds else "disabled",
        )
        iterations = 0
        try:
            while not self._stop.is_set():
                if max_iterations is not None and iterations >= max_iterations:
                    break
                iterations += 1

                release_ready = await self._retry_pending_release()
                wait_timeout = self.idle_poll_seconds
                try:
                    # Match the sync loop: the sweep runs even while a stranded
                    # release pauses claiming, because recovery must not stop with it.
                    await self.maybe_heartbeat()
                    await self.maybe_sweep()
                    if not release_ready:
                        # Match the sync loop: never accumulate fresh claims while rows
                        # already owned by this dispatcher remain stranded.
                        dispatched = 0
                    else:
                        dispatched = await self.dispatch_batch()
                    next_due_fn = getattr(self.backend, "next_due", None)
                    next_due = await next_due_fn() if next_due_fn is not None else None
                    wait_timeout = _bounded_wait(
                        self.idle_poll_seconds, next_due, self._wall_clock()
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # See the sync dispatcher: surviving a database blip is the whole
                    # point of a long-running loop.
                    logger.warning(
                        "Dispatcher iteration failed; backing off and retrying",
                        exc_info=True,
                    )
                    self._db_health.note_failure()
                    dispatched = 0
                else:
                    if not self._pending_release:
                        self._db_health.note_success()

                delay = max(
                    self._health.remaining_delay,
                    self._db_health.remaining_delay,
                )
                if delay:
                    await self._sleep(delay)
                    continue
                if dispatched >= self.batch_size:
                    continue
                await self.backend.wait_for_work(wait_timeout)
        except asyncio.CancelledError:
            logger.info("Dewey async dispatcher cancelled")
            raise
        finally:
            logger.info("Dewey async dispatcher stopped after %d iteration(s)", iterations)
            await self.backend.close()

    async def _release_quietly(self, task_ids: Sequence[str]) -> None:
        """Release claims, tolerating a database that is itself unavailable.

        Remembered and retried next iteration rather than left to the dispatch-timeout
        sweep, which would strand the work for minutes after a recovered blip.
        """
        try:
            await self.backend.release(task_ids)
        except Exception:
            self._pending_release = list(dict.fromkeys([*self._pending_release, *task_ids]))
            self._db_health.note_failure()
            logger.warning(
                "Could not return %d claimed task(s) to pending; will retry",
                len(task_ids),
                exc_info=True,
            )

    async def _retry_pending_release(self) -> bool:
        """Re-attempt stranded releases before claiming more work."""
        if not self._pending_release:
            return True
        task_ids, self._pending_release = self._pending_release, []
        try:
            await self.backend.release(task_ids)
        except Exception:
            self._pending_release = task_ids
            self._db_health.note_failure()
            logger.warning("Still cannot return %d claimed task(s) to pending", len(task_ids))
            return False
        self._db_health.note_success()
        logger.info("Returned %d previously stranded task(s) to pending", len(task_ids))
        return True

    async def _sleep(self, seconds: float) -> None:
        """Sleep, but wake immediately if asked to stop."""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            pass

    def stop(self) -> None:
        """Ask the loop to finish after the current pass."""
        self._stop.set()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()
