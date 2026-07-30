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
from typing import Any, Protocol, runtime_checkable

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


class _TransportHealth:
    """Backoff bookkeeping for a transport that is refusing work.

    Shared by both dispatchers: a broker outage should look the same whichever loop
    is driving.
    """

    def __init__(self, base_seconds: float, cap_seconds: float) -> None:
        self.base_seconds = base_seconds
        self.cap_seconds = cap_seconds
        self.retry_delay = 0.0

    def note_failure(self) -> None:
        if self.retry_delay:
            self.retry_delay = min(self.retry_delay * 2, self.cap_seconds)
        else:
            self.retry_delay = self.base_seconds

    def note_success(self) -> None:
        if self.retry_delay:
            logger.info("Transport recovered; resuming normal dispatch")
        self.retry_delay = 0.0


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
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size!r}")
        self.backend = backend
        self.dispatch_fn = dispatch_fn
        self.batch_size = batch_size
        self.idle_poll_seconds = idle_poll_seconds
        self.sweep_interval_seconds = sweep_interval_seconds
        self._health = _TransportHealth(dispatch_retry_base_seconds, dispatch_retry_cap_seconds)
        self._sweep_clock = _SweepClock(sweep_interval_seconds, monotonic)
        self._stop = threading.Event()

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
                self.backend.release(unsent)
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

    # --- the loop ---

    def run(self, *, max_iterations: int | None = None) -> None:
        """Dispatch until :meth:`stop` is called.

        ``max_iterations`` bounds the loop, which is what tests use instead of
        racing a thread.
        """
        logger.info(
            "Dewey dispatcher starting (batch_size=%d idle_poll=%.1fs sweep=%s)",
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

                self.maybe_sweep()
                dispatched = self.dispatch_batch()

                if self._health.retry_delay:
                    # The transport is unhealthy. Wait, rather than spinning on a
                    # broker that is refusing connections.
                    self._stop.wait(self._health.retry_delay)
                    continue
                if dispatched >= self.batch_size:
                    # A full batch usually means more is waiting; skip the wait.
                    continue
                self.backend.wait_for_work(self.idle_poll_seconds)
        finally:
            logger.info("Dewey dispatcher stopped after %d iteration(s)", iterations)
            self.backend.close()

    def stop(self) -> None:
        """Ask the loop to finish. Safe to call from a signal handler."""
        self._stop.set()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()


__all__ = [
    "DEFAULT_BATCH_SIZE",
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
    code (:class:`_TransportHealth`, :class:`_SweepClock`), so the two loops cannot
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
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size!r}")
        self.backend = backend
        self.dispatch_fn = dispatch_fn
        self.batch_size = batch_size
        self.idle_poll_seconds = idle_poll_seconds
        self.sweep_interval_seconds = sweep_interval_seconds
        self._health = _TransportHealth(dispatch_retry_base_seconds, dispatch_retry_cap_seconds)
        self._sweep_clock = _SweepClock(sweep_interval_seconds, monotonic)
        self._stop = asyncio.Event()

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
                await self.backend.release(unsent)
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

    # --- the loop ---

    async def run(self, *, max_iterations: int | None = None) -> None:
        """Dispatch until :meth:`stop` is called, or the task is cancelled."""
        logger.info(
            "Dewey async dispatcher starting (batch_size=%d idle_poll=%.1fs sweep=%s)",
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

                await self.maybe_sweep()
                dispatched = await self.dispatch_batch()

                if self._health.retry_delay:
                    await self._sleep(self._health.retry_delay)
                    continue
                if dispatched >= self.batch_size:
                    continue
                await self.backend.wait_for_work(self.idle_poll_seconds)
        except asyncio.CancelledError:
            logger.info("Dewey async dispatcher cancelled")
            raise
        finally:
            logger.info("Dewey async dispatcher stopped after %d iteration(s)", iterations)
            await self.backend.close()

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
