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
        self.dispatch_retry_base_seconds = dispatch_retry_base_seconds
        self.dispatch_retry_cap_seconds = dispatch_retry_cap_seconds
        self._monotonic = monotonic

        self._stop = threading.Event()
        self._last_sweep: float | None = None
        self._retry_delay = 0.0

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
            self._note_dispatch_success()
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
                self._note_dispatch_failure()
                return index
            logger.debug("Dispatched task_id=%s", task_id)

        self._note_dispatch_success()
        logger.info("Dispatched %d task(s)", len(task_ids))
        return len(task_ids)

    def maybe_sweep(self) -> dict[str, list[str]] | None:
        """Run the sweep if its interval has elapsed. Returns what it touched."""
        if self.sweep_interval_seconds is None:
            return None
        now = self._monotonic()
        if self._last_sweep is not None and now - self._last_sweep < self.sweep_interval_seconds:
            return None
        self._last_sweep = now
        try:
            result = self.backend.run_sweep()
        except Exception:
            # Recovery failing must not stop dispatch; the next tick tries again.
            logger.warning("Dewey sweep failed; will retry next tick", exc_info=True)
            return None
        touched = {key: ids for key, ids in result.items() if ids}
        if touched:
            logger.info("Sweep recovered %s", {key: len(ids) for key, ids in touched.items()})
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

                if self._retry_delay:
                    # The transport is unhealthy. Wait, rather than spinning on a
                    # broker that is refusing connections.
                    self._stop.wait(self._retry_delay)
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

    # --- transport health ---

    def _note_dispatch_failure(self) -> None:
        if self._retry_delay:
            self._retry_delay = min(self._retry_delay * 2, self.dispatch_retry_cap_seconds)
        else:
            self._retry_delay = self.dispatch_retry_base_seconds

    def _note_dispatch_success(self) -> None:
        if self._retry_delay:
            logger.info("Transport recovered; resuming normal dispatch")
        self._retry_delay = 0.0


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_DISPATCH_RETRY_CAP_SECONDS",
    "DEFAULT_IDLE_POLL_SECONDS",
    "DEFAULT_SWEEP_INTERVAL_SECONDS",
    "DispatchBackend",
    "DispatchFn",
    "Dispatcher",
]
