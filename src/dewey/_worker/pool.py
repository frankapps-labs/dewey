"""Bounded process supervision, independent of storage and frameworks.

This is not a DispatcherAdapter or a production worker runner. In particular,
RETURNED means only that the integration callback returned, not that a task
succeeded. The caller must reconcile durable ownership/outcome before acknowledging
an event and reusing capacity. The pool never retries a task or releases a DB claim.

Children are spawned (a fresh interpreter each) unless the pool is created with
``start_method="fork"``, in which case they are forked from the supervising
process after it has imported the application, and share its pages. The caller
owns fork safety: ``prepare`` runs right before every child start so it can close
connections that must not cross the fork.

Use from one supervising thread, call poll frequently (wait only paces those
calls), and always shut down or use a context manager. Signal handling and containment are
responsibilities of the eventual runner, not guarantees of this primitive.
"""

from __future__ import annotations

import importlib
import inspect
import math
import multiprocessing
import os
import signal
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from multiprocessing.connection import wait as wait_for_objects
from multiprocessing.process import BaseProcess
from typing import Any, Protocol, cast
from uuid import uuid4

from dewey.runner import RunnerAssignment as Assignment


class ChildRuntime(Protocol):
    """Created inside each child; never carry parent connections through IPC."""

    def execute(self, assignment: Assignment) -> object: ...

    def close(self) -> None: ...


class Outcome(StrEnum):
    RETURNED = "returned"
    EXECUTION_ERROR = "execution_error"
    LOST = "lost"
    TIMED_OUT = "timed_out"
    STOPPED = "stopped"


@dataclass(frozen=True)
class WorkerEvent:
    """An execution-slot observation, not a persisted task outcome."""

    ticket: str
    assignment: Assignment
    worker_pid: int
    outcome: Outcome


class NoCapacity(RuntimeError):
    """No acknowledged, ready execution slot is available."""


class PoolBroken(RuntimeError):
    """Bootstrap, supervision or replacement failed; stop accepting work."""


class _ControlChannel(Protocol):
    """Common structural surface for POSIX and Windows multiprocessing pipes."""

    def send(self, obj: object) -> None: ...

    def recv(self) -> Any: ...

    def poll(self, timeout: float = 0.0) -> bool: ...

    def fileno(self) -> int: ...

    def close(self) -> None: ...


@dataclass
class _Slot:
    process: BaseProcess
    connection: _ControlChannel
    started_at: float
    ready: bool = False
    ticket: str | None = None
    assignment: Assignment | None = None
    assigned_at: float = 0.0
    event: WorkerEvent | None = None
    restarts: int = 0
    planned_recycle: bool = False


def _load_factory(path: str) -> Callable[[], ChildRuntime]:
    module, name = path.split(":")
    factory = getattr(importlib.import_module(module), name)
    if (
        not callable(factory)
        or inspect.iscoroutinefunction(factory)
        or inspect.isasyncgenfunction(factory)
    ):
        raise TypeError("Worker factory must be a synchronous callable")
    return cast(Callable[[], ChildRuntime], factory)


def _require_sync_result(result: object) -> None:
    if inspect.isawaitable(result) or inspect.isasyncgen(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError("Async result returned by synchronous child lifecycle")


START_METHODS = ("spawn", "fork")
_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def _child_main(
    connection: _ControlChannel, factory_path: str, close_fds: Sequence[int] = ()
) -> None:
    runtime: ChildRuntime | None = None
    initialized = False
    # A forked child inherits the supervising process's ends of every sibling's
    # pipe and whatever it was told to close: holding them would keep a sibling
    # from ever seeing EOF when the supervising process dies, and would tie the
    # service's own pipes to this process's lifetime.
    for fd in close_fds:
        try:
            os.close(fd)
        except OSError:
            pass
    # A forked child inherits the supervising process's handlers; a task process
    # must stop on SIGTERM the way a spawned one does. The parent kept the stop
    # signals blocked across the fork so none could reach the inherited handler
    # before this point; they are delivered from here on, to the defaults.
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.default_int_handler)
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(signal.SIG_UNBLOCK, _STOP_SIGNALS)
    try:
        try:
            created = _load_factory(factory_path)()
            _require_sync_result(created)
            runtime = created
            if not callable(getattr(runtime, "execute", None)) or not callable(
                getattr(runtime, "close", None)
            ):
                raise TypeError("Invalid child runtime")
            if any(
                inspect.iscoroutinefunction(method) or inspect.isasyncgenfunction(method)
                for method in (runtime.execute, runtime.close)
            ):
                raise TypeError("ChildRuntime lifecycle methods must be synchronous")
            initialized = True
        except BaseException:
            connection.send(("bootstrap_failed", None))
            return
        connection.send(("ready", None))
        while True:
            message = connection.recv()
            if message is None:
                break
            ticket, assignment = message
            try:
                result = runtime.execute(assignment)
                _require_sync_result(result)
            except BaseException:
                # Do not transmit payloads, exception messages or tracebacks over IPC.
                connection.send(("execution_error", ticket))
                return
            connection.send(("returned", ticket))
    except (EOFError, OSError):
        pass
    finally:
        if runtime is not None:
            try:
                _require_sync_result(runtime.close())
            except BaseException:
                if initialized:
                    try:
                        connection.send(("cleanup_failed", None))
                    except (EOFError, OSError):
                        pass
        connection.close()


def _seconds(name: str, value: float, *, allow_zero: bool = False) -> None:
    if not math.isfinite(value) or value < 0 or (not allow_zero and value == 0):
        raise ValueError(f"{name} must be finite and {'nonnegative' if allow_zero else 'positive'}")


class WorkerPool:
    """A fixed number of children with no hidden work backlog.

    A slot holds at most one assignment. Even when execution finishes it remains
    occupied until acknowledge(ticket), allowing the caller to finish durable
    reconciliation first. A failed child is replaced only after acknowledgement,
    subject to a finite per-slot health-failure replacement budget. Intentional
    task-lifecycle recycling after stop(recycle=True) preserves that budget;
    observed crashes, execution errors and cleanup/bootstrap failures do not.
    No task is resubmitted here.

    Timeouts are enforced when poll is called, measured from assignment submission
    (including integration claim overhead). An already queued return is observed before
    checking the deadline; timeout is not retroactive elapsed-runtime validation. The
    runner must bound its polling cadence and any blocking storage operations.
    Factory setup is separately bounded, per child and from its own start.

    Children start one after another: start() returns once the first is ready,
    and poll() starts the next one each time the newest has become ready, so a
    slow application import on a small CPU costs each child only its own
    startup budget and capacity is usable before every child is up.
    """

    def __init__(
        self,
        factory_path: str,
        *,
        concurrency: int,
        task_timeout_seconds: float,
        startup_timeout_seconds: float = 10.0,
        termination_timeout_seconds: float = 1.0,
        max_restarts: int = 3,
        start_method: str = "spawn",
        prepare: Callable[[], None] | None = None,
        progress: Callable[[], None] | None = None,
        cleanup_timeout_seconds: float | None = None,
        close_in_children: Sequence[int] = (),
    ) -> None:
        if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
            raise ValueError("concurrency must be a positive integer")
        if isinstance(max_restarts, bool) or not isinstance(max_restarts, int) or max_restarts < 0:
            raise ValueError("max_restarts must be a nonnegative integer")
        if (
            not isinstance(factory_path, str)
            or factory_path.count(":") != 1
            or not all(
                part and all(p.isidentifier() for p in part.split("."))
                for part in factory_path.split(":")
            )
            or "." in factory_path.split(":")[1]
        ):
            raise ValueError("factory_path must be 'module:factory'")
        _seconds("task_timeout_seconds", task_timeout_seconds)
        _seconds("startup_timeout_seconds", startup_timeout_seconds)
        _seconds("termination_timeout_seconds", termination_timeout_seconds)
        if cleanup_timeout_seconds is None:
            cleanup_timeout_seconds = termination_timeout_seconds
        _seconds("cleanup_timeout_seconds", cleanup_timeout_seconds)
        if start_method not in START_METHODS:
            raise ValueError(f"start_method must be one of {', '.join(START_METHODS)}")
        self.factory_path = factory_path
        self.concurrency = concurrency
        self.task_timeout_seconds = task_timeout_seconds
        self.startup_timeout_seconds = startup_timeout_seconds
        self.termination_timeout_seconds = termination_timeout_seconds
        #: How long idle children get, together, to run their runtime's close()
        #: at shutdown (a telemetry flush, for one) before they are terminated.
        self.cleanup_timeout_seconds = max(cleanup_timeout_seconds, termination_timeout_seconds)
        #: File descriptors a forked child closes at once: the caller's own
        #: pipes, which a child must not keep open (see _child_main).
        self.close_in_children = tuple(close_in_children)
        self.max_restarts = max_restarts
        self.start_method = start_method
        #: Called in this process right before every child start. With fork it
        #: must close every connection the child would otherwise inherit.
        self.prepare = prepare
        self._progress = progress
        self._context: Any = multiprocessing.get_context(start_method)
        self._slots: list[_Slot] = []
        self._events: dict[str, WorkerEvent] = {}
        self._owner = (os.getpid(), threading.get_ident())
        self._started = False
        self._closing = False
        self._draining = False
        self._closed = False
        self._broken = False
        self._cleanup_failed = False
        self._protocol_failed = False
        self._wait_readable: set[tuple[int, int, bool, str | None]] = set()

    def _check_owner(self) -> None:
        if (os.getpid(), threading.get_ident()) != self._owner:
            raise RuntimeError("WorkerPool must be used from its owning process and thread")

    def _check_running(self) -> None:
        self._check_owner()
        if not self._started or self._closing or self._closed or self._broken:
            raise PoolBroken("Worker pool is not accepting work")

    def _spawn(self, restarts: int = 0) -> _Slot:
        if self.prepare is not None:
            self.prepare()
        parent, child = self._context.Pipe()
        close_fds: tuple[int, ...] = ()
        if self.start_method == "fork":
            # Our end of the new pipe, our ends of every sibling's pipe, and the
            # caller's own descriptors: none of them belongs in the child.
            close_fds = (
                parent.fileno(),
                *(slot.connection.fileno() for slot in self._slots),
                *self.close_in_children,
            )
        process = self._context.Process(
            target=_child_main,
            args=(child, self.factory_path, close_fds),
            name="dewey-worker",
            daemon=True,
        )
        # A forked child inherits this process's signal handlers until
        # _child_main resets them; a stop signal in that window would run a
        # handler whose state is a dead copy. Block the stop signals across the
        # fork; the child unblocks them once its handlers are its own.
        blocked = None
        if self.start_method == "fork" and hasattr(signal, "pthread_sigmask"):
            blocked = signal.pthread_sigmask(signal.SIG_BLOCK, _STOP_SIGNALS)
        try:
            process.start()
        except BaseException:
            parent.close()
            child.close()
            process.close()
            raise
        finally:
            if blocked is not None:
                signal.pthread_sigmask(signal.SIG_SETMASK, blocked)
        child.close()
        return _Slot(process, parent, time.monotonic(), restarts=restarts)

    def start(self) -> None:
        """Start the first child and return once it is ready; the rest follow in poll()."""
        self._check_owner()
        if self._started or self._closed:
            raise PoolBroken("Worker pool can only be started once")
        self._started = True
        try:
            self._slots.append(self._spawn())
            reported = time.monotonic()
            while not self._slots[0].ready:
                self.poll()
                if self._progress is not None and time.monotonic() - reported >= 0.1:
                    self._progress()
                    reported = time.monotonic()
                time.sleep(0.005)
        except BaseException:
            self._broken = True
            self.shutdown(grace_seconds=0)
            raise

    @property
    def capacity(self) -> int:
        """Ready acknowledged slots. Call poll first to refresh liveness."""
        self._check_owner()
        if self._closing or self._closed or self._broken:
            return 0
        return sum(
            slot.ready and slot.ticket is None and slot.process.is_alive() for slot in self._slots
        )

    @property
    def pending_events(self) -> tuple[WorkerEvent, ...]:
        """Unacknowledged observations, also recoverable after a poll error."""
        self._check_owner()
        return tuple(self._events.values())

    @property
    def worker_pids(self) -> tuple[int, ...]:
        self._check_owner()
        return tuple(slot.process.pid for slot in self._slots if slot.process.pid is not None)

    def wait(self, timeout_seconds: float) -> None:
        """Pause at most the requested interval, waking for child activity.

        This is only a pacing hint: it never receives messages, polls deadlines,
        reconciles outcomes or releases capacity. Call poll/runner.step after it.
        Pending observations return immediately. With no active or initializing
        child, retain the caller's idle cadence (no database listener is added).
        A pipe that stays readable without slot progress, including live-child
        EOF, is suppressed until progress. Other pipes and exit sentinels remain
        watched within the requested cadence; no whole-pool fallback or spin.
        """
        self._check_running()
        if isinstance(timeout_seconds, bool):
            raise ValueError("timeout_seconds must be finite and nonnegative")
        _seconds("timeout_seconds", timeout_seconds, allow_zero=True)
        if self._events:
            self._wait_readable.clear()
            return
        slots = {
            (id(slot), id(slot.connection), slot.ready, slot.ticket): slot
            for slot in self._slots
            if slot.ticket is not None or not slot.ready
        }
        self._wait_readable.intersection_update(slots)
        if not slots:
            time.sleep(timeout_seconds)
            return
        # Suppress only the unchanged pipe, never a sibling or exit sentinel.
        # Keep Connection objects for multiprocessing's platform-specific wait.
        objects: list[Any] = []
        for key, slot in slots.items():
            if key not in self._wait_readable:
                objects.append(slot.connection)
            objects.append(slot.process.sentinel)
        ready = {id(obj) for obj in wait_for_objects(objects, timeout_seconds)}
        self._wait_readable.update(key for key in slots if key[1] in ready)

    def submit(self, assignment: Assignment) -> str:
        self._check_running()
        if not isinstance(assignment, Assignment):
            raise TypeError("submit requires an Assignment")
        slot = next(
            (
                slot
                for slot in self._slots
                if slot.ready and slot.ticket is None and slot.process.is_alive()
            ),
            None,
        )
        if slot is None:
            raise NoCapacity("No ready worker slot")
        slot.ticket = uuid4().hex
        slot.assignment = assignment
        slot.assigned_at = time.monotonic()
        try:
            slot.connection.send((slot.ticket, assignment))
        except (OSError, EOFError):
            # Delivery might be uncertain; retain the slot and report it through
            # poll. Never let a failed pipe write silently return capacity.
            try:
                self._terminate(slot)
            except PoolBroken:
                self._broken = True
                if slot.event is None:
                    self._event(slot, Outcome.LOST)
                raise
        return slot.ticket

    def _event(self, slot: _Slot, outcome: Outcome) -> WorkerEvent:
        assert slot.ticket is not None and slot.assignment is not None
        assert slot.process.pid is not None
        event = WorkerEvent(slot.ticket, slot.assignment, slot.process.pid, outcome)
        slot.event = event
        self._events[event.ticket] = event
        return event

    def poll(self) -> list[WorkerEvent]:
        """Observe children once; terminal events are emitted once and held for ack."""
        self._check_owner()
        if self._closed:
            return []
        events: list[WorkerEvent] = []
        for slot in list(self._slots):
            if slot.event is not None:
                continue
            message = None
            try:
                if slot.connection.poll():
                    message = self._receive(slot)
            except (EOFError, OSError):
                pass
            if message is not None:
                kind, ticket = message
                if kind == "ready" and not slot.ready and slot.ticket is None:
                    slot.ready = True
                    if (
                        slot is self._slots[-1]
                        and len(self._slots) < self.concurrency
                        and not (
                            self._closing or self._draining or self._broken
                        )  # none on the way out
                    ):
                        try:
                            self._slots.append(self._spawn())
                        except BaseException:
                            self._broken = True
                            raise
                elif kind in ("returned", "execution_error") and ticket == slot.ticket and ticket:
                    outcome = Outcome.RETURNED if kind == "returned" else Outcome.EXECUTION_ERROR
                    # The child exits through runtime.close() after an error.
                    # Keep capacity reserved; acknowledge/shutdown gives cleanup a
                    # bounded grace before escalating to forced termination.
                    events.append(self._event(slot, outcome))
                    continue
                elif kind == "bootstrap_failed":
                    self._broken = True
                    raise PoolBroken("Worker bootstrap failed")
                elif kind == "cleanup_failed":
                    self._cleanup_failed = self._broken = True
                    raise PoolBroken("Worker lifecycle cleanup failed")
                else:
                    self._protocol_error()
            if not slot.process.is_alive():
                if slot.ticket is not None:
                    events.append(self._event(slot, Outcome.LOST))
                else:
                    self._broken = True
                    raise PoolBroken("An unassigned worker exited")
            elif (
                not slot.ready
                and time.monotonic() - slot.started_at >= self.startup_timeout_seconds
            ):
                self._broken = True
                raise PoolBroken(
                    "Worker initialization timed out: a task process took longer than "
                    f"startup_timeout_seconds={self.startup_timeout_seconds:g} to import "
                    "the application"
                )
            elif slot.ticket and time.monotonic() - slot.assigned_at >= self.task_timeout_seconds:
                self._terminate(slot)
                events.append(self._event(slot, Outcome.TIMED_OUT))
        return events

    def _protocol_error(self) -> None:
        self._protocol_failed = self._broken = True
        raise PoolBroken("Worker control protocol failed")

    def _receive(self, slot: _Slot) -> tuple[str, str | None]:
        message = slot.connection.recv()
        if not isinstance(message, tuple) or len(message) != 2:
            self._protocol_error()
        kind, ticket = message
        if kind in ("returned", "execution_error"):
            if not isinstance(ticket, str) or not ticket or ticket != slot.ticket:
                self._protocol_error()
        elif kind in ("ready", "bootstrap_failed", "cleanup_failed"):
            if ticket is not None:
                self._protocol_error()
        else:
            self._protocol_error()
        return kind, ticket

    def _terminate(self, slot: _Slot) -> None:
        try:
            if slot.process.is_alive():
                slot.process.terminate()
            slot.process.join(self.termination_timeout_seconds)
            if slot.process.is_alive():
                slot.process.kill()
                slot.process.join(self.termination_timeout_seconds)
            if slot.process.is_alive():
                raise OSError("Worker remains alive")
        except (OSError, ValueError) as exc:
            self._broken = True
            if slot.ticket is not None and slot.event is None:
                self._event(slot, Outcome.LOST)
            raise PoolBroken("Could not reap worker; capacity remains unavailable") from exc

    def _observe_cleanup(self, slot: _Slot) -> None:
        # Called only after reaping: at most ready, execution outcome and cleanup
        # observation can remain. Bound reads even if the protocol was corrupted.
        for _ in range(3):
            try:
                if not slot.connection.poll():
                    break
                kind, _ticket = self._receive(slot)
            except (EOFError, OSError, PoolBroken):
                break
            if kind == "cleanup_failed":
                self._cleanup_failed = True
            elif kind in ("returned", "execution_error") and slot.event is None:
                self._event(slot, Outcome(kind))

    def stop(self, ticket: str, *, recycle: bool = False) -> WorkerEvent:
        """Reap one assignment before durable recovery; retain its occupied slot.

        Stopping is not acknowledgement or proof of a missing database outcome.
        An already observed return is preserved, even when we terminate its child.
        recycle marks an intentional task-lifecycle stop, not a health failure.
        Observed crashes/errors still spend the finite replacement budget.
        """
        self._check_owner()
        for slot in self._slots:
            if slot.ticket == ticket:
                # A known dead child or queued error must not be hidden by a
                # caller's successful ledger observation. Cleanup below reads
                # queued observations without consuming other slots' events.
                intentional_stop = slot.event is not None and slot.event.outcome in (
                    Outcome.STOPPED,
                    Outcome.TIMED_OUT,
                )
                planned = recycle and (
                    intentional_stop
                    or (
                        slot.process.is_alive()
                        and (slot.event is None or slot.event.outcome == Outcome.RETURNED)
                    )
                )
                self._terminate(slot)
                self._observe_cleanup(slot)
                slot.planned_recycle = planned and (
                    slot.event is None
                    or slot.event.outcome in (Outcome.RETURNED, Outcome.STOPPED, Outcome.TIMED_OUT)
                )
                if slot.event is None:
                    self._event(slot, Outcome.STOPPED)
                assert slot.event is not None
                return slot.event
        raise ValueError("Unknown or already acknowledged ticket")

    def acknowledge(self, ticket: str) -> None:
        """Return capacity only after the caller has reconciled the durable outcome."""
        self._check_running()
        for index, slot in enumerate(self._slots):
            if slot.ticket != ticket:
                continue
            if slot.event is None:
                raise ValueError("Cannot acknowledge an unfinished assignment")
            if slot.event.outcome != Outcome.RETURNED or not slot.process.is_alive():
                if slot.event.outcome == Outcome.EXECUTION_ERROR:
                    slot.process.join(self.termination_timeout_seconds)
                self._terminate(slot)
                self._observe_cleanup(slot)
                if self._protocol_failed:
                    raise PoolBroken("Worker control protocol failed; stop the pool")
                if self._cleanup_failed:
                    self._broken = True
                    raise PoolBroken("Worker lifecycle cleanup failed; stop the pool")
                restarts = slot.restarts + (not slot.planned_recycle)
                if restarts > self.max_restarts:
                    self._broken = True
                    raise PoolBroken("Worker replacement budget exhausted")
                try:
                    replacement = self._spawn(restarts)
                except BaseException:
                    self._broken = True
                    raise
                slot.connection.close()
                slot.process.close()
                self._slots[index] = replacement
            else:
                slot.ticket = None
                slot.assignment = None
                slot.event = None
            del self._events[ticket]
            return
        raise ValueError("Unknown or already acknowledged ticket")

    def stop_starting(self) -> None:
        """Start no further staged children: the worker is draining.

        Children already running keep their work; shutdown() closes them later.
        """
        self._check_owner()
        self._draining = True

    def shutdown(self, *, grace_seconds: float = 0.0) -> list[WorkerEvent]:
        """Drain for a bounded grace, then terminate/reap all owned children.

        Returns all unacknowledged events, including previously polled events and
        STOPPED for slots without an observed result after reaping. STOPPED/LOST
        mean unknown durable outcome, not proof that the task did not finish.
        Queued terminal observations found during cleanup are preserved. Reconcile
        by ticket, not by notification count. It never requeues assignments. Cleanup may additionally take up to
        three termination intervals per slot. Invalid async cleanup or a cleanup
        exception raises PoolBroken after reaping; retained observations remain in
        pending_events. A completed cleanup failure is distinct from incomplete OS
        reaping, which retains slots for a shutdown retry.
        """
        self._check_owner()
        _seconds("grace_seconds", grace_seconds, allow_zero=True)
        if self._closed:
            return []
        self._closing = True
        deadline = time.monotonic() + grace_seconds
        try:
            while any(slot.ticket and slot.event is None for slot in self._slots):
                try:
                    self.poll()
                except PoolBroken:
                    break
                if time.monotonic() >= deadline:
                    break
                time.sleep(min(0.005, max(0.0, deadline - time.monotonic())))
        finally:
            failures = []
            remaining = []
            # Every idle child is asked to close at once, so their cleanups (a
            # telemetry flush, a connection disposal) overlap, and each is joined
            # against one shared budget rather than a per-slot termination slice.
            closing: list[_Slot] = []
            for slot in self._slots:
                if slot.ticket is not None and slot.event is None:
                    continue  # busy: reaped below, never asked to close first
                if slot.process.is_alive():
                    try:
                        slot.connection.send(None)
                        closing.append(slot)
                    except (EOFError, OSError):
                        pass
            cleanup_deadline = time.monotonic() + self.cleanup_timeout_seconds
            for slot in self._slots:
                try:
                    busy = slot.ticket is not None and slot.event is None
                    if slot in closing:
                        slot.process.join(
                            max(
                                self.termination_timeout_seconds,
                                cleanup_deadline - time.monotonic(),
                            )
                        )
                    self._terminate(slot)
                    self._observe_cleanup(slot)
                    if busy and slot.event is None:
                        self._event(slot, Outcome.STOPPED)
                    slot.connection.close()
                    slot.process.close()
                except Exception as exc:
                    # Still attempt cleanup of every other child. Retain failed
                    # slots so the caller can retry cleanup rather than orphaning them.
                    if slot.ticket is not None and slot.event is None:
                        self._event(slot, Outcome.LOST)
                    remaining.append(slot)
                    failures.append(exc)
            self._slots = remaining
            self._closed = not remaining
            if failures:
                raise PoolBroken("Worker cleanup incomplete; retry shutdown") from failures[0]
            if self._protocol_failed:
                raise PoolBroken("Worker control protocol failed; all children reaped")
            if self._cleanup_failed:
                self._broken = True
                raise PoolBroken("Worker lifecycle cleanup failed; all children reaped")
        return list(self._events.values())

    def __enter__(self) -> WorkerPool:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.shutdown(grace_seconds=0)
