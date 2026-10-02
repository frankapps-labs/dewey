"""Bounded, spawn-only process supervision, independent of storage and frameworks.

This is not a DispatcherAdapter or a production worker runner. In particular,
RETURNED means only that the integration callback returned, not that a task
succeeded. The caller must reconcile durable ownership/outcome before acknowledging
an event and reusing capacity. The pool never retries a task or releases a DB claim.

Use from one supervising thread, call poll frequently, and always shut down (or
use a context manager). Signal handling and parent-death/container containment are
responsibilities of the eventual runner, not guarantees of this primitive.
"""

from __future__ import annotations

import importlib
import inspect
import math
import multiprocessing
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from multiprocessing.process import BaseProcess
from typing import Any, Protocol, cast
from uuid import uuid4


@dataclass(frozen=True)
class Assignment:
    """Small IPC envelope; ownership tokens are opaque to this process primitive."""

    task_id: str
    dispatch_token: str

    def __post_init__(self) -> None:
        for name, value in (("task_id", self.task_id), ("dispatch_token", self.dispatch_token)):
            if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 256:
                raise ValueError(f"{name} must be a nonempty string of at most 256 UTF-8 bytes")


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


def _child_main(connection: _ControlChannel, factory_path: str) -> None:
    runtime: ChildRuntime | None = None
    initialized = False
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
    subject to a finite per-slot replacement budget. No task is resubmitted here.

    Timeouts are enforced when poll is called, measured from assignment submission
    (including integration claim overhead). An already queued return is observed before
    checking the deadline; timeout is not retroactive elapsed-runtime validation. The
    runner must bound its polling cadence and any blocking storage operations.
    Factory setup is separately bounded.
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
        self.factory_path = factory_path
        self.concurrency = concurrency
        self.task_timeout_seconds = task_timeout_seconds
        self.startup_timeout_seconds = startup_timeout_seconds
        self.termination_timeout_seconds = termination_timeout_seconds
        self.max_restarts = max_restarts
        self._context = multiprocessing.get_context("spawn")
        self._slots: list[_Slot] = []
        self._events: dict[str, WorkerEvent] = {}
        self._owner = (os.getpid(), threading.get_ident())
        self._started = False
        self._closing = False
        self._closed = False
        self._broken = False
        self._cleanup_failed = False
        self._protocol_failed = False

    def _check_owner(self) -> None:
        if (os.getpid(), threading.get_ident()) != self._owner:
            raise RuntimeError("WorkerPool must be used from its owning process and thread")

    def _check_running(self) -> None:
        self._check_owner()
        if not self._started or self._closing or self._closed or self._broken:
            raise PoolBroken("Worker pool is not accepting work")

    def _spawn(self, restarts: int = 0) -> _Slot:
        parent, child = self._context.Pipe()
        process = self._context.Process(
            target=_child_main,
            args=(child, self.factory_path),
            name="dewey-worker",
            daemon=True,
        )
        try:
            process.start()
        except BaseException:
            parent.close()
            child.close()
            process.close()
            raise
        child.close()
        return _Slot(process, parent, time.monotonic(), restarts=restarts)

    def start(self) -> None:
        self._check_owner()
        if self._started or self._closed:
            raise PoolBroken("Worker pool can only be started once")
        self._started = True
        try:
            for _ in range(self.concurrency):
                self._slots.append(self._spawn())
            while not all(slot.ready for slot in self._slots):
                self.poll()
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
        for slot in self._slots:
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
                raise PoolBroken("Worker initialization timed out")
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
                if slot.restarts >= self.max_restarts:
                    self._broken = True
                    raise PoolBroken("Worker replacement budget exhausted")
                try:
                    replacement = self._spawn(slot.restarts + 1)
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
            for slot in self._slots:
                try:
                    busy = slot.ticket is not None and slot.event is None
                    if not busy and slot.process.is_alive():
                        try:
                            slot.connection.send(None)
                        except (EOFError, OSError):
                            pass
                        slot.process.join(self.termination_timeout_seconds)
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
