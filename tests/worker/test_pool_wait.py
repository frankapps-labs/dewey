"""Non-consuming pool readiness and service pacing; no database required."""

import multiprocessing
import os
import signal
import threading
import time
from types import SimpleNamespace

import pytest

from dewey._worker import pool as pool_module
from dewey._worker.pool import Assignment, Outcome, PoolBroken, WorkerPool, _Slot
from dewey.worker import _process


@pytest.fixture
def probe_pool():
    """No OS children: inspect precisely which readiness handles are used."""
    pool = WorkerPool(
        "tests._helpers.worker_runtime:create_runtime",
        concurrency=2,
        task_timeout_seconds=5,
    )
    pool._started = True
    for index in range(2):
        process = SimpleNamespace(pid=100 + index, sentinel=200 + index, is_alive=lambda: True)
        pool._slots.append(_Slot(process, object(), 0, ready=True))
    return pool


def test_idle_wait_keeps_requested_cadence_without_reading_or_watching_pipes(
    probe_pool, monkeypatch
):
    sleeps = []
    monkeypatch.setattr(pool_module.time, "sleep", sleeps.append)
    monkeypatch.setattr(
        pool_module, "wait_for_objects", lambda *args: pytest.fail("Idle pipe wait")
    )
    probe_pool.wait(0.1)
    probe_pool.wait(0)
    assert sleeps == [0.1, 0]
    assert probe_pool.capacity == 2
    assert probe_pool.pending_events == ()


@pytest.mark.parametrize("initializing", [False, True])
def test_wait_watches_only_active_or_initializing_children(probe_pool, monkeypatch, initializing):
    slot = probe_pool._slots[0]
    if initializing:
        slot.ready = False
    else:
        slot.ticket = "owned-ticket"
        slot.assignment = Assignment("job", "generation")
    calls = []

    def wait(objects, timeout):
        calls.append((objects, timeout))
        return []  # A bounded timer is not an observation or permission to reuse.

    monkeypatch.setattr(pool_module, "wait_for_objects", wait)
    probe_pool.wait(0.1)
    probe_pool.wait(0.02)
    assert calls == [([slot.connection, slot.process.sentinel], 0.1), (calls[0][0], 0.02)]
    assert slot.event is None
    assert probe_pool.pending_events == ()
    assert probe_pool.capacity == 1


def test_unchanged_pipe_readability_keeps_sentinel_wait_until_slot_progress(
    probe_pool, monkeypatch
):
    slot = probe_pool._slots[0]
    slot.ticket = "first"
    calls = []

    def readable(objects, timeout):
        calls.append((objects, timeout))
        return [slot.connection] if slot.connection in objects else []

    monkeypatch.setattr(pool_module, "wait_for_objects", readable)
    monkeypatch.setattr(pool_module.time, "sleep", lambda _: pytest.fail("Whole-pool sleep"))
    probe_pool.wait(0.1)
    for _ in range(3):
        probe_pool.wait(0.1)
    assert (
        calls
        == [([slot.connection, slot.process.sentinel], 0.1)] + [([slot.process.sentinel], 0.1)] * 3
    )
    assert slot.event is None and probe_pool.pending_events == ()
    slot.ticket = "next-generation"
    probe_pool.wait(0.1)
    assert calls[-1] == ([slot.connection, slot.process.sentinel], 0.1)


@pytest.mark.parametrize("signal_kind", ["sibling_pipe", "own_sentinel", "sibling_sentinel"])
def test_unchanged_pipe_does_not_suppress_other_readiness(probe_pool, monkeypatch, signal_kind):
    first, second = probe_pool._slots
    first.ticket, second.ticket = "first", "second"
    signal_object = {
        "sibling_pipe": second.connection,
        "own_sentinel": first.process.sentinel,
        "sibling_sentinel": second.process.sentinel,
    }[signal_kind]
    responses = [[first.connection], [signal_object], []]
    calls = []

    def wait(objects, timeout):
        calls.append((objects, timeout))
        result = responses.pop(0)
        assert all(obj in objects for obj in result)
        return result

    monkeypatch.setattr(pool_module, "wait_for_objects", wait)
    monkeypatch.setattr(pool_module.time, "sleep", lambda _: pytest.fail("Whole-pool sleep"))
    for _ in range(3):
        probe_pool.wait(0.1)
    assert calls[1] == ([first.process.sentinel, second.connection, second.process.sentinel], 0.1)
    expected = [first.process.sentinel, second.process.sentinel]
    if signal_kind != "sibling_pipe":
        expected.insert(1, second.connection)
    assert calls[2] == (expected, 0.1)
    assert probe_pool.pending_events == () and probe_pool.capacity == 0
    assert [slot.ticket for slot in probe_pool._slots] == ["first", "second"]


@pytest.mark.parametrize("progress", ["connection", "replacement"])
def test_pipe_suppression_does_not_survive_connection_or_slot_replacement(
    probe_pool, monkeypatch, progress
):
    slot = probe_pool._slots[0]
    slot.ticket = "first"
    calls = []

    def wait(objects, timeout):
        calls.append((objects, timeout))
        return [objects[0]] if len(objects) == 2 else []

    monkeypatch.setattr(pool_module, "wait_for_objects", wait)
    monkeypatch.setattr(pool_module.time, "sleep", lambda _: pytest.fail("Whole-pool sleep"))
    probe_pool.wait(0.1)
    probe_pool.wait(0.1)
    if progress == "connection":
        slot.connection = object()
    else:
        slot = _Slot(slot.process, slot.connection, 0, ready=True, ticket="first")
        probe_pool._slots[0] = slot
    probe_pool.wait(0.1)
    assert calls[-1] == ([slot.connection, slot.process.sentinel], 0.1)


def test_pending_event_returns_without_wait_or_ack(probe_pool, monkeypatch):
    slot = probe_pool._slots[0]
    slot.ticket = "ticket"
    slot.assignment = Assignment("job", "generation")
    event = probe_pool._event(slot, Outcome.RETURNED)
    monkeypatch.setattr(pool_module.time, "sleep", lambda *args: pytest.fail("Pending sleep"))
    monkeypatch.setattr(pool_module, "wait_for_objects", lambda *args: pytest.fail("Pending wait"))
    probe_pool.wait(0.1)
    assert probe_pool.pending_events == (event,)
    assert slot.ticket == event.ticket and probe_pool.capacity == 1


@pytest.mark.parametrize("seconds", [-1, float("inf"), float("nan"), True])
def test_wait_rejects_invalid_bound_before_any_io(probe_pool, monkeypatch, seconds):
    monkeypatch.setattr(pool_module.time, "sleep", lambda *args: pytest.fail("Invalid sleep"))
    monkeypatch.setattr(pool_module, "wait_for_objects", lambda *args: pytest.fail("Invalid wait"))
    with pytest.raises(ValueError, match="finite and nonnegative"):
        probe_pool.wait(seconds)


@pytest.mark.parametrize("state", ["not_started", "closing", "closed", "broken"])
def test_wait_rejects_unusable_pool(probe_pool, state):
    if state == "not_started":
        probe_pool._started = False
    else:
        setattr(probe_pool, "_" + state, True)
    with pytest.raises(PoolBroken):
        probe_pool.wait(0)


def test_wait_requires_owner_process_and_thread(probe_pool, monkeypatch):
    errors = []

    def other_thread():
        try:
            probe_pool.wait(0)
        except RuntimeError as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert errors == ["WorkerPool must be used from its owning process and thread"]
    monkeypatch.setattr(pool_module.os, "getpid", lambda: probe_pool._owner[0] + 1)
    with pytest.raises(RuntimeError, match="owning process and thread"):
        probe_pool.wait(0)


def test_readiness_error_is_not_success_or_capacity_release(probe_pool, monkeypatch):
    slot = probe_pool._slots[0]
    slot.ticket = "ticket"

    def broken(*args):
        raise OSError("synthetic bad handle")

    monkeypatch.setattr(pool_module, "wait_for_objects", broken)
    with pytest.raises(OSError):
        probe_pool.wait(0.1)
    assert slot.ticket == "ticket" and slot.event is None and probe_pool.capacity == 1


def test_other_slot_progress_does_not_reenable_unchanged_pipe(probe_pool, monkeypatch):
    first, second = probe_pool._slots
    first.ticket, second.ticket = "first", "second"
    calls = []

    def wait(objects, timeout):
        calls.append(objects)
        return [first.connection] if first.connection in objects else []

    monkeypatch.setattr(pool_module, "wait_for_objects", wait)
    monkeypatch.setattr(pool_module.time, "sleep", lambda _: pytest.fail("Whole-pool sleep"))
    probe_pool.wait(0.1)
    second.ticket = "new-second-generation"
    probe_pool.wait(0.1)
    assert calls[-1] == [first.process.sentinel, second.connection, second.process.sentinel]


@pytest.fixture
def selectable_pool():
    """Owned real pipes; synthetic alive process handles, not subprocess-death proof."""
    pool = WorkerPool(
        "tests._helpers.worker_runtime:create_runtime",
        concurrency=2,
        task_timeout_seconds=5,
    )
    pool._started = True
    peers, descriptors = [], []
    for index in range(2):
        parent, peer = multiprocessing.Pipe()
        sentinel, writer = os.pipe()
        process = SimpleNamespace(
            pid=100 + index, sentinel=sentinel, sentinel_writer=writer, is_alive=lambda: True
        )
        pool._slots.append(_Slot(process, parent, 0, ready=True))
        peers.append(peer)
        descriptors.extend((sentinel, writer))
    try:
        yield pool, peers
    finally:
        for slot in pool._slots:
            slot.connection.close()
        for peer in peers:
            peer.close()
        for descriptor in descriptors:
            os.close(descriptor)


@pytest.mark.parametrize("signal_kind", ["sibling_return", "own_sentinel", "sibling_sentinel"])
def test_real_unchanged_eof_allows_independent_early_wake(selectable_pool, signal_kind):
    pool, peers = selectable_pool
    first = pool.submit(Assignment("first", "generation-1"))
    second = pool.submit(Assignment("second", "generation-2"))
    for peer in peers:
        peer.recv()
    peers[0].close()
    pool.wait(1)
    assert pool.poll() == [] and pool.capacity == 0
    signaled = threading.Event()
    signal_slot = pool._slots[0 if signal_kind == "own_sentinel" else 1]

    def signal_ready():
        if signal_kind == "sibling_return":
            peers[1].send(("returned", second))
        else:
            os.write(signal_slot.process.sentinel_writer, b"!")
        signaled.set()

    timer = threading.Timer(0.025, signal_ready)
    timer.start()
    try:
        began = time.monotonic()
        pool.wait(1)
        elapsed = time.monotonic() - began
    finally:
        timer.join(timeout=2)
    assert not timer.is_alive() and signaled.is_set()
    assert elapsed < 0.8  # Generous headroom; the old whole-pool fallback takes a full second.
    assert pool.pending_events == () and pool.capacity == 0
    assert [slot.ticket for slot in pool._slots] == [first, second]
    if signal_kind == "sibling_return":
        assert pool._slots[1].connection.poll()
        assert [(event.ticket, event.outcome) for event in pool.poll()] == [
            (second, Outcome.RETURNED)
        ]
        assert pool.capacity == 0
        pool.acknowledge(second)
        assert pool.capacity == 1 and pool._slots[0].ticket == first
    else:
        assert os.read(signal_slot.process.sentinel, 1) == b"!"
        assert pool.poll() == [] and pool.pending_events == () and pool.capacity == 0


@pytest.fixture
def live_pool():
    with WorkerPool(
        "tests._helpers.worker_runtime:create_runtime",
        concurrency=1,
        task_timeout_seconds=5,
        termination_timeout_seconds=0.2,
    ) as pool:
        yield pool


def test_real_return_wakes_without_receiving_or_releasing(live_pool):
    ticket = live_pool.submit(Assignment("sleep", "generation"))
    live_pool.wait(5)
    assert live_pool._slots[0].connection.poll()
    assert live_pool._slots[0].event is None and live_pool.pending_events == ()
    assert live_pool.capacity == 0
    events = live_pool.poll()
    assert [(event.ticket, event.outcome) for event in events] == [(ticket, Outcome.RETURNED)]
    live_pool.acknowledge(ticket)
    assert live_pool.capacity == 1


def test_real_readable_eof_with_live_child_does_not_spin_or_release(live_pool, monkeypatch):
    live_pool.submit(Assignment("hang", "generation"))
    slot = live_pool._slots[0]
    original = slot.connection
    read_end, write_end = multiprocessing.Pipe()
    write_end.close()
    slot.connection = read_end
    try:
        live_pool.wait(0.1)  # Real EOF readiness, not a completed assignment.
        assert live_pool.poll() == []
        assert slot.process.is_alive() and live_pool.capacity == 0
        calls = []
        original_wait = pool_module.wait_for_objects

        def bounded_sentinel_wait(objects, timeout):
            calls.append((objects, timeout))
            return original_wait(objects, timeout)

        monkeypatch.setattr(pool_module, "wait_for_objects", bounded_sentinel_wait)
        began = time.monotonic()
        for _ in range(3):
            live_pool.wait(0.02)
        assert calls == [([slot.process.sentinel], 0.02)] * 3
        assert time.monotonic() - began >= 0.05
        assert live_pool.pending_events == () and slot.event is None
    finally:
        slot.connection = original
        read_end.close()


def test_real_child_death_wakes_then_poll_records_loss(live_pool):
    ticket = live_pool.submit(Assignment("exit", "generation"))
    live_pool.wait(5)
    # EOF may become readable just before the process sentinel; wait for actual
    # death before expecting LOST, not a fabricated completion from readiness.
    live_pool._slots[0].process.join(timeout=5)
    assert not live_pool._slots[0].process.is_alive()
    assert live_pool.pending_events == () and live_pool.capacity == 0
    assert [(event.ticket, event.outcome) for event in live_pool.poll()] == [(ticket, Outcome.LOST)]


def test_replacement_ready_wakes_but_only_poll_admits_capacity(live_pool):
    ticket = live_pool.submit(Assignment("hang", "generation"))
    live_pool.stop(ticket, recycle=True)
    live_pool.acknowledge(ticket)
    assert not live_pool._slots[0].ready and live_pool.capacity == 0
    live_pool.wait(5)
    assert not live_pool._slots[0].ready  # wait did not consume the ready message
    assert live_pool.poll() == []
    assert live_pool.capacity == 1


def test_real_busy_wait_expires_without_polling_task_timeout(live_pool):
    ticket = live_pool.submit(Assignment("hang", "generation"))
    live_pool._slots[0].assigned_at = 0
    began = time.monotonic()
    live_pool.wait(0.02)
    assert time.monotonic() - began >= 0.015
    assert live_pool.pending_events == () and live_pool._slots[0].event is None
    assert live_pool._slots[0].process.is_alive() and live_pool.capacity == 0
    assert [(event.ticket, event.outcome) for event in live_pool.poll()] == [
        (ticket, Outcome.TIMED_OUT)
    ]


def test_service_uses_pool_wait_and_caps_it_at_drain_deadline(monkeypatch):
    now = [100.0]
    waits, handlers = [], {}
    config = SimpleNamespace(
        concurrency=1,
        startup_seconds=5,
        max_restarts=0,
        shutdown_seconds=0.05,
        auto_apply_schedules=False,
        alert_hook=None,
        timing=SimpleNamespace(
            task_timeout_seconds=5, termination_seconds=1, poll_interval_seconds=0.1
        ),
    )
    ctx = SimpleNamespace(config=config)
    app = SimpleNamespace(storage=object(), close_resources=lambda: None)

    class Pool:
        def __init__(self, *args, **kwargs):
            pass

        def wait(self, seconds):
            waits.append(seconds)
            if len(waits) == 1:
                handlers[signal.SIGTERM](signal.SIGTERM, None)

    class Runner:
        def __init__(self, *args, **kwargs):
            self.assignments = {"ticket": object()}
            self.steps = 0
            self.can_claim = kwargs["can_claim"]

        def start(self):
            pass

        def step(self):
            self.steps += 1
            if self.steps == 2:
                assert not self.can_claim()
                now[0] += 0.06
                self.assignments.clear()

        def close(self):
            pass

    monkeypatch.setenv(_process.STATUS_FD, "123")
    monkeypatch.setattr(_process, "context", lambda role: ctx)
    monkeypatch.setattr(_process, "application", lambda context: app)
    monkeypatch.setattr(_process, "WorkerPool", Pool)
    monkeypatch.setattr(_process, "NativeRunner", Runner)
    monkeypatch.setattr(_process.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(_process.time, "sleep", lambda *args: pytest.fail("Fixed service sleep"))
    monkeypatch.setattr(signal, "signal", lambda sig, handler: handlers.update({sig: handler}))
    monkeypatch.setattr(os, "write", lambda *args: None)
    monkeypatch.setattr(os, "close", lambda *args: None)
    assert _process.service() == 0
    assert waits == [0.1, 0.0]
