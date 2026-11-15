"""Deterministic control-channel fault injection; no DB or owned OS children."""

from collections import deque

import pytest

from dewey._worker.pool import Assignment, Outcome, PoolBroken, WorkerPool, _Slot


class ProcessProbe:
    pid = 12345

    def __init__(self):
        self.alive = True
        self.closed = False
        self.fail_termination = False
        self.joins = []

    def is_alive(self):
        return self.alive

    def terminate(self):
        if self.fail_termination:
            raise OSError("synthetic termination failure")
        self.alive = False

    def kill(self):
        self.alive = False

    def join(self, timeout):
        self.joins.append(timeout)

    def close(self):
        self.closed = True


class ChannelProbe:
    def __init__(self, messages=()):
        self.messages = deque(messages)
        self.closed = False
        self.fail_send = False

    def poll(self, timeout=0):
        return bool(self.messages)

    def recv(self):
        return self.messages.popleft()

    def send(self, message):
        if self.fail_send:
            raise OSError("synthetic uncertain delivery")

    def close(self):
        self.closed = True


def ready_pool():
    pool = WorkerPool(
        "tests._helpers.worker_runtime:create_runtime",
        concurrency=1,
        task_timeout_seconds=5,
        termination_timeout_seconds=0.1,
    )
    process, channel = ProcessProbe(), ChannelProbe()
    slot = _Slot(process, channel, 0, ready=True)
    pool._slots = [slot]
    pool._started = True
    return pool, slot


@pytest.mark.parametrize(
    "message",
    [
        (),
        ("returned",),
        ("returned", "ticket", "extra"),
        None,
        42,
        {"kind": "returned", "ticket": "ticket"},
    ],
)
def test_malformed_control_message_fails_closed_and_preserves_assignment(message):
    pool, slot = ready_pool()
    ticket = pool.submit(Assignment("synthetic-task", "generation"))
    slot.connection.messages.append(message)
    try:
        with pytest.raises(PoolBroken, match="protocol"):
            pool.poll()
        assert pool.capacity == 0
        with pytest.raises(PoolBroken):
            pool.submit(Assignment("not-admitted", "generation"))
    finally:
        with pytest.raises(PoolBroken, match="protocol"):
            pool.shutdown()
        events = pool.pending_events
    assert [(e.ticket, e.outcome) for e in events] == [(ticket, Outcome.STOPPED)]
    assert slot.process.closed


def test_cleanup_corruption_reaps_all_children_without_retry_livelock():
    pool, slot = ready_pool()
    slot.connection.messages.append(("invalid",))
    with pytest.raises(PoolBroken, match="protocol"):
        pool.shutdown()
    assert pool.worker_pids == ()
    assert slot.process.closed and slot.connection.closed
    assert pool.shutdown() == []


def test_shutdown_preserves_return_queued_after_last_grace_poll(monkeypatch):
    pool, slot = ready_pool()
    ticket = pool.submit(Assignment("synthetic-task", "generation"))

    def deadline_race():
        slot.connection.messages.append(("returned", ticket))
        return []

    monkeypatch.setattr(pool, "poll", deadline_race)
    events = pool.shutdown(grace_seconds=0)
    assert [(e.ticket, e.outcome) for e in events] == [(ticket, Outcome.RETURNED)]
    assert pool.pending_events == tuple(events)


def test_uncertain_send_and_failed_termination_fail_closed_without_erasing_ticket():
    pool, slot = ready_pool()
    slot.connection.fail_send = True
    slot.process.fail_termination = True
    try:
        with pytest.raises(PoolBroken):
            pool.submit(Assignment("synthetic-task", "generation"))
        assert pool.capacity == 0
        with pytest.raises(PoolBroken):
            pool.submit(Assignment("not-admitted", "generation"))
        assert slot.ticket and slot.assignment == Assignment("synthetic-task", "generation")
    finally:
        slot.process.fail_termination = False
        events = pool.shutdown()
    assert len(events) == 1
    assert events[0].ticket == slot.ticket
    assert events[0].outcome == Outcome.LOST


def test_shutdown_reaps_children_even_if_grace_poll_raises(monkeypatch):
    pool, slot = ready_pool()
    ticket = pool.submit(Assignment("synthetic-task", "generation"))

    def broken_poll():
        raise PoolBroken("synthetic protocol failure")

    monkeypatch.setattr(pool, "poll", broken_poll)
    events = pool.shutdown(grace_seconds=1)
    assert [(e.ticket, e.outcome) for e in events] == [(ticket, Outcome.STOPPED)]
    assert slot.process.closed
    assert pool.worker_pids == ()


def test_timeout_reap_failure_retains_an_unknown_observation():
    pool, slot = ready_pool()
    ticket = pool.submit(Assignment("synthetic-task", "generation"))
    slot.assigned_at = 0
    slot.process.fail_termination = True
    try:
        with pytest.raises(PoolBroken):
            pool.poll()
        assert [(e.ticket, e.outcome) for e in pool.pending_events] == [(ticket, Outcome.LOST)]
        assert pool.capacity == 0
    finally:
        slot.process.fail_termination = False
        pool.shutdown()


def test_idle_shutdown_join_is_bounded():
    pool, slot = ready_pool()
    pool.shutdown()
    assert slot.process.joins
    assert all(timeout == pool.termination_timeout_seconds for timeout in slot.process.joins)
