"""Real spawned-process tests. No broker, ORM fixtures or database required."""

from __future__ import annotations

import multiprocessing
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from dewey._worker.pool import Assignment, NoCapacity, Outcome, PoolBroken, WorkerPool

FACTORY = "tests._helpers.worker_runtime:create_runtime"


def make_pool(**kwargs) -> WorkerPool:
    return WorkerPool(
        kwargs.pop("factory_path", FACTORY),
        concurrency=kwargs.pop("concurrency", 1),
        task_timeout_seconds=kwargs.pop("task_timeout_seconds", 5),
        startup_timeout_seconds=kwargs.pop("startup_timeout_seconds", 5),
        termination_timeout_seconds=kwargs.pop("termination_timeout_seconds", 0.2),
        **kwargs,
    )


def wait_events(pool: WorkerPool, count: int = 1):
    events = []
    deadline = time.monotonic() + 10
    while len(events) < count and time.monotonic() < deadline:
        events.extend(pool.poll())
        time.sleep(0.005)
    assert len(events) == count, events
    return events


def wait_capacity(pool: WorkerPool, capacity: int = 1) -> None:
    deadline = time.monotonic() + 10
    while pool.capacity != capacity and time.monotonic() < deadline:
        pool.poll()
        time.sleep(0.005)
    assert pool.capacity == capacity


def assert_reaped(pids: tuple[int, ...]) -> None:
    # Only probe our managed child handles. Signal-zero is not a portable liveness
    # check; successful pool cleanup also closes handles (which rejects live children).
    assert not set(pids).intersection(p.pid for p in multiprocessing.active_children())


def test_reaping_assertion_never_sends_a_signal(monkeypatch):
    def forbidden_signal(*args):
        pytest.fail("A liveness assertion must not send OS signals")

    monkeypatch.setattr(os, "kill", forbidden_signal)
    assert_reaped(())


def test_ready_children_are_spawned_and_initialized_locally(tmp_path, monkeypatch):
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    with make_pool(concurrency=2) as pool:
        wait_capacity(pool, 2)  # the second child starts once the first is ready
        pids = pool.worker_pids
        assert len(set(pids)) == 2
        assert os.getpid() not in pids
        assert len(list(tmp_path.glob("*-initialized"))) == 2
    assert len(list(tmp_path.glob("*-closed"))) == 2
    assert_reaped(pids)


def test_async_handler_is_awaited_inside_spawned_child(tmp_path, monkeypatch):
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    with make_pool(factory_path="tests._helpers.async_worker_runtime:create_runtime") as pool:
        pids = pool.worker_pids
        ticket = pool.submit(Assignment("async-effect", "token"))
        event = wait_events(pool)[0]
        assert event.outcome == Outcome.RETURNED
        assert int((tmp_path / "async-effect").read_text()) == pids[0]
        assert pool.capacity == 0  # async return still is not durable acknowledgement
        pool.acknowledge(ticket)
    assert int((tmp_path / "loop-closed").read_text()) == pids[0]
    assert_reaped(pids)


def test_async_handler_hard_timeout_terminates_process(tmp_path, monkeypatch):
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    with make_pool(
        factory_path="tests._helpers.async_worker_runtime:create_runtime",
        task_timeout_seconds=0.1,
    ) as pool:
        pids = pool.worker_pids
        pool.submit(Assignment("hang", "token"))
        event = wait_events(pool)[0]
        assert event.outcome == Outcome.TIMED_OUT
        assert not (tmp_path / "hang").exists()
        assert pool.capacity == 0
        assert_reaped(pids)
    assert_reaped(pids)


def test_slots_stay_reserved_until_acknowledged():
    with make_pool(concurrency=2) as pool:
        wait_capacity(pool, 2)
        assignments = [Assignment(f"job-{i}", f"token-{i}") for i in range(2)]
        tickets = [pool.submit(assignment) for assignment in assignments]
        with pytest.raises(NoCapacity):
            pool.submit(Assignment("excess", "token"))
        events = wait_events(pool, 2)
        assert {event.ticket for event in events} == set(tickets)
        assert {event.assignment for event in events} == set(assignments)
        assert all(event.outcome == Outcome.RETURNED for event in events)
        assert pool.capacity == 0  # callback returned, but outcome not reconciled
        assert pool.poll() == []  # observations are not repeatedly emitted
        with pytest.raises(NoCapacity):
            pool.submit(Assignment("still-excess", "token"))
        pool.acknowledge(tickets[0])
        assert pool.capacity == 1
        second = pool.submit(Assignment("next-job", "different-token"))
        assert second not in tickets
        with pytest.raises(ValueError, match="Unknown"):
            pool.acknowledge(tickets[0])  # stale ticket cannot free the new slot
        wait_events(pool)
        pool.acknowledge(second)
        pool.acknowledge(tickets[1])
        assert pool.capacity == 2


def test_stop_reaps_without_freeing_capacity():
    with make_pool() as pool:
        pids = pool.worker_pids
        ticket = pool.submit(Assignment("hang", "token"))
        event = pool.stop(ticket)
        assert event.outcome == Outcome.STOPPED
        assert pool.capacity == 0
        assert_reaped(pids)
        pool.acknowledge(ticket)
        wait_capacity(pool)
        assert pool.worker_pids != pids


def test_stop_preserves_observed_return_for_durable_reconciliation():
    with make_pool() as pool:
        pids = pool.worker_pids
        ticket = pool.submit(Assignment("job", "token"))
        event = wait_events(pool)[0]
        assert pool.stop(ticket) == event
        assert event.outcome == Outcome.RETURNED
        assert pool.capacity == 0
        assert_reaped(pids)
        pool.acknowledge(ticket)
        wait_capacity(pool)


def test_early_ack_is_rejected():
    with make_pool() as pool:
        ticket = pool.submit(Assignment("hang", "token"))
        with pytest.raises(ValueError, match="unfinished"):
            pool.acknowledge(ticket)
        assert pool.capacity == 0


@pytest.mark.parametrize(
    ("task", "outcome"),
    [
        ("fail", Outcome.EXECUTION_ERROR),
        ("exit", Outcome.LOST),
        ("awaitable", Outcome.EXECUTION_ERROR),
        ("async-generator-result", Outcome.EXECUTION_ERROR),
    ],
)
def test_failures_are_observed_not_retried_and_replacement_requires_ack(task, outcome):
    with make_pool() as pool:
        original_pids = pool.worker_pids
        ticket = pool.submit(Assignment(task, "generation-1"))
        event = wait_events(pool)[0]
        assert event.ticket == ticket
        assert event.outcome == outcome
        assert "private" not in repr(event)
        assert pool.capacity == 0
        pool.acknowledge(ticket)
        wait_capacity(pool)
        assert pool.worker_pids != original_pids
        assert_reaped(original_pids)
        # There was no hidden resubmission of the failed assignment.
        next_ticket = pool.submit(Assignment("healthy", "generation-2"))
        assert wait_events(pool)[0].ticket == next_ticket


@pytest.mark.skipif(sys.platform == "win32", reason="SIGTERM-ignore is a POSIX-only fixture")
def test_hard_timeout_kills_and_reaps_a_child_ignoring_termination(tmp_path, monkeypatch):
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    with make_pool(task_timeout_seconds=0.5) as pool:
        pids = pool.worker_pids
        pool.submit(Assignment("ignore-term", "token"))
        event = wait_events(pool)[0]
        assert event.outcome == Outcome.TIMED_OUT
        assert list(tmp_path.glob("*-started-ignore-term"))
        assert_reaped(pids)
        assert pool.capacity == 0
        pool.acknowledge(event.ticket)
        wait_capacity(pool)


@pytest.mark.parametrize(
    "factory",
    [
        "broken_factory",
        "invalid_factory",
        "async_runtime_factory",
        "does_not_exist",
        "async_generator_factory",
        "async_generator_runtime_factory",
        "async_generator_close_factory",
    ],
)
def test_bootstrap_failure_is_fail_closed_and_reaped(factory, capfd):
    before = {p.pid for p in multiprocessing.active_children()}
    pool = make_pool(factory_path=f"tests._helpers.worker_runtime:{factory}", concurrency=2)
    with pytest.raises(PoolBroken):
        pool.start()
    assert pool.capacity == 0
    assert pool.worker_pids == ()
    assert {p.pid for p in multiprocessing.active_children()} == before
    assert_bootstrap_privacy(capfd.readouterr().err)


def assert_bootstrap_privacy(stderr: str) -> None:
    assert "private-bootstrap-error" not in stderr


def test_bootstrap_privacy_guard_detects_child_stderr(capfd):
    pool = make_pool(factory_path="tests._helpers.worker_runtime:leaky_bootstrap_factory")
    with pytest.raises(PoolBroken):
        pool.start()
    assert pool.worker_pids == ()
    stderr = capfd.readouterr().err
    assert "private-bootstrap-error" in stderr
    with pytest.raises(AssertionError):
        assert_bootstrap_privacy(stderr)


def test_hung_bootstrap_is_bounded():
    pool = make_pool(
        factory_path="tests._helpers.worker_runtime:hung_factory",
        startup_timeout_seconds=0.5,
    )
    before = {p.pid for p in multiprocessing.active_children()}
    with pytest.raises(PoolBroken, match="initialization timed out") as timed_out:
        pool.start()
    assert "startup_timeout_seconds=0.5" in str(timed_out.value)  # the pool's own parameter
    assert "startup_seconds" not in str(
        timed_out.value
    )  # a worker-level name the pool does not know
    assert {p.pid for p in multiprocessing.active_children()} == before


def test_replacement_budget_prevents_unlimited_crash_loop():
    with make_pool(max_restarts=1) as pool:
        first = pool.submit(Assignment("exit", "token-1"))
        wait_events(pool)
        pool.acknowledge(first)
        wait_capacity(pool)
        second = pool.submit(Assignment("exit", "token-2"))
        wait_events(pool)
        with pytest.raises(PoolBroken, match="budget exhausted"):
            pool.acknowledge(second)
        assert pool.capacity == 0
        with pytest.raises(PoolBroken):
            pool.submit(Assignment("not-accepted", "token-3"))


@pytest.mark.parametrize("task", ["fail", "exit"])
def test_planned_recycle_cannot_hide_observed_health_failure(task):
    with make_pool(max_restarts=0) as pool:
        ticket = pool.submit(Assignment(task, "token"))
        wait_events(pool)
        pool.stop(ticket, recycle=True)
        with pytest.raises(PoolBroken, match="budget exhausted"):
            pool.acknowledge(ticket)


def test_planned_recycle_preserves_previously_spent_crash_budget():
    with make_pool(max_restarts=1) as pool:
        crashed = pool.submit(Assignment("exit", "first"))
        wait_events(pool)
        pool.acknowledge(crashed)
        wait_capacity(pool)
        for index in range(2):
            ticket = pool.submit(Assignment("hang", f"recycle-{index}"))
            pids = pool.worker_pids
            pool.stop(ticket, recycle=True)
            assert pool.capacity == 0
            assert_reaped(pids)
            pool.acknowledge(ticket)
            wait_capacity(pool)
        crashed = pool.submit(Assignment("exit", "last"))
        wait_events(pool)
        with pytest.raises(PoolBroken, match="budget exhausted"):
            pool.acknowledge(crashed)


def test_shutdown_drains_and_closes_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    pool = make_pool()
    pool.start()
    pids = pool.worker_pids
    ticket = pool.submit(Assignment("sleep", "token"))
    events = pool.shutdown(grace_seconds=2)
    assert [(event.ticket, event.outcome) for event in events] == [(ticket, Outcome.RETURNED)]
    assert list(tmp_path.glob("*-closed"))
    assert_reaped(pids)
    assert pool.shutdown() == []
    with pytest.raises(PoolBroken):
        pool.submit(Assignment("late", "token"))


@pytest.mark.parametrize(
    "factory",
    ["awaitable_close_factory", "async_generator_result_close_factory", "failing_close_factory"],
)
def test_cleanup_failure_is_observable_after_all_children_are_reaped(
    factory, tmp_path, monkeypatch, capfd
):
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    pool = make_pool(factory_path=f"tests._helpers.worker_runtime:{factory}", concurrency=2)
    pool.start()
    pids = pool.worker_pids
    ticket = pool.submit(Assignment("healthy", "token"))
    event = wait_events(pool)[0]
    try:
        with pytest.raises(PoolBroken, match="lifecycle cleanup failed"):
            pool.shutdown(grace_seconds=1)
        assert pool.capacity == 0
        assert pool.worker_pids == ()
        assert pool.pending_events == (event,)
        assert pool.pending_events[0].ticket == ticket
        assert pool.shutdown() == []  # Reaping succeeded; no incomplete cleanup retry.
        assert_reaped(pids)
        assert not list(tmp_path.glob("*-async-generator-body"))
        stderr = capfd.readouterr().err
        assert "never awaited" not in stderr
        assert "private-cleanup-error" not in stderr
    finally:
        pool.shutdown()


def test_late_poll_observes_return_instead_of_retroactive_timeout(tmp_path, monkeypatch):
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    with make_pool(task_timeout_seconds=0.02) as pool:
        ticket = pool.submit(Assignment("sleep", "token"))
        deadline = time.monotonic() + 5
        # Deliberately do not poll while the callback exceeds its configured deadline.
        while not pool._slots[0].connection.poll() and time.monotonic() < deadline:
            time.sleep(0.005)
        assert pool._slots[0].connection.poll()  # Return is queued, not merely about to be sent.
        assert list(tmp_path.glob("*-finished-sleep"))
        event = wait_events(pool)[0]
        assert event.outcome == Outcome.RETURNED
        assert pool.capacity == 0
        pool.acknowledge(ticket)
        assert pool.capacity == 1


def test_shutdown_interrupts_busy_child_without_requeueing():
    pool = make_pool()
    pool.start()
    pids = pool.worker_pids
    ticket = pool.submit(Assignment("hang", "token"))
    events = pool.shutdown(grace_seconds=0)
    assert [(event.ticket, event.outcome) for event in events] == [(ticket, Outcome.STOPPED)]
    assert_reaped(pids)


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_timeouts_must_be_finite_and_positive(value):
    with pytest.raises(ValueError):
        make_pool(task_timeout_seconds=value)


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_concurrency_must_be_a_positive_integer(value):
    with pytest.raises(ValueError):
        make_pool(concurrency=value)


@pytest.mark.parametrize("path", ["", "module", "module:factory:extra", "a:b.c", ":f"])
def test_invalid_factory_paths_rejected_before_spawning(path):
    with pytest.raises(ValueError):
        make_pool(factory_path=path)


@pytest.mark.parametrize("task,token", [("", "token"), ("task", ""), ("x" * 257, "token")])
def test_assignment_is_a_bounded_control_envelope(task, token):
    with pytest.raises(ValueError):
        Assignment(task, token)


def test_unacknowledged_events_survive_shutdown():
    pool = make_pool()
    pool.start()
    ticket = pool.submit(Assignment("healthy", "token"))
    event = wait_events(pool)[0]
    assert pool.pending_events == (event,)
    assert pool.shutdown() == [event]
    assert pool.pending_events == (event,)
    assert pool.pending_events[0].ticket == ticket


def test_ack_removes_pending_observation():
    with make_pool() as pool:
        ticket = pool.submit(Assignment("healthy", "token"))
        wait_events(pool)
        pool.acknowledge(ticket)
        assert pool.pending_events == ()


def test_idle_child_death_fails_closed():
    with make_pool(concurrency=2) as pool:
        process = pool._slots[0].process
        process.kill()
        process.join(timeout=5)
        assert not process.is_alive()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                pool.poll()
            except PoolBroken:
                break
            time.sleep(0.005)
        else:
            pytest.fail("Idle worker death did not fail the pool")
        assert pool.capacity == 0
        with pytest.raises(PoolBroken):
            pool.submit(Assignment("not-accepted", "token"))


def test_partial_start_failure_cleans_up_started_children(monkeypatch):
    pool = make_pool(concurrency=2)
    spawn = pool._spawn
    spawned = []

    def fail_second_spawn(restarts=0):
        if spawned:
            raise OSError("spawn unavailable")
        slot = spawn(restarts)
        spawned.append(slot.process.pid)
        return slot

    monkeypatch.setattr(pool, "_spawn", fail_second_spawn)
    with pytest.raises(OSError, match="spawn unavailable"):
        pool.start()
    assert_reaped(tuple(spawned))


def test_cleanup_attempts_other_children_and_can_be_retried(monkeypatch):
    pool = make_pool(concurrency=2)
    pool.start()
    pids = pool.worker_pids
    terminate = pool._terminate
    calls = []

    def fail_first_termination(slot):
        calls.append(slot.process.pid)
        if len(calls) == 1:
            raise PoolBroken("simulated reap failure")
        terminate(slot)

    monkeypatch.setattr(pool, "_terminate", fail_first_termination)
    try:
        with pytest.raises(PoolBroken, match="cleanup incomplete"):
            pool.shutdown()
        assert set(calls) == set(pids)
        assert pool.capacity == 0
    finally:
        monkeypatch.setattr(pool, "_terminate", terminate)
        pool.shutdown()
    assert_reaped(pids)


def test_pool_rejects_cross_thread_use():
    pool = make_pool()
    errors = []

    def other_thread():
        try:
            pool.start()
        except RuntimeError as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=other_thread)
    thread.start()
    thread.join(timeout=5)
    assert errors == ["WorkerPool must be used from its owning process and thread"]
    assert pool.worker_pids == ()


@pytest.mark.parametrize("failing", [False, True])
def test_execution_error_allows_bounded_cleanup_before_replacement(
    failing, tmp_path, monkeypatch, capfd
):
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    factory = "barrier_failing_close_factory" if failing else "barrier_close_factory"
    pool = make_pool(factory_path=f"tests._helpers.worker_runtime:{factory}")
    pool.start()
    pids = pool.worker_pids
    ticket = pool.submit(Assignment("fail", "generation"))
    try:
        assert wait_events(pool)[0].outcome == Outcome.EXECUTION_ERROR
        (tmp_path / "allow-close").touch()
        if failing:
            with pytest.raises(PoolBroken, match="lifecycle cleanup failed"):
                pool.acknowledge(ticket)
            assert pool.pending_events[0].ticket == ticket
        else:
            pool.acknowledge(ticket)
            wait_capacity(pool)
        assert list(tmp_path.glob("*-closed")), "Execution failure killed runtime cleanup"
        assert_reaped(pids)
        assert "private-cleanup-error" not in capfd.readouterr().err
    finally:
        if failing and pool._cleanup_failed:
            with pytest.raises(PoolBroken, match="lifecycle cleanup failed"):
                pool.shutdown()
        else:
            pool.shutdown()


def test_hung_cleanup_after_execution_error_is_still_bounded(tmp_path, monkeypatch):
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    pool = make_pool(factory_path="tests._helpers.worker_runtime:barrier_close_factory")
    pool.start()
    pids = pool.worker_pids
    ticket = pool.submit(Assignment("fail", "generation"))
    try:
        assert wait_events(pool)[0].outcome == Outcome.EXECUTION_ERROR
        started = time.monotonic()
        pool.acknowledge(ticket)
        assert time.monotonic() - started < 2
        assert_reaped(pids)
    finally:
        pool.shutdown()


def test_core_import_does_not_import_optional_frameworks():
    src = str(Path(__file__).resolve().parents[2] / "src")
    script = (
        "import sys; sys.path.insert(0, sys.argv[1]); "
        "import dewey._worker.pool; "
        "assert not {'django', 'sqlalchemy'} & sys.modules.keys()"
    )
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script, src], capture_output=True, text=True, timeout=10
    )
    assert completed.returncode == 0, completed.stderr


def test_children_start_one_after_another(tmp_path, monkeypatch):
    """The next child starts only once the newest is ready: slow imports on a small
    CPU cost each child its own startup budget, not all of them at once."""
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    waiting_when_first_gate_opened = []

    def open_gates():
        for ordinal in range(2):
            until = time.monotonic() + 10
            while len(list(tmp_path.glob("*-waiting"))) <= ordinal and time.monotonic() < until:
                time.sleep(0.005)
            if ordinal == 0:
                time.sleep(0.5)  # a second child would have announced itself by now
                waiting_when_first_gate_opened.append(len(list(tmp_path.glob("*-waiting"))))
            newest = max(tmp_path.glob("*-waiting"), key=lambda path: path.stat().st_mtime_ns)
            (tmp_path / f"go-{newest.name.split('-')[0]}").touch()

    opener = threading.Thread(target=open_gates, daemon=True)
    opener.start()
    pool = make_pool(factory_path="tests._helpers.worker_runtime:gated_factory", concurrency=2)
    with pool:
        assert pool.capacity == 1  # start() returns with the first child ready
        wait_capacity(pool, 2)
        opener.join(timeout=5)
    assert waiting_when_first_gate_opened == [1]


def test_fork_children_are_prepared_and_reset_inherited_signal_handlers(tmp_path, monkeypatch):
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    prepared = []
    previous = signal.signal(signal.SIGTERM, lambda signum, frame: None)
    try:
        pool = make_pool(
            concurrency=2, max_restarts=1, start_method="fork", prepare=lambda: prepared.append(1)
        )
        with pool:
            wait_capacity(pool, 2)
            assert len(prepared) == 2  # once before every fork, in the parent
            assert len(list(tmp_path.glob("*-sigterm-default"))) == 2
            ticket = pool.submit(Assignment("hang", "token"))
            started = time.monotonic()
            event = pool.stop(ticket)  # SIGTERM must end the child, not reach our handler
            assert event.outcome == Outcome.STOPPED
            assert time.monotonic() - started < pool.termination_timeout_seconds
            pool.acknowledge(ticket)
            assert len(prepared) == 3  # the replacement was prepared too
            wait_capacity(pool, 2)
    finally:
        signal.signal(signal.SIGTERM, previous)


def test_a_rejected_start_method_fails_before_any_child():
    with pytest.raises(ValueError, match="start_method"):
        make_pool(start_method="forkserver")


def test_no_child_is_started_while_the_pool_is_closing(tmp_path, monkeypatch):
    """A child that reports ready during shutdown must not trigger the next
    staged start: a closing pool only gets smaller."""
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    monkeypatch.setenv("DEWEY_WORKER_TEST_READY_SECONDS", "0.4")
    pool = make_pool(
        factory_path="tests._helpers.worker_runtime:slow_ready_factory",
        concurrency=3,
        task_timeout_seconds=5,
    )
    spawns = []
    original = pool._spawn

    def counted(*args, **kwargs):
        spawns.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(pool, "_spawn", counted)
    pool.start()  # the first child is ready; the second is starting
    assert len(spawns) == 2
    pool.submit(Assignment("hang", "d"))  # keep the pool busy, so shutdown polls during its grace
    pool.shutdown(grace_seconds=1.0)  # the second child reports ready in this window
    assert len(spawns) == 2  # no third start on the way out


def test_no_child_is_started_while_the_worker_drains(tmp_path, monkeypatch):
    """The service drains through poll() before it shuts the pool down: once it
    stops starting, a child that reports ready must not trigger the next start."""
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    monkeypatch.setenv("DEWEY_WORKER_TEST_READY_SECONDS", "0.4")
    pool = make_pool(
        factory_path="tests._helpers.worker_runtime:slow_ready_factory",
        concurrency=3,
        task_timeout_seconds=5,
    )
    spawns = []
    original = pool._spawn

    def counted(*args, **kwargs):
        spawns.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(pool, "_spawn", counted)
    try:
        pool.start()  # the first child is ready; the second is starting
        assert len(spawns) == 2
        pool.stop_starting()  # the service saw a stop and drains
        until = time.monotonic() + 3
        while time.monotonic() < until and pool.capacity < 2:
            pool.poll()  # what the drain loop's runner.step() does
            time.sleep(0.02)
        assert pool.capacity == 2  # the second child came up and serves the drain
        assert len(spawns) == 2  # but no third start
    finally:
        pool.shutdown(grace_seconds=0)


def test_a_forked_child_closes_the_pools_other_pipe_ends(tmp_path, monkeypatch):
    """A forked child inherits the supervising process's ends of every sibling's
    pipe and the caller's own descriptors; it must not hold them."""
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    extra_r, extra_w = os.pipe()  # stands in for the service's status pipe
    monkeypatch.setenv("DEWEY_WORKER_TEST_FDS", str(extra_w))
    pool = make_pool(
        factory_path="tests._helpers.worker_runtime:fd_check_factory",
        concurrency=2,
        start_method="fork",
        close_in_children=(extra_w,),
    )
    try:
        pool.start()
        until = time.monotonic() + 5
        while pool.capacity < 2 and time.monotonic() < until:
            pool.poll()
            time.sleep(0.01)
        assert pool.capacity == 2
        assert len(list(tmp_path.glob(f"*-fd-{extra_w}-closed"))) == 2
        assert list(tmp_path.glob("*-fd-*-open")) == []
        os.fstat(extra_w)  # still open here: the pool closes it in children only
    finally:
        pool.shutdown(grace_seconds=0)
        os.close(extra_r)
        os.close(extra_w)


def test_idle_children_share_the_cleanup_budget_at_shutdown(tmp_path, monkeypatch):
    """A child's close() (a telemetry flush, for one) may take longer than one
    termination slice; idle children are asked to close together and joined
    against the pool's cleanup budget, so both finish instead of being killed."""
    monkeypatch.setenv("DEWEY_WORKER_TEST_DIRECTORY", str(tmp_path))
    monkeypatch.setenv("DEWEY_WORKER_TEST_CLOSE_SECONDS", "0.6")
    pool = make_pool(
        factory_path="tests._helpers.worker_runtime:slow_close_factory",
        concurrency=2,
        termination_timeout_seconds=0.25,
        cleanup_timeout_seconds=3.0,
    )
    pool.start()
    until = time.monotonic() + 5
    while pool.capacity < 2 and time.monotonic() < until:
        pool.poll()
        time.sleep(0.01)
    assert pool.capacity == 2
    started = time.monotonic()
    pool.shutdown(grace_seconds=0)
    assert len(list(tmp_path.glob("*-closed"))) == 2  # neither was killed mid-close
    assert time.monotonic() - started < 3.0  # they closed together, not one after another
