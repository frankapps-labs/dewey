"""Storage-free native lifecycle contracts; never touch the default test DB."""

import asyncio
from dataclasses import replace
from threading import Thread

import pytest

from dewey._worker.contracts import Reconciliation, Timing
from dewey._worker.loop import ChildLoop


def valid_timing():
    return Timing(10, 10, 2, 1, 0.1, 1, 30)


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan"), True])
@pytest.mark.parametrize("field", list(Timing.__dataclass_fields__))
def test_timing_rejects_invalid_limits(field, value):
    with pytest.raises(ValueError, match="positive and finite"):
        replace(valid_timing(), **{field: value})


def test_timing_requires_strict_safety_margins():
    timing = valid_timing()
    with pytest.raises(ValueError, match="Execution lease"):
        replace(timing, execution_lease_seconds=4.1)
    with pytest.raises(ValueError, match="Assignment lease"):
        replace(timing, assignment_lease_seconds=2.1)
    with pytest.raises(ValueError, match="Task timeout"):
        replace(timing, task_timeout_seconds=2.1)


@pytest.mark.parametrize("state", list(Reconciliation))
def test_reconciliation_does_not_release_unknown_or_active_slots(state):
    assert state.durable_release is (state in (Reconciliation.SETTLED, Reconciliation.INVALIDATED))


def test_child_loop_runs_sync_and_async_handlers_once_on_same_loop():
    bridge = ChildLoop()
    calls = []

    def sync(value):
        calls.append(value)
        return value + 1

    async def async_handler(value):
        calls.append(value)
        await asyncio.sleep(0)
        return asyncio.get_running_loop()

    try:
        assert bridge.invoke(sync, 1) == 2
        first = bridge.invoke(async_handler, 2)
        assert bridge.invoke(async_handler, 3) is first
        assert calls == [1, 2, 3]
    finally:
        bridge.close()
    assert first.is_closed()
    bridge.close()
    with pytest.raises(RuntimeError, match="closed"):
        bridge.invoke(sync, 4)
    assert calls == [1, 2, 3]


def test_sync_callable_returning_coroutine_is_awaited():
    bridge = ChildLoop()

    async def value():
        return 42

    try:
        assert bridge.invoke(lambda: value()) == 42
    finally:
        bridge.close()


@pytest.mark.parametrize("asynchronous", [False, True])
def test_handler_failure_propagates_without_poisoning_loop(asynchronous):
    bridge = ChildLoop()

    def fail():
        raise LookupError("synthetic failure")

    async def async_fail():
        fail()

    try:
        with pytest.raises(LookupError, match="synthetic failure"):
            bridge.invoke(async_fail if asynchronous else fail)
        assert bridge.invoke(lambda: 1) == 1
    finally:
        bridge.close()


def test_async_generator_is_rejected_not_reported_as_success():
    bridge = ChildLoop()

    async def stream():
        yield 1

    try:
        with pytest.raises(TypeError, match="Async generator"):
            bridge.invoke(stream)
    finally:
        bridge.close()


def test_nested_loop_rejected_before_invoking_handler():
    bridge = ChildLoop()
    calls = []

    async def nested():
        with pytest.raises(RuntimeError, match="cannot nest"):
            bridge.invoke(lambda: calls.append(1))

    try:
        asyncio.run(nested())
        assert calls == []
    finally:
        bridge.close()


def test_thread_boundary_rejected_before_invoking_handler():
    bridge = ChildLoop()
    errors = []
    calls = []

    def other_thread():
        try:
            bridge.invoke(lambda: calls.append(1))
        except RuntimeError as exc:
            errors.append(str(exc))

    try:
        thread = Thread(target=other_thread)
        thread.start()
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert errors == ["ChildLoop cannot cross process or thread boundaries"]
        assert calls == []
    finally:
        bridge.close()


def test_nested_coroutine_result_is_closed_not_silently_ignored():
    bridge = ChildLoop()

    async def inner():
        pytest.fail("Nested coroutine must not execute")

    async def outer():
        return inner()

    try:
        with pytest.raises(TypeError, match="Nested coroutine"):
            bridge.invoke_handler(outer)
    finally:
        bridge.close()


def test_handler_background_tasks_are_cancelled_before_next_assignment():
    bridge = ChildLoop()
    cancelled = []

    async def background():
        try:
            await asyncio.sleep(60)
        finally:
            cancelled.append(True)

    async def handler():
        asyncio.create_task(background())
        await asyncio.sleep(0)

    try:
        with pytest.raises(TypeError, match="unawaited background"):
            bridge.invoke_handler(handler)
        assert cancelled == [True]
        assert bridge.invoke_handler(lambda: 42) == 42
        assert asyncio.all_tasks(bridge._runner.get_loop()) == set()
    finally:
        bridge.close()


def test_tasks_spawned_during_cancellation_abort_child_lifecycle():
    from dewey._worker.loop import UnsafeBackgroundTasks

    bridge = ChildLoop()

    async def background():
        try:
            await asyncio.sleep(60)
        finally:
            asyncio.create_task(asyncio.sleep(60))

    async def handler():
        asyncio.create_task(background())
        await asyncio.sleep(0)

    try:
        with pytest.raises(UnsafeBackgroundTasks, match="survived cancellation"):
            bridge.invoke_handler(handler)
    finally:
        bridge.close()
