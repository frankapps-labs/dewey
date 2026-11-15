"""Transport-independent effect keys and isolated reused native-loop identity."""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

import dewey
from dewey._worker.pool import Assignment
from dewey._worker.runtime import LedgerRuntime
from dewey.context import TaskContext, _bind_task


def test_effect_keys_are_stable_and_separate_named_operations():
    first = TaskContext("synthetic-task-a", "synthetic.effect", 1)
    retry = TaskContext("synthetic-task-a", "synthetic.effect", 2)
    other = TaskContext("synthetic-task-b", "synthetic.effect", 1)
    key = first.effect_key("charge")
    assert key == "179fd53fe7726c2ba2e679d2747fec4febbea9dcef53ae1f55bcce1432f9dac8"
    assert len(key) == 64 and all(ch in "0123456789abcdef" for ch in key)
    assert retry.effect_key("charge") == key
    assert other.effect_key("charge") != key
    assert first.effect_key("email") != key


@pytest.mark.parametrize(
    "operation", ["", "two words", "9charge", "charge/now", "é", "a" * 65, None]
)
def test_invalid_effect_names_are_rejected(operation):
    with pytest.raises(ValueError):
        TaskContext("synthetic-task", "synthetic.effect", 1).effect_key(operation)


def test_context_restores_after_nested_error_and_does_not_cross_threads():
    outer = TaskContext("synthetic-task-a", "synthetic.effect", 1)
    inner = TaskContext("synthetic-task-b", "synthetic.effect", 1)
    with pytest.raises(RuntimeError):
        dewey.current_task()
    with _bind_task(outer):
        with pytest.raises(ValueError), _bind_task(inner):
            assert dewey.current_task() == inner
            raise ValueError("synthetic error")
        assert dewey.current_task() == outer
        with ThreadPoolExecutor(max_workers=1) as executor:
            with pytest.raises(RuntimeError):
                executor.submit(dewey.current_task).result()
    with pytest.raises(RuntimeError):
        dewey.current_task()


def test_awaited_tasks_keep_independent_context():
    async def task(task_id):
        with _bind_task(TaskContext(task_id, "synthetic.effect", 1)):
            await asyncio.sleep(0)
            return dewey.current_task().task_id

    async def run():
        return await asyncio.gather(task("synthetic-task-a"), task("synthetic-task-b"))

    assert asyncio.run(run()) == ["synthetic-task-a", "synthetic-task-b"]
    with pytest.raises(RuntimeError):
        dewey.current_task()


@pytest.mark.parametrize("async_storage", [False, True])
@pytest.mark.parametrize("handler_style", ["sync", "async", "awaitable", "error", "abort"])
def test_native_runtime_context_tracks_each_admitted_attempt(
    monkeypatch, async_storage, handler_style
):
    seen = []

    def observe():
        seen.append(dewey.current_task())

    async def async_handler():
        await asyncio.sleep(0)
        observe()

    def handler():
        if handler_style in ("async", "awaitable"):
            return async_handler()
        observe()
        if handler_style == "error":
            raise ValueError("synthetic handler failure")
        if handler_style == "abort":
            raise SystemExit("synthetic lifecycle abort")
        return None

    resolved = async_handler if handler_style == "async" else handler
    monkeypatch.setattr("dewey._worker.runtime.resolve_handler", lambda *args: resolved)

    class Storage:
        attempt = 1
        finishes = []

        def operation(self, operation, args):
            with pytest.raises(RuntimeError):
                dewey.current_task()  # No handler identity inside ledger transactions.
            if operation == "start":
                return {
                    "task_type": "synthetic.effect",
                    "args": [],
                    "kwargs": {},
                    "execution_token": "synthetic-token",
                    "attempts": self.attempt,
                }
            if operation == "finish":
                self.finishes.append(args[-1])
            return None

        async def awaited(self, operation, args):
            await asyncio.sleep(0)
            return self.operation(operation, args)

        def call(self, operation, *args):
            if async_storage:
                return self.awaited(operation, args)
            return self.operation(operation, args)

    storage = Storage()
    runtime = LedgerRuntime(storage, close_resources=lambda: None)
    try:
        for attempt in (1, 2):
            storage.attempt = attempt
            if handler_style == "abort":
                with pytest.raises(SystemExit):
                    runtime.execute(Assignment("synthetic-task", "synthetic-generation"))
            else:
                runtime.execute(Assignment("synthetic-task", "synthetic-generation"))
            with pytest.raises(RuntimeError):
                dewey.current_task()
    finally:
        runtime.close()
    assert [context.attempt for context in seen] == [1, 2]
    assert seen[0].effect_key("charge") == seen[1].effect_key("charge")
    assert len(storage.finishes) == (0 if handler_style == "abort" else 2)
    if handler_style == "error":
        assert all(isinstance(failure, ValueError) for failure in storage.finishes)
