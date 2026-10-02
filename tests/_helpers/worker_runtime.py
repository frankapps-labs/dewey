"""Importable spawn fixtures; deliberately independent of Django/SQLAlchemy."""

from __future__ import annotations

import os
import signal
import time
from pathlib import Path

from dewey._worker.pool import Assignment


def _record(name: str) -> None:
    directory = os.environ.get("DEWEY_WORKER_TEST_DIRECTORY")
    if directory:
        (Path(directory) / f"{os.getpid()}-{name}").touch()


class Runtime:
    def __init__(self) -> None:
        _record("initialized")

    def execute(self, assignment: Assignment) -> object:
        if assignment.task_id == "ignore-term":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        _record(f"started-{assignment.task_id}")
        if assignment.task_id == "fail":
            raise RuntimeError("private-sensitive-error-must-not-cross-IPC")
        if assignment.task_id == "exit":
            os._exit(17)
        if assignment.task_id in ("hang", "ignore-term"):
            time.sleep(60)
        if assignment.task_id == "sleep":
            time.sleep(0.1)
        if assignment.task_id == "awaitable":
            return _async_work()
        if assignment.task_id == "async-generator-result":
            return _async_generator_work()
        _record(f"finished-{assignment.task_id}")
        return False  # Returning is not proof of successful business execution.

    def close(self) -> None:
        _record("closed")


def create_runtime() -> Runtime:
    return Runtime()


def broken_factory() -> Runtime:
    raise RuntimeError("private-bootstrap-error")


def leaky_bootstrap_factory() -> Runtime:
    # Synthetic negative control: intentionally leak the same marker the guard forbids.
    os.write(2, b"private-bootstrap-error\n")
    raise RuntimeError("private-bootstrap-error")


def hung_factory() -> Runtime:
    time.sleep(60)
    return Runtime()


def invalid_factory() -> object:
    return object()


async def _async_work() -> None:
    pass


class AsyncRuntime:
    async def execute(self, assignment: Assignment) -> None:
        pass

    def close(self) -> None:
        pass


def async_runtime_factory() -> AsyncRuntime:
    return AsyncRuntime()


async def _async_generator_work():
    _record("async-generator-body")
    yield None


class AsyncGeneratorRuntime:
    async def execute(self, assignment: Assignment):
        _record("async-generator-body")
        yield None

    def close(self) -> None:
        pass


def async_generator_runtime_factory() -> AsyncGeneratorRuntime:
    return AsyncGeneratorRuntime()


class AsyncGeneratorCloseRuntime(Runtime):
    async def close(self):
        _record("async-generator-close-body")
        yield None


def async_generator_close_factory() -> AsyncGeneratorCloseRuntime:
    return AsyncGeneratorCloseRuntime()


async def async_generator_factory():
    yield Runtime()


class AwaitableCloseRuntime(Runtime):
    def close(self) -> object:
        _record("close-called")
        return _async_work()


def awaitable_close_factory() -> AwaitableCloseRuntime:
    return AwaitableCloseRuntime()


class AsyncGeneratorResultCloseRuntime(Runtime):
    def close(self) -> object:
        _record("close-called")
        return _async_generator_work()


def async_generator_result_close_factory() -> AsyncGeneratorResultCloseRuntime:
    return AsyncGeneratorResultCloseRuntime()


class FailingCloseRuntime(Runtime):
    def close(self) -> None:
        raise RuntimeError("private-cleanup-error")


def failing_close_factory() -> FailingCloseRuntime:
    return FailingCloseRuntime()


class BarrierCloseRuntime(Runtime):
    def __init__(self, *, fail_close: bool = False) -> None:
        super().__init__()
        self.fail_close = fail_close

    def close(self) -> None:
        _record("close-entered")
        directory = Path(os.environ["DEWEY_WORKER_TEST_DIRECTORY"])
        deadline = time.monotonic() + 5
        while not (directory / "allow-close").exists() and time.monotonic() < deadline:
            time.sleep(0.005)
        _record("closed")
        if self.fail_close:
            raise RuntimeError("private-cleanup-error")


def barrier_close_factory() -> BarrierCloseRuntime:
    return BarrierCloseRuntime()


def barrier_failing_close_factory() -> BarrierCloseRuntime:
    return BarrierCloseRuntime(fail_close=True)
