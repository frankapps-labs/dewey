"""Adapter protocol surface and contract tests."""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from dewey.adapters import DispatcherAdapter, ProcessTaskFn


class _StubDispatcher:
    """Minimal conformer used to lock in the DispatcherAdapter shape."""

    def __init__(self) -> None:
        self.registered: ProcessTaskFn | None = None
        self.dispatched: list[str] = []

    def register(self, process_fn: ProcessTaskFn) -> None:
        self.registered = process_fn

    def dispatch(self, task_id: str) -> Any:
        self.dispatched.append(task_id)
        return task_id


class _MissingDispatch:
    def register(self, process_fn: ProcessTaskFn) -> None:  # pragma: no cover
        pass


class _MissingRegister:
    def dispatch(self, task_id: str) -> Any:  # pragma: no cover
        return task_id


def test_dispatcher_adapter_protocol_is_importable():
    assert DispatcherAdapter is not None
    assert ProcessTaskFn is not None


def test_stub_satisfies_dispatcher_adapter_at_runtime():
    """`@runtime_checkable` lets adapters be validated at wiring time."""
    assert isinstance(_StubDispatcher(), DispatcherAdapter)


@pytest.mark.parametrize("cls", [_MissingDispatch, _MissingRegister])
def test_partial_conformers_fail_runtime_check(cls):
    assert not isinstance(cls(), DispatcherAdapter)


def test_register_signature():
    """Lock in the register(process_fn) parameter shape."""
    sig = inspect.signature(DispatcherAdapter.register)
    params = list(sig.parameters)
    assert params == ["self", "process_fn"], params


def test_dispatch_signature():
    """Lock in the dispatch(task_id) parameter shape."""
    sig = inspect.signature(DispatcherAdapter.dispatch)
    params = list(sig.parameters)
    assert params == ["self", "task_id"], params


def test_register_then_dispatch_roundtrip():
    """Sanity: a conforming stub records both calls in the documented order."""
    received: list[str] = []
    stub = _StubDispatcher()
    stub.register(received.append)
    stub.dispatch("abc-123")
    stub.dispatch("def-456")

    assert stub.registered == received.append
    assert stub.dispatched == ["abc-123", "def-456"]
