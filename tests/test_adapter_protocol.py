"""Adapter protocol surface tests."""

from dewey.adapters import DispatcherAdapter, ProcessTaskFn


def test_dispatcher_adapter_protocol_is_importable():
    assert DispatcherAdapter is not None
    assert ProcessTaskFn is not None
