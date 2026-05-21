"""Queue transport adapters — broker-agnostic."""

from dewey.adapters.base import BaseAdapter, DispatcherAdapter, ProcessTaskFn

__all__ = ["BaseAdapter", "DispatcherAdapter", "ProcessTaskFn"]
