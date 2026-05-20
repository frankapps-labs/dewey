"""Core module — pure Python, zero dependencies."""

from dewey.core.backoff import (
    BackoffFn,
    default_notification_backoff,
    default_task_backoff,
    retry_delay,
)
from dewey.core.logging import (
    TRACE_METADATA_KEY,
    TraceContextFilter,
    bind_to_metadata,
    extract_trace_context,
    get_trace_context,
    reset_trace_context,
    restore_trace_context,
    set_trace_context,
    update_trace_context,
)
from dewey.core.notifications import (
    Channel,
    ChannelBinding,
    ChannelRegistry,
    ChannelResult,
    NotificationAttempt,
    NotificationEntry,
    NotificationStatus,
)
from dewey.core.states import TaskStatus, should_die, should_retry
from dewey.core.types import TaskEntry

__all__ = [
    "TaskStatus",
    "should_retry",
    "should_die",
    "BackoffFn",
    "default_notification_backoff",
    "default_task_backoff",
    "retry_delay",
    "TRACE_METADATA_KEY",
    "TraceContextFilter",
    "bind_to_metadata",
    "extract_trace_context",
    "get_trace_context",
    "reset_trace_context",
    "restore_trace_context",
    "set_trace_context",
    "update_trace_context",
    "TaskEntry",
    "NotificationStatus",
    "NotificationEntry",
    "NotificationAttempt",
    "Channel",
    "ChannelResult",
    "ChannelBinding",
    "ChannelRegistry",
]
