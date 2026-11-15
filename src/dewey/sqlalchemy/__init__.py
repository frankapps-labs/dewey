"""SQLAlchemy integration for Dewey — sync/async task creation, native storage and queries."""

# Async API
from dewey.sqlalchemy.async_executor import (
    create_or_get_task_async,
    create_task_async,
    process_task_async,
)
from dewey.sqlalchemy.async_queries import (
    bulk_retry_async,
    get_dead_async,
    get_dispatching_async,
    get_expired_async,
    get_failed_async,
    get_pending_async,
    get_processing_async,
    get_recent_async,
    get_stats_async,
    get_stuck_async,
    get_task_async,
    get_workers_async,
    kill_task_async,
    purge_completed_async,
    retry_task_async,
)
from dewey.sqlalchemy.executor import create_or_get_task, create_task, process_task
from dewey.sqlalchemy.models import Base, TaskEntryModel, WorkerHeartbeatModel
from dewey.sqlalchemy.queries import (
    bulk_retry,
    get_dead,
    get_dispatching,
    get_expired,
    get_failed,
    get_pending,
    get_processing,
    get_recent,
    get_stats,
    get_stuck,
    get_task,
    get_workers,
    kill_task,
    purge_completed,
    retry_task,
)
from dewey.sqlalchemy.readiness import ReadinessError, check_readiness, check_readiness_async
from dewey.sqlalchemy.timeouts import apply_task_timeouts, apply_task_timeouts_async

__all__ = [
    "ReadinessError",
    "check_readiness",
    "check_readiness_async",
    "apply_task_timeouts",
    "apply_task_timeouts_async",
    # Models
    "Base",
    "TaskEntryModel",
    "WorkerHeartbeatModel",
    # --- Sync API ---
    # Task executor
    "create_or_get_task",
    "create_task",
    "process_task",
    # Task queries & actions
    "get_stats",
    "get_pending",
    "get_workers",
    "get_dispatching",
    "get_processing",
    "get_stuck",
    "get_expired",
    "get_failed",
    "get_dead",
    "get_task",
    "get_recent",
    "retry_task",
    "bulk_retry",
    "kill_task",
    "purge_completed",
    # --- Async API ---
    # Task executor
    "create_or_get_task_async",
    "create_task_async",
    "process_task_async",
    # Task queries & actions
    "get_stats_async",
    "get_pending_async",
    "get_workers_async",
    "get_dispatching_async",
    "get_processing_async",
    "get_stuck_async",
    "get_expired_async",
    "get_failed_async",
    "get_dead_async",
    "get_task_async",
    "get_recent_async",
    "retry_task_async",
    "bulk_retry_async",
    "kill_task_async",
    "purge_completed_async",
]
