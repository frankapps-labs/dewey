"""SQLAlchemy integration for dewey — sync + async executor, sweep, queries, notifications."""

# Async API
from dewey.sqlalchemy.async_executor import create_task_async, process_task_async
from dewey.sqlalchemy.async_queries import (
    bulk_retry_async,
    get_dead_async,
    get_dispatching_async,
    get_failed_async,
    get_pending_async,
    get_processing_async,
    get_recent_async,
    get_stats_async,
    get_stuck_async,
    get_task_async,
    kill_task_async,
    purge_completed_async,
    retry_task_async,
)
from dewey.sqlalchemy.async_sweep import (
    sweep_async,
    sweep_dispatching_async,
    sweep_failed_async,
    sweep_stuck_async,
)
from dewey.sqlalchemy.dispatch import (
    AsyncSQLAlchemyDispatchBackend,
    SQLAlchemyDispatchBackend,
)
from dewey.sqlalchemy.executor import create_task, process_task
from dewey.sqlalchemy.listen import (
    DEFAULT_WORK_CHANNEL,
    AsyncPostgresWorkListener,
    WorkNotification,
    notify_work_available,
    notify_work_available_async,
)
from dewey.sqlalchemy.models import Base, TaskEntryModel
from dewey.sqlalchemy.queries import (
    bulk_retry,
    get_dead,
    get_dispatching,
    get_failed,
    get_pending,
    get_processing,
    get_recent,
    get_stats,
    get_stuck,
    get_task,
    kill_task,
    purge_completed,
    retry_task,
)
from dewey.sqlalchemy.sweep import sweep, sweep_dispatching, sweep_failed, sweep_stuck

__all__ = [
    # Dispatch
    "SQLAlchemyDispatchBackend",
    "AsyncSQLAlchemyDispatchBackend",
    # Models
    "Base",
    "TaskEntryModel",
    "DEFAULT_WORK_CHANNEL",
    "AsyncPostgresWorkListener",
    "WorkNotification",
    "notify_work_available",
    "notify_work_available_async",
    # --- Sync API ---
    # Task executor
    "create_task",
    "process_task",
    # Task sweep
    "sweep",
    "sweep_failed",
    "sweep_stuck",
    "sweep_dispatching",
    # Task queries & actions
    "get_stats",
    "get_pending",
    "get_dispatching",
    "get_processing",
    "get_stuck",
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
    "create_task_async",
    "process_task_async",
    # Task sweep
    "sweep_async",
    "sweep_failed_async",
    "sweep_stuck_async",
    "sweep_dispatching_async",
    # Task queries & actions
    "get_stats_async",
    "get_pending_async",
    "get_dispatching_async",
    "get_processing_async",
    "get_stuck_async",
    "get_failed_async",
    "get_dead_async",
    "get_task_async",
    "get_recent_async",
    "retry_task_async",
    "bulk_retry_async",
    "kill_task_async",
    "purge_completed_async",
]
