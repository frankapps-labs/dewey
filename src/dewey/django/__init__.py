"""Django adapter for dewey — models, executor, sweep, queries, notifications."""


def __getattr__(name: str):
    """Lazy imports — avoid importing models before Django app registry is ready."""
    _executor_names = {"create_task", "process_task"}
    _sweep_names = {"sweep", "sweep_failed", "sweep_stuck"}
    _query_names = {
        "get_stats",
        "get_pending",
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
    }
    _notification_names = {
        "create_notification",
        "create_notifications_for_event",
        "send_notification",
        "process_notification",
        "sweep_notifications",
        "sweep_failed_notifications",
        "sweep_stuck_notifications",
        "get_notification",
        "get_notification_attempts",
        "get_notification_stats",
        "get_notifications_for_task",
        "get_pending_notifications",
        "get_failed_notifications",
        "get_dead_notifications",
        "retry_notification",
        "kill_notification",
        "purge_sent_notifications",
    }

    if name in _executor_names:
        from dewey.django.executor import create_task, process_task

        return locals()[name]
    if name in _sweep_names:
        from dewey.django.sweep import sweep, sweep_failed, sweep_stuck

        return locals()[name]
    if name in _query_names:
        from dewey.django import queries

        return getattr(queries, name)
    if name in _notification_names:
        from dewey.django import notifications

        return getattr(notifications, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
