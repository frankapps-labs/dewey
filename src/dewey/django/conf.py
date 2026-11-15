"""The Django settings contract for Dewey.

One dict, so it is obvious in a diff what Dewey is configured to do::

    DEWEY = {
        # All optional, shown with their defaults:
        "DATABASE": "default",          # alias the worker and operator commands use
        "QUEUES": ("default",),         # queues `manage.py dewey run` serves
        "CONCURRENCY": 2,               # task processes per worker
        "AUTODISCOVER": "tasks",        # import <app>.tasks from every installed app
        "AUTO_APPLY_SCHEDULES": False,  # single-node installs: apply at worker start
        "ALERT_HOOK": None,             # "module:function" called with alert dicts
        "TIMING": {},                   # overrides for dewey.worker.default_timing()
        "START_METHOD": "spawn",        # or "fork": task processes share the imported app
        "STARTUP_SECONDS": 30.0,        # budget for the app import and for each task process
        "SUPERVISION_SECONDS": 30.0,    # how long the parent may go without progress
        "SHUTDOWN_SECONDS": 30.0,       # how long a stopping worker waits for running tasks
        "MAX_RESTARTS": 3,              # task-process crashes tolerated per service lifetime
        "TRACING": False,               # an OpenTelemetry span per attempt (needs dewey[otel])
        "FAILURE_DETAILS": "type",      # or "full": exception message and traceback in the log
    }
"""

from __future__ import annotations

from dataclasses import fields
from typing import Any

from django.conf import settings

from dewey.worker import WorkerConfig

#: The Django setting for each worker option, so the two cannot drift: the
#: defaults below are the worker's own, and only a key the project configured is
#: forwarded to it (see ``dewey.django.worker.worker_settings``).
WORKER_SETTINGS: dict[str, str] = {
    "QUEUES": "queues",
    "CONCURRENCY": "concurrency",
    "AUTO_APPLY_SCHEDULES": "auto_apply_schedules",
    "ALERT_HOOK": "alert_hook",
    "TIMING": "timing",
    "START_METHOD": "start_method",
    "STARTUP_SECONDS": "startup_seconds",
    "SUPERVISION_SECONDS": "supervision_seconds",
    "SHUTDOWN_SECONDS": "shutdown_seconds",
    "MAX_RESTARTS": "max_restarts",
    "TRACING": "tracing",
    "FAILURE_DETAILS": "failure_details",
}
_WORKER_DEFAULTS = {field.name: field.default for field in fields(WorkerConfig)}

DEFAULTS: dict[str, Any] = {
    "DATABASE": "default",
    "AUTODISCOVER": "tasks",
    "TIMING": {},  # the worker derives its timing from the concurrency
    **{key: _WORKER_DEFAULTS[name] for key, name in WORKER_SETTINGS.items() if name != "timing"},
}


def get_settings() -> dict[str, Any]:
    """Merge ``settings.DEWEY`` over the defaults.

    Raises:
        ImproperlyConfigured: an unknown key is present. A typo in a setting name
            would otherwise be silently ignored until an incident.
    """
    from django.core.exceptions import ImproperlyConfigured

    configured = getattr(settings, "DEWEY", None) or {}
    if not isinstance(configured, dict):
        raise ImproperlyConfigured(f"DEWEY must be a dict, got {type(configured).__name__}.")

    unknown = set(configured) - set(DEFAULTS)
    if unknown:
        raise ImproperlyConfigured(
            f"Unknown DEWEY setting(s): {', '.join(sorted(unknown))}. "
            f"Supported keys: {', '.join(sorted(DEFAULTS))}."
        )

    return {**DEFAULTS, **configured}


__all__ = ["DEFAULTS", "WORKER_SETTINGS", "get_settings"]
