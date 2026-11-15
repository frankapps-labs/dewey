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
    }
"""

from __future__ import annotations

from typing import Any

from django.conf import settings

DEFAULTS: dict[str, Any] = {
    "DATABASE": "default",
    "QUEUES": ("default",),
    "CONCURRENCY": 2,
    "AUTODISCOVER": "tasks",
    "AUTO_APPLY_SCHEDULES": False,
    "ALERT_HOOK": None,
    "TIMING": {},
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


__all__ = ["DEFAULTS", "get_settings"]
