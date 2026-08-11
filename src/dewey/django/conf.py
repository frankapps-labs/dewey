"""The Django settings contract for Dewey.

One dict, so it is obvious in a diff what Dewey is configured to do::

    DEWEY = {
        # Dotted path to the transport's dispatch callable. Required to run the
        # dispatcher; usually "myapp.tasks.adapter.dispatch".
        "DISPATCH": "myapp.tasks.adapter.dispatch",

        # Optional, shown with their defaults:
        "QUEUES": None,                    # None means every queue
        "BATCH_SIZE": 100,
        "IDLE_POLL_SECONDS": 5.0,          # the correctness path; LISTEN only shortens it
        "SWEEP_INTERVAL_SECONDS": 60.0,    # None disables recovery — see the docs first
        "DISPATCH_TIMEOUT_SECONDS": 300,   # must exceed your worst-case broker backlog
        "STUCK_THRESHOLD_MINUTES": 10,
        "SWEEP_LIMIT": 100,
        "DATABASE": "default",             # alias, for a dedicated Dewey connection
    }
"""

from __future__ import annotations

from typing import Any

from django.conf import settings

DEFAULTS: dict[str, Any] = {
    "DISPATCH": None,
    "QUEUES": None,
    "BATCH_SIZE": 100,
    "IDLE_POLL_SECONDS": 5.0,
    "SWEEP_INTERVAL_SECONDS": 60.0,
    "DISPATCH_TIMEOUT_SECONDS": 300,
    "STUCK_THRESHOLD_MINUTES": 10,
    "SWEEP_LIMIT": 100,
    "DATABASE": "default",
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


def get_dispatch_fn() -> Any:
    """Import the configured transport dispatch callable.

    Raises:
        ImproperlyConfigured: not set, not importable, or not callable.
    """
    from django.core.exceptions import ImproperlyConfigured
    from django.utils.module_loading import import_string

    path = get_settings()["DISPATCH"]
    if not path:
        raise ImproperlyConfigured(
            'DEWEY["DISPATCH"] is not set. Point it at your transport adapter\'s '
            'dispatch method, e.g. "myapp.tasks.adapter.dispatch". The dispatcher has '
            "nothing to hand work to without it."
        )
    if callable(path):
        return path
    try:
        dispatch = import_string(path)
    except ImportError as exc:
        raise ImproperlyConfigured(
            f'DEWEY["DISPATCH"] = {path!r} could not be imported: {exc}'
        ) from exc
    if not callable(dispatch):
        raise ImproperlyConfigured(f'DEWEY["DISPATCH"] = {path!r} is not callable.')
    return dispatch


__all__ = ["DEFAULTS", "get_dispatch_fn", "get_settings"]
