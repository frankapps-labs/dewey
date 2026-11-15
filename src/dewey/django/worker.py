"""The worker application for a Django project, driven by ``settings.DEWEY``.

``manage.py dewey run`` uses this. Set ``DJANGO_SETTINGS_MODULE`` and it also
works with the plain command: ``dewey --app dewey.django.worker:create_application``.
"""

from __future__ import annotations

from typing import Any

from dewey.worker import WorkerApplication, WorkerContext

FACTORY = "dewey.django.worker:create_application"


def create_application(context: WorkerContext) -> WorkerApplication:
    """Set Django up in this process, import every app's task module, bind the alias."""
    import django
    from django.utils.module_loading import autodiscover_modules

    django.setup()
    from dewey.django.conf import get_settings

    settings = get_settings()
    if settings["AUTODISCOVER"]:
        autodiscover_modules(settings["AUTODISCOVER"])
    return context.django(using=settings["DATABASE"])


def worker_settings() -> dict[str, Any]:
    """``settings.DEWEY`` as worker configuration.

    Only the keys the project configured are forwarded; everything else takes
    the worker's own default, so the two cannot drift.
    """
    from django.conf import settings as django_settings

    from dewey.django.conf import WORKER_SETTINGS, get_settings

    settings = get_settings()  # validates the keys
    configured = set(getattr(django_settings, "DEWEY", None) or {})
    options: dict[str, Any] = {"factory": FACTORY}
    for key, name in WORKER_SETTINGS.items():
        if key not in configured:
            continue
        value = settings[key]
        if key == "QUEUES":
            value = tuple(value)
        elif key == "TIMING":
            value = dict(value)
        options[name] = value
    return options


__all__ = ["FACTORY", "create_application", "worker_settings"]
