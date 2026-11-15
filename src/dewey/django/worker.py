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
    """``settings.DEWEY`` as worker configuration."""
    from dewey.django.conf import get_settings

    settings = get_settings()
    return {
        "factory": FACTORY,
        "queues": tuple(settings["QUEUES"]),
        "concurrency": settings["CONCURRENCY"],
        "auto_apply_schedules": settings["AUTO_APPLY_SCHEDULES"],
        "alert_hook": settings["ALERT_HOOK"],
        "timing": dict(settings["TIMING"]),
    }


__all__ = ["FACTORY", "create_application", "worker_settings"]
