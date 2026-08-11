"""Lightweight, non-destructive Django system checks for Dewey configuration."""

from __future__ import annotations

from django.conf import settings
from django.core.checks import Error, Warning, register
from django.db import router


def _postgresql(alias: str) -> bool:
    engine = settings.DATABASES[alias].get("ENGINE", "")
    return engine in {
        "django.db.backends.postgresql",
        "django.db.backends.postgresql_psycopg2",
    }


@register()
def check_dewey_configuration(app_configs=None, **kwargs):
    from dewey.django.conf import get_dispatch_fn, get_settings
    from dewey.django.models import TaskEntry

    findings = []
    try:
        config = get_settings()
    except Exception as exc:
        return [Error(str(exc), id="dewey.E001")]

    aliases = {
        "dispatcher": config["DATABASE"],
        "worker": config["WORKER_DATABASE"],
    }
    for role, alias in aliases.items():
        if alias is None:
            continue
        if not isinstance(alias, str) or alias not in settings.DATABASES:
            findings.append(
                Error(
                    f"The Dewey {role} alias {alias!r} is not in settings.DATABASES.",
                    id="dewey.E002",
                )
            )
        elif not _postgresql(alias):
            findings.append(
                Error(
                    f"The Dewey {role} alias {alias!r} must use PostgreSQL.",
                    id="dewey.E003",
                )
            )

    if not config["DISPATCH"]:
        findings.append(
            Warning(
                "DEWEY['DISPATCH'] is not set; producers can migrate/create rows, but "
                "a dispatcher cannot start until a module-level callable is configured.",
                id="dewey.W003",
            )
        )
    else:
        try:
            get_dispatch_fn()
        except Exception as exc:
            findings.append(Error(str(exc), id="dewey.E004"))

    sweep_interval = config["SWEEP_INTERVAL_SECONDS"]
    if sweep_interval is None:
        findings.append(
            Warning(
                "Dewey recovery sweeps are disabled; crash recovery requires another runner.",
                id="dewey.W001",
            )
        )
    elif not isinstance(sweep_interval, (int, float)) or sweep_interval <= 0:
        findings.append(
            Error(
                "DEWEY['SWEEP_INTERVAL_SECONDS'] must be a positive number or None.",
                id="dewey.E005",
            )
        )

    producer_alias = router.db_for_write(TaskEntry)
    dispatcher_alias = config["DATABASE"]
    if producer_alias == dispatcher_alias and dispatcher_alias != "default":
        findings.append(
            Warning(
                "A database router sends producer TaskEntry writes to the dispatcher alias. "
                "This splits task creation from business transactions on another alias; "
                "pass using= for the business transaction or change the router.",
                id="dewey.W002",
            )
        )

    dispatch_path = config["DISPATCH"]
    if dispatch_path == "dewey.contrib.django_huey.dispatch":
        try:
            from dewey.contrib.django_huey import adapter

            if adapter._process_task is None:
                raise RuntimeError("Dewey's Huey processor is not registered")
        except Exception as exc:
            findings.append(
                Error(
                    f"Dewey Django/Huey contrib wiring is incomplete: {exc}",
                    id="dewey.E006",
                )
            )

    return findings


__all__ = ["check_dewey_configuration"]
