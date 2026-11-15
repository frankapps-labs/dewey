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
    from dewey.django.conf import get_settings
    from dewey.django.models import TaskEntry

    findings = []
    try:
        config = get_settings()
    except Exception as exc:
        return [Error(str(exc), id="dewey.E001")]

    producer_alias = router.db_for_write(TaskEntry)
    for role, alias in {"producer": producer_alias, "worker": config["DATABASE"]}.items():
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

    queues = config["QUEUES"]
    if isinstance(queues, str) or not queues or any(not isinstance(q, str) for q in queues):
        findings.append(
            Error("DEWEY['QUEUES'] must be a nonempty list of queue names.", id="dewey.E004")
        )

    if producer_alias != config["DATABASE"]:
        findings.append(
            Warning(
                f"Producers write tasks to alias {producer_alias!r} but the worker reads "
                f"{config['DATABASE']!r}. Unless both point at the same database, created "
                "tasks are never claimed.",
                id="dewey.W002",
            )
        )

    return findings


__all__ = ["check_dewey_configuration"]
