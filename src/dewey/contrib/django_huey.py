"""First-class Django + Huey wiring: one import, no bespoke glue.

Point the dispatcher at this module and let the Huey consumer import it::

    # settings.py
    HUEY = {...}                        # your huey.contrib.djhuey configuration
    DEWEY = {
        "DISPATCH": "dewey.contrib.django_huey.dispatch",
        "WORKER_DATABASE": None,        # worker alias; None lets your routers decide
    }

    # myapp/tasks.py — any module the run_huey consumer imports
    import dewey.contrib.django_huey  # noqa: F401 — registers the Dewey processor

Importing this module registers Dewey's processor on the project's Huey
instance (``huey.contrib.djhuey.HUEY``) exactly once per process — reloading
the module adopts the existing registration instead of binding a second
callable to the task name — and with Huey retries disabled, because Dewey owns
retry scheduling and the attempt budget. Worker execution is wrapped in
``huey.contrib.djhuey.close_db``, the same connection hygiene Huey's own
``db_task`` applies.

``DEWEY["WORKER_DATABASE"]`` is the worker's database alias and is deliberately
independent of the dispatcher's ``DEWEY["DATABASE"]``: dispatcher and worker
are separate post-commit connections. The default ``None`` defers to your
database routers, exactly like any other ORM write.

There is no autodiscovery. The dispatcher reaches this module through
``DEWEY["DISPATCH"]``; the consumer reaches it through an import you can see.
Core ``dewey`` never imports this module, so Django and Huey stay optional.
"""

# ruff: noqa: E402 — imports are guarded so a missing dependency fails actionably

from __future__ import annotations

from typing import Any, cast

try:
    from django.core.exceptions import ImproperlyConfigured
except ModuleNotFoundError as exc:  # pragma: no cover — needs an env without Django
    raise ModuleNotFoundError(
        "dewey.contrib.django_huey needs Django, which is not installed. Install it "
        "with pip install 'dewey[django,huey]'.",
        name=exc.name,
    ) from exc

from django.conf import settings

if not settings.configured:
    raise ImproperlyConfigured(
        "dewey.contrib.django_huey needs configured Django settings. Set "
        "DJANGO_SETTINGS_MODULE or call settings.configure() before importing it."
    )
if getattr(settings, "HUEY", None) is None:
    raise ImproperlyConfigured(
        "dewey.contrib.django_huey needs a HUEY setting. Configure Huey's Django "
        "integration with a Huey instance or configuration dict."
    )

try:
    from huey.exceptions import ConfigurationError as _HueyConfigurationError
except ModuleNotFoundError as exc:
    if (exc.name or "").split(".")[0] != "huey":
        raise
    raise ImproperlyConfigured(
        "dewey.contrib.django_huey needs Huey, which is not installed. Install it "
        "with pip install 'dewey[huey]'."
    ) from exc

try:
    from huey.contrib.djhuey import HUEY, close_db
except ImproperlyConfigured as exc:
    # djhuey raises this itself for an unusable HUEY setting, and django.conf
    # raises it when settings are not configured at all.
    raise ImproperlyConfigured(
        f"huey.contrib.djhuey could not initialize: {exc} — dewey.contrib.django_huey "
        "needs configured Django settings (set DJANGO_SETTINGS_MODULE or call "
        "settings.configure()) and a valid HUEY setting."
    ) from exc
except (_HueyConfigurationError, SystemExit) as exc:
    raise ImproperlyConfigured(
        "huey.contrib.djhuey could not build a Huey instance. Set the HUEY setting "
        "in Django to a valid Huey instance or configuration dict."
    ) from exc

from dewey.adapters.huey import HueyAdapter
from dewey.django.conf import get_settings

_TASK_NAME = "dewey_process_task"

# The adapter that performed the one registration is stashed on the Huey
# instance, which survives importlib.reload() of this module. The registry it
# feeds would refuse a second registration under the same task name.
_REGISTRATION_ATTR = "_dewey_contrib_django_huey_adapter"


def _resolve_worker_database() -> str | None:
    """Validate ``DEWEY["WORKER_DATABASE"]`` against ``settings.DATABASES``."""
    from django.conf import settings

    alias = get_settings()["WORKER_DATABASE"]
    if alias is None:
        return None
    if not isinstance(alias, str):
        raise ImproperlyConfigured(
            'DEWEY["WORKER_DATABASE"] must be a database alias string or None, '
            f"got {type(alias).__name__}."
        )
    if alias not in settings.DATABASES:
        known = ", ".join(sorted(settings.DATABASES)) or "(none configured)"
        raise ImproperlyConfigured(
            f'DEWEY["WORKER_DATABASE"] = {alias!r} is not a DATABASES alias on this '
            f"project. Configured aliases: {known}."
        )
    return alias


WORKER_DATABASE: str | None = _resolve_worker_database()


def _process(task_id: str) -> bool:
    """Process one Dewey task on the worker's database alias.

    ``dewey.django.executor`` is imported per call so that importing this module
    never touches Dewey's Django models before the app registry is ready.
    """
    from dewey.django.executor import process_task

    return process_task(task_id, using=WORKER_DATABASE)


_worker = close_db(_process)

_existing_adapter = cast(HueyAdapter | None, getattr(HUEY, _REGISTRATION_ATTR, None))
if _existing_adapter is None:
    adapter = HueyAdapter(HUEY, task_name=_TASK_NAME)
    adapter.register(_worker)
    setattr(HUEY, _REGISTRATION_ATTR, adapter)
else:
    adapter = _existing_adapter
# On a reload the already-registered worker keeps reading WORKER_DATABASE
# through this module's namespace, so the value resolved above still applies.


def dispatch(task_id: str) -> Any:
    """Hand a claimed task ID to the Huey worker pool.

    The target for ``DEWEY["DISPATCH"] = "dewey.contrib.django_huey.dispatch"``:
    a module-level callable, because Django's ``import_string`` resolves
    ``module.attribute`` and cannot traverse into objects.
    """
    return adapter.dispatch(task_id)


__all__ = ["WORKER_DATABASE", "adapter", "dispatch"]
