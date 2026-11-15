"""Django adapter for Dewey — models, task creation and queries.

Everything here is imported lazily. Dewey's Django models must not be imported
before Django's app registry is ready, and producers should not pay import-time
cost for worker and operational APIs they do not use.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

_LAZY: dict[str, str] = {
    # Executor
    "create_task": "dewey.django.executor",
    "create_or_get_task": "dewey.django.executor",
    "process_task": "dewey.django.executor",
    # Queries and actions
    "get_stats": "dewey.django.queries",
    "get_pending": "dewey.django.queries",
    "get_dispatching": "dewey.django.queries",
    "get_workers": "dewey.django.queries",
    "get_processing": "dewey.django.queries",
    "get_stuck": "dewey.django.queries",
    "get_failed": "dewey.django.queries",
    "get_dead": "dewey.django.queries",
    "get_expired": "dewey.django.queries",
    "get_task": "dewey.django.queries",
    "get_recent": "dewey.django.queries",
    "retry_task": "dewey.django.queries",
    "bulk_retry": "dewey.django.queries",
    "kill_task": "dewey.django.queries",
    "purge_completed": "dewey.django.queries",
}

if TYPE_CHECKING:  # re-exported for type checkers and IDE completion
    from dewey.django.executor import (
        create_or_get_task as create_or_get_task,
    )
    from dewey.django.executor import (
        create_task as create_task,
    )
    from dewey.django.executor import (
        process_task as process_task,
    )
    from dewey.django.queries import (
        bulk_retry as bulk_retry,
    )
    from dewey.django.queries import (
        get_dead as get_dead,
    )
    from dewey.django.queries import (
        get_dispatching as get_dispatching,
    )
    from dewey.django.queries import (
        get_expired as get_expired,
    )
    from dewey.django.queries import (
        get_failed as get_failed,
    )
    from dewey.django.queries import (
        get_pending as get_pending,
    )
    from dewey.django.queries import (
        get_processing as get_processing,
    )
    from dewey.django.queries import (
        get_recent as get_recent,
    )
    from dewey.django.queries import (
        get_stats as get_stats,
    )
    from dewey.django.queries import (
        get_stuck as get_stuck,
    )
    from dewey.django.queries import (
        get_task as get_task,
    )
    from dewey.django.queries import (
        get_workers as get_workers,
    )
    from dewey.django.queries import (
        kill_task as kill_task,
    )
    from dewey.django.queries import (
        purge_completed as purge_completed,
    )
    from dewey.django.queries import (
        retry_task as retry_task,
    )

__all__ = [
    "bulk_retry",
    "create_or_get_task",
    "create_task",
    "get_dead",
    "get_workers",
    "get_dispatching",
    "get_expired",
    "get_failed",
    "get_pending",
    "get_processing",
    "get_recent",
    "get_stats",
    "get_stuck",
    "get_task",
    "kill_task",
    "process_task",
    "purge_completed",
    "retry_task",
]


def __getattr__(name: str) -> Any:
    """Resolve a public name to its implementation, once.

    The resolved object is cached in this module's namespace, so a name that is
    both a submodule and a function resolves to the function every time.
    """
    module_path = _LAZY.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    try:
        module = import_module(module_path)
    except ModuleNotFoundError as exc:
        # `import dewey.django` succeeds without Django, because this package imports
        # nothing at module level. The failure therefore surfaces here, on first use,
        # where a bare "No module named 'django'" would not say what to do about it.
        if (exc.name or "").split(".")[0] == "django":
            raise ModuleNotFoundError(
                f"dewey.django needs Django, which is not installed. Install it with "
                f"pip install 'dewey[django]'. (Reaching for {name!r}.)",
                name=exc.name,
            ) from exc
        raise
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_LAZY})
