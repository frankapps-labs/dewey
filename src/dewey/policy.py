"""Task policy — declare how a task behaves, once, as data.

A task type's behaviour is a :class:`TaskPolicy`. Policies are resolved by
merging three layers, lowest precedence first::

    TASK_DEFAULTS  <  @dewey.task(...)  <  configure_policies(...)

``TASK_DEFAULTS`` is Dewey's baseline. The decorator is where the person writing
the handler leaves their hints. ``configure_policies`` is the project-wide layer
a platform team owns and reviews in one place — it wins, so operational policy
is never scattered across handler modules.

Runtime database overrides are a deliberate future boundary: the resolver takes
layers in precedence order, so an override source can be added without changing
this API.
"""

from __future__ import annotations

import logging
import random
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Any, Protocol, runtime_checkable

from dewey.errors import DuplicateTaskTypeError, NonRetryableError

logger = logging.getLogger(__name__)


# --- Backoff policies -------------------------------------------------------


@runtime_checkable
class BackoffPolicy(Protocol):
    """How long to wait before retrying a task that just failed."""

    def delay_for(self, attempt: int) -> timedelta:
        """Delay after ``attempt`` (1-based: 1 is the first failed attempt)."""
        ...


def _jittered(seconds: float, jitter: float, cap: float | None) -> float:
    if jitter > 0:
        spread = seconds * jitter
        seconds += random.uniform(-spread, spread)
    if cap is not None:
        seconds = min(seconds, cap)
    return max(0.0, seconds)


@dataclass(frozen=True)
class Constant:
    """Wait the same amount of time after every failure.

    The right shape for fast local delivery, where a failure usually means
    "the recipient blinked" rather than "the service is overloaded".
    """

    seconds: float = 3.0
    jitter: float = 0.0

    def delay_for(self, attempt: int) -> timedelta:
        return timedelta(seconds=_jittered(self.seconds, self.jitter, None))


@dataclass(frozen=True)
class Exponential:
    """Double the wait after each failure, with a cap and jitter.

    ``attempt=1`` waits ``base_s``, ``attempt=2`` waits ``base_s * factor``, and
    so on up to ``cap_s``. Jitter spreads a batch of simultaneous failures out
    instead of retrying them in lockstep.
    """

    base_s: float = 120.0
    factor: float = 2.0
    cap_s: float = 3600.0
    jitter: float = 0.25

    def delay_for(self, attempt: int) -> timedelta:
        raw = self.base_s * (self.factor ** max(0, attempt - 1))
        return timedelta(seconds=_jittered(min(raw, self.cap_s), self.jitter, self.cap_s))


@dataclass(frozen=True)
class Custom:
    """Delegate to your own function of the attempt number.

    The function may return a :class:`~datetime.timedelta` or a number of
    seconds.
    """

    fn: Callable[[int], timedelta | float]

    def delay_for(self, attempt: int) -> timedelta:
        result = self.fn(attempt)
        if isinstance(result, timedelta):
            return result
        return timedelta(seconds=float(result))


# --- Policy -----------------------------------------------------------------

#: Fields a policy layer may override. Anything else is rejected early, so a
#: typo in a decorator or config entry fails at import time rather than silently
#: doing nothing at dispatch time.
POLICY_FIELDS = frozenset(
    {
        "queue",
        "priority",
        "max_attempts",
        "backoff",
        "retry_on",
        "fail_fast_on",
    }
)


@dataclass(frozen=True)
class TaskPolicy:
    """The resolved behaviour of one task type.

    Tier 1 only: execution routing, attempt budget, backoff, and failure
    classification. Fairness, rate limits, timeouts, dedupe windows, lifecycle
    hooks and safety metadata are deliberately absent from the first release —
    see the project docs for what is deferred.
    """

    task_type: str
    handler: Callable[..., Any] | None = None

    # Dispatch routing
    queue: str = "default"
    priority: int = 0

    # Retry / failure
    max_attempts: int = 5
    backoff: BackoffPolicy = field(default_factory=Exponential)
    #: Exceptions eligible for retry. Empty means "any exception is retryable",
    #: which is the forgiving default: a task fails, it retries, and the attempt
    #: budget is what eventually stops it.
    retry_on: tuple[type[BaseException], ...] = ()
    #: Exceptions that dead-letter immediately, ignoring remaining attempts.
    fail_fast_on: tuple[type[BaseException], ...] = (NonRetryableError,)

    def is_retryable(self, exc: BaseException) -> bool:
        """Should ``exc`` be retried (attempt budget permitting)?"""
        if self.fail_fast_on and isinstance(exc, self.fail_fast_on):
            return False
        if not self.retry_on:
            return True
        return isinstance(exc, self.retry_on)

    def delay_for(self, attempt: int) -> timedelta:
        """Backoff delay after ``attempt`` (1-based)."""
        return self.backoff.delay_for(attempt)


#: Dewey's baseline layer — the field defaults on :class:`TaskPolicy`, with no
#: task type or handler bound yet. Use ``configure_policies`` to change
#: project-wide defaults rather than mutating this.
TASK_DEFAULTS = TaskPolicy(task_type="")


def _validate(task_type: str, overrides: dict[str, Any]) -> dict[str, Any]:
    unknown = set(overrides) - POLICY_FIELDS
    if unknown:
        allowed = ", ".join(sorted(POLICY_FIELDS))
        raise TypeError(
            f"Unknown policy field(s) for task {task_type!r}: "
            f"{', '.join(sorted(unknown))}. Supported fields: {allowed}."
        )
    if "max_attempts" in overrides:
        value = overrides["max_attempts"]
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ValueError(
                f"max_attempts for task {task_type!r} must be an int >= 1, got {value!r}"
            )
    for key in ("retry_on", "fail_fast_on"):
        if key in overrides and not isinstance(overrides[key], tuple):
            value = overrides[key]
            # A bare exception class is the obvious slip; accept it.
            if isinstance(value, type) and issubclass(value, BaseException):
                overrides[key] = (value,)
            else:
                raise TypeError(
                    f"{key} for task {task_type!r} must be a tuple of exception "
                    f"classes, got {value!r}"
                )
    if "backoff" in overrides and not isinstance(overrides["backoff"], BackoffPolicy):
        raise TypeError(
            f"backoff for task {task_type!r} must implement delay_for(attempt), "
            f"got {overrides['backoff']!r}"
        )
    return overrides


# --- Registry ---------------------------------------------------------------


class PolicyRegistry:
    """Process-local map of task type to declared policy.

    Registration happens at import time, in the worker process. Producers never
    need the registry: they create rows by task type and the row is the contract.
    """

    def __init__(self) -> None:
        self._policies: dict[str, TaskPolicy] = {}

    def register(self, policy: TaskPolicy, *, replace_existing: bool = False) -> TaskPolicy:
        existing = self._policies.get(policy.task_type)
        if existing is not None and not replace_existing:
            if existing == policy:
                # Same declaration seen twice — a re-imported module, not a bug.
                return existing
            raise DuplicateTaskTypeError(
                f"Task type {policy.task_type!r} is already registered to "
                f"{_describe(existing.handler)}; refusing to rebind it to "
                f"{_describe(policy.handler)}. Task types are a public contract — "
                f"pick a distinct name, or pass replace_existing=True if you meant "
                f"to override it."
            )
        self._policies[policy.task_type] = policy
        return policy

    def get(self, task_type: str) -> TaskPolicy | None:
        return self._policies.get(task_type)

    def task_types(self) -> tuple[str, ...]:
        return tuple(sorted(self._policies))

    def unregister(self, task_type: str) -> None:
        self._policies.pop(task_type, None)

    def clear(self) -> None:
        self._policies.clear()


def _describe(handler: Callable[..., Any] | None) -> str:
    if handler is None:
        return "<no handler>"
    module = getattr(handler, "__module__", "?")
    name = getattr(handler, "__qualname__", repr(handler))
    return f"{module}.{name}"


#: The process-local registry the decorator writes to and workers read from.
registry = PolicyRegistry()

#: Project-wide layer, highest precedence. Keyed by task type.
_project_policies: dict[str, dict[str, Any]] = {}


def task(
    task_type: str, **policy_kwargs: Any
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Register a handler and its policy hints for ``task_type``.

    The decorated function is returned unchanged — it stays an ordinary,
    directly callable, directly testable function. It is deliberately *not* a
    proxy with a ``.delay()``: producers create work with ``create_task`` and
    never import handler modules.

    ::

        @dewey.task("agent.notify", max_attempts=5, backoff=dewey.Constant(3))
        def notify_agent(command_id: str) -> None:
            ...
    """
    overrides = _validate(task_type, dict(policy_kwargs))

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        registry.register(replace(TASK_DEFAULTS, task_type=task_type, handler=fn, **overrides))
        return fn

    return decorator


def configure_policies(policies: dict[str, dict[str, Any]]) -> None:
    """Set the project-wide policy layer, which outranks decorator hints.

    Call this once at startup — from a ``dewey_config`` module, Django settings
    import, or your app factory. Passing a task type that has no registered
    handler is fine: policy may be declared before the handler module is
    imported.
    """
    for task_type, overrides in policies.items():
        _validate(task_type, dict(overrides))
    for task_type, overrides in policies.items():
        _project_policies[task_type] = dict(overrides)


def clear_project_policies() -> None:
    """Drop the project-wide layer. Mainly for tests."""
    _project_policies.clear()


def resolve_policy(task_type: str) -> TaskPolicy:
    """Merge every layer into the effective policy for ``task_type``.

    An unregistered task type resolves to the defaults with no handler — the
    row is still governed by policy, which is what lets the dispatcher and the
    sweep treat unknown types as ordinary failures instead of crashing.
    """
    declared = registry.get(task_type)
    policy = declared or replace(TASK_DEFAULTS, task_type=task_type)
    overrides = _project_policies.get(task_type)
    if overrides:
        policy = replace(policy, **overrides)
    return policy


__all__ = [
    "POLICY_FIELDS",
    "TASK_DEFAULTS",
    "BackoffPolicy",
    "Constant",
    "Custom",
    "Exponential",
    "PolicyRegistry",
    "TaskPolicy",
    "clear_project_policies",
    "configure_policies",
    "registry",
    "resolve_policy",
    "task",
]
