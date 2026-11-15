"""Explicit supervised native worker bootstrap, with process-local applications.

Run under an enclosing container/cgroup or equivalent kill-all supervisor boundary.
No broker/framework is imported until the application's process-local factory runs.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, replace
from typing import Any, Literal

from dewey._worker.contracts import Timing as WorkerTiming
from dewey._worker.contracts import round_budget
from dewey._worker.runtime import LedgerStorage
from dewey.policy import registry


def default_timing(concurrency: int = 2, **overrides: float) -> WorkerTiming:
    """Conservative limits for a worker with ``concurrency`` task processes.

    Leases are sized so a worst-case supervision round fits inside them with
    room to spare; a task that outlives its worker is retried about one lease later. A task may
    run for up to an hour unless it sets a shorter ``execution_timeout_s``.
    Override any field by name, e.g. ``default_timing(task_timeout_seconds=600)``.
    """
    values: dict[str, float] = {
        "renewal_interval_seconds": 5.0,
        "db_wait_seconds": 2.0,
        "poll_interval_seconds": 1.0,
        "termination_seconds": 4.0,
        "task_timeout_seconds": 3600.0,
    }
    unknown = set(overrides) - set(WorkerTiming.__dataclass_fields__)
    if unknown:
        raise ValueError(f"Unknown timing setting(s): {', '.join(sorted(unknown))}")
    values.update(overrides)
    # Size the leases from the other limits, unless both were given.
    unbounded = {"assignment_lease_seconds": 1e9, "execution_lease_seconds": 1e9}
    probe = WorkerTiming(**{**values, **unbounded})
    lease = max(60.0, 1.25 * (probe.renewal_interval_seconds + round_budget(probe, concurrency)))
    return WorkerTiming(**{**dict.fromkeys(unbounded, lease), **values})


@dataclass(frozen=True)
class WorkerConfig:
    """One worker service. Only the application factory is required.

    Every limit is explicit in ``to_dict()`` and can be set; the defaults are
    ``default_timing(concurrency)`` and half-minute service deadlines.
    """

    factory: str
    queues: tuple[str, ...] = ("default",)
    #: Defaults to ``default_timing(concurrency)``.
    timing: WorkerTiming = None  # type: ignore[assignment]
    concurrency: int = 2
    max_restarts: int = 3
    startup_seconds: float = 30.0
    supervision_seconds: float = 30.0
    shutdown_seconds: float = 30.0
    #: Apply declared schedules when the worker starts. For single-node installs
    #: only: with several workers, apply once in the release step instead.
    auto_apply_schedules: bool = False
    #: ``module:function`` called with alert dicts — see ``dewey.alerts``.
    alert_hook: str | None = None
    #: How the service and its task processes are created. ``spawn`` imports the
    #: application once per process; ``fork`` imports it once and forks, so the
    #: processes share it. Fork needs a fork-safe application: see the worker docs.
    start_method: Literal["spawn", "fork"] = "spawn"
    #: Open an OpenTelemetry span per attempt in the task processes. Needs
    #: ``opentelemetry-api`` (``dewey[otel]``); the application configures the
    #: SDK or vendor that exports them. See ``dewey.core.tracing``.
    tracing: bool = False
    #: What a failed attempt's log record says about the handler's exception:
    #: ``type`` (its class only) or ``full`` (message and traceback as well, for
    #: a log pipeline that scrubs). See ``dewey.core.execution.configure_failure_details``.
    failure_details: Literal["type", "full"] = "type"

    def __post_init__(self) -> None:
        if not isinstance(self.factory, str) or self.factory.count(":") != 1:
            raise ValueError("factory must be module:function")
        module, function = self.factory.split(":")
        if (
            not module
            or not all(part.isidentifier() for part in module.split("."))
            or not function.isidentifier()
        ):
            raise ValueError("factory must be module:function")
        if (
            isinstance(self.queues, str)
            or not self.queues
            or any(not isinstance(q, str) or not q or len(q) > 50 for q in self.queues)
        ):
            raise ValueError("Explicit nonempty queues of at most 50 characters are required")
        object.__setattr__(self, "queues", tuple(self.queues))
        for name, minimum in (("concurrency", 1), ("max_restarts", 0)):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"{name} must be an integer >= {minimum}")
        for name in ("startup_seconds", "supervision_seconds", "shutdown_seconds"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be positive and finite")
        if not isinstance(self.auto_apply_schedules, bool):
            raise ValueError("auto_apply_schedules must be true or false")
        if self.alert_hook is not None and (
            not isinstance(self.alert_hook, str) or self.alert_hook.count(":") != 1
        ):
            raise ValueError("alert_hook must be module:function")
        if self.start_method not in ("spawn", "fork"):
            raise ValueError("start_method must be spawn or fork")
        if not isinstance(self.tracing, bool):
            raise ValueError("tracing must be true or false")
        if self.failure_details not in ("type", "full"):
            raise ValueError("failure_details must be type or full")
        if self.timing is None:
            object.__setattr__(self, "timing", default_timing(self.concurrency))
        if not isinstance(self.timing, WorkerTiming):
            raise ValueError("timing must be WorkerTiming")
        budget = round_budget(self.timing, self.concurrency)
        if self.timing.renewal_interval_seconds + budget >= self.timing.execution_lease_seconds:
            raise ValueError("Execution lease is too short for concurrency and DB budget")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> WorkerConfig:
        """Parse trusted operator configuration; unknown options are rejected.

        Anything left out takes its default. ``timing`` may name only the
        limits to change.
        """
        try:
            fields = dict(value)
            timing = fields.pop("timing", None)
            config = cls(**fields)
            if timing is not None:
                config = replace(config, timing=default_timing(config.concurrency, **timing))
            return config
        except (TypeError, KeyError) as exc:
            raise ValueError("Invalid worker configuration shape") from exc


@dataclass(frozen=True)
class WorkerApplication:
    """Resources created separately in the service parent and each spawned child."""

    storage: LedgerStorage
    close_resources: Callable[[], Any]

    def __post_init__(self) -> None:
        if not callable(getattr(self.storage, "call", None)) or not callable(self.close_resources):
            raise ValueError("Application requires storage.call and resource disposal")


@dataclass(frozen=True)
class WorkerContext:
    """Factory inputs; no engines, credentials or event loops travel through IPC.

    The builders return a worker that claims only task types registered in
    its own process, so import your handler modules before returning.
    """

    config: WorkerConfig
    owner: str
    role: Literal["parent", "task"]

    def sqlalchemy(self, sessions: Any, *, close_resources: Callable[[], Any]) -> WorkerApplication:
        from dewey.sqlalchemy.native import NativeSQLAlchemyStorage

        return WorkerApplication(
            NativeSQLAlchemyStorage(
                sessions,
                owner=self.owner,
                timing=self.config.timing,
                queues=self.config.queues,
                task_types=registry.task_types,
            ),
            close_resources,
        )

    def async_sqlalchemy(
        self, sessions: Any, *, close_resources: Callable[[], Any]
    ) -> WorkerApplication:
        from dewey.sqlalchemy.native import AsyncNativeSQLAlchemyStorage

        return WorkerApplication(
            AsyncNativeSQLAlchemyStorage(
                sessions,
                owner=self.owner,
                timing=self.config.timing,
                queues=self.config.queues,
                task_types=registry.task_types,
            ),
            close_resources,
        )

    def django(self, *, using: str = "default") -> WorkerApplication:
        """Call after django.setup/importing handlers; alias and driver waits are explicit."""
        from django.db import connections

        from dewey.django.native import NativeDjangoStorage

        return WorkerApplication(
            NativeDjangoStorage(
                owner=self.owner,
                timing=self.config.timing,
                queues=self.config.queues,
                using=using,
                task_types=registry.task_types,
            ),
            connections.close_all,
        )


def run_worker(config: WorkerConfig) -> int:
    """Run an owned supervised worker until stop/fault; return a service exit code.

    POSIX main-thread entrypoint. No automatic restart, migration or schema creation.
    Exit 0: requested stop; 70: contained fault; 71: uncertain cleanup (do not restart
    until the enclosing runtime proves containment). External effects need idempotency.
    """
    from dewey.worker._process import supervise

    return supervise(config)


__all__ = [
    "WorkerApplication",
    "WorkerConfig",
    "WorkerContext",
    "WorkerTiming",
    "default_timing",
    "run_worker",
]
