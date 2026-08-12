"""Dewey — Guaranteed delivery engine. Frankapps Built.

Postgres is the durable scheduler and backlog; a broker is only a worker-pool
transport. Declare a task, create work inside your own transaction, and let the
dispatcher and the ledger handle delivery::

    import dewey

    @dewey.task("agent.notify", max_attempts=5, backoff=dewey.Constant(3))
    def notify_agent(command_id: str) -> None:
        ...

Importing ``dewey`` pulls in no framework: the producer and worker APIs live in
``dewey.sqlalchemy`` and ``dewey.django``.
"""

from importlib.metadata import PackageNotFoundError, version

from dewey.core.states import TaskStatus
from dewey.errors import (
    DeweyError,
    DuplicateTaskTypeError,
    IdempotencyConflictError,
    NonRetryableError,
    RetryAfter,
    SerializationError,
    TransientError,
    UnknownTaskTypeError,
)
from dewey.policy import (
    TASK_DEFAULTS,
    BackoffPolicy,
    Constant,
    Custom,
    Exponential,
    PolicyRegistry,
    TaskPolicy,
    clear_project_policies,
    configure_policies,
    registry,
    resolve_policy,
    task,
)
from dewey.serialization import encode_args, encode_kwargs

try:
    __version__ = version("dewey")
except PackageNotFoundError:  # pragma: no cover — source tree without an install
    __version__ = "0.0.0.dev0"

__all__ = [
    "TASK_DEFAULTS",
    "BackoffPolicy",
    "Constant",
    "Custom",
    "DeweyError",
    "DuplicateTaskTypeError",
    "Exponential",
    "IdempotencyConflictError",
    "NonRetryableError",
    "PolicyRegistry",
    "RetryAfter",
    "SerializationError",
    "TaskPolicy",
    "TaskStatus",
    "TransientError",
    "UnknownTaskTypeError",
    "__version__",
    "clear_project_policies",
    "configure_policies",
    "encode_args",
    "encode_kwargs",
    "registry",
    "resolve_policy",
    "task",
]
