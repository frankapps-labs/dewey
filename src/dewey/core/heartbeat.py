"""Dispatcher heartbeat contract — framework-agnostic, like the rest of core.

A running dispatcher periodically upserts one heartbeat row keyed by a random
instance identifier. Readiness checks compare ``last_seen_at`` against a
freshness window and match on backend kind, database identifier, and queues.

The contract deliberately carries no hostname, PID, DSN, credentials, or
process environment: heartbeats are written to a shared database and must
never leak host or connection secrets.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 15.0
DEFAULT_HEARTBEAT_STALE_SECONDS = 45.0
DEFAULT_HEARTBEAT_RETENTION_DAYS = 7


@dataclass(frozen=True)
class DispatcherHeartbeat:
    """Read-only snapshot of a dispatcher heartbeat row."""

    #: Random identifier minted by the dispatcher at startup — not a hostname or PID.
    instance_id: str
    dewey_version: str
    #: Backend kind, e.g. ``"django"`` or ``"sqlalchemy"``.
    backend: str
    #: Non-secret database alias/identifier the dispatcher operates on — never a DSN.
    database: str
    #: Queues this dispatcher serves; None means all queues.
    queues: tuple[str, ...] | None
    started_at: datetime
    last_seen_at: datetime

    def is_fresh(
        self,
        now: datetime,
        stale_after_seconds: float = DEFAULT_HEARTBEAT_STALE_SECONDS,
    ) -> bool:
        return self.last_seen_at >= now - timedelta(seconds=stale_after_seconds)

    def serves(self, requested_queues: tuple[str, ...] | None) -> bool:
        """Whether this dispatcher covers every requested queue."""
        if requested_queues is None or self.queues is None:
            return True
        return set(requested_queues).issubset(self.queues)
