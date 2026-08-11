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
from datetime import datetime


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
