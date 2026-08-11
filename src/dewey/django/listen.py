"""Wake the dispatcher from Django, without a rollback ever waking it wrongly.

Postgres queues ``NOTIFY`` until the sending transaction commits. That single fact
does all the work here: a notification issued inside the same transaction as the
task row is delivered if and only if that row is durably committed. If the
business transaction rolls back, the row never existed and the wake-up never
happened.

That is why this needs no ``transaction.on_commit`` bookkeeping: the guarantee is
the database's, not the framework's, so it holds under nested ``atomic`` blocks,
in autocommit, and in code paths that never see Dewey at all.
"""

from __future__ import annotations

import logging

from django.db import connections

from dewey.listen_sync import DEFAULT_WORK_CHANNEL, work_payload

logger = logging.getLogger(__name__)


def notify_work_available(
    *,
    kind: str,
    entry_id: str,
    queue: str | None = None,
    channel: str = DEFAULT_WORK_CHANNEL,
    using: str = "default",
) -> bool:
    """Queue a NOTIFY for commit time.

    Returns ``True`` when a notification was queued, ``False`` on a non-Postgres
    database, where the dispatcher's poll is the only wake-up path. Callers do not
    need to branch.
    """
    conn = connections[using]
    if conn.vendor != "postgresql":
        return False

    with conn.cursor() as cursor:
        cursor.execute("SELECT pg_notify(%s, %s)", [channel, work_payload(kind, entry_id, queue)])
    return True


__all__ = ["DEFAULT_WORK_CHANNEL", "notify_work_available"]
