"""Django dispatch backend — the claim query and its recovery passes."""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime

from django.db import connection, connections, models, transaction
from django.db.models.functions import Coalesce

from dewey.core.states import TaskStatus
from dewey.django.models import TaskEntry
from dewey.django.sweep import (
    DEFAULT_DISPATCH_TIMEOUT_SECONDS,
    DEFAULT_STUCK_THRESHOLD_MINUTES,
    sweep,
)
from dewey.listen_sync import DEFAULT_WORK_CHANNEL, SyncWorkListener

logger = logging.getLogger(__name__)


class DjangoDispatchBackend:
    """Claim, release and sweep using Django's ORM.

    Args:
        queues: Restrict this dispatcher to these queues. ``None`` means all.
        stuck_threshold_minutes: How long a row may sit in PROCESSING before the
            sweep assumes the worker died.
        dispatch_timeout_seconds: How long a row may sit in DISPATCHING before the
            sweep reclaims it. Must exceed the worst-case wait in your broker.
        using: Database alias, for projects that give Dewey its own connection.
        channel: Postgres channel to listen on for wake-ups.
    """

    def __init__(
        self,
        *,
        queues: Sequence[str] | None = None,
        stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
        dispatch_timeout_seconds: int = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
        sweep_limit: int = 100,
        using: str = "default",
        channel: str = DEFAULT_WORK_CHANNEL,
    ) -> None:
        self.queues = list(queues) if queues else None
        self.stuck_threshold_minutes = stuck_threshold_minutes
        self.dispatch_timeout_seconds = dispatch_timeout_seconds
        self.sweep_limit = sweep_limit
        self.using = using
        self._listener = SyncWorkListener(self._raw_connection, channel=channel)
        self._listener_opened = False

    # --- claim / release ---

    def claim(self, limit: int) -> list[str]:
        """Move up to ``limit`` ready rows to DISPATCHING, committed before return.

        Ordering is highest ``priority`` first, then by effective due time —
        ``scheduled_for`` if set, else ``created_at`` — then oldest first.
        Immediate and due scheduled work share one line, so a steady stream of
        fresh tasks cannot starve a retry that has been due for minutes.
        ``select_for_update(skip_locked=True)`` is what lets several dispatchers
        run without double-claiming.
        """
        now = datetime.now(UTC)
        with transaction.atomic(using=self.using):
            queryset = (
                TaskEntry.objects.using(self.using)
                .select_for_update(skip_locked=True)
                .filter(
                    models.Q(scheduled_for__isnull=True) | models.Q(scheduled_for__lte=now),
                    status=TaskStatus.PENDING.value,
                )
                .order_by("-priority", Coalesce("scheduled_for", "created_at").asc(), "created_at")
            )
            if self.queues is not None:
                queryset = queryset.filter(queue__in=self.queues)

            task_ids = list(queryset.values_list("id", flat=True)[:limit])
            if not task_ids:
                return []

            TaskEntry.objects.using(self.using).filter(
                id__in=task_ids, status=TaskStatus.PENDING.value
            ).update(status=TaskStatus.DISPATCHING.value, dispatching_at=now)
        return task_ids

    def release(self, task_ids: Sequence[str]) -> None:
        """Return claimed rows to PENDING, leaving the attempt count untouched."""
        if not task_ids:
            return
        TaskEntry.objects.using(self.using).filter(
            id__in=list(task_ids), status=TaskStatus.DISPATCHING.value
        ).update(status=TaskStatus.PENDING.value, dispatching_at=None)

    # --- recovery ---

    def run_sweep(self) -> dict[str, list[str]]:
        return sweep(
            stuck_threshold_minutes=self.stuck_threshold_minutes,
            dispatch_timeout_seconds=self.dispatch_timeout_seconds,
            limit=self.sweep_limit,
            using=self.using,
        )

    # --- wake-up ---

    def wait_for_work(self, timeout: float) -> bool:
        if not self._listener_opened:
            self._listener_opened = True
            try:
                self._listener.open()
            except Exception:
                logger.warning(
                    "Dewey dispatcher: could not start LISTEN; polling only", exc_info=True
                )
        if self._listener.supported:
            return self._listener.wait(timeout)
        time.sleep(max(0.0, timeout))
        return False

    def close(self) -> None:
        self._listener.close()

    def _raw_connection(self):
        """Open a second connection dedicated to LISTEN.

        Django's own connection is per-thread and gets wrapped in transactions; a
        connection parked in LISTEN cannot be used for anything else. Building one
        from the same connection parameters keeps settings, TLS and credentials
        identical without depending on psycopg directly — which is what allows
        both psycopg2 and psycopg3 projects to get wake-ups.
        """
        target = connections[self.using] if self.using != "default" else connection
        return target.get_new_connection(target.get_connection_params())


__all__ = ["DjangoDispatchBackend"]
