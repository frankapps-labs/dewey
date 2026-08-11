"""Django dispatch backend — the claim query and its recovery passes."""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import cast

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
        """Atomically expire deadlines and claim due PENDING or retryable FAILED rows."""
        now = datetime.now(UTC)
        due = models.Q(scheduled_for__isnull=True) | models.Q(scheduled_for__lte=now)
        claimable = cast(
            models.Q,
            (models.Q(status=TaskStatus.PENDING.value) & due)
            | models.Q(  # pyright: ignore[reportOperatorIssue]
                status=TaskStatus.FAILED.value,
                scheduled_for__lte=now,
                attempts__lt=models.F("max_attempts"),
            ),
        )
        with transaction.atomic(using=self.using):
            expired = (
                TaskEntry.objects.using(self.using)
                .select_for_update(skip_locked=True)
                .filter(
                    status__in=[TaskStatus.PENDING.value, TaskStatus.FAILED.value],
                    expires_at__isnull=False,
                    expires_at__lte=now,
                )
            )
            if self.queues is not None:
                expired = expired.filter(queue__in=self.queues)
            expired_ids = list(expired.values_list("id", flat=True)[:limit])
            if expired_ids:
                TaskEntry.objects.using(self.using).filter(id__in=expired_ids).update(
                    status=TaskStatus.EXPIRED.value,
                    expired_at=now,
                    dispatching_at=None,
                )

            queryset = (
                TaskEntry.objects.using(self.using)
                .select_for_update(skip_locked=True)
                .filter(claimable)
                .filter(models.Q(expires_at__isnull=True) | models.Q(expires_at__gt=now))
                .order_by("-priority", Coalesce("scheduled_for", "created_at").asc(), "created_at")
            )
            if self.queues is not None:
                queryset = queryset.filter(queue__in=self.queues)
            task_ids = list(queryset.values_list("id", flat=True)[:limit])
            if not task_ids:
                return []
            TaskEntry.objects.using(self.using).filter(
                id__in=task_ids,
                status__in=[TaskStatus.PENDING.value, TaskStatus.FAILED.value],
            ).update(status=TaskStatus.DISPATCHING.value, dispatching_at=now)
        return task_ids

    def next_due(self) -> datetime | None:
        """Earliest future schedule or deadline in this dispatcher's queue scope."""
        now = datetime.now(UTC)
        queryset = TaskEntry.objects.using(self.using).filter(
            models.Q(status=TaskStatus.PENDING.value)
            | models.Q(
                status=TaskStatus.FAILED.value,
                attempts__lt=models.F("max_attempts"),
            ),
            scheduled_for__isnull=False,
        )
        if self.queues is not None:
            queryset = queryset.filter(queue__in=self.queues)
        wake_at = models.Case(
            models.When(expires_at__lt=models.F("scheduled_for"), then=models.F("expires_at")),
            default=models.F("scheduled_for"),
            output_field=models.DateTimeField(),
        )
        due_at = queryset.aggregate(due=models.Min(wake_at))["due"]
        return due_at if due_at is None or due_at >= now else now

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
