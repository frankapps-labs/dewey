"""Sweep — catches tasks the broker dropped or workers left stuck."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from django.db import models, transaction

from dewey.core.states import TaskStatus
from dewey.django.models import TaskEntry

logger = logging.getLogger(__name__)

# Default: tasks stuck in PROCESSING for >10 minutes are considered abandoned
DEFAULT_STUCK_THRESHOLD_MINUTES = 10

# Default: a row claimed for dispatch but not started within 5 minutes is assumed
# lost. Must stay above the worst-case broker backlog wait for your deployment.
DEFAULT_DISPATCH_TIMEOUT_SECONDS = 300


def sweep_failed(limit: int = 100) -> list[str]:
    """
    Find FAILED tasks ready for retry (scheduled_for has passed).
    Resets them to PENDING so the broker can pick them up.

    Returns list of task IDs that were re-enqueued.
    """
    now = datetime.now(UTC)

    with transaction.atomic():
        task_ids = list(
            TaskEntry.objects.select_for_update()
            .filter(
                status=TaskStatus.FAILED.value,
                scheduled_for__lte=now,
            )
            .order_by("scheduled_for")
            .values_list("id", flat=True)[:limit]
        )

        if not task_ids:
            return []

        retry_ids = list(
            TaskEntry.objects.filter(
                id__in=task_ids,
                status=TaskStatus.FAILED.value,
                attempts__lt=models.F("max_attempts"),
            ).values_list("id", flat=True)
        )
        dead_ids = list(
            TaskEntry.objects.filter(
                id__in=task_ids,
                status=TaskStatus.FAILED.value,
                attempts__gte=models.F("max_attempts"),
            ).values_list("id", flat=True)
        )

        TaskEntry.objects.filter(id__in=retry_ids, status=TaskStatus.FAILED.value).update(
            status=TaskStatus.PENDING.value,
        )
        TaskEntry.objects.filter(id__in=dead_ids, status=TaskStatus.FAILED.value).update(
            status=TaskStatus.DEAD.value,
        )

    if dead_ids:
        logger.warning("Sweep dead-lettered %d exhausted failed tasks", len(dead_ids))
    logger.info("Sweep re-enqueued %d failed tasks", len(retry_ids))
    return retry_ids


def sweep_stuck(
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    limit: int = 100,
) -> list[str]:
    """
    Find tasks stuck in PROCESSING (worker died mid-task).
    Resets them to PENDING for re-processing.

    Returns list of task IDs that were unstuck.
    """
    threshold = datetime.now(UTC) - timedelta(minutes=stuck_threshold_minutes)

    with transaction.atomic():
        task_ids = list(
            TaskEntry.objects.select_for_update()
            .filter(
                status=TaskStatus.PROCESSING.value,
                started_at__lt=threshold,
            )
            .order_by("started_at")
            .values_list("id", flat=True)[:limit]
        )

        if not task_ids:
            return []

        retry_ids = list(
            TaskEntry.objects.filter(
                id__in=task_ids,
                status=TaskStatus.PROCESSING.value,
                attempts__lt=models.F("max_attempts"),
            ).values_list("id", flat=True)
        )
        dead_ids = list(
            TaskEntry.objects.filter(
                id__in=task_ids,
                status=TaskStatus.PROCESSING.value,
                attempts__gte=models.F("max_attempts"),
            ).values_list("id", flat=True)
        )

        TaskEntry.objects.filter(id__in=retry_ids, status=TaskStatus.PROCESSING.value).update(
            status=TaskStatus.PENDING.value,
        )
        TaskEntry.objects.filter(id__in=dead_ids, status=TaskStatus.PROCESSING.value).update(
            status=TaskStatus.DEAD.value,
        )

    if dead_ids:
        logger.warning("Sweep dead-lettered %d exhausted stuck tasks", len(dead_ids))
    logger.warning(
        "Sweep unstuck %d processing tasks (threshold=%dm)",
        len(retry_ids),
        stuck_threshold_minutes,
    )
    return retry_ids


def sweep_dispatching(
    dispatch_timeout_seconds: int = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
    limit: int = 100,
) -> list[str]:
    """
    Find tasks a dispatcher claimed but no worker ever picked up, and return them
    to PENDING so a dispatcher can hand them out again.

    Backstop for a dispatcher that died between committing the claim and reaching
    the transport. ``dispatch_timeout_seconds`` must exceed the worst-case wait in
    the broker before a worker starts a task.

    Returns list of task IDs that were reclaimed.
    """
    threshold = datetime.now(UTC) - timedelta(seconds=dispatch_timeout_seconds)

    with transaction.atomic():
        task_ids = list(
            TaskEntry.objects.select_for_update()
            .filter(
                status=TaskStatus.DISPATCHING.value,
                dispatching_at__lt=threshold,
            )
            .order_by("dispatching_at")
            .values_list("id", flat=True)[:limit]
        )

        if not task_ids:
            return []

        retry_ids = list(
            TaskEntry.objects.filter(
                id__in=task_ids,
                status=TaskStatus.DISPATCHING.value,
                attempts__lt=models.F("max_attempts"),
            ).values_list("id", flat=True)
        )
        dead_ids = list(
            TaskEntry.objects.filter(
                id__in=task_ids,
                status=TaskStatus.DISPATCHING.value,
                attempts__gte=models.F("max_attempts"),
            ).values_list("id", flat=True)
        )

        TaskEntry.objects.filter(id__in=retry_ids, status=TaskStatus.DISPATCHING.value).update(
            status=TaskStatus.PENDING.value,
            dispatching_at=None,
        )
        TaskEntry.objects.filter(id__in=dead_ids, status=TaskStatus.DISPATCHING.value).update(
            status=TaskStatus.DEAD.value,
            dispatching_at=None,
        )

    if dead_ids:
        logger.warning("Sweep dead-lettered %d exhausted dispatching tasks", len(dead_ids))
    if retry_ids:
        logger.warning(
            "Sweep reclaimed %d dispatching tasks (timeout=%ds)",
            len(retry_ids),
            dispatch_timeout_seconds,
        )
    return retry_ids


def sweep(
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    dispatch_timeout_seconds: int = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
    limit: int = 100,
) -> dict[str, list[str]]:
    """
    Run every recovery pass. Returns dict with 'failed', 'dispatching' and
    'stuck' task ID lists.

    The dispatcher calls this on its own interval. Without something calling it,
    failed tasks never become eligible for retry.
    """
    return {
        "failed": sweep_failed(limit=limit),
        "dispatching": sweep_dispatching(
            dispatch_timeout_seconds=dispatch_timeout_seconds, limit=limit
        ),
        "stuck": sweep_stuck(stuck_threshold_minutes=stuck_threshold_minutes, limit=limit),
    }
