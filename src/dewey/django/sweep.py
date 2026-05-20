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


def sweep_failed(limit: int = 100) -> list[str]:
    """
    Find FAILED tasks ready for retry (process_after has passed).
    Resets them to PENDING so the broker can pick them up.

    Returns list of task IDs that were re-enqueued.
    """
    now = datetime.now(UTC)

    with transaction.atomic():
        task_ids = list(
            TaskEntry.objects.select_for_update()
            .filter(
                status=TaskStatus.FAILED.value,
                process_after__lte=now,
            )
            .order_by("process_after")
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


def sweep(
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    limit: int = 100,
) -> dict[str, list[str]]:
    """
    Run both sweeps. Returns dict with 'failed' and 'stuck' task ID lists.

    Call this from a periodic task (e.g. every 5 minutes).
    """
    return {
        "failed": sweep_failed(limit=limit),
        "stuck": sweep_stuck(stuck_threshold_minutes=stuck_threshold_minutes, limit=limit),
    }
