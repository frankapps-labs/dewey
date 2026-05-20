"""Notification executor, sweep, and queries for Django."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from django.db import models, transaction
from django.db.models import Count

from dewey.core.backoff import BackoffFn, default_notification_backoff
from dewey.core.logging import (
    extract_trace_context,
    reset_trace_context,
    set_trace_context,
)
from dewey.core.notifications import (
    Channel,
    ChannelRegistry,
    ChannelResult,
    NotificationStatus,
)
from dewey.core.notifications import (
    NotificationAttempt as NotificationAttemptDC,
)
from dewey.core.notifications import (
    NotificationEntry as NotificationEntryDC,
)
from dewey.django.notification_models import NotificationAttempt, NotificationEntry

logger = logging.getLogger(__name__)


# --- Helpers ---


def _to_list(qs) -> list[NotificationEntryDC]:
    return [obj.to_dataclass() for obj in qs]


# --- Create ---


def create_notification(
    *,
    event_type: str,
    channel: str,
    recipient: str,
    subject: str = "",
    body: str = "",
    payload: dict[str, Any] | None = None,
    task_id: str | None = None,
    max_attempts: int = 3,
    process_after=None,
    metadata: dict[str, Any] | None = None,
) -> NotificationEntryDC:
    """Write a notification to the ledger."""
    notif = NotificationEntry.objects.create(
        event_type=event_type,
        channel=channel,
        recipient=recipient,
        subject=subject,
        body=body,
        payload=payload or {},
        task_id=task_id,
        max_attempts=max_attempts,
        process_after=process_after,
        metadata=metadata or {},
    )
    logger.info(
        "Notification created id=%s event=%s channel=%s recipient=%s",
        notif.id,
        event_type,
        channel,
        recipient,
    )
    return notif.to_dataclass()


def create_notifications_for_event(
    *,
    registry: ChannelRegistry,
    event_type: str,
    payload: dict[str, Any],
    task_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[NotificationEntryDC]:
    """Create notifications for all channel bindings registered for an event type."""
    bindings = registry.get_bindings(event_type)
    if not bindings:
        return []

    notifications = []
    for binding in bindings:
        recipient = binding.recipient_resolver(payload)
        subject, body = binding.body_renderer(event_type, payload)

        notif = create_notification(
            event_type=event_type,
            channel=binding.channel_name,
            recipient=recipient,
            subject=subject,
            body=body,
            payload=payload,
            task_id=task_id,
            max_attempts=binding.max_attempts,
            metadata=metadata,
        )
        notifications.append(notif)

    return notifications


# --- Send ---


def send_notification(
    notification_id: str,
    channel: Channel,
    *,
    backoff: BackoffFn | None = None,
) -> bool:
    """
    Attempt to deliver a single notification.

    `backoff` is an optional ``(attempts: int) -> timedelta`` function that
    decides how long to wait before retrying a failed send. Defaults to
    :func:`dewey.core.backoff.default_notification_backoff` (1 min base,
    30 min cap).

    Phase 1: PENDING → SENDING (committed)
    Phase 2: Call channel.send()
    Phase 3: SENDING → SENT/FAILED/DEAD (committed)

    Returns True if sent successfully.
    """
    now = datetime.now(UTC)

    # Phase 1: Claim
    with transaction.atomic():
        try:
            notif = NotificationEntry.objects.select_for_update().get(id=notification_id)
        except NotificationEntry.DoesNotExist:
            logger.warning("Notification not found id=%s", notification_id)
            return False

        current = NotificationStatus(notif.status)
        if current.is_terminal:
            logger.info(
                "Notification already terminal id=%s status=%s", notification_id, notif.status
            )
            return False

        if current != NotificationStatus.PENDING:
            logger.info("Notification not pending id=%s status=%s", notification_id, notif.status)
            return False

        if notif.process_after and notif.process_after > now:
            logger.info("Notification not ready id=%s", notification_id)
            return False

        if not current.can_transition_to(NotificationStatus.SENDING):
            logger.warning(
                "Invalid transition id=%s from=%s to=SENDING", notification_id, notif.status
            )
            return False

        notif.status = NotificationStatus.SENDING.value
        notif.attempts += 1
        notif.save(update_fields=["status", "attempts", "updated_at"])

    # Cache
    recipient = notif.recipient
    subject = notif.subject
    body = notif.body
    payload = notif.payload
    attempts = notif.attempts
    max_attempts = notif.max_attempts
    notif_metadata = dict(notif.metadata or {})

    # Restore the trace context captured at task/notification creation
    # time so every log line through Phase 2 and Phase 3 is correlated
    # with the originating request.
    _trace_token = set_trace_context(extract_trace_context(notif_metadata))
    try:
        # Phase 2: Send
        try:
            result: ChannelResult = channel.send(
                recipient=recipient,
                subject=subject,
                body=body,
                payload=payload,
            )
        except Exception as exc:
            result = ChannelResult(success=False, error=str(exc))

        # Phase 3: Record attempt + update status
        with transaction.atomic():
            try:
                notif = NotificationEntry.objects.select_for_update().get(id=notification_id)
            except NotificationEntry.DoesNotExist:
                logger.warning("Notification disappeared during send id=%s", notification_id)
                return False

            current = NotificationStatus(notif.status)
            if current != NotificationStatus.SENDING:
                logger.info("Notification status changed during send id=%s", notification_id)
                return False

            # Log attempt
            NotificationAttempt.objects.create(
                notification=notif,
                attempt_number=attempts,
                status="sent" if result.success else "failed",
                error=result.error,
                response_data=result.response_data,
            )

            if result.success:
                notif.status = NotificationStatus.SENT.value
                notif.sent_at = now
                notif.error = ""
                notif.save(update_fields=["status", "sent_at", "error", "updated_at"])
                logger.info("Notification sent id=%s channel=%s", notification_id, channel.name)
                return True

            # Failed
            notif.error = result.error

            if attempts >= max_attempts:
                notif.status = NotificationStatus.DEAD.value
                logger.error(
                    "Notification dead-lettered id=%s channel=%s attempts=%d",
                    notification_id,
                    channel.name,
                    attempts,
                )
            else:
                notif.status = NotificationStatus.FAILED.value
                notif.process_after = now + (backoff or default_notification_backoff)(attempts)
                logger.warning(
                    "Notification failed id=%s channel=%s attempts=%d/%d",
                    notification_id,
                    channel.name,
                    attempts,
                    max_attempts,
                )

            notif.save(update_fields=["status", "error", "process_after", "updated_at"])

        return False
    finally:
        reset_trace_context(_trace_token)


def process_notification(
    notification_id: str,
    registry: ChannelRegistry,
    *,
    backoff: BackoffFn | None = None,
) -> bool:
    """Process a notification using the registry to find the right channel.

    ``backoff`` is forwarded to send_notification."""
    try:
        notif = NotificationEntry.objects.get(id=notification_id)
    except NotificationEntry.DoesNotExist:
        logger.warning("Notification not found id=%s", notification_id)
        return False

    channel = registry.get_channel(notif.channel)
    if channel is None:
        logger.error(
            "No channel registered for %r (notification id=%s)", notif.channel, notification_id
        )
        return False

    return send_notification(notification_id, channel, backoff=backoff)


# --- Sweep ---

DEFAULT_STUCK_THRESHOLD_MINUTES = 5


def sweep_failed_notifications(limit: int = 100) -> list[str]:
    """Find FAILED notifications ready for retry. Reset to PENDING."""
    now = datetime.now(UTC)
    ids = list(
        NotificationEntry.objects.filter(
            status=NotificationStatus.FAILED.value,
            process_after__lte=now,
        )
        .order_by("process_after")
        .values_list("id", flat=True)[:limit]
    )
    if not ids:
        return []

    NotificationEntry.objects.filter(id__in=ids).update(
        status=NotificationStatus.PENDING.value,
    )
    logger.info("Sweep re-enqueued %d failed notifications", len(ids))
    return ids


def sweep_stuck_notifications(
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    limit: int = 100,
) -> list[str]:
    """Find notifications stuck in SENDING. Reset to PENDING."""
    threshold = datetime.now(UTC) - timedelta(minutes=stuck_threshold_minutes)
    ids = list(
        NotificationEntry.objects.filter(
            status=NotificationStatus.SENDING.value,
            updated_at__lt=threshold,
        )
        .order_by("updated_at")
        .values_list("id", flat=True)[:limit]
    )
    if not ids:
        return []

    NotificationEntry.objects.filter(id__in=ids).update(
        status=NotificationStatus.PENDING.value,
    )
    logger.warning("Sweep unstuck %d sending notifications", len(ids))
    return ids


def sweep_notifications(
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    limit: int = 100,
) -> dict[str, list[str]]:
    """Run both notification sweeps."""
    return {
        "failed": sweep_failed_notifications(limit=limit),
        "stuck": sweep_stuck_notifications(
            stuck_threshold_minutes=stuck_threshold_minutes, limit=limit
        ),
    }


# --- Queries ---


def get_notification_stats() -> dict[str, int]:
    """Counts by status."""
    rows = NotificationEntry.objects.values("status").annotate(count=Count("id")).order_by()
    stats = {s.value: 0 for s in NotificationStatus}
    for row in rows:
        stats[row["status"]] = row["count"]
    return stats


def get_notification(notification_id: str) -> NotificationEntryDC | None:
    """Single notification by ID."""
    try:
        return NotificationEntry.objects.get(id=notification_id).to_dataclass()
    except NotificationEntry.DoesNotExist:
        return None


def get_notification_attempts(notification_id: str) -> list[NotificationAttemptDC]:
    """All delivery attempts for a notification."""
    return [
        a.to_dataclass()
        for a in NotificationAttempt.objects.filter(notification_id=notification_id).order_by(
            "attempt_number"
        )
    ]


def get_notifications_for_task(
    task_id: str,
    limit: int = 50,
) -> list[NotificationEntryDC]:
    """All notifications linked to a task."""
    qs = NotificationEntry.objects.filter(task_id=task_id).order_by("created_at")
    return _to_list(qs[:limit])


def get_pending_notifications(
    limit: int = 50,
    channel: str | None = None,
) -> list[NotificationEntryDC]:
    """Pending notifications ready to send (process_after has passed or is NULL)."""
    now = datetime.now(UTC)
    qs = NotificationEntry.objects.filter(
        status=NotificationStatus.PENDING.value,
    ).filter(
        models.Q(process_after__isnull=True) | models.Q(process_after__lte=now),
    )
    if channel:
        qs = qs.filter(channel=channel)
    return _to_list(qs.order_by("created_at")[:limit])


def get_failed_notifications(
    limit: int = 50,
    channel: str | None = None,
) -> list[NotificationEntryDC]:
    """Failed notifications eligible for retry."""
    qs = NotificationEntry.objects.filter(status=NotificationStatus.FAILED.value)
    if channel:
        qs = qs.filter(channel=channel)
    return _to_list(qs.order_by("-created_at")[:limit])


def get_dead_notifications(
    limit: int = 50,
    channel: str | None = None,
) -> list[NotificationEntryDC]:
    """Dead-lettered notifications."""
    qs = NotificationEntry.objects.filter(status=NotificationStatus.DEAD.value)
    if channel:
        qs = qs.filter(channel=channel)
    return _to_list(qs.order_by("-created_at")[:limit])


@transaction.atomic
def retry_notification(notification_id: str) -> NotificationEntryDC | None:
    """Reset a failed/dead notification to pending."""
    try:
        notif = NotificationEntry.objects.select_for_update().get(id=notification_id)
    except NotificationEntry.DoesNotExist:
        return None

    status = NotificationStatus(notif.status)
    if not status.can_transition_to(NotificationStatus.PENDING):
        return notif.to_dataclass()

    notif.status = NotificationStatus.PENDING.value
    notif.process_after = None
    notif.error = ""
    notif.attempts = 0
    notif.save(update_fields=["status", "process_after", "error", "attempts", "updated_at"])
    return notif.to_dataclass()


@transaction.atomic
def kill_notification(notification_id: str) -> NotificationEntryDC | None:
    """Force a notification to DEAD."""
    try:
        notif = NotificationEntry.objects.select_for_update().get(id=notification_id)
    except NotificationEntry.DoesNotExist:
        return None

    status = NotificationStatus(notif.status)
    if not status.can_transition_to(NotificationStatus.DEAD):
        return notif.to_dataclass()

    notif.status = NotificationStatus.DEAD.value
    notif.save(update_fields=["status", "updated_at"])
    return notif.to_dataclass()


def purge_sent_notifications(
    older_than_days: int = 30,
    channel: str | None = None,
) -> int:
    """Delete sent notifications older than N days."""
    threshold = datetime.now(UTC) - timedelta(days=older_than_days)
    qs = NotificationEntry.objects.filter(
        status=NotificationStatus.SENT.value,
        sent_at__lt=threshold,
    )
    if channel:
        qs = qs.filter(channel=channel)
    count, _ = qs.delete()
    return count
