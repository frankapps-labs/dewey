"""Notification executor, sweep, and queries for SQLAlchemy."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session

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
    NotificationEntry,
    NotificationStatus,
)
from dewey.core.notifications import (
    NotificationAttempt as NotificationAttemptDC,
)
from dewey.sqlalchemy.listen import notify_work_available
from dewey.sqlalchemy.notification_models import (
    NotificationAttemptModel,
    NotificationEntryModel,
)

logger = logging.getLogger(__name__)


# --- Conversion helpers ---


def _to_entry(row: NotificationEntryModel) -> NotificationEntry:
    return NotificationEntry(
        id=row.id,
        task_id=row.task_id,
        event_type=row.event_type,
        channel=row.channel,
        recipient=row.recipient,
        subject=row.subject,
        body=row.body,
        payload=row.payload,
        status=NotificationStatus(row.status),
        attempts=row.attempts,
        max_attempts=row.max_attempts,
        error=row.error,
        created_at=row.created_at,
        updated_at=row.updated_at,
        process_after=row.process_after,
        sent_at=row.sent_at,
        metadata=row.notification_metadata,
    )


def _to_attempt(row: NotificationAttemptModel) -> NotificationAttemptDC:
    return NotificationAttemptDC(
        id=row.id,
        notification_id=row.notification_id,
        attempt_number=row.attempt_number,
        status=row.status,
        error=row.error,
        response_data=row.response_data,
        created_at=row.created_at,
    )


def _to_entry_list(rows: Sequence[NotificationEntryModel]) -> list[NotificationEntry]:
    return [_to_entry(r) for r in rows]


# --- Create ---


def create_notification(
    session: Session,
    *,
    event_type: str,
    channel: str,
    recipient: str,
    subject: str = "",
    body: str = "",
    payload: dict[str, Any] | None = None,
    task_id: str | None = None,
    max_attempts: int = 3,
    process_after: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> NotificationEntry:
    """
    Write a notification to the ledger.

    Returns the created NotificationEntry dataclass.
    """
    notif = NotificationEntryModel(
        event_type=event_type,
        channel=channel,
        recipient=recipient,
        subject=subject,
        body=body,
        payload=payload or {},
        task_id=task_id,
        max_attempts=max_attempts,
        process_after=process_after,
        notification_metadata=metadata or {},
    )
    session.add(notif)
    session.flush()

    notify_work_available(session, kind="notification", entry_id=notif.id, queue=channel)

    logger.info(
        "Notification created id=%s event=%s channel=%s recipient=%s",
        notif.id,
        event_type,
        channel,
        recipient,
    )
    return _to_entry(notif)


def create_notifications_for_event(
    session: Session,
    *,
    registry: ChannelRegistry,
    event_type: str,
    payload: dict[str, Any],
    task_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[NotificationEntry]:
    """
    Create notifications for all channel bindings registered for an event type.

    Uses the registry to resolve recipients and render content for each channel.
    Returns list of created notifications.
    """
    bindings = registry.get_bindings(event_type)
    if not bindings:
        return []

    notifications = []
    for binding in bindings:
        recipient = binding.recipient_resolver(payload)
        subject, body = binding.body_renderer(event_type, payload)

        notif = create_notification(
            session,
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


# --- Send (process a single notification) ---


def send_notification(
    session: Session,
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
    30 min cap). Useful for fast-retry queues or deterministic tests.

    Phase 1: PENDING → SENDING (committed)
    Phase 2: Call channel.send()
    Phase 3: SENDING → SENT/FAILED/DEAD (committed)

    Each attempt is logged to notification_attempts.
    Uses SELECT FOR UPDATE to prevent concurrent sends.

    Returns True if sent successfully.
    """
    now = datetime.now(UTC)

    # Phase 1: Claim
    stmt = (
        select(NotificationEntryModel)
        .where(NotificationEntryModel.id == notification_id)
        .with_for_update()
    )
    notif = session.execute(stmt).scalar_one_or_none()

    if notif is None:
        logger.warning("Notification not found id=%s", notification_id)
        return False

    current = NotificationStatus(notif.status)
    if current.is_terminal:
        logger.info("Notification already terminal id=%s status=%s", notification_id, notif.status)
        return False

    if current != NotificationStatus.PENDING:
        logger.info("Notification not pending id=%s status=%s", notification_id, notif.status)
        return False

    if notif.process_after and notif.process_after > now:
        logger.info(
            "Notification not ready id=%s process_after=%s", notification_id, notif.process_after
        )
        return False

    if not current.can_transition_to(NotificationStatus.SENDING):
        logger.warning("Invalid transition id=%s from=%s to=SENDING", notification_id, notif.status)
        return False

    notif.status = NotificationStatus.SENDING.value
    notif.attempts += 1

    # Cache before commit
    recipient = notif.recipient
    subject = notif.subject
    body = notif.body
    payload = dict(notif.payload)
    attempts = notif.attempts
    max_attempts = notif.max_attempts
    notif_metadata = dict(notif.notification_metadata or {})

    session.commit()

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
        stmt = (
            select(NotificationEntryModel)
            .where(NotificationEntryModel.id == notification_id)
            .with_for_update()
        )
        notif = session.execute(stmt).scalar_one_or_none()

        if notif is None:
            logger.warning("Notification disappeared during send id=%s", notification_id)
            return False

        current = NotificationStatus(notif.status)
        if current != NotificationStatus.SENDING:
            logger.info(
                "Notification status changed during send id=%s status=%s",
                notification_id,
                notif.status,
            )
            session.commit()
            return False

        # Log the attempt
        attempt = NotificationAttemptModel(
            notification_id=notification_id,
            attempt_number=attempts,
            status="sent" if result.success else "failed",
            error=result.error,
            response_data=result.response_data,
        )
        session.add(attempt)

        if result.success:
            notif.status = NotificationStatus.SENT.value
            notif.sent_at = now
            notif.error = ""
            session.commit()
            logger.info(
                "Notification sent id=%s channel=%s recipient=%s",
                notification_id,
                channel.name,
                recipient,
            )
            return True

        # Failed
        notif.error = result.error

        if attempts >= max_attempts:
            notif.status = NotificationStatus.DEAD.value
            logger.error(
                "Notification dead-lettered id=%s channel=%s attempts=%d error=%s",
                notification_id,
                channel.name,
                attempts,
                result.error,
            )
        else:
            notif.status = NotificationStatus.FAILED.value
            notif.process_after = now + (backoff or default_notification_backoff)(attempts)
            logger.warning(
                "Notification failed id=%s channel=%s attempts=%d/%d error=%s",
                notification_id,
                channel.name,
                attempts,
                max_attempts,
                result.error,
            )

        session.commit()
        return False
    finally:
        reset_trace_context(_trace_token)


def process_notification(
    session: Session,
    notification_id: str,
    registry: ChannelRegistry,
    *,
    backoff: BackoffFn | None = None,
) -> bool:
    """
    Process a notification using the registry to find the right channel.

    Convenience wrapper around send_notification that looks up the channel
    from the registry based on the notification's channel field. ``backoff``
    is forwarded to send_notification.
    """
    stmt = select(NotificationEntryModel).where(NotificationEntryModel.id == notification_id)
    notif = session.execute(stmt).scalar_one_or_none()

    if notif is None:
        logger.warning("Notification not found id=%s", notification_id)
        return False

    channel = registry.get_channel(notif.channel)
    if channel is None:
        logger.error(
            "No channel registered for %r (notification id=%s)", notif.channel, notification_id
        )
        return False

    return send_notification(session, notification_id, channel, backoff=backoff)


# --- Sweep ---

DEFAULT_STUCK_THRESHOLD_MINUTES = 5


def sweep_failed_notifications(
    session: Session,
    limit: int = 100,
) -> list[str]:
    """
    Find FAILED notifications ready for retry. Reset to PENDING.
    Returns list of notification IDs.
    """
    now = datetime.now(UTC)

    stmt = (
        select(NotificationEntryModel.id)
        .where(
            NotificationEntryModel.status == NotificationStatus.FAILED.value,
            NotificationEntryModel.process_after <= now,
        )
        .order_by(NotificationEntryModel.process_after)
        .limit(limit)
    )
    ids = list(session.execute(stmt).scalars().all())

    if not ids:
        return []

    session.execute(
        update(NotificationEntryModel)
        .where(NotificationEntryModel.id.in_(ids))
        .values(status=NotificationStatus.PENDING.value)
    )
    session.flush()
    for notification_id in ids:
        notify_work_available(session, kind="notification", entry_id=notification_id)

    logger.info("Sweep re-enqueued %d failed notifications", len(ids))
    return ids


def sweep_stuck_notifications(
    session: Session,
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    limit: int = 100,
) -> list[str]:
    """
    Find notifications stuck in SENDING. Reset to PENDING.
    Returns list of notification IDs.
    """
    threshold = datetime.now(UTC) - timedelta(minutes=stuck_threshold_minutes)

    stmt = (
        select(NotificationEntryModel.id)
        .where(
            NotificationEntryModel.status == NotificationStatus.SENDING.value,
            NotificationEntryModel.updated_at < threshold,
        )
        .order_by(NotificationEntryModel.updated_at)
        .limit(limit)
    )
    ids = list(session.execute(stmt).scalars().all())

    if not ids:
        return []

    session.execute(
        update(NotificationEntryModel)
        .where(NotificationEntryModel.id.in_(ids))
        .values(status=NotificationStatus.PENDING.value)
    )
    session.flush()
    for notification_id in ids:
        notify_work_available(session, kind="notification", entry_id=notification_id)

    logger.warning(
        "Sweep unstuck %d sending notifications (threshold=%dm)", len(ids), stuck_threshold_minutes
    )
    return ids


def sweep_notifications(
    session: Session,
    stuck_threshold_minutes: int = DEFAULT_STUCK_THRESHOLD_MINUTES,
    limit: int = 100,
) -> dict[str, list[str]]:
    """Run both notification sweeps. Returns {'failed': [...], 'stuck': [...]}."""
    return {
        "failed": sweep_failed_notifications(session, limit=limit),
        "stuck": sweep_stuck_notifications(
            session, stuck_threshold_minutes=stuck_threshold_minutes, limit=limit
        ),
    }


# --- Queries ---


def get_notification_stats(session: Session) -> dict[str, int]:
    """Counts by status."""
    stmt = select(NotificationEntryModel.status, func.count()).group_by(
        NotificationEntryModel.status
    )
    results = session.execute(stmt).all()
    stats = {s.value: 0 for s in NotificationStatus}
    for status, count in results:
        stats[status] = count
    return stats


def get_notification(session: Session, notification_id: str) -> NotificationEntry | None:
    """Single notification by ID."""
    stmt = select(NotificationEntryModel).where(NotificationEntryModel.id == notification_id)
    row = session.execute(stmt).scalar_one_or_none()
    return _to_entry(row) if row else None


def get_notification_attempts(
    session: Session, notification_id: str
) -> list[NotificationAttemptDC]:
    """All delivery attempts for a notification, ordered by attempt number."""
    stmt = (
        select(NotificationAttemptModel)
        .where(NotificationAttemptModel.notification_id == notification_id)
        .order_by(NotificationAttemptModel.attempt_number)
    )
    return [_to_attempt(r) for r in session.execute(stmt).scalars().all()]


def get_notifications_for_task(
    session: Session,
    task_id: str,
    limit: int = 50,
) -> list[NotificationEntry]:
    """All notifications linked to a task."""
    stmt = (
        select(NotificationEntryModel)
        .where(NotificationEntryModel.task_id == task_id)
        .order_by(NotificationEntryModel.created_at)
        .limit(limit)
    )
    return _to_entry_list(session.execute(stmt).scalars().all())


def get_pending_notifications(
    session: Session,
    limit: int = 50,
    channel: str | None = None,
) -> list[NotificationEntry]:
    """Pending notifications ready to send (process_after has passed or is NULL)."""
    now = datetime.now(UTC)
    stmt = select(NotificationEntryModel).where(
        NotificationEntryModel.status == NotificationStatus.PENDING.value,
        (
            NotificationEntryModel.process_after.is_(None)
            | (NotificationEntryModel.process_after <= now)
        ),
    )
    if channel:
        stmt = stmt.where(NotificationEntryModel.channel == channel)
    stmt = stmt.order_by(NotificationEntryModel.created_at).limit(limit)
    return _to_entry_list(session.execute(stmt).scalars().all())


def get_failed_notifications(
    session: Session,
    limit: int = 50,
    channel: str | None = None,
) -> list[NotificationEntry]:
    """Failed notifications eligible for retry."""
    stmt = select(NotificationEntryModel).where(
        NotificationEntryModel.status == NotificationStatus.FAILED.value
    )
    if channel:
        stmt = stmt.where(NotificationEntryModel.channel == channel)
    stmt = stmt.order_by(NotificationEntryModel.created_at.desc()).limit(limit)
    return _to_entry_list(session.execute(stmt).scalars().all())


def get_dead_notifications(
    session: Session,
    limit: int = 50,
    channel: str | None = None,
) -> list[NotificationEntry]:
    """Dead-lettered notifications."""
    stmt = select(NotificationEntryModel).where(
        NotificationEntryModel.status == NotificationStatus.DEAD.value
    )
    if channel:
        stmt = stmt.where(NotificationEntryModel.channel == channel)
    stmt = stmt.order_by(NotificationEntryModel.created_at.desc()).limit(limit)
    return _to_entry_list(session.execute(stmt).scalars().all())


def retry_notification(session: Session, notification_id: str) -> NotificationEntry | None:
    """Reset a failed/dead notification to pending."""
    stmt = (
        select(NotificationEntryModel)
        .where(NotificationEntryModel.id == notification_id)
        .with_for_update()
    )
    notif = session.execute(stmt).scalar_one_or_none()
    if notif is None:
        return None

    status = NotificationStatus(notif.status)
    if not status.can_transition_to(NotificationStatus.PENDING):
        return _to_entry(notif)

    notif.status = NotificationStatus.PENDING.value
    notif.process_after = None
    notif.error = ""
    notif.attempts = 0
    session.flush()
    notify_work_available(session, kind="notification", entry_id=notif.id, queue=notif.channel)
    return _to_entry(notif)


def kill_notification(session: Session, notification_id: str) -> NotificationEntry | None:
    """Force a notification to DEAD."""
    stmt = (
        select(NotificationEntryModel)
        .where(NotificationEntryModel.id == notification_id)
        .with_for_update()
    )
    notif = session.execute(stmt).scalar_one_or_none()
    if notif is None:
        return None

    status = NotificationStatus(notif.status)
    if not status.can_transition_to(NotificationStatus.DEAD):
        return _to_entry(notif)

    notif.status = NotificationStatus.DEAD.value
    session.flush()
    return _to_entry(notif)


def purge_sent_notifications(
    session: Session,
    older_than_days: int = 30,
    channel: str | None = None,
) -> int:
    """Delete sent notifications older than N days."""
    threshold = datetime.now(UTC) - timedelta(days=older_than_days)
    stmt = delete(NotificationEntryModel).where(
        NotificationEntryModel.status == NotificationStatus.SENT.value,
        NotificationEntryModel.sent_at < threshold,
    )
    if channel:
        stmt = stmt.where(NotificationEntryModel.channel == channel)
    result = session.execute(stmt)
    session.flush()
    return result.rowcount  # type: ignore[return-value]
