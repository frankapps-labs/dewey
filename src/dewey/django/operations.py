"""Explicit diagnostics enrichment and locked single-task redrive for Django."""

from __future__ import annotations

from django.conf import settings
from django.db import connections, transaction

from dewey._worker.ledger import OWNERSHIP_FIELDS
from dewey.dashboard.operations import Diagnostics, FailureCategory, RetryDecision
from dewey.django.models import HistoryState, TaskEntry, TaskEvent, resolve_db_alias


def database_now(using):
    with connections[using].cursor() as cursor:
        cursor.execute("SELECT clock_timestamp()")
        return cursor.fetchone()[0]


def history_start(using):
    if connections[using].vendor != "postgresql":
        return None
    with connections[using].cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid='task_entries'::regclass "
            "AND tgname='dewey_task_event_transition' AND tgenabled IN ('O', 'A'))"
        )
        if not cursor.fetchone()[0]:
            return None
    return (
        HistoryState.objects.using(using)
        .filter(pk="task-events")
        .values_list("started_at", flat=True)
        .first()
    )


def latest_event(task, using):
    return (
        TaskEvent.objects.using(using)
        .filter(task_id=task.pk)
        .order_by("-occurred_at", "-id")
        .first()
    )


def diagnostic_data(event):
    """Validate even persisted data: never forward arbitrary JSON fields."""
    if event is None or not event.diagnostics:
        return None
    try:
        data = dict(event.diagnostics)
        data["category"] = FailureCategory(data.get("category", "unknown"))
        return Diagnostics(**data).validated(
            tuple(getattr(settings, "DEWEY_DASHBOARD_REFERENCE_ORIGINS", ()))
        )
    except (TypeError, ValueError):
        return None


def record_diagnostics(
    event_id: str, attempt: int, diagnostics: Diagnostics, *, using=None
) -> bool:
    """Enrich exactly the named failure event after outcome commit.

    Hosts catch reporting errors separately from execution. No reporter is called
    from Dewey's outcome transaction; event absence leaves local correlation only.
    An old event may be enriched, but never becomes the current attempt's reference.
    """
    alias = resolve_db_alias(using)
    data = diagnostics.validated(tuple(getattr(settings, "DEWEY_DASHBOARD_REFERENCE_ORIGINS", ())))
    return bool(
        TaskEvent.objects.using(alias)
        .filter(pk=event_id, attempt=attempt, kind__in=["failed", "dead"])
        .update(diagnostics=data)
    )


def retry_decision(task, event, now) -> RetryDecision:
    denied = RetryDecision()
    if (
        task.status != "dead"
        or event is None
        or event.kind != "dead"
        or event.attempt != task.attempts
    ):
        return RetryDecision(
            False, "A current DEAD event is required; legacy history may be unavailable."
        )
    if task.execution_token is not None or any(
        getattr(task, name) is not None for name in OWNERSHIP_FIELDS
    ):
        return RetryDecision(False, "Execution ownership must be absent before retry.")
    if task.expires_at is not None and task.expires_at <= now:
        return RetryDecision(False, "The task's absolute expiry has passed.")
    data = diagnostic_data(event)
    if not data or data["category"] != "transient":
        return RetryDecision(False, "Reviewed transient diagnostics are required before retry.")
    policy = getattr(settings, "DEWEY_DASHBOARD_RETRY_POLICY", None)
    if not callable(policy):
        return denied
    try:
        decision = policy(task, event)
        return decision.validated() if isinstance(decision, RetryDecision) else denied
    except Exception:
        return denied


#: Why a retry can be refused: the only text about it an HTTP response may carry.
RETRY_REJECTIONS = {
    "reason_required": "A safe reason of 1–500 characters is required.",
    "actor_required": "Actor identity is required.",
    "history_unavailable": "Transition history is unavailable.",
    "task_not_found": "Task not found.",
    "task_changed": "Task changed; refresh diagnostics before retry.",
    "not_retryable": "This task cannot be retried now; its diagnostics say why.",
}


class RetryRejected(ValueError):
    """A refused retry. ``code`` is a key of ``RETRY_REJECTIONS``."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(detail or RETRY_REJECTIONS[code])
        self.code = code


def retry_task_once(
    task_id: str, *, event_id: str, attempt: int, actor: str, reason: str, using=None
):
    """One extra attempt, unchanged identity/expiry/history, atomic audit.

    Authorization/CSRF/confirmation belong to the host command adapter. Duplicate
    or stale intents fail under the task lock; this never resets attempts.
    """
    if (
        not isinstance(reason, str)
        or not 1 <= len(reason.strip()) <= 500
        or any(ord(c) < 32 for c in reason)
    ):
        raise RetryRejected("reason_required")
    if not isinstance(actor, str) or not 1 <= len(actor) <= 200:
        raise RetryRejected("actor_required")
    alias = resolve_db_alias(using)
    with transaction.atomic(using=alias):
        if history_start(alias) is None:
            raise RetryRejected("history_unavailable")
        task = TaskEntry.objects.using(alias).select_for_update().filter(pk=task_id).first()
        if task is None:
            raise RetryRejected("task_not_found")
        now = database_now(alias)  # Fresh after waiting for the row lock.
        event = (
            TaskEvent.objects.using(alias)
            .select_for_update()
            .filter(task_id=task.pk)
            .order_by("-occurred_at", "-id")
            .first()
        )
        if event is None or event.pk != event_id or task.attempts != attempt:
            raise RetryRejected("task_changed")
        decision = retry_decision(task, event, now)
        if not decision.allowed:
            raise RetryRejected("not_retryable", decision.explanation)
        old_budget = task.max_attempts
        task.max_attempts = task.attempts + 1
        task.status = "pending"
        task.scheduled_for = now
        task.save(
            using=alias, update_fields=["max_attempts", "status", "scheduled_for", "updated_at"]
        )
        audit = TaskEvent.objects.using(alias).create(
            task=task,
            queue=task.queue,
            attempt=task.attempts,
            kind="retry_requested",
            occurred_at=now,
            audit={
                "actor": actor,
                "reason": reason.strip(),
                "source_event_id": event.pk,
                "old_max_attempts": old_budget,
                "new_max_attempts": task.max_attempts,
            },
        )
        return {
            "version": 1,
            "event_id": audit.pk,
            "task_id": task.pk,
            "attempts": task.attempts,
            "max_attempts": task.max_attempts,
        }
