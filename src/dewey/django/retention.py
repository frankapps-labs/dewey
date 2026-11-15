"""Audited configuration and one bounded, default-deny completed-task cleanup batch.

No scheduler or HTTP deletion endpoint. Hosts invoke the job outside transactions,
under their own enclosing time/resource bounds. Business references block deletion.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import timedelta

from django.conf import settings
from django.db import connections, transaction
from django.db.models import Exists, OuterRef
from django.utils import timezone

from dewey._worker.ledger import OWNERSHIP_FIELDS
from dewey.dashboard.retention import RetentionConfig, RetentionDecision
from dewey.django.models import (
    TaskEntry,
    TaskEvent,
    TaskRetention,
    TaskRetentionAudit,
    TaskRetentionRun,
    resolve_db_alias,
)
from dewey.django.operations import database_now, history_start

logger = logging.getLogger(__name__)
PREVIEW_LIMIT = 1000
EVENT_LIMIT = 1000
ERRORS = {
    "host_not_approved": "The host has not approved history and deduplication disposal at this retention period.",
    "history_unavailable": "Installed transition history is required before cleanup.",
    "unreviewed_task_references": "Task references outside transition history prevent managed cleanup.",
    "unreviewed_delete_triggers": "Application delete triggers require a separate retention integration.",
    "event_budget_exceeded": "This batch exceeds the event deletion budget; use a smaller host batch size.",
    "unsupported_database": "Managed retention requires PostgreSQL.",
    "incompatible_isolation": "Managed cleanup requires READ COMMITTED isolation.",
    "cleanup_failed": "Cleanup failed or its acknowledgement was uncertain. Reconcile the recorded run ID with host logs.",
}


class RetentionRejected(ValueError):
    pass


class RetentionCleanupError(RuntimeError):
    pass


def _now(alias):
    return database_now(alias) if connections[alias].vendor == "postgresql" else timezone.now()


def _timeouts(alias):
    if connections[alias].vendor == "postgresql":
        with connections[alias].cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '2000ms'")
            cursor.execute("SET LOCAL lock_timeout = '1000ms'")


def _config(row=None):
    return RetentionConfig(row.days, row.enabled, row.revision) if row else RetentionConfig()


def _locked_policy(alias):
    TaskRetention.objects.using(alias).get_or_create(pk="completed")
    return TaskRetention.objects.using(alias).select_for_update().get(pk="completed")


def safety_decision(config, alias):
    callback = getattr(settings, "DEWEY_RETENTION_SAFETY", None)
    if callable(callback):
        try:
            verdict = callback(config, alias)
            if isinstance(verdict, RetentionDecision):
                return verdict.validated()
        except Exception:
            pass
    return RetentionDecision()


def _schema_block(alias):
    if connections[alias].vendor != "postgresql":
        return "unsupported_database"
    if history_start(alias) is None:
        return "history_unavailable"
    with connections[alias].cursor() as cursor:
        cursor.execute(
            "SELECT confrelid='task_entries'::regclass AND conrelid='dewey_task_events'::regclass, confdeltype::text FROM pg_constraint WHERE contype='f' AND confrelid IN ('task_entries'::regclass, 'dewey_task_events'::regclass)"
        )
        refs = cursor.fetchall()
        if refs != [(True, "c")]:
            return "unreviewed_task_references"
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN ('task_entries'::regclass, 'dewey_task_events'::regclass) AND (tgtype::int & 8) != 0)"
        )
        if cursor.fetchone()[0]:
            return "unreviewed_delete_triggers"
    return ""


def _eligible(alias, cutoff):
    # Completion age alone is insufficient: preserve recent creation-dedupe keys,
    # recent terminal/retry events and any unexplained execution ownership.
    recent = TaskEvent.objects.using(alias).filter(task_id=OuterRef("pk"), occurred_at__gte=cutoff)
    return (
        TaskEntry.objects.using(alias)
        .filter(
            status="completed",
            created_at__lt=cutoff,
            completed_at__lt=cutoff,
            execution_token__isnull=True,
            **{f + "__isnull": True for f in OWNERSHIP_FIELDS},
        )
        .annotate(recent_event=Exists(recent))
        .filter(recent_event=False)
    )


def _audit_config(value):
    try:
        return asdict(RetentionConfig(**{k: value[k] for k in ("days", "enabled", "revision")}))
    except (ValueError, TypeError, KeyError):
        return dict.fromkeys(("days", "enabled", "revision"))


def _run_data(run):
    if run is None:
        return None
    status = (
        run.status
        if run.status in ("running", "completed", "disabled", "blocked", "failed")
        else "unknown"
    )
    code = run.error_code if run.error_code in ERRORS else ""
    return {
        "id": run.pk,
        "status": status,
        "revision": run.revision,
        "days": run.days,
        "started_at": run.started_at.isoformat(),
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "cutoff_at": run.cutoff_at.isoformat() if run.cutoff_at else None,
        "deleted_count": run.deleted_count,
        "error_code": code,
        "error": ERRORS.get(code, ""),
    }


def retention_state(*, using=None, preview_days=None):
    """Read-only bounded preview; missing policy reads as default-off, never inserts."""
    alias = resolve_db_alias(using)
    with transaction.atomic(using=alias):
        _timeouts(alias)
        config = _config(TaskRetention.objects.using(alias).filter(pk="completed").first())
        proposed = RetentionConfig(
            config.days if preview_days is None else preview_days, config.enabled, config.revision
        )
        now = _now(alias)
        cutoff = now - timedelta(days=proposed.days)
        candidates = list(
            _eligible(alias, cutoff)
            .order_by("completed_at", "id")
            .values_list("id", flat=True)[: PREVIEW_LIMIT + 1]
        )
        decision = safety_decision(proposed, alias)
        block = _schema_block(alias)
        allowed = decision.allows(proposed) and not block
        audits = list(TaskRetentionAudit.objects.using(alias).order_by("-occurred_at", "-id")[:21])
        run = TaskRetentionRun.objects.using(alias).order_by("-started_at", "-id").first()
    return {
        "version": 1,
        "observed_at": now.isoformat(),
        "config": asdict(config),
        "preview_days": proposed.days,
        "cutoff_at": cutoff.isoformat(),
        "eligible_count": min(len(candidates), PREVIEW_LIMIT),
        "eligible_truncated": len(candidates) > PREVIEW_LIMIT,
        "safety": {
            "allowed": allowed,
            "minimum_days": decision.minimum_days,
            "explanation": ERRORS[block]
            if block
            else decision.explanation
            if proposed.days >= decision.minimum_days
            else f"Host requires at least {decision.minimum_days} days.",
        },
        "last_run": _run_data(run),
        "audits_truncated": len(audits) > 20,
        "audits": [
            {
                "at": a.occurred_at.isoformat(),
                "revision": a.revision,
                "actor": a.actor,
                "reason": a.reason,
                "old_config": _audit_config(a.old_config),
                "new_config": _audit_config(a.new_config),
            }
            for a in audits[:20]
        ],
        "limitations": [
            "Policy covers completed tasks in this database, not the selected observation window or queue.",
            "Deletion removes task creation-deduplication keys and cascades event history. Host safety approval is mandatory.",
            "DEAD tasks and business records are excluded. Unreviewed task references/delete triggers block cleanup.",
            "Dewey does not install a scheduler. Enablement only permits the host's separately scheduled job; disabling prevents the next batch, not an already-running batch.",
            "Running is a start receipt, not a liveness verdict; interrupted jobs need host reconciliation. Preview is bounded and may change before execution.",
        ],
    }


def update_retention(*, days, enabled, revision, actor, reason, using=None):
    """Trusted command adapter supplies authorization/CSRF/confirmation separately."""
    try:
        config = RetentionConfig(days, enabled, revision)
    except ValueError as exc:
        raise RetentionRejected(str(exc)) from exc
    if not isinstance(actor, str) or not 1 <= len(actor) <= 200 or any(ord(c) < 32 for c in actor):
        raise RetentionRejected("A valid actor is required.")
    if (
        not isinstance(reason, str)
        or not 1 <= len(reason.strip()) <= 500
        or any(ord(c) < 32 for c in reason)
    ):
        raise RetentionRejected("A safe audit reason of 1–500 characters is required.")
    alias = resolve_db_alias(using)
    if connections[alias].vendor != "postgresql":
        raise RetentionRejected(ERRORS["unsupported_database"])
    with transaction.atomic(using=alias):
        _timeouts(alias)
        row = _locked_policy(alias)
        if row.revision != revision:
            raise RetentionRejected("Retention configuration changed; refresh before saving.")
        if enabled:
            decision = safety_decision(config, alias)
            if not decision.allows(config):
                raise RetentionRejected(ERRORS["host_not_approved"])
            block = _schema_block(alias)
            if block:
                raise RetentionRejected(ERRORS[block])
        old = _config(row)
        if (days, enabled) == (old.days, old.enabled):
            return {"version": 1, "changed": False, "config": asdict(old)}
        now = _now(alias)
        row.days, row.enabled, row.revision, row.updated_at = days, enabled, revision + 1, now
        row.save(using=alias)
        new = _config(row)
        TaskRetentionAudit.objects.using(alias).create(
            occurred_at=now,
            revision=row.revision,
            actor=actor,
            reason=reason.strip(),
            old_config=asdict(old),
            new_config=asdict(new),
        )
    return {"version": 1, "changed": True, "config": asdict(new)}


def cleanup_completed_batch(*, using=None, batch_size=100):
    """One bounded job, no loop/scheduler. Does not call ORM deletion signals.

    At most1000 task rows and1000 cascaded event rows. Locks policy to serialize
    configuration/job execution, skips locked tasks, and reevaluates eligibility
    with database time. A durable start receipt survives a failed delete transaction.
    Deletion and successful result are atomic; uncertain acknowledgements never
    overwrite a committed completion receipt as failed.
    """
    if type(batch_size) is not int or not 1 <= batch_size <= 1000:
        raise ValueError("Batch size must be an integer between 1 and 1000.")
    alias = resolve_db_alias(using)
    if connections[alias].in_atomic_block:
        raise ValueError("Run retention outside an existing transaction.")
    run = TaskRetentionRun.objects.using(alias).create(started_at=_now(alias))
    try:
        with transaction.atomic(using=alias):
            _timeouts(alias)
            row = _locked_policy(alias)
            config = _config(row)
            now = _now(alias)  # Fresh after policy lock; no sleeping on task locks.
            run.revision, run.days = config.revision, config.days
            run.cutoff_at = now - timedelta(days=config.days)
            run.status = "disabled"
            if config.enabled:
                if connections[alias].vendor == "postgresql":
                    # Keep the reviewed cascade/trigger contract stable against
                    # concurrent DDL while allowing ordinary application writers.
                    with connections[alias].cursor() as cursor:
                        cursor.execute(
                            "LOCK TABLE task_entries, dewey_task_events IN ROW EXCLUSIVE MODE"
                        )
                block = _schema_block(alias)
                if not block and not safety_decision(config, alias).allows(config):
                    block = "host_not_approved"
                if not block:
                    with connections[alias].cursor() as cursor:
                        cursor.execute("SHOW transaction_isolation")
                        if cursor.fetchone()[0] != "read committed":
                            block = "incompatible_isolation"
                if not block:
                    ids = list(
                        _eligible(alias, run.cutoff_at)
                        .order_by("completed_at", "id")
                        .select_for_update(skip_locked=True)
                        .values_list("id", flat=True)[:batch_size]
                    )
                    event_ids = list(
                        TaskEvent.objects.using(alias)
                        .filter(task_id__in=ids)
                        .values_list("id", flat=True)[: EVENT_LIMIT + 1]
                    )
                    if len(event_ids) > EVENT_LIMIT:
                        block = "event_budget_exceeded"
                    elif ids:
                        with connections[alias].cursor() as cursor:
                            cursor.execute(
                                "DELETE FROM task_entries WHERE id IN ("
                                + ",".join(["%s"] * len(ids))
                                + ") RETURNING id",
                                ids,
                            )
                            run.deleted_count = len(cursor.fetchall())
                run.status = "blocked" if block else "completed"
                run.error_code = block
            run.finished_at = _now(alias)
            run.save(using=alias)
    except Exception as exc:
        logger.error(
            "Retention cleanup requires reconciliation run_id=%s error_type=%s",
            run.pk,
            type(exc).__name__,
        )
        try:
            # If COMMIT succeeded but its acknowledgement was lost, leave the
            # already-committed completion/deleted count unchanged.
            TaskRetentionRun.objects.using(alias).filter(pk=run.pk, status="running").update(
                status="failed", finished_at=_now(alias), error_code="cleanup_failed"
            )
        except Exception:
            pass  # Uncertain result remains a start receipt, never guessed rollback.
        raise RetentionCleanupError(f"Reconcile cleanup run {run.pk} with host logs.") from exc
    return {"version": 1, "run": _run_data(run)}
