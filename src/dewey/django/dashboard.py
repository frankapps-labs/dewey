"""Django projections for portable observations and diagnosed operations."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any

from django.conf import settings
from django.db import connections, router
from django.db.models import Count, F, Q
from django.utils import timezone

from dewey.dashboard.contract import TaskFilter, next_cursor
from dewey.dashboard.observations import recovery_observation
from dewey.django.models import TaskEntry, TaskEvent
from dewey.django.operations import diagnostic_data, history_start, latest_event, retry_decision

# Payloads, error text, metadata, effect keys and fencing tokens are deliberately
# absent. Reading a row does not authorize exposing arguments/provider secrets.
FIELDS = (
    "id",
    "task_type",
    "queue",
    "status",
    "priority",
    "attempts",
    "max_attempts",
    "created_at",
    "scheduled_for",
    "started_at",
    "completed_at",
    "worker_id",
    "lease_expires_at",
    "execution_deadline_at",
    "latest_outcome",
)


def stamp(value):
    return value.isoformat() if value is not None else None


def serialize(row) -> dict[str, Any]:
    return {
        field: stamp(getattr(row, field))
        if field.endswith("_at") or field == "scheduled_for"
        else getattr(row, field)
        for field in FIELDS
    }


class DjangoDashboardProvider:
    def __init__(self, using: str | None = None):
        self.using = (
            using
            or getattr(settings, "DEWEY_DASHBOARD_DATABASE_ALIAS", None)
            or router.db_for_read(TaskEntry)
        )

    def base(self):
        return TaskEntry.objects.using(self.using)

    def overview(self, filters: TaskFilter) -> dict[str, Any]:
        now = timezone.now()
        rows = self.base().filter(created_at__gte=now - timedelta(hours=filters.hours))
        due = Q(scheduled_for__isnull=True) | Q(scheduled_for__lte=now)
        unexpired = Q(expires_at__isnull=True) | Q(expires_at__gt=now)
        eligible = Q(unexpired, status__in=["pending", "failed"], attempts__lt=F("max_attempts"))
        ready = Q(eligible, due)
        scheduled = Q(eligible, scheduled_for__gt=now)
        metrics = {
            "ready": Count("pk", filter=ready),
            "scheduled": Count("pk", filter=scheduled),
            "processing": Count("pk", filter=Q(status="processing")),
            "dispatching": Count("pk", filter=Q(status="dispatching")),
            "completed": Count("pk", filter=Q(status="completed")),
            "dead": Count("pk", filter=Q(status="dead")),
        }
        if filters.queue:
            rows = rows.filter(queue=filters.queue)
        queues = list(rows.values("queue").annotate(**metrics).order_by("queue")[:51])
        configured = getattr(settings, "DEWEY_DASHBOARD_QUEUES", ())
        if isinstance(configured, str) or any(
            not isinstance(q, str) or not 1 <= len(q) <= 50 for q in configured
        ):
            raise ValueError("DEWEY_DASHBOARD_QUEUES must contain queue names")
        catalog = set(configured)
        if filters.queue:
            catalog &= {filters.queue}
        observed = {q["queue"]: q for q in queues}
        names = sorted(catalog | observed.keys())
        queues = [
            {
                **observed.get(name, {"queue": name, **dict.fromkeys(metrics, 0)}),
                "configured": name in catalog,
            }
            for name in names[:51]
        ]
        owners = list(
            rows.filter(worker_id__isnull=False, status__in=["processing", "dispatching"])
            .values("worker_id")
            .annotate(
                active=Count("pk"),
                expired_leases=Count("pk", filter=Q(lease_expires_at__lte=now)),
            )
            .order_by("worker_id")[:21]
        )
        return {
            "version": 1,
            "observed_at": stamp(now),
            "hours": filters.hours,
            "summary": rows.aggregate(**metrics),
            "queues": queues[:50],
            "queues_truncated": len(queues) > 50,
            "owners": owners[:20],
            "owners_truncated": len(owners) > 20,
            "throughput": self.throughput(filters, now),
            "limitations": [
                "Counts cover tasks created in the selected window; reads may change during refresh.",
                "Owners are observed active task claims, not a worker registry or health verdict. Idle workers are not visible.",
                "Task completion is not proof of business completion or external delivery.",
                "Configured queues include idle names; task priority, not alphabetical order, controls claiming.",
                "Graphs count recorded terminal transitions, not task creation; purged tasks remove their history.",
            ],
        }

    def throughput(self, filters, now):
        recorded_from = history_start(self.using)
        seconds = {1: 300, 24: 3600, 168: 21600}[filters.hours]
        start = now - timedelta(hours=filters.hours)
        first = math.floor(start.timestamp() / seconds) * seconds
        counts = {}
        if recorded_from is not None:
            queue_sql = " AND queue=%s" if filters.queue else ""
            params = [seconds, start, now]
            if filters.queue:
                params.append(filters.queue)
            with connections[self.using].cursor() as cursor:
                cursor.execute(
                    "SELECT floor(extract(epoch FROM occurred_at)/%s)::bigint, kind, count(*) "
                    "FROM dewey_task_events WHERE occurred_at >= %s AND occurred_at < %s "
                    "AND kind IN ('completed', 'dead')" + queue_sql + " GROUP BY 1, 2 ORDER BY 1",
                    params,
                )
                counts = {
                    (bucket * seconds, kind): count for bucket, kind, count in cursor.fetchall()
                }
        buckets = []
        recording_stamp = recorded_from.timestamp() if recorded_from is not None else math.inf
        for bucket in range(first, math.ceil(now.timestamp()), seconds):
            available = bucket + seconds > recording_stamp
            buckets.append(
                {
                    "at": stamp(datetime.fromtimestamp(bucket, UTC)),
                    "coverage": "unavailable"
                    if not available
                    else "partial"
                    if bucket < max(start.timestamp(), recording_stamp)
                    or bucket + seconds > now.timestamp()
                    else "recorded",
                    "completed": counts.get((bucket, "completed"), 0) if available else None,
                    "dead": counts.get((bucket, "dead"), 0) if available else None,
                }
            )
        return {
            "recorded_from": stamp(recorded_from),
            "bucket_seconds": seconds,
            "buckets": buckets,
        }

    def tasks(self, filters: TaskFilter) -> dict[str, Any]:
        rows = self.base().filter(created_at__gte=timezone.now() - timedelta(hours=filters.hours))
        if filters.queue:
            rows = rows.filter(queue=filters.queue)
        if filters.status:
            rows = rows.filter(status=filters.status)
        if filters.search:
            rows = rows.filter(
                Q(id__istartswith=filters.search) | Q(task_type__istartswith=filters.search)
            )
        if filters.cursor:
            date, row_id = filters.cursor
            rows = rows.filter(Q(created_at__lt=date) | Q(created_at=date, id__lt=row_id))
        page = list(rows.only(*FIELDS).order_by("-created_at", "-id")[: filters.limit + 1])
        items = page[: filters.limit]
        cursor = (
            next_cursor(items[-1].created_at, items[-1].id) if len(page) > filters.limit else None
        )
        return {"version": 1, "items": [serialize(row) for row in items], "next_cursor": cursor}

    def detail(self, task_id: str) -> dict[str, Any] | None:
        if not 1 <= len(task_id) <= 36:
            return None
        row = (
            self.base()
            .filter(pk=task_id)
            .only(*FIELDS, "error", "expires_at", "execution_token", "dispatch_token")
            .first()
        )
        if row is None:
            return None
        now = timezone.now()
        task = serialize(row)
        task["expires_at"] = stamp(row.expires_at)
        task["recovery"] = recovery_observation(row, now)
        task["error_present"] = bool(row.error)
        # No automatic regex 'sanitization': it cannot reliably remove arbitrary
        # personal data/provider secrets. Hosts may explicitly supply a safe summary.
        formatter = getattr(settings, "DEWEY_DASHBOARD_ERROR_SUMMARY", None)
        task["error_summary"] = str(formatter(row))[:500] if callable(formatter) else None
        event = latest_event(row, self.using)
        current = event is not None and event.attempt == row.attempts and event.kind == row.status
        task["diagnostics"] = diagnostic_data(event) if current else None
        task["diagnostic_event_id"] = event.pk if current and event is not None else None
        task["retry"] = {"allowed": False, "explanation": "Transition history is unavailable."}
        if history_start(self.using) is not None:
            decision = retry_decision(row, event, timezone.now())
            task["retry"] = {"allowed": decision.allowed, "explanation": decision.explanation}
        events = list(
            TaskEvent.objects.using(self.using)
            .filter(task_id=row.pk)
            .order_by("-occurred_at", "-id")[:26]
        )
        task["events"] = [
            {
                "id": e.pk,
                "kind": e.kind,
                "attempt": e.attempt,
                "occurred_at": stamp(e.occurred_at),
                "diagnostics": diagnostic_data(e),
                "audit": {
                    key: value
                    for key, value in e.audit.items()
                    if key
                    in (
                        "actor",
                        "reason",
                        "source_event_id",
                        "old_max_attempts",
                        "new_max_attempts",
                    )
                    and isinstance(value, (str, int))
                },
            }
            for e in events[:25]
        ]
        task["events_truncated"] = len(events) > 25
        return {"version": 1, "observed_at": stamp(now), "task": task}
