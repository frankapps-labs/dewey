"""Operator recovery: safe diagnostics first, one same-identity attempt, no reset."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from django.contrib.auth.models import Permission
from django.db import close_old_connections, connection, transaction
from django.test import Client
from django.urls import reverse
from django.utils import timezone

from dewey.dashboard.contract import TaskFilter
from dewey.dashboard.operations import Diagnostics, FailureCategory, RetryDecision, safe_link
from dewey.django.dashboard import DjangoDashboardProvider
from dewey.django.models import HistoryState, TaskEntry, TaskEvent
from dewey.django.operations import RetryRejected, record_diagnostics, retry_task_once


@pytest.fixture
def dead_task(db, settings):
    # Transactional tests flush the installation marker; restore only in fixtures.
    HistoryState.objects.get_or_create(id="task-events", defaults={"started_at": timezone.now()})
    settings.DEWEY_DASHBOARD_RETRY_POLICY = lambda task, event: RetryDecision(
        True, "Recovered; durable receipt replay is safe."
    )
    row = TaskEntry.objects.create(
        task_type="test.recover",
        attempts=1,
        max_attempts=1,
        priority=7,
        args=["TEST-PRIVATE"],
        kwargs={"private": "TEST-PRIVATE"},
        idempotency_key="TEST-PRIVATE",
        error="TEST-PRIVATE",
        expires_at=timezone.now() + timedelta(hours=1),
        latest_outcome="handler_failure",
    )
    TaskEntry.objects.filter(pk=row.pk).update(status="dead")
    row.refresh_from_db()
    event = TaskEvent.objects.get(task=row, kind="dead")
    assert record_diagnostics(
        event.pk,
        row.attempts,
        Diagnostics(
            category=FailureCategory.TRANSIENT,
            summary="Reviewed temporary outage",
            business_label="Test business record",
            business_url="/admin/test/batch/1/change/",
        ),
    )
    return row, event


def retry(row, event, **overrides):
    values = {
        "event_id": event.pk,
        "attempt": row.attempts,
        "actor": "test-operator",
        "reason": "Recovered outage; replay-safe receipt reviewed",
    }
    values.update(overrides)
    return retry_task_once(row.pk, **values)


@pytest.mark.django_db
def test_catalog_includes_idle_queues_alphabetically(settings):
    settings.DEWEY_DASHBOARD_QUEUES = ["zebra", "alpha", "alpha"]
    TaskEntry.objects.create(task_type="test.observed", queue="middle", priority=9)
    data = DjangoDashboardProvider().overview(TaskFilter())
    assert [q["queue"] for q in data["queues"]] == ["alpha", "middle", "zebra"]
    assert [q["configured"] for q in data["queues"]] == [True, False, True]
    assert data["queues"][0]["ready"] == data["queues"][2]["ready"] == 0
    assert TaskEntry.objects.get().priority == 9


@pytest.mark.django_db
def test_catalog_and_queue_filtered_graph_are_bounded(settings):
    settings.DEWEY_DASHBOARD_QUEUES = [f"idle-{i:03}" for i in range(55)]
    data = DjangoDashboardProvider().overview(TaskFilter())
    assert len(data["queues"]) == 50 and data["queues_truncated"]
    filtered = DjangoDashboardProvider().overview(TaskFilter(queue="idle-054"))
    assert [q["queue"] for q in filtered["queues"]] == ["idle-054"]


@pytest.mark.django_db
def test_retry_preserves_identity_effect_inputs_expiry_attempts_and_failure(dead_task):
    row, event = dead_task
    preserved = {
        name: getattr(row, name)
        for name in (
            "id",
            "attempts",
            "args",
            "kwargs",
            "priority",
            "queue",
            "expires_at",
            "idempotency_key",
            "error",
            "latest_outcome",
            "created_at",
            "initial_scheduled_for",
        )
    }
    result = retry(row, event)
    row.refresh_from_db()
    assert {name: getattr(row, name) for name in preserved} == preserved
    assert row.status == "pending" and row.max_attempts == 2
    assert row.scheduled_for <= timezone.now()
    assert result["task_id"] == row.pk
    audit = TaskEvent.objects.get(task=row, kind="retry_requested")
    assert audit.audit == {
        "actor": "test-operator",
        "reason": "Recovered outage; replay-safe receipt reviewed",
        "source_event_id": event.pk,
        "old_max_attempts": 1,
        "new_max_attempts": 2,
    }
    assert TaskEvent.objects.filter(task=row).count() == 2
    snapshot = list(TaskEntry.objects.values())
    with pytest.raises(RetryRejected):
        retry(row, event)
    assert list(TaskEntry.objects.values()) == snapshot
    assert TaskEvent.objects.filter(task=row).count() == 2


@pytest.mark.django_db
@pytest.mark.parametrize(
    "change",
    [
        "expiry",
        "execution_token",
        "dispatch_token",
        "worker_id",
        "lease_expires_at",
        "execution_deadline_at",
        "category",
        "policy",
        "policy_error",
        "stale",
        "attempt",
        "reason",
        "no_history",
    ],
)
def test_retry_denials_are_full_noops(dead_task, settings, change):
    row, event = dead_task
    overrides = {}
    if change == "expiry":
        with connection.cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            expired_at = cursor.fetchone()[0] - timedelta(seconds=1)
        TaskEntry.objects.filter(pk=row.pk).update(expires_at=expired_at)
    elif change in ("execution_token", "dispatch_token", "worker_id"):
        TaskEntry.objects.filter(pk=row.pk).update(**{change: "test-ownership"})
    elif change in ("lease_expires_at", "execution_deadline_at"):
        TaskEntry.objects.filter(pk=row.pk).update(**{change: timezone.now()})
    elif change == "category":
        record_diagnostics(
            event.pk, 1, Diagnostics(FailureCategory.BUSINESS, "Reviewed permanent failure")
        )
    elif change == "policy":
        settings.DEWEY_DASHBOARD_RETRY_POLICY = None
    elif change == "policy_error":

        def broken(task, event):
            raise RuntimeError("TEST-PRIVATE")

        settings.DEWEY_DASHBOARD_RETRY_POLICY = broken
    elif change == "stale":
        overrides["event_id"] = "test-stale-event"
    elif change == "attempt":
        overrides["attempt"] = 0
    elif change == "reason":
        overrides["reason"] = ""
    elif change == "no_history":
        HistoryState.objects.all().delete()
    before_tasks = list(TaskEntry.objects.values())
    before_events = list(TaskEvent.objects.values())
    with pytest.raises(RetryRejected):
        retry(row, event, **overrides)
    assert list(TaskEntry.objects.values()) == before_tasks
    assert list(TaskEvent.objects.values()) == before_events


@pytest.mark.django_db
def test_unknown_validation_and_legacy_dead_are_not_retryable(settings):
    settings.DEWEY_DASHBOARD_RETRY_POLICY = lambda *args: RetryDecision(True, "Approved")
    old = TaskEntry.objects.create(task_type="test.legacy", status="dead", attempts=1)
    data = DjangoDashboardProvider().detail(old.pk)["task"]
    assert not data["retry"]["allowed"] and data["diagnostic_event_id"] is None
    TaskEntry.objects.filter(pk=old.pk).update(status="pending")
    TaskEntry.objects.filter(pk=old.pk).update(status="dead")
    event = TaskEvent.objects.get(task=old)
    for category in (FailureCategory.UNKNOWN, FailureCategory.VALIDATION):
        record_diagnostics(event.pk, 1, Diagnostics(category, "Reviewed diagnosis"))
        assert not DjangoDashboardProvider().detail(old.pk)["task"]["retry"]["allowed"]


@pytest.mark.django_db
def test_diagnostics_and_references_are_attempt_scoped_and_private(dead_task, settings):
    row, event = dead_task
    settings.DEWEY_DASHBOARD_REFERENCE_ORIGINS = ("https://sentry.example.invalid",)
    diag = Diagnostics(
        FailureCategory.TRANSIENT,
        "Reviewed outage",
        provider="sentry",
        reference="test-event-1",
        reference_url="https://sentry.example.invalid/issues/1/",
    )
    assert not record_diagnostics(event.pk, 2, diag)
    assert record_diagnostics(event.pk, 1, diag)
    before = DjangoDashboardProvider().detail(row.pk)["task"]
    assert before["diagnostics"]["reference"] == "test-event-1"
    assert "TEST-PRIVATE" not in str(before)
    retry(row, event)
    TaskEntry.objects.filter(pk=row.pk).update(status="processing", attempts=2)
    TaskEntry.objects.filter(pk=row.pk).update(status="dead")
    after = DjangoDashboardProvider().detail(row.pk)["task"]
    assert after["diagnostics"] is None
    assert after["diagnostic_event_id"] != event.pk
    assert any(
        e["diagnostics"] and e["diagnostics"]["reference"] == "test-event-1"
        for e in after["events"]
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://user:secret@sentry.example.invalid/a",
        "https://sentry.example.invalid/a?token=secret",
        "https://sentry.example.invalid/a#secret",
        "https://other.example.invalid/a",
        "javascript:alert(1)",
        "//sentry.example.invalid/a",
        "https://sentry.example.invalid/%3fsecret",
        "https://sentry.example.invalid/\\evil",
        "https://sentry.example.invalid/\nsecret",
    ],
)
def test_reference_links_fail_closed(url):
    with pytest.raises(ValueError):
        safe_link(url, ("https://sentry.example.invalid",))


@pytest.mark.parametrize(
    "url",
    [
        "//evil.example.invalid",
        "https://same.example.invalid/admin/",
        "/admin/?secret=x",
        "/%2fevil",
        "/\\evil",
    ],
)
def test_business_links_must_be_plain_local_paths(url):
    with pytest.raises(ValueError):
        safe_link(url, business=True)


@pytest.mark.django_db
def test_retry_requires_separate_permission_post_confirmation_and_csrf(dead_task, viewer):
    row, event = dead_task
    url = reverse("admin:dewey_taskentry_dashboard_retry", args=[row.pk])
    params = {
        "event_id": event.pk,
        "attempt": 1,
        "reason": "Recovered and reviewed",
        "confirm": "yes",
    }
    assert viewer.get(url).status_code == 405
    assert viewer.post(url, params).status_code == 403
    user = viewer.session["_auth_user_id"]
    from django.contrib.auth import get_user_model

    operator = get_user_model().objects.get(pk=user)
    operator.user_permissions.add(Permission.objects.get(codename="retry_taskentry"))
    secure = Client(enforce_csrf_checks=True)
    secure.force_login(operator)
    before = list(TaskEntry.objects.values())
    assert secure.post(url, params).status_code == 403
    secure.get(reverse("admin:dewey_taskentry_changelist"))
    token = secure.cookies["csrftoken"].value
    assert secure.post(url, {**params, "confirm": "no"}, HTTP_X_CSRFTOKEN=token).status_code == 400
    assert list(TaskEntry.objects.values()) == before
    assert secure.post(url, params, HTTP_X_CSRFTOKEN=token).status_code == 200
    assert secure.post(url, params, HTTP_X_CSRFTOKEN=token).status_code == 409
    assert TaskEvent.objects.filter(task=row, kind="retry_requested").count() == 1


@pytest.mark.django_db(transaction=True)
def test_concurrent_retry_intents_grant_only_one_attempt(dead_task):
    row, event = dead_task

    def submit():
        close_old_connections()
        try:
            try:
                retry(row, event)
                return "accepted"
            except RetryRejected:
                return "rejected"
        finally:
            close_old_connections()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: submit(), range(2)))
    assert sorted(results) == ["accepted", "rejected"]
    row.refresh_from_db()
    assert (row.attempts, row.max_attempts) == (1, 2)
    assert TaskEvent.objects.filter(task=row, kind="retry_requested").count() == 1


@pytest.mark.django_db
def test_history_and_audit_rollback_with_task(dead_task):
    row, event = dead_task
    before = list(TaskEvent.objects.values())
    with pytest.raises(RuntimeError):
        with transaction.atomic():
            retry(row, event)
            raise RuntimeError("Rollback the entire intent")
    row.refresh_from_db()
    assert (row.status, row.max_attempts) == ("dead", 1)
    assert list(TaskEvent.objects.values()) == before
    with pytest.raises(RuntimeError):
        with transaction.atomic():
            TaskEntry.objects.filter(pk=row.pk).update(status="completed")
            raise RuntimeError("Rollback transition")
    assert list(TaskEvent.objects.values()) == before


@pytest.mark.django_db
def test_graph_uses_transition_time_and_preserves_reentered_dead(dead_task):
    row, event = dead_task
    TaskEntry.objects.filter(pk=row.pk).update(created_at=timezone.now() - timedelta(days=8))
    retry(row, event)
    TaskEntry.objects.filter(pk=row.pk).update(status="processing", attempts=2)
    TaskEntry.objects.filter(pk=row.pk).update(status="dead")
    other = TaskEntry.objects.create(task_type="test.other", queue="other")
    TaskEntry.objects.filter(pk=other.pk).update(status="completed")
    TaskEntry.objects.create(task_type="test.legacy", status="dead")  # No invented event.
    provider = DjangoDashboardProvider()
    data = provider.overview(TaskFilter(queue="default"))
    graph = data["throughput"]
    assert sum(b["dead"] or 0 for b in graph["buckets"]) == 2
    assert sum(b["completed"] or 0 for b in graph["buckets"]) == 0
    assert data["summary"]["dead"] == 1  # Only the newly created legacy row.
    assert graph["recorded_from"] and len(graph["buckets"]) <= 25
    assert any(b["coverage"] == "unavailable" and b["dead"] is None for b in graph["buckets"])
    assert any(b["coverage"] == "partial" for b in graph["buckets"])
    for hours, size, cap in ((1, 300, 13), (24, 3600, 25), (168, 21600, 29)):
        graph = provider.overview(TaskFilter(hours=hours))["throughput"]
        assert graph["bucket_seconds"] == size and len(graph["buckets"]) <= cap
    with connection.cursor() as cursor:
        cursor.execute("DELETE FROM task_entries WHERE id=%s", [row.pk])
    assert not TaskEvent.objects.filter(task_id=row.pk).exists()
