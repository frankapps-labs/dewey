from datetime import timedelta
from uuid import uuid4

import pytest
from django.contrib.auth import get_user_model
from django.urls import reverse
from django.utils import timezone

from dewey.dashboard import asset
from dewey.dashboard.contract import TaskFilter, next_cursor
from dewey.django.dashboard import DjangoDashboardProvider
from dewey.django.models import TaskEntry


def api(section):
    return reverse("admin:dewey_taskentry_dashboard_api", args=[section])


@pytest.mark.parametrize(
    "params",
    [
        {"hours": 0},
        {"hours": 999999},
        {"limit": 0},
        {"limit": 101},
        {"status": "invented"},
        {"queue": "x" * 51},
        {"search": "x" * 101},
        {"cursor": "not-json"},
        {"cursor": "x" * 257},
    ],
)
def test_invalid_filters(params):
    with pytest.raises(ValueError, match="Invalid dashboard filters"):
        TaskFilter.parse(params)


def test_cursor_round_trip():
    now = timezone.now()
    row_id = str(uuid4())
    assert TaskFilter.parse({"cursor": next_cursor(now, row_id)}).cursor == (now, row_id)


@pytest.mark.parametrize("name", ["dashboard.js", "dashboard.css", "chart.js", "chart.LICENSE.txt"])
def test_packaged_assets(name):
    body, mime = asset(name)
    assert body
    assert mime in ("text/javascript", "text/css", "text/plain")


@pytest.mark.parametrize("name", ["../contract.py", "assets/dashboard.js", "dashboard.html"])
def test_asset_allowlist(name):
    with pytest.raises(ValueError):
        asset(name)


@pytest.mark.django_db
def test_admin_requires_staff_and_view_permission(client):
    url = api("overview")
    assert client.get(url).status_code == 302
    user = get_user_model().objects.create_user(
        username="test-staff-without-permission", is_staff=True
    )
    client.force_login(user)
    assert client.get(url).status_code == 403
    user.is_staff = False
    user.save()
    assert client.get(url).status_code == 302


@pytest.mark.django_db
def test_admin_page_and_assets_are_packaged(viewer):
    response = viewer.get(reverse("admin:dewey_taskentry_changelist"))
    assert response.status_code == 200
    assert b"data-dewey-dashboard" in response.content
    assert b'data-theme="host"' in response.content
    # The portable widget owns the single page heading; admin must not duplicate it.
    assert b"<h1>Dewey</h1>" not in response.content
    for name in ("dashboard.js", "dashboard.css", "chart.js", "chart.LICENSE.txt"):
        response = viewer.get(reverse("admin:dewey_taskentry_dashboard_asset", args=[name]))
        assert response.status_code == 200
        assert b"private" in response.headers["Cache-Control"].encode()


@pytest.mark.django_db
def test_post_and_crud_are_denied_and_ledger_unchanged(viewer):
    row = TaskEntry.objects.create(task_type="test.batch", kwargs={"secret": "TEST-SECRET"})
    before = list(TaskEntry.objects.values())
    for url in (
        api("overview"),
        api("tasks"),
        reverse("admin:dewey_taskentry_dashboard_detail", args=[row.pk]),
    ):
        assert viewer.post(url).status_code == 405
    for suffix in ("add/", f"{row.pk}/change/", f"{row.pk}/delete/"):
        assert viewer.get(reverse("admin:dewey_taskentry_changelist") + suffix).status_code == 404
    assert list(TaskEntry.objects.values()) == before


@pytest.mark.django_db
def test_due_counts_expiry_and_owner_observations(viewer):
    now = timezone.now()
    owner = str(uuid4())
    TaskEntry.objects.create(task_type="test.ready", queue="reports")
    TaskEntry.objects.create(
        task_type="test.future", queue="reports", scheduled_for=now + timedelta(hours=1)
    )
    TaskEntry.objects.create(task_type="test.expired", expires_at=now)
    TaskEntry.objects.create(
        task_type="test.exhausted", status="failed", attempts=5, max_attempts=5
    )
    TaskEntry.objects.create(
        task_type="test.processing",
        status="processing",
        worker_id=owner,
        lease_expires_at=now - timedelta(seconds=1),
    )
    TaskEntry.objects.create(task_type="test.dead", status="dead")
    TaskEntry.objects.create(task_type="test.old", created_at=now - timedelta(days=8))
    before = list(TaskEntry.objects.values())
    data = viewer.get(api("overview")).json()
    assert data["version"] == 1
    assert data["summary"] == {
        "ready": 1,
        "scheduled": 1,
        "processing": 1,
        "dispatching": 0,
        "completed": 0,
        "dead": 1,
    }
    assert data["owners"] == [{"worker_id": owner, "active": 1, "expired_leases": 1}]
    assert "not a worker registry" in data["limitations"][1]
    assert list(TaskEntry.objects.values()) == before


@pytest.mark.django_db
def test_task_filters_and_keyset_pagination(viewer):
    now = timezone.now()
    rows = [
        TaskEntry.objects.create(
            task_type="test.batch", queue="reports", created_at=now, status="dead"
        )
        for _ in range(3)
    ]
    TaskEntry.objects.create(task_type="test.other", queue="other", status="pending")
    first = viewer.get(
        api("tasks"), {"queue": "reports", "status": "dead", "search": "test.b", "limit": 2}
    ).json()
    second = viewer.get(
        api("tasks"),
        {
            "queue": "reports",
            "status": "dead",
            "search": "test.b",
            "limit": 2,
            "cursor": first["next_cursor"],
        },
    ).json()
    ids = [item["id"] for item in first["items"] + second["items"]]
    assert ids == sorted([row.pk for row in rows], reverse=True)
    assert len(set(ids)) == 3
    assert second["next_cursor"] is None
    assert viewer.get(api("tasks"), {"status": "DEAD"}).status_code == 400
    assert viewer.get(api("missing")).status_code == 404


@pytest.mark.django_db
def test_detail_hides_payloads_error_secrets_and_effect_keys(viewer, settings):
    row = TaskEntry.objects.create(
        task_type="test.failure",
        status="dead",
        args=["TEST-PRIVATE"],
        kwargs={"token": "TEST-PRIVATE"},
        metadata={"private": "TEST-PRIVATE"},
        error="TEST-PRIVATE",
        idempotency_key="TEST-PRIVATE",
        execution_token="TEST-PRIVATE",
    )
    url = reverse("admin:dewey_taskentry_dashboard_detail", args=[row.pk])
    response = viewer.get(url)
    assert b"TEST-PRIVATE" not in response.content
    assert response.json()["task"]["error_present"] is True
    assert response.json()["task"]["error_summary"] is None
    assert response.json()["observed_at"]
    assert response.json()["task"]["recovery"]["title"] == "Needs attention"
    assert response.json()["task"]["expires_at"] is None
    assert (
        not {
            "args",
            "kwargs",
            "metadata",
            "error",
            "idempotency_key",
            "execution_token",
            "dispatch_token",
        }
        & response.json()["task"].keys()
    )
    settings.DEWEY_DASHBOARD_ERROR_SUMMARY = lambda task: "Reviewed safe summary"
    assert viewer.get(url).json()["task"]["error_summary"] == "Reviewed safe summary"
    assert (
        viewer.get(reverse("admin:dewey_taskentry_dashboard_detail", args=[uuid4()])).status_code
        == 404
    )


@pytest.mark.django_db
def test_summary_lists_are_bounded():
    for i in range(55):
        TaskEntry.objects.create(
            task_type="test.owner",
            queue=f"queue-{i:03}",
            worker_id=str(uuid4()),
            status="processing",
        )
    data = DjangoDashboardProvider().overview(TaskFilter())
    assert len(data["queues"]) == 50 and data["queues_truncated"]
    assert len(data["owners"]) == 20 and data["owners_truncated"]
