"""Database-scoped retention: permissioned configuration, host-approved bounded jobs."""

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from io import StringIO
from threading import Event

import pytest
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.core.management import call_command
from django.db import close_old_connections, connection, transaction
from django.test import Client
from django.urls import reverse
from django.utils import timezone

from dewey.dashboard.retention import RetentionDecision
from dewey.django.models import (
    HistoryState,
    TaskEntry,
    TaskEvent,
    TaskRetention,
    TaskRetentionAudit,
    TaskRetentionRun,
)
from dewey.django.retention import (
    EVENT_LIMIT,
    RetentionCleanupError,
    RetentionRejected,
    cleanup_completed_batch,
    retention_state,
    update_retention,
)

pytestmark = pytest.mark.django_db(transaction=True)


def endpoint():
    return reverse("admin:dewey_taskentry_dashboard_api", args=["retention"])


def configure_url():
    return reverse("admin:dewey_taskentry_dashboard_retention_configure")


@pytest.fixture
def retention_host(settings):
    HistoryState.objects.get_or_create(pk="task-events", defaults={"started_at": timezone.now()})
    settings.DEWEY_RETENTION_SAFETY = lambda config, using: RetentionDecision(
        True, True, 1, "Synthetic test host: independent receipts and history retained."
    )


@pytest.fixture
def completed_task(retention_host):
    def create(days=31, **changes):
        # Explicit historical synthetic fixture, never a production backfill.
        then = timezone.now() - timedelta(days=days)
        row = TaskEntry.objects.create(
            task_type="test.retention",
            created_at=then,
            args=["TEST-PRIVATE"],
            idempotency_key="test-retention-" + str(TaskEntry.objects.count()),
            **changes,
        )
        TaskEntry.objects.filter(pk=row.pk).update(status="completed", completed_at=then)
        event = TaskEvent.objects.get(task=row)
        TaskEvent.objects.filter(pk=event.pk).update(occurred_at=then)
        row.refresh_from_db()
        return row

    return create


def save_config(**changes):
    options = {
        "days": 30,
        "enabled": True,
        "revision": 0,
        "actor": "test-operator",
        "reason": "Reviewed test host retention",
    }
    options.update(changes)
    return update_retention(**options)


def snapshot_tasks():
    return list(TaskEntry.objects.order_by("pk").values()), list(
        TaskEvent.objects.order_by("pk").values()
    )


def in_thread(fn):
    close_old_connections()
    try:
        return fn()
    finally:
        close_old_connections()


def test_default_preview_is_read_only_global_and_default_off(
    viewer, retention_host, completed_task
):
    completed_task()
    before = snapshot_tasks()
    assert not TaskRetention.objects.exists()
    data = viewer.get(endpoint(), {"hours": 1, "queue": "other", "status": "dead"}).json()
    assert data["config"] == {"days": 30, "enabled": False, "revision": 0}
    assert data["eligible_count"] == 1 and data["can_manage"] is False and data["last_run"] is None
    assert not TaskRetention.objects.exists() and not TaskRetentionRun.objects.exists()
    assert not TaskRetentionAudit.objects.exists() and snapshot_tasks() == before
    assert "TEST-PRIVATE" not in json.dumps(data)


def test_disabled_job_records_no_deletion(completed_task):
    completed_task()
    before = snapshot_tasks()
    data = cleanup_completed_batch()
    assert data["run"]["status"] == "disabled" and data["run"]["deleted_count"] == 0
    assert snapshot_tasks() == before
    assert TaskRetentionRun.objects.count() == 1 and not TaskRetentionAudit.objects.exists()


@pytest.mark.parametrize(
    "decision",
    [
        None,
        "wrong",
        RetentionDecision(True, False),
        RetentionDecision(False, True),
        RetentionDecision(True, True, 60, "Requires60days"),
        RetentionDecision(1, True),
        RetentionDecision(True, True, 0),
        RetentionDecision(True, True, 30, "TEST\nPRIVATE"),
    ],
)
def test_host_approval_is_required_for_enablement_and_each_run(completed_task, settings, decision):
    completed_task()
    row = TaskRetention.objects.create(enabled=False)
    before = snapshot_tasks()
    settings.DEWEY_RETENTION_SAFETY = lambda *args: decision
    with pytest.raises(RetentionRejected):
        save_config()
    row.refresh_from_db()
    assert (row.enabled, row.revision) == (False, 0)
    assert not TaskRetentionAudit.objects.exists() and snapshot_tasks() == before
    # Simulate an old approved setting whose current host approval is withdrawn.
    TaskRetention.objects.filter(pk=row.pk).update(enabled=True)
    data = cleanup_completed_batch()
    assert data["run"]["status"] == "blocked" and data["run"]["deleted_count"] == 0
    assert snapshot_tasks() == before


def test_broken_host_callback_denies_without_leaking_error(completed_task, settings, viewer):
    completed_task()

    def broken(*args):
        raise RuntimeError("TEST-PRIVATE-CREDENTIAL")

    settings.DEWEY_RETENTION_SAFETY = broken
    with pytest.raises(RetentionRejected):
        save_config()
    assert b"TEST-PRIVATE" not in viewer.get(endpoint()).content


def test_setting_changes_are_audited_stale_replays_and_noops_do_not_mutate(completed_task):
    completed_task()
    before = snapshot_tasks()
    first = save_config(days=45, enabled=False)
    assert first["changed"] and first["config"]["revision"] == 1
    audit = TaskRetentionAudit.objects.get()
    assert audit.old_config == {"days": 30, "enabled": False, "revision": 0}
    assert audit.new_config == {"days": 45, "enabled": False, "revision": 1}
    assert audit.actor == "test-operator" and audit.reason == "Reviewed test host retention"
    policy = list(TaskRetention.objects.values())
    no_op = save_config(days=45, enabled=False, revision=1)
    assert no_op["changed"] is False and list(TaskRetention.objects.values()) == policy
    with pytest.raises(RetentionRejected):
        save_config(days=60, enabled=False)
    assert (
        list(TaskRetention.objects.values()) == policy and TaskRetentionAudit.objects.count() == 1
    )
    assert snapshot_tasks() == before and not TaskRetentionRun.objects.exists()


@pytest.mark.parametrize(
    "change",
    [
        {"days": 0},
        {"days": 3651},
        {"days": True},
        {"enabled": 1},
        {"revision": -1},
        {"actor": ""},
        {"reason": ""},
        {"reason": "TEST\nPRIVATE"},
    ],
)
def test_invalid_settings_are_noops(retention_host, change):
    before = list(TaskRetention.objects.values())
    with pytest.raises(RetentionRejected):
        save_config(**change)
    assert (
        list(TaskRetention.objects.values()) == before and not TaskRetentionAudit.objects.exists()
    )


def test_retention_permission_csrf_post_and_confirmation_are_distinct(viewer, retention_host):
    body = {
        "days": 45,
        "enabled": "no",
        "revision": 0,
        "confirm": "yes",
        "reason": "Keep test completion evidence45days",
    }
    assert viewer.get(configure_url()).status_code == 405
    assert viewer.post(endpoint()).status_code == 405
    assert viewer.post(configure_url(), body).status_code == 403
    user = get_user_model().objects.get(pk=viewer.session["_auth_user_id"])
    user.user_permissions.add(Permission.objects.get(codename="retry_taskentry"))
    assert viewer.post(configure_url(), body).status_code == 403
    user.user_permissions.add(Permission.objects.get(codename="manage_task_retention"))
    secure = Client(enforce_csrf_checks=True)
    secure.force_login(user)
    assert secure.post(configure_url(), body).status_code == 403
    secure.get(reverse("admin:dewey_taskentry_changelist"))
    token = secure.cookies["csrftoken"].value
    assert (
        secure.post(configure_url(), {**body, "confirm": "no"}, HTTP_X_CSRFTOKEN=token).status_code
        == 400
    )
    assert (
        secure.post(configure_url(), {**body, "days": "bad"}, HTTP_X_CSRFTOKEN=token).status_code
        == 400
    )
    assert not TaskRetentionAudit.objects.exists()
    assert secure.post(configure_url(), body, HTTP_X_CSRFTOKEN=token).status_code == 200
    assert secure.post(configure_url(), body, HTTP_X_CSRFTOKEN=token).status_code == 409
    assert TaskRetentionAudit.objects.get().actor == str(user.pk)
    assert secure.get(endpoint()).json()["can_manage"] is True
    assert not TaskRetentionRun.objects.exists()


def test_non_viewing_staff_cannot_configure_retention(client, retention_host):
    user = get_user_model().objects.create_user(username="test-retention-no-view", is_staff=True)
    user.user_permissions.add(Permission.objects.get(codename="manage_task_retention"))
    client.force_login(user)
    assert client.get(endpoint()).status_code == 403
    assert client.post(configure_url(), {"confirm": "yes"}).status_code == 403


def test_one_batch_deletes_only_old_unowned_completed_tasks_and_their_events(completed_task):
    eligible = [completed_task() for _ in range(3)]
    recent = completed_task(days=1)
    dead = TaskEntry.objects.create(
        task_type="test.dead", created_at=timezone.now() - timedelta(days=40)
    )
    TaskEntry.objects.filter(pk=dead.pk).update(status="dead")
    active = TaskEntry.objects.create(task_type="test.active", status="processing")
    save_config()
    before_other = list(
        TaskEntry.objects.filter(pk__in=[recent.pk, dead.pk, active.pk]).order_by("pk").values()
    )
    result = cleanup_completed_batch(batch_size=2)["run"]
    assert result["status"] == "completed" and result["deleted_count"] == 2
    remaining = [row for row in eligible if TaskEntry.objects.filter(pk=row.pk).exists()]
    assert len(remaining) == 1
    assert TaskEvent.objects.filter(task=remaining[0]).get().task == remaining[0]
    for row in eligible:
        if row not in remaining:
            assert not TaskEvent.objects.filter(task_id=row.pk).exists()
    assert (
        list(
            TaskEntry.objects.filter(pk__in=[recent.pk, dead.pk, active.pk]).order_by("pk").values()
        )
        == before_other
    )
    assert TaskRetentionAudit.objects.count() == 1  # Audit survives deletion.
    assert cleanup_completed_batch(batch_size=2)["run"]["deleted_count"] == 1
    assert cleanup_completed_batch(batch_size=2)["run"]["deleted_count"] == 0


@pytest.mark.parametrize(
    "field",
    [
        "execution_token",
        "dispatch_token",
        "worker_id",
        "lease_expires_at",
        "execution_deadline_at",
        "recent_event",
        "recent_creation",
        "null_completion",
    ],
)
def test_unexplained_ownership_and_recent_or_unknown_evidence_is_preserved(completed_task, field):
    row = completed_task()
    if field in ("execution_token", "dispatch_token", "worker_id"):
        TaskEntry.objects.filter(pk=row.pk).update(**{field: "test-owned"})
    elif field in ("lease_expires_at", "execution_deadline_at"):
        TaskEntry.objects.filter(pk=row.pk).update(**{field: timezone.now()})
    elif field == "recent_event":
        TaskEvent.objects.filter(task=row).update(occurred_at=timezone.now())
    elif field == "recent_creation":
        TaskEntry.objects.filter(pk=row.pk).update(created_at=timezone.now())
    else:
        TaskEntry.objects.filter(pk=row.pk).update(completed_at=None)
    save_config()
    before = snapshot_tasks()
    assert cleanup_completed_batch()["run"]["deleted_count"] == 0
    assert snapshot_tasks() == before


def test_missing_transition_marker_blocks_cleanup(completed_task):
    completed_task()
    save_config()
    HistoryState.objects.all().delete()
    before = snapshot_tasks()
    result = cleanup_completed_batch()["run"]
    assert result["status"] == "blocked" and result["error_code"] == "history_unavailable"
    assert snapshot_tasks() == before


@pytest.mark.parametrize("parent_table", ["task_entries", "dewey_task_events"])
def test_unknown_business_cascade_is_not_followed(completed_task, parent_table):
    row = completed_task()
    key = row.pk if parent_table == "task_entries" else TaskEvent.objects.get(task=row).pk
    save_config()
    with connection.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE test_retention_business (id int PRIMARY KEY, task_id varchar(36) REFERENCES {parent_table}(id) ON DELETE CASCADE)"
        )
        cursor.execute("INSERT INTO test_retention_business VALUES (1,%s)", [key])
    try:
        before = snapshot_tasks()
        result = cleanup_completed_batch()["run"]
        assert (
            result["status"] == "blocked" and result["error_code"] == "unreviewed_task_references"
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT task_id FROM test_retention_business")
            assert cursor.fetchall() == [(key,)]
        assert snapshot_tasks() == before
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE test_retention_business")


def test_application_delete_trigger_blocks_managed_cleanup(completed_task):
    completed_task()
    save_config()
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE FUNCTION test_retention_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'TEST-PRIVATE'; END $$"
        )
        cursor.execute(
            "CREATE TRIGGER test_retention_delete BEFORE DELETE ON task_entries FOR EACH ROW EXECUTE FUNCTION test_retention_delete()"
        )
    try:
        before = snapshot_tasks()
        result = cleanup_completed_batch()["run"]
        assert (
            result["status"] == "blocked" and result["error_code"] == "unreviewed_delete_triggers"
        )
        assert snapshot_tasks() == before
    finally:
        with connection.cursor() as cursor:
            cursor.execute("DROP TRIGGER test_retention_delete ON task_entries")
            cursor.execute("DROP FUNCTION test_retention_delete()")


def test_event_budget_is_independent_of_task_batch_bound(completed_task):
    row = completed_task()
    then = timezone.now() - timedelta(days=31)
    TaskEvent.objects.bulk_create(
        [
            TaskEvent(task=row, queue=row.queue, attempt=1, kind="failed", occurred_at=then)
            for _ in range(EVENT_LIMIT)
        ]
    )
    save_config()
    before = snapshot_tasks()
    result = cleanup_completed_batch(batch_size=1)["run"]
    assert result["status"] == "blocked" and result["error_code"] == "event_budget_exceeded"
    assert snapshot_tasks() == before


def test_failed_delete_transaction_rolls_back_and_records_safe_error(
    completed_task, monkeypatch, viewer
):
    completed_task()
    save_config()
    before = snapshot_tasks()
    original = TaskRetentionRun.save

    def fail_after_delete(self, *args, **kwargs):
        if self.status == "completed":
            raise RuntimeError("TEST-PRIVATE-CREDENTIAL")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(TaskRetentionRun, "save", fail_after_delete)
    with pytest.raises(RetentionCleanupError, match="Reconcile cleanup run"):
        cleanup_completed_batch()
    assert snapshot_tasks() == before
    run = TaskRetentionRun.objects.get()
    assert run.status == "failed" and run.deleted_count == 0 and run.error_code == "cleanup_failed"
    assert b"TEST-PRIVATE" not in viewer.get(endpoint()).content


def test_exception_after_commit_does_not_overwrite_completion_receipt(completed_task, monkeypatch):
    """Application-level ACK simulation, not a wire-fault qualification."""
    row = completed_task()
    save_config()
    original_atomic = transaction.atomic

    @contextmanager
    def lost_ack(*args, **kwargs):
        with original_atomic(*args, **kwargs):
            yield
        raise RuntimeError("TEST-LOST-ACK")

    monkeypatch.setattr(transaction, "atomic", lost_ack)
    with pytest.raises(RetentionCleanupError):
        cleanup_completed_batch()
    run = TaskRetentionRun.objects.get()
    assert run.status == "completed" and run.deleted_count == 1 and run.error_code == ""
    assert not TaskEntry.objects.filter(pk=row.pk).exists()
    assert not TaskEvent.objects.filter(task_id=row.pk).exists()


def test_locked_tasks_are_skipped_not_waited_on(completed_task):
    locked = completed_task()
    available = completed_task()
    save_config()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with transaction.atomic():
            TaskEntry.objects.select_for_update().get(pk=locked.pk)
            result = pool.submit(in_thread, cleanup_completed_batch).result(timeout=3)["run"]
        assert result["deleted_count"] == 1
    assert TaskEntry.objects.filter(pk=locked.pk).exists()
    assert not TaskEntry.objects.filter(pk=available.pk).exists()


def test_concurrent_configuration_intents_accept_only_one_revision(retention_host):
    TaskRetention.objects.create()

    def submit(days):
        try:
            save_config(days=days, enabled=False)
            return "accepted"
        except RetentionRejected:
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(in_thread, lambda days=days: submit(days)) for days in (40, 45)]
        assert sorted(f.result(timeout=4) for f in futures) == ["accepted", "rejected"]
    assert TaskRetention.objects.get().revision == 1 and TaskRetentionAudit.objects.count() == 1


def test_disabling_waits_for_current_batch_then_prevents_next(completed_task, monkeypatch):
    from dewey.django import retention

    completed_task()
    save_config()
    entered, release = Event(), Event()
    original = retention._schema_block

    def gated(alias):
        entered.set()
        assert release.wait(timeout=3)
        return original(alias)

    monkeypatch.setattr(retention, "_schema_block", gated)
    with ThreadPoolExecutor(max_workers=2) as pool:
        job = pool.submit(in_thread, cleanup_completed_batch)
        assert entered.wait(timeout=3)
        disable = pool.submit(in_thread, lambda: save_config(enabled=False, revision=1))
        assert not disable.done()
        release.set()
        assert job.result(timeout=4)["run"]["deleted_count"] == 1
        assert disable.result(timeout=4)["config"]["enabled"] is False
    assert cleanup_completed_batch()["run"]["status"] == "disabled"


@pytest.mark.parametrize("size", [0, 1001, True, -1])
def test_invalid_batch_bounds_do_not_create_run(size, retention_host):
    with pytest.raises(ValueError):
        cleanup_completed_batch(batch_size=size)
    assert not TaskRetentionRun.objects.exists()


def test_job_requires_own_transaction(retention_host):
    with transaction.atomic(), pytest.raises(ValueError, match="outside an existing transaction"):
        cleanup_completed_batch()
    assert not TaskRetentionRun.objects.exists()


def test_background_command_is_one_batch_not_a_scheduler(completed_task):
    completed_task()
    completed_task()
    save_config()
    output = StringIO()
    call_command("dewey_cleanup", database="default", batch_size=1, stdout=output)
    result = json.loads(output.getvalue())
    assert result["run"]["deleted_count"] == 1 and TaskEntry.objects.count() == 1
    assert TaskRetentionRun.objects.count() == 1


def test_preview_and_audit_outputs_are_bounded(completed_task):
    # Creation is already terminal, deliberately no invented transition history.
    then = timezone.now() - timedelta(days=31)
    TaskEntry.objects.bulk_create(
        [
            TaskEntry(
                task_type="test.legacy", status="completed", created_at=then, completed_at=then
            )
            for _ in range(1001)
        ]
    )
    for revision in range(22):
        save_config(days=31 + revision, enabled=False, revision=revision)
    # Use a one-day disabled preview, so all synthetic31-day rows remain eligible.
    save_config(days=1, enabled=False, revision=22)
    data = retention_state()
    assert data["eligible_count"] == 1000 and data["eligible_truncated"]
    assert len(data["audits"]) == 20 and data["audits_truncated"]


def test_selected_days_preview_does_not_save_or_delete(viewer, completed_task):
    completed_task()
    before = snapshot_tasks()
    data = viewer.get(endpoint(), {"days": 60}).json()
    assert data["config"] == {"days": 30, "enabled": False, "revision": 0}
    assert data["preview_days"] == 60 and data["eligible_count"] == 0
    assert snapshot_tasks() == before
    assert not TaskRetention.objects.exists() and not TaskRetentionAudit.objects.exists()
    assert not TaskRetentionRun.objects.exists()
    for days in ("bad", "0", "3651"):
        assert viewer.get(endpoint(), {"days": days}).status_code == 400


def test_corrupt_audit_config_does_not_forward_arbitrary_json(viewer, retention_host):
    save_config(enabled=False, days=45)
    TaskRetentionAudit.objects.update(
        old_config={"days": "TEST-PRIVATE", "private": "TEST-PRIVATE"}
    )
    data = viewer.get(endpoint())
    assert b"TEST-PRIVATE" not in data.content
    assert data.json()["audits"][0]["old_config"] == {
        "days": None,
        "enabled": None,
        "revision": None,
    }


def test_exact_cutoff_is_preserved(completed_task, monkeypatch):
    from dewey.django import retention

    row = completed_task()
    now = timezone.now()
    TaskEntry.objects.filter(pk=row.pk).update(completed_at=now - timedelta(days=30))
    monkeypatch.setattr(retention, "database_now", lambda using: now)
    save_config()
    before = snapshot_tasks()
    assert cleanup_completed_batch()["run"]["deleted_count"] == 0
    assert snapshot_tasks() == before


def test_job_clock_is_read_after_policy_lock(completed_task, monkeypatch):
    from dewey.django import retention

    completed_task()
    save_config()
    before_lock = timezone.now()
    after_lock = before_lock + timedelta(seconds=10)
    locked = False
    original = retention._locked_policy

    def acquire(alias):
        nonlocal locked
        row = original(alias)
        locked = True
        return row

    monkeypatch.setattr(retention, "_locked_policy", acquire)
    monkeypatch.setattr(
        retention, "database_now", lambda using: after_lock if locked else before_lock
    )
    result = cleanup_completed_batch()["run"]
    assert result["started_at"] == before_lock.isoformat()
    assert result["cutoff_at"] == (after_lock - timedelta(days=30)).isoformat()


@pytest.mark.django_db(transaction=True, databases=["default", "second"])
def test_admin_and_job_use_selected_alias_not_auth_database(viewer, completed_task, settings):
    completed_task()
    before_default = snapshot_tasks()
    then = timezone.now() - timedelta(days=31)
    row = TaskEntry.objects.using("second").create(
        task_type="test.second", status="completed", created_at=then, completed_at=then
    )
    HistoryState.objects.using("second").get_or_create(
        pk="task-events", defaults={"started_at": timezone.now()}
    )
    user = get_user_model().objects.get(pk=viewer.session["_auth_user_id"])
    user.user_permissions.add(Permission.objects.get(codename="manage_task_retention"))
    settings.DEWEY_DASHBOARD_DATABASE_ALIAS = "second"
    assert viewer.get(endpoint()).json()["eligible_count"] == 1
    answer = viewer.post(
        configure_url(),
        {
            "days": 30,
            "enabled": "yes",
            "revision": 0,
            "confirm": "yes",
            "reason": "Test separate database policy",
        },
    )
    assert answer.status_code == 200
    assert cleanup_completed_batch(using="second")["run"]["deleted_count"] == 1
    assert not TaskEntry.objects.using("second").filter(pk=row.pk).exists()
    assert TaskRetentionAudit.objects.using("second").count() == 1
    assert TaskRetentionRun.objects.using("second").count() == 1
    assert (
        not TaskRetention.objects.exists()
        and not TaskRetentionAudit.objects.exists()
        and not TaskRetentionRun.objects.exists()
    )
    assert snapshot_tasks() == before_default
