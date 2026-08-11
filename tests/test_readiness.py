"""Dispatcher heartbeat, Django checks, and doctor readiness contracts."""

from __future__ import annotations

# ruff: noqa: E402 — django.setup() must run before model imports
import json
import os
from datetime import UTC, datetime, timedelta
from io import StringIO

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.core.management import CommandError, call_command
from django.test import override_settings

from dewey import __version__
from dewey.core.heartbeat import DispatcherHeartbeat as HeartbeatDC
from dewey.dispatcher import Dispatcher
from dewey.django.checks import check_dewey_configuration
from dewey.django.dispatch import DjangoDispatchBackend
from dewey.django.models import DispatcherHeartbeat
from dewey.django.queries import get_dispatchers as get_django_dispatchers
from dewey.sqlalchemy.dispatch import AsyncSQLAlchemyDispatchBackend, SQLAlchemyDispatchBackend
from dewey.sqlalchemy.models import DispatcherHeartbeatModel
from dewey.sqlalchemy.queries import get_dispatchers


def record_dispatch(task_id: str) -> None:
    """Importable transport stand-in for settings and doctor checks."""


class TestSQLAlchemyHeartbeat:
    def test_emit_query_refresh_and_close(self, engine, session):
        backend = SQLAlchemyDispatchBackend(
            engine,
            queues=["critical"],
            database_identity="primary",
        )
        backend.heartbeat()

        [heartbeat] = get_dispatchers(
            session,
            database="primary",
            queues=["critical"],
        )
        assert heartbeat.instance_id == backend.instance_id
        assert heartbeat.dewey_version == __version__
        assert heartbeat.backend == "sqlalchemy"
        first_seen = heartbeat.last_seen_at

        backend.heartbeat()
        session.expire_all()
        [refreshed] = get_dispatchers(session, database="primary")
        assert refreshed.last_seen_at >= first_seen
        assert get_dispatchers(session, database="primary", queues=["bulk"]) == []

        backend.close()
        session.expire_all()
        assert get_dispatchers(session, database="primary") == []

    def test_stale_rows_and_queue_mismatches_fail_closed(self, session):
        now = datetime.now(UTC)
        session.add(
            DispatcherHeartbeatModel(
                instance_id="stale-instance",
                dewey_version=__version__,
                backend="sqlalchemy",
                database="primary",
                queues=["critical"],
                started_at=now - timedelta(minutes=5),
                last_seen_at=now - timedelta(minutes=2),
            )
        )
        session.commit()

        assert get_dispatchers(session, database="primary", now=now) == []

    def test_heartbeat_failure_does_not_stop_dispatch(self, engine, monkeypatch):
        backend = SQLAlchemyDispatchBackend(engine)
        monkeypatch.setattr(backend, "heartbeat", lambda: (_ for _ in ()).throw(OSError("no")))
        monkeypatch.setattr(backend, "wait_for_work", lambda timeout: False)

        Dispatcher(
            backend,
            record_dispatch,
            idle_poll_seconds=0,
            sweep_interval_seconds=None,
        ).run(max_iterations=1)


@pytest.mark.asyncio
async def test_async_heartbeat_emit_query_and_close(async_engine, async_session):
    from dewey.sqlalchemy.async_queries import get_dispatchers_async

    backend = AsyncSQLAlchemyDispatchBackend(
        async_engine,
        queues=["bulk"],
        database_identity="async-primary",
    )
    await backend.heartbeat()
    [heartbeat] = await get_dispatchers_async(
        async_session,
        database="async-primary",
        queues=["bulk"],
    )
    assert heartbeat.backend == "sqlalchemy-async"
    await backend.close()
    async_session.expire_all()
    assert await get_dispatchers_async(async_session, database="async-primary") == []


@pytest.mark.django_db(transaction=True)
class TestDjangoHeartbeat:
    def test_emit_query_queue_match_and_close(self):
        backend = DjangoDispatchBackend(queues=["critical"])
        backend.heartbeat()

        [heartbeat] = get_django_dispatchers(database="default", queues=["critical"])
        assert heartbeat.instance_id == backend.instance_id
        assert heartbeat.backend == "django"
        assert get_django_dispatchers(database="default", queues=["bulk"]) == []

        backend.close()
        assert get_django_dispatchers(database="default") == []


class TestSystemChecks:
    def test_invalid_settings_shape_is_an_error(self):
        with override_settings(DEWEY=["not", "a", "dict"]):
            ids = {finding.id for finding in check_dewey_configuration()}
        assert "dewey.E001" in ids

    def test_missing_dispatch_is_actionable_without_blocking_migrations(self):
        ids = {finding.id for finding in check_dewey_configuration()}
        assert "dewey.W003" in ids

    @override_settings(
        DEWEY={
            "DISPATCH": record_dispatch,
            "SWEEP_INTERVAL_SECONDS": None,
        }
    )
    def test_disabled_recovery_is_visible(self):
        ids = {finding.id for finding in check_dewey_configuration()}
        assert "dewey.W001" in ids

    @override_settings(
        DEWEY={
            "DISPATCH": record_dispatch,
            "DATABASE": "missing",
        }
    )
    def test_unknown_alias_is_an_error(self):
        ids = {finding.id for finding in check_dewey_configuration()}
        assert "dewey.E002" in ids

    @override_settings(
        DEWEY={
            "DISPATCH": record_dispatch,
            "SWEEP_INTERVAL_SECONDS": 0,
        }
    )
    def test_invalid_recovery_interval_is_an_error(self):
        ids = {finding.id for finding in check_dewey_configuration()}
        assert "dewey.E005" in ids

    @override_settings(DEWEY={"DISPATCH": record_dispatch})
    def test_non_postgresql_alias_is_an_error(self, monkeypatch):
        from django.conf import settings

        monkeypatch.setitem(
            settings.DATABASES,
            "default",
            {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"},
        )
        ids = {finding.id for finding in check_dewey_configuration()}
        assert "dewey.E003" in ids

    @override_settings(DEWEY={"DISPATCH": record_dispatch, "DATABASE": "second"})
    def test_router_to_background_alias_warns_about_producer_atomicity(self, monkeypatch):
        monkeypatch.setattr(
            "dewey.django.checks.router.db_for_write",
            lambda model: "second",
        )
        ids = {finding.id for finding in check_dewey_configuration()}
        assert "dewey.W002" in ids


@pytest.mark.django_db(transaction=True)
class TestDoctor:
    def _heartbeat(self, *, queues=None, age_seconds=0):
        now = datetime.now(UTC)
        return DispatcherHeartbeat.objects.create(
            instance_id=f"doctor-{age_seconds}-{queues}",
            dewey_version=__version__,
            backend="django",
            database="default",
            queues=queues,
            started_at=now - timedelta(minutes=1),
            last_seen_at=now - timedelta(seconds=age_seconds),
        )

    @override_settings(
        DEWEY={
            "DISPATCH": record_dispatch,
            "QUEUES": ["critical"],
        }
    )
    def test_json_reports_ready_with_a_fresh_matching_dispatcher(self):
        self._heartbeat(queues=["critical"])
        output = StringIO()

        call_command("dewey_doctor", "--format", "json", stdout=output)

        payload = json.loads(output.getvalue())
        assert payload["ok"] is True
        assert not [finding for finding in payload["findings"] if finding["level"] == "error"]

    @override_settings(DEWEY={"DISPATCH": record_dispatch})
    def test_human_output_reports_ready(self):
        self._heartbeat()
        output = StringIO()

        call_command("dewey_doctor", stdout=output)

        assert "Dewey doctor: ready" in output.getvalue()

    @override_settings(DEWEY={"DISPATCH": record_dispatch})
    def test_absent_or_stale_heartbeat_is_a_nonzero_json_finding(self):
        self._heartbeat(age_seconds=120)
        output = StringIO()

        with pytest.raises(CommandError):
            call_command("dewey_doctor", "--format", "json", stdout=output)

        payload = json.loads(output.getvalue())
        assert payload["ok"] is False
        assert "dewey.doctor.dispatcher_heartbeat" in {
            finding["id"] for finding in payload["findings"]
        }

    @override_settings(DEWEY={"DISPATCH": record_dispatch})
    def test_scoped_heartbeat_cannot_claim_readiness_for_all_queues(self):
        self._heartbeat(queues=["critical"])
        with pytest.raises(CommandError):
            call_command("dewey_doctor", "--format", "json", stdout=StringIO())

    @override_settings(DEWEY={"DISPATCH": record_dispatch, "QUEUES": ["critical"]})
    def test_wrong_queue_heartbeat_fails_closed(self):
        self._heartbeat(queues=["bulk"])
        with pytest.raises(CommandError):
            call_command("dewey_doctor", "--format", "json", stdout=StringIO())


def test_framework_heartbeat_helpers_are_frozen_and_queue_aware():
    now = datetime.now(UTC)
    heartbeat = HeartbeatDC(
        instance_id="test-instance",
        dewey_version="0.5.0",
        backend="test",
        database="test",
        queues=("critical", "bulk"),
        started_at=now,
        last_seen_at=now,
    )
    assert heartbeat.is_fresh(now)
    assert heartbeat.serves(("critical",))
    assert not heartbeat.serves(("missing",))
