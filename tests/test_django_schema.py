"""Django 0.5 schema contracts — migration 0002, model parity, no behaviour.

Covers the frozen P2 slice: expiry/snapshot fields and EXPIRED on TaskEntry,
the dispatcher heartbeat table, and an additive, reversible migration 0002
that maps 0.5-only terminal data safely on rollback and matches the models exactly.
"""

# ruff: noqa: E402 — django.setup() must run before model imports

import dataclasses
import os
from datetime import UTC, datetime, timedelta
from importlib import import_module

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.django_settings")
django.setup()

from django.db import migrations as dj_migrations
from django.db import models as dj_models

from dewey.core.heartbeat import DispatcherHeartbeat as HeartbeatDC
from dewey.core.states import TaskStatus
from dewey.core.types import TaskEntry as TaskEntryDC
from dewey.django.models import DispatcherHeartbeat, TaskEntry
from dewey.sqlalchemy.models import DispatcherHeartbeatModel, TaskEntryModel

MIGRATION_0002 = "0002_task_expiry_and_dispatcher_heartbeat"


def _load_migration_0002():
    return import_module(f"dewey.django.migrations.{MIGRATION_0002}").Migration


class TestMigrationsShip:
    def test_migration_0002_is_part_of_the_package(self):
        """The wheel has to carry migrations, or a consumer cannot upgrade."""
        from importlib import resources

        files = {path.name for path in resources.files("dewey.django.migrations").iterdir()}
        assert f"{MIGRATION_0002}.py" in files

    def test_migration_0002_depends_on_0001(self):
        assert ("dewey", "0001_initial") in _load_migration_0002().dependencies

    @pytest.mark.django_db
    def test_models_and_migrations_do_not_drift(self):
        """makemigrations --check catches a model edit that never got a migration."""
        from django.core.management import call_command

        call_command("makemigrations", "dewey", check=True, dry_run=True, verbosity=0)


class TestMigration0002Shape:
    def test_forward_path_is_additive_only(self):
        """The forward path has no removals; reverse-only repair makes downgrade safe."""
        allowed = (
            dj_migrations.CreateModel,
            dj_migrations.AddField,
            dj_migrations.AlterField,
            dj_migrations.RunPython,
            dj_migrations.AddIndex,
        )
        for operation in _load_migration_0002().operations:
            assert isinstance(operation, allowed), operation

    def test_every_operation_is_reversible(self):
        for operation in _load_migration_0002().operations:
            assert operation.reversible, operation

    def test_adds_the_three_task_expiry_fields(self):
        added = {
            op.name
            for op in _load_migration_0002().operations
            if isinstance(op, dj_migrations.AddField) and op.model_name == "taskentry"
        }
        assert added == {"expires_at", "expired_at", "initial_scheduled_for"}

    def test_alters_status_choices_to_include_expired(self):
        alters = [
            op
            for op in _load_migration_0002().operations
            if isinstance(op, dj_migrations.AlterField) and op.name == "status"
        ]
        assert len(alters) == 1
        choices = dict(alters[0].field.choices)
        assert choices["expired"] == "Expired"

    def test_adds_the_expiry_partial_index(self):
        indexes = [
            op.index
            for op in _load_migration_0002().operations
            if isinstance(op, dj_migrations.AddIndex) and op.model_name == "taskentry"
        ]
        assert [idx.name for idx in indexes] == ["ix_task_expires"]
        assert indexes[0].fields == ["expires_at"]
        assert indexes[0].condition is not None

    def test_creates_the_heartbeat_table_with_its_indexes(self):
        creates = [
            op
            for op in _load_migration_0002().operations
            if isinstance(op, dj_migrations.CreateModel)
        ]
        assert [op.name for op in creates] == ["DispatcherHeartbeat"]
        options = creates[0].options
        assert options["db_table"] == "dewey_dispatcher_heartbeats"
        assert {idx.name for idx in options["indexes"]} == {
            "ix_heartbeat_last_seen",
            "ix_heartbeat_backend_db",
        }


class TestTaskEntryModelParity:
    def test_status_choices_cover_every_task_status(self):
        assert {value for value, _ in TaskEntry.Status.choices} == {s.value for s in TaskStatus}
        assert TaskEntry.Status.EXPIRED.value == TaskStatus.EXPIRED.value

    def test_expiry_fields_are_nullable_datetimes(self):
        for name in ("expires_at", "initial_scheduled_for", "expired_at"):
            field = TaskEntry._meta.get_field(name)
            assert isinstance(field, dj_models.DateTimeField), name
            assert field.null is True, name

    def test_initial_scheduled_for_is_an_internal_field(self):
        """The snapshot is written once at creation — not an editable second API."""
        assert TaskEntry._meta.get_field("initial_scheduled_for").editable is False

    def test_expiry_partial_index_matches_nonterminal_deadline_candidates(self):
        index = {idx.name: idx for idx in TaskEntry._meta.indexes}["ix_task_expires"]
        assert index.fields == ["expires_at"]
        children = dict(index.condition.children)
        assert children["expires_at__isnull"] is False
        assert set(children["status__in"]) == {"pending", "dispatching", "failed"}

    def test_columns_match_the_sqlalchemy_table(self):
        django_columns = {f.column for f in TaskEntry._meta.concrete_fields}
        sqlalchemy_columns = {c.name for c in TaskEntryModel.__table__.columns}
        assert django_columns == sqlalchemy_columns

    def test_to_dataclass_carries_the_new_fields(self):
        created = datetime.now(UTC)
        deadline = created + timedelta(hours=1)
        entry = TaskEntry(
            id="task-1",
            task_type="order.confirmed",
            status=TaskStatus.EXPIRED.value,
            args=[],
            kwargs={},
            metadata={},
            queue="default",
            priority=0,
            attempts=1,
            max_attempts=5,
            error="",
            created_at=created,
            scheduled_for=created + timedelta(minutes=30),
            initial_scheduled_for=created,
            expires_at=deadline,
            expired_at=deadline + timedelta(seconds=1),
        )
        dc = entry.to_dataclass()
        assert dc.status == TaskStatus.EXPIRED
        assert dc.expires_at == deadline
        assert dc.initial_scheduled_for == created
        assert dc.expired_at == deadline + timedelta(seconds=1)

    def test_to_dataclass_and_model_share_one_field_set(self):
        """A field added to either side without the other fails here."""
        dataclass_fields = {f.name for f in dataclasses.fields(TaskEntryDC)}
        model_fields = {f.name for f in TaskEntry._meta.concrete_fields}
        assert dataclass_fields == model_fields


class TestDispatcherHeartbeatModelParity:
    def test_table_name(self):
        assert DispatcherHeartbeat._meta.db_table == "dewey_dispatcher_heartbeats"

    def test_primary_key_is_instance_id(self):
        assert DispatcherHeartbeat._meta.pk.name == "instance_id"

    def test_fields_match_the_framework_free_contract(self):
        model_fields = {f.name for f in DispatcherHeartbeat._meta.concrete_fields}
        contract_fields = {f.name for f in dataclasses.fields(HeartbeatDC)}
        assert model_fields == contract_fields

    def test_columns_match_the_sqlalchemy_table(self):
        django_columns = {f.column for f in DispatcherHeartbeat._meta.concrete_fields}
        sqlalchemy_columns = {c.name for c in DispatcherHeartbeatModel.__table__.columns}
        assert django_columns == sqlalchemy_columns

    def test_queues_is_nullable_json(self):
        field = DispatcherHeartbeat._meta.get_field("queues")
        assert isinstance(field, dj_models.JSONField)
        assert field.null is True

    def test_bounded_use_indexes(self):
        indexes = {idx.name: idx for idx in DispatcherHeartbeat._meta.indexes}
        assert indexes["ix_heartbeat_last_seen"].fields == ["last_seen_at"]
        assert indexes["ix_heartbeat_backend_db"].fields == ["backend", "database"]


@pytest.mark.django_db(transaction=True)
class TestMigration0002Reversibility:
    """Prove 0002 applies backwards and forwards on real Postgres."""

    def test_roundtrip_to_0001_and_back_maps_expired_to_dead(self):
        from django.db import connection
        from django.db.migrations.executor import MigrationExecutor

        expired_id = "rollback-expired-task"
        TaskEntry.objects.create(
            id=expired_id,
            task_type="rollback.expired",
            status=TaskStatus.EXPIRED.value,
        )

        def task_columns():
            with connection.cursor() as cursor:
                description = connection.introspection.get_table_description(cursor, "task_entries")
            return {column.name for column in description}

        def table_names():
            with connection.cursor() as cursor:
                return set(connection.introspection.table_names(cursor))

        executor = MigrationExecutor(connection)
        try:
            executor.migrate([("dewey", "0001_initial")])

            columns = task_columns()
            assert "expires_at" not in columns
            assert "initial_scheduled_for" not in columns
            assert "expired_at" not in columns
            assert "dewey_dispatcher_heartbeats" not in table_names()
            with connection.cursor() as cursor:
                cursor.execute("SELECT status FROM task_entries WHERE id = %s", [expired_id])
                assert cursor.fetchone() == (TaskStatus.DEAD.value,)
        finally:
            executor.loader.build_graph()
            executor.migrate([("dewey", MIGRATION_0002)])

        columns = task_columns()
        assert {"expires_at", "initial_scheduled_for", "expired_at"} <= columns
        assert "dewey_dispatcher_heartbeats" in table_names()

        with connection.cursor() as cursor:
            constraints = connection.introspection.get_constraints(cursor, "task_entries")
        assert "ix_task_expires" in constraints
