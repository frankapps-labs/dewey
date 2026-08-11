"""SQLAlchemy 0.5 schema contracts — pure metadata inspection, no behaviour.

These tests pin the frozen P2 schema: expiry/snapshot columns on task_entries,
the nonterminal-deadline partial index, and the dispatcher heartbeat table.
"""

import dataclasses

from sqlalchemy import JSON, DateTime, String
from sqlalchemy.dialects import postgresql

from dewey.core.heartbeat import DispatcherHeartbeat
from dewey.sqlalchemy.models import Base, DispatcherHeartbeatModel, TaskEntryModel


def _index(table, name):
    by_name = {idx.name: idx for idx in table.indexes}
    assert name in by_name, f"missing index {name!r}; have {sorted(by_name)}"
    return by_name[name]


def _compiled_where(index) -> str:
    clause = index.dialect_options["postgresql"]["where"]
    compiled = clause.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    return str(compiled)


class TestTaskEntryExpiryColumns:
    def test_new_columns_exist_nullable_and_timezone_aware(self):
        table = TaskEntryModel.__table__
        for name in ("expires_at", "initial_scheduled_for", "expired_at"):
            column = table.c[name]
            assert column.nullable is True, name
            assert isinstance(column.type, DateTime), name
            assert column.type.timezone is True, name

    def test_expiry_partial_index_targets_nonterminal_deadline_candidates(self):
        index = _index(TaskEntryModel.__table__, "ix_task_entries_expires_at")
        assert [col.name for col in index.columns] == ["expires_at"]

        where = _compiled_where(index)
        assert "expires_at IS NOT NULL" in where
        for candidate in ("pending", "dispatching", "processing", "failed"):
            assert candidate in where
        # Terminal rows can never expire and must stay out of the index.
        for terminal in ("completed", "dead", "expired"):
            assert terminal not in where


class TestDispatcherHeartbeatModel:
    def test_table_name_and_registration(self):
        assert DispatcherHeartbeatModel.__tablename__ == "dewey_dispatcher_heartbeats"
        assert "dewey_dispatcher_heartbeats" in Base.metadata.tables

    def test_primary_key_is_instance_id(self):
        table = DispatcherHeartbeatModel.__table__
        assert [col.name for col in table.primary_key.columns] == ["instance_id"]
        assert isinstance(table.c.instance_id.type, String)

    def test_columns_match_the_framework_free_contract(self):
        """One source of truth: the table carries exactly the dataclass fields."""
        table_columns = {col.name for col in DispatcherHeartbeatModel.__table__.columns}
        contract_fields = {f.name for f in dataclasses.fields(DispatcherHeartbeat)}
        assert table_columns == contract_fields

    def test_queues_is_nullable_json(self):
        column = DispatcherHeartbeatModel.__table__.c.queues
        assert column.nullable is True
        assert isinstance(column.type, JSON)

    def test_timestamps_are_required_and_timezone_aware(self):
        table = DispatcherHeartbeatModel.__table__
        for name in ("started_at", "last_seen_at"):
            column = table.c[name]
            assert column.nullable is False, name
            assert isinstance(column.type, DateTime), name
            assert column.type.timezone is True, name

    def test_last_seen_index_bounds_freshness_and_cleanup_scans(self):
        index = _index(DispatcherHeartbeatModel.__table__, "ix_dewey_heartbeats_last_seen")
        assert [col.name for col in index.columns] == ["last_seen_at"]

    def test_backend_database_index_bounds_readiness_matching(self):
        index = _index(DispatcherHeartbeatModel.__table__, "ix_dewey_heartbeats_backend_database")
        assert [col.name for col in index.columns] == ["backend", "database"]

    def test_no_ttl_or_host_identity_columns(self):
        """Staleness is reader-side; the table stores no TTL, hostname, or PID."""
        names = {col.name for col in DispatcherHeartbeatModel.__table__.columns}
        assert not names & {"ttl", "expires_at", "hostname", "pid", "dsn", "environment"}
