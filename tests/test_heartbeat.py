"""Tests for the framework-free dispatcher heartbeat contract."""

import dataclasses
from datetime import UTC, datetime

import pytest

from dewey.core.heartbeat import DispatcherHeartbeat


def _make_heartbeat(**overrides):
    defaults = {
        "instance_id": "0b6bafd5-3a3c-4bb0-8f4f-2f0e7a2f8a11",
        "dewey_version": "0.5.0",
        "backend": "django",
        "database": "default",
        "queues": ("default", "high"),
        "started_at": datetime.now(UTC),
        "last_seen_at": datetime.now(UTC),
    }
    defaults.update(overrides)
    return DispatcherHeartbeat(**defaults)


class TestDispatcherHeartbeat:
    def test_carries_its_fields(self):
        started = datetime.now(UTC)
        hb = _make_heartbeat(started_at=started, last_seen_at=started)
        assert hb.instance_id == "0b6bafd5-3a3c-4bb0-8f4f-2f0e7a2f8a11"
        assert hb.dewey_version == "0.5.0"
        assert hb.backend == "django"
        assert hb.database == "default"
        assert hb.queues == ("default", "high")
        assert hb.started_at == started
        assert hb.last_seen_at == started

    def test_is_frozen(self):
        hb = _make_heartbeat()
        with pytest.raises(dataclasses.FrozenInstanceError):
            hb.last_seen_at = datetime.now(UTC)  # type: ignore[misc]

    def test_queues_none_means_all_queues(self):
        hb = _make_heartbeat(queues=None)
        assert hb.queues is None

    def test_is_importable_from_dewey_core(self):
        """The contract is public API — exported from the core namespace."""
        import dewey.core

        assert dewey.core.DispatcherHeartbeat is DispatcherHeartbeat
        assert "DispatcherHeartbeat" in dewey.core.__all__

    def test_exposes_exactly_the_frozen_contract_fields(self):
        """Heartbeats live in a shared database: no hostname, PID, DSN,
        credentials, or process environment — only these seven fields."""
        names = {f.name for f in dataclasses.fields(DispatcherHeartbeat)}
        assert names == {
            "instance_id",
            "dewey_version",
            "backend",
            "database",
            "queues",
            "started_at",
            "last_seen_at",
        }
