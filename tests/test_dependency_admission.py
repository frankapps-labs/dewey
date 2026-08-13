"""Supply-chain admission checks that run before dependency installation."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scripts import dependency_admission

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def cleanup():
    """Override the DB-backed autouse cleanup: these tests inspect metadata only."""
    yield


def current_lock():
    return dependency_admission.load_toml(ROOT / "uv.lock")


def current_project():
    return dependency_admission.load_toml(ROOT / "pyproject.toml")


def first_registry_package(lock):
    return next(package for package in lock["package"] if package["name"] != "dewey")


def test_current_dependency_graph_is_structurally_admitted():
    assert dependency_admission.validate_lock(current_lock()) == []
    assert dependency_admission.validate_requirements(current_project()) == []
    assert dependency_admission.validate_actions() == []


def test_non_pypi_source_is_rejected():
    lock = deepcopy(current_lock())
    package = first_registry_package(lock)
    package["source"] = {"git": "https://example.invalid/repository"}

    assert any(
        "canonical PyPI registry" in error for error in dependency_admission.validate_lock(lock)
    )


def test_missing_or_malformed_artifact_hash_is_rejected():
    lock = deepcopy(current_lock())
    package = first_registry_package(lock)
    artifact = package.get("sdist") or package["wheels"][0]
    artifact["hash"] = "sha256:not-a-digest"

    assert any("full SHA-256" in error for error in dependency_admission.validate_lock(lock))


def test_missing_upload_timestamp_is_rejected_without_crashing():
    lock = deepcopy(current_lock())
    package = first_registry_package(lock)
    artifact = package.get("sdist") or package["wheels"][0]
    artifact.pop("upload-time")

    assert any(
        "valid upload timestamp" in error for error in dependency_admission.validate_lock(lock)
    )


def test_direct_url_requirement_is_rejected():
    project = deepcopy(current_project())
    project["project"]["optional-dependencies"]["django"].append(
        "malicious @ https://example.invalid/malicious.whl"
    )

    assert any(
        "direct URL/path" in error for error in dependency_admission.validate_requirements(project)
    )


def changed_lock_with_upload_time(uploaded: datetime):
    current = deepcopy(current_lock())
    base = deepcopy(current)
    package = first_registry_package(current)
    base_package = next(item for item in base["package"] if item["name"] == package["name"])
    base_package["version"] = "0.0-base"
    timestamp = uploaded.isoformat().replace("+00:00", "Z")
    if "sdist" in package:
        package["sdist"]["upload-time"] = timestamp
    for wheel in package.get("wheels", []):
        wheel["upload-time"] = timestamp
    return current, base, package


def write_event(path, *, labels=(), body=""):
    path.write_text(
        json.dumps(
            {
                "pull_request": {
                    "labels": [{"name": label} for label in labels],
                    "body": body,
                }
            }
        )
    )


def test_changed_artifact_younger_than_seven_days_is_rejected(tmp_path):
    now = datetime(2026, 8, 13, tzinfo=UTC)
    current, base, package = changed_lock_with_upload_time(now - timedelta(days=6))

    errors = dependency_admission.validate_cooldown(
        current,
        base,
        now=now,
        event_path=None,
    )

    assert any(package["name"] in error and "only 6.0 days old" in error for error in errors)


def test_seven_day_old_artifact_is_admitted(tmp_path):
    now = datetime(2026, 8, 13, tzinfo=UTC)
    current, base, _ = changed_lock_with_upload_time(now - timedelta(days=7))

    assert (
        dependency_admission.validate_cooldown(
            current,
            base,
            now=now,
            event_path=None,
        )
        == []
    )


def test_hotfix_label_requires_structured_rationale_and_upstream(tmp_path):
    event = tmp_path / "event.json"
    write_event(event, labels=[dependency_admission.HOTFIX_LABEL], body="urgent")

    admitted, reason = dependency_admission.hotfix_exception(str(event))

    assert admitted is False
    assert "Hotfix rationale" in reason


def test_complete_hotfix_evidence_bypasses_age_only(tmp_path):
    now = datetime(2026, 8, 13, tzinfo=UTC)
    current, base, _ = changed_lock_with_upload_time(now - timedelta(hours=1))
    event = tmp_path / "event.json"
    write_event(
        event,
        labels=[dependency_admission.HOTFIX_LABEL],
        body="Hotfix rationale: fixes active breakage\nUpstream: https://example.com/fix",
    )

    assert (
        dependency_admission.validate_cooldown(
            current,
            base,
            now=now,
            event_path=str(event),
        )
        == []
    )


def test_inventory_calls_out_hash_only_artifact_changes():
    current = deepcopy(current_lock())
    base = deepcopy(current)
    package = first_registry_package(current)
    base_package = next(item for item in base["package"] if item["name"] == package["name"])
    artifact = base_package.get("sdist") or base_package["wheels"][0]
    artifact["hash"] = "sha256:" + "0" * 64

    report = dependency_admission.inventory(
        current,
        base,
        current_project(),
        current_project(),
    )

    assert any(
        "Artifact/dependency metadata changed" in line and package["name"] in line
        for line in report
    )


def test_inventory_calls_out_new_transitive_packages():
    current = deepcopy(current_lock())
    base = deepcopy(current)
    removed = first_registry_package(base)
    base["package"].remove(removed)

    report = dependency_admission.inventory(
        current,
        base,
        current_project(),
        current_project(),
    )

    assert any(
        "New transitive/direct package" in line and removed["name"] in line for line in report
    )
