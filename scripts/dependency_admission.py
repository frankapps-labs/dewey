#!/usr/bin/env python3
"""Validate and inventory Dewey's admitted dependency graph without installing it."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
PYPI_REGISTRY = "https://pypi.org/simple"
PYPI_FILES = "https://files.pythonhosted.org/packages/"
SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
ACTION_PIN = re.compile(r"^\s*(?:-\s*)?uses:\s+([^\s#]+)(?:\s+#.*)?$")
FULL_COMMIT = re.compile(r"^[0-9a-f]{40}$")
COOLDOWN = timedelta(days=7)
HOTFIX_LABEL = "dependency-hotfix"
HOTFIX_RATIONALE = re.compile(r"(?im)^Hotfix rationale:\s*\S.+$")
HOTFIX_UPSTREAM = re.compile(r"(?im)^Upstream:\s*https://\S+$")


def load_toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text())


def git_file(ref: str, path: str) -> dict[str, Any] | None:
    if not ref or set(ref) == {"0"}:
        return None
    result = subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        print(f"notice: {path} is unavailable at {ref}; skipping dependency diff")
        return None
    return tomllib.loads(result.stdout)


def package_versions(lock: dict[str, Any]) -> dict[str, set[str]]:
    versions: dict[str, set[str]] = {}
    for package in lock.get("package", []):
        versions.setdefault(package["name"], set()).add(package["version"])
    return versions


def requirement_groups(project: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    groups: dict[str, tuple[str, ...]] = {
        "build-system": tuple(project.get("build-system", {}).get("requires", [])),
        "project": tuple(project.get("project", {}).get("dependencies", [])),
    }
    for name, requirements in project.get("project", {}).get("optional-dependencies", {}).items():
        groups[f"extra:{name}"] = tuple(requirements)
    for name, requirements in project.get("dependency-groups", {}).items():
        groups[f"group:{name}"] = tuple(requirements)
    return groups


def validate_lock(lock: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    packages = lock.get("package", [])
    if not packages:
        return ["uv.lock contains no packages"]

    for package in packages:
        name = package.get("name", "<unnamed>")
        source = package.get("source", {})
        if name == "dewey":
            if source != {"editable": "."}:
                errors.append(f"project package {name!r} must remain editable from '.'")
        elif source != {"registry": PYPI_REGISTRY}:
            errors.append(f"{name}: source must be the canonical PyPI registry, got {source!r}")

        artifacts: list[dict[str, Any]] = []
        if "sdist" in package:
            artifacts.append(package["sdist"])
        artifacts.extend(package.get("wheels", []))
        if name != "dewey" and not artifacts:
            errors.append(f"{name}: registry package has no hashed sdist or wheel artifacts")
        for artifact in artifacts:
            url = artifact.get("url", "")
            digest = artifact.get("hash", "")
            upload_time = artifact.get("upload-time", "")
            if not url.startswith(PYPI_FILES):
                errors.append(f"{name}: artifact is not hosted on files.pythonhosted.org: {url}")
            if not SHA256.fullmatch(digest):
                errors.append(f"{name}: artifact lacks a full SHA-256 digest: {digest!r}")
            try:
                datetime.fromisoformat(upload_time.replace("Z", "+00:00"))
            except (AttributeError, ValueError):
                errors.append(f"{name}: artifact lacks a valid upload timestamp: {upload_time!r}")
    return errors


def iter_requirements(groups: dict[str, tuple[str, ...]]) -> Iterable[tuple[str, str]]:
    for group, requirements in groups.items():
        for requirement in requirements:
            yield group, requirement


def validate_requirements(project: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for group, requirement in iter_requirements(requirement_groups(project)):
        lowered = requirement.lower()
        if " @ " in requirement or any(
            marker in lowered for marker in ("git+", "http://", "https://", "file:")
        ):
            errors.append(f"{group}: direct URL/path requirement is not admitted: {requirement}")
    return errors


def validate_actions() -> list[str]:
    errors: list[str] = []
    workflows = ROOT / ".github" / "workflows"
    paths = sorted({*workflows.glob("*.yml"), *workflows.glob("*.yaml")})
    for path in paths:
        for number, line in enumerate(path.read_text().splitlines(), 1):
            match = ACTION_PIN.match(line)
            if not match:
                continue
            reference = match.group(1)
            if reference.startswith("./"):
                continue
            if "@" not in reference or not FULL_COMMIT.fullmatch(reference.rsplit("@", 1)[1]):
                errors.append(
                    f"{path.relative_to(ROOT)}:{number}: mutable action ref {reference!r}"
                )
    return errors


def package_key(package: dict[str, Any]) -> tuple[str, str]:
    return package["name"], package["version"]


def latest_upload(package: dict[str, Any]) -> datetime | None:
    artifacts = ([package["sdist"]] if "sdist" in package else []) + package.get("wheels", [])
    timestamps: list[datetime] = []
    for artifact in artifacts:
        try:
            timestamps.append(
                datetime.fromisoformat(artifact["upload-time"].replace("Z", "+00:00"))
            )
        except (AttributeError, KeyError, ValueError):
            return None
    return max(timestamps, default=None)


def hotfix_exception(event_path: str | None) -> tuple[bool, str]:
    if not event_path:
        return False, f"add the maintainer-only {HOTFIX_LABEL!r} label with the required PR fields"
    event = json.loads(Path(event_path).read_text())
    pull_request = event.get("pull_request", {})
    labels = {label.get("name") for label in pull_request.get("labels", [])}
    body = pull_request.get("body") or ""
    if HOTFIX_LABEL not in labels:
        return False, f"add the maintainer-only {HOTFIX_LABEL!r} label"
    if not HOTFIX_RATIONALE.search(body):
        return False, "add a non-empty 'Hotfix rationale:' line to the PR body"
    if not HOTFIX_UPSTREAM.search(body):
        return False, "add an 'Upstream: https://…' line to the PR body"
    return True, "maintainer-labeled hotfix with rationale and upstream evidence"


def validate_cooldown(
    current_lock: dict[str, Any],
    base_lock: dict[str, Any] | None,
    *,
    now: datetime,
    event_path: str | None,
) -> list[str]:
    if base_lock is None:
        return ["cooldown enforcement requires the base uv.lock"]

    base = {package_key(package): package for package in base_lock.get("package", [])}
    changed = [
        package
        for package in current_lock.get("package", [])
        if package["name"] != "dewey" and base.get(package_key(package)) != package
    ]
    too_fresh: list[tuple[dict[str, Any], datetime]] = []
    for package in changed:
        uploaded = latest_upload(package)
        if uploaded is None or now - uploaded < COOLDOWN:
            too_fresh.append((package, uploaded or now))
    if not too_fresh:
        return []

    bypassed, reason = hotfix_exception(event_path)
    if bypassed:
        print(f"notice: seven-day cooldown bypassed: {reason}")
        return []

    errors = []
    for package, uploaded in too_fresh:
        age = max(0.0, (now - uploaded).total_seconds() / 86400)
        errors.append(
            f"{package['name']} {package['version']} is only {age:.1f} days old; "
            f"wait seven days or {reason}"
        )
    return errors


def inventory(
    current_lock: dict[str, Any],
    base_lock: dict[str, Any] | None,
    current_project: dict[str, Any],
    base_project: dict[str, Any] | None,
) -> list[str]:
    lines = ["## Dependency admission inventory", ""]
    if base_lock is None:
        lines.append("No base lockfile was available; structural validation only.")
    else:
        current = package_versions(current_lock)
        base = package_versions(base_lock)
        added = sorted(current.keys() - base.keys())
        removed = sorted(base.keys() - current.keys())
        changed = sorted(
            name for name in current.keys() & base.keys() if current[name] != base[name]
        )
        current_packages = {
            package_key(package): package for package in current_lock.get("package", [])
        }
        base_packages = {package_key(package): package for package in base_lock.get("package", [])}
        metadata_changed = sorted(
            key
            for key in current_packages.keys() & base_packages.keys()
            if key[0] != "dewey" and current_packages[key] != base_packages[key]
        )
        if not (added or removed or changed or metadata_changed):
            lines.append("No locked package versions or artifact metadata changed.")
        for name in changed:
            lines.append(
                f"- Updated `{name}`: `{', '.join(sorted(base[name]))}` → "
                f"`{', '.join(sorted(current[name]))}`"
            )
        for name in added:
            versions = ", ".join(sorted(current[name]))
            lines.append(f"- **New transitive/direct package:** `{name}` `{versions}`")
        for name in removed:
            lines.append(f"- Removed package: `{name}` `{', '.join(sorted(base[name]))}`")
        for name, version in metadata_changed:
            lines.append(f"- **Artifact/dependency metadata changed:** `{name}` `{version}`")

    if base_project is not None:
        before = requirement_groups(base_project)
        after = requirement_groups(current_project)
        for group in sorted(before.keys() | after.keys()):
            if before.get(group) != after.get(group):
                lines.append(f"- **Manifest changed:** `{group}`")
    lines.extend(
        [
            "",
            "New packages, manifest changes, majors, and maintainer/source changes require explicit human review.",
        ]
    )
    return lines


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--base-ref",
        default="origin/main",
        help="Git ref used to inventory dependency changes (default: origin/main)",
    )
    parser.add_argument(
        "--enforce-cooldown",
        action="store_true",
        help="Reject changed PyPI artifacts uploaded less than seven days ago",
    )
    args = parser.parse_args()

    lock = load_toml(ROOT / "uv.lock")
    project = load_toml(ROOT / "pyproject.toml")
    base_lock = git_file(args.base_ref, "uv.lock")
    base_project = git_file(args.base_ref, "pyproject.toml")

    report = inventory(lock, base_lock, project, base_project)
    print("\n".join(report))
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a") as handle:
            handle.write("\n".join(report) + "\n")

    errors = validate_lock(lock) + validate_requirements(project) + validate_actions()
    if args.enforce_cooldown:
        errors += validate_cooldown(
            lock,
            base_lock,
            now=datetime.now(UTC),
            event_path=os.environ.get("GITHUB_EVENT_PATH"),
        )
    if errors:
        print("\nDependency admission failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("\nDependency structure, artifact hashes, sources, and action pins are admitted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
