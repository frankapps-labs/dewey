#!/usr/bin/env python3
"""Validate and inventory an admitted uv dependency graph without installing it."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
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
EXACT_REQUIREMENT = re.compile(r"^([A-Za-z0-9_.-]+)==([^;\s]+)$")
DEFAULT_COOLDOWN_DAYS = 7
DEFAULT_HOTFIX_LABEL = "dependency-hotfix"
HOTFIX_RATIONALE = re.compile(r"(?im)^Hotfix rationale:\s*\S.+$")
HOTFIX_UPSTREAM = re.compile(r"(?im)^Upstream:\s*https://\S+$")


def load_toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text())


def admission_config(project: dict[str, Any]) -> dict[str, Any]:
    configured = project.get("tool", {}).get("dependency-admission", {})
    return {
        "project_package": configured.get("project-package", project["project"]["name"]),
        "build_group": configured.get("build-group", "build"),
        "parser_group": configured.get("parser-group", "admission"),
        "cooldown_days": int(configured.get("cooldown-days", DEFAULT_COOLDOWN_DAYS)),
        "hotfix_label": configured.get("hotfix-label", DEFAULT_HOTFIX_LABEL),
    }


def git_text(ref: str, path: str) -> str | None:
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
        return None
    return result.stdout


def git_file(ref: str, path: str) -> dict[str, Any] | None:
    if not ref or set(ref) == {"0"}:
        return None
    text = git_text(ref, path)
    if text is None:
        print(f"notice: {path} is unavailable at {ref}; skipping dependency diff")
        return None
    return tomllib.loads(text)


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


def validate_lock(lock: dict[str, Any], *, project_package: str = "dewey") -> list[str]:
    errors: list[str] = []
    packages = lock.get("package", [])
    if not packages:
        return ["uv.lock contains no packages"]

    for package in packages:
        name = package.get("name", "<unnamed>")
        source = package.get("source", {})
        if name == project_package:
            if source != {"editable": "."}:
                errors.append(f"project package {name!r} must remain editable from '.'")
        elif source != {"registry": PYPI_REGISTRY}:
            errors.append(f"{name}: source must be the canonical PyPI registry, got {source!r}")

        artifacts: list[dict[str, Any]] = []
        if "sdist" in package:
            artifacts.append(package["sdist"])
        artifacts.extend(package.get("wheels", []))
        if name != project_package and not artifacts:
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


def normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def validate_requirements(project: dict[str, Any], lock: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for group, requirement in iter_requirements(requirement_groups(project)):
        lowered = requirement.lower()
        if " @ " in requirement or any(
            marker in lowered for marker in ("git+", "http://", "https://", "file:")
        ):
            errors.append(f"{group}: direct URL/path requirement is not admitted: {requirement}")

    locked = {
        (normalize_name(package["name"]), package["version"]) for package in lock.get("package", [])
    }
    config = admission_config(project)
    build_requirements = project.get("build-system", {}).get("requires", [])
    admitted_build_group = project.get("dependency-groups", {}).get(config["build_group"], [])
    if sorted(build_requirements) != sorted(admitted_build_group):
        errors.append(
            f"build-system requirements must exactly match dependency group "
            f"{config['build_group']!r}"
        )
    for requirement in build_requirements:
        match = EXACT_REQUIREMENT.fullmatch(requirement)
        if not match:
            errors.append(f"build-system requirement must be an exact pin: {requirement}")
            continue
        key = normalize_name(match.group(1)), match.group(2)
        if key not in locked:
            errors.append(f"build-system requirement is not present in uv.lock: {requirement}")

    parser_requirements = project.get("dependency-groups", {}).get(config["parser_group"], [])
    if len(parser_requirements) != 1:
        errors.append(
            f"dependency group {config['parser_group']!r} must contain exactly one YAML parser pin"
        )
    else:
        parser_requirement = parser_requirements[0]
        match = EXACT_REQUIREMENT.fullmatch(parser_requirement)
        if not match or normalize_name(match.group(1)) != "pyyaml":
            errors.append(f"dependency group {config['parser_group']!r} must exactly pin PyYAML")
        elif ("pyyaml", match.group(2)) not in locked:
            errors.append(
                f"YAML parser requirement is not present in uv.lock: {parser_requirement}"
            )
    return errors


def yaml_uses_nodes(path: Path, text: str) -> tuple[list[tuple[int, str | None]], list[str]]:
    """Return semantic ``uses`` mapping entries from the composed YAML node tree."""
    import yaml

    relative = path.relative_to(ROOT)
    try:
        document = yaml.compose(text, Loader=yaml.SafeLoader)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        number = mark.line + 1 if mark is not None else 1
        problem = getattr(exc, "problem", None) or str(exc).splitlines()[0]
        return [], [f"{relative}:{number}: invalid workflow YAML: {problem}"]

    if not isinstance(document, yaml.MappingNode):
        return [], [f"{relative}:1: workflow YAML must contain one top-level mapping"]

    entries: list[tuple[int, str | None]] = []
    errors: list[str] = []
    active: set[int] = set()

    def walk(node: yaml.Node) -> None:
        identity = id(node)
        if identity in active:
            errors.append(
                f"{relative}:{node.start_mark.line + 1}: recursive YAML aliases are not admitted"
            )
            return
        active.add(identity)
        try:
            if isinstance(node, yaml.MappingNode):
                for key, value in node.value:
                    if isinstance(key, yaml.ScalarNode) and key.value == "uses":
                        reference = (
                            value.value
                            if isinstance(value, yaml.ScalarNode)
                            and value.tag == "tag:yaml.org,2002:str"
                            else None
                        )
                        entries.append((key.start_mark.line + 1, reference))
                    walk(key)
                    walk(value)
            elif isinstance(node, yaml.SequenceNode):
                for value in node.value:
                    walk(value)
        finally:
            active.remove(identity)

    walk(document)
    return entries, errors


def parse_actions(path: Path, text: str) -> tuple[dict[str, str], list[str]]:
    actions: dict[str, str] = {}
    occurrences: dict[str, int] = {}
    entries, errors = yaml_uses_nodes(path, text)
    lines = text.splitlines()
    reported_syntax_lines: set[int] = set()
    for number, semantic_reference in entries:
        line = lines[number - 1]
        match = ACTION_PIN.match(line)
        if not match or semantic_reference is None:
            if number not in reported_syntax_lines:
                errors.append(
                    f"{path.relative_to(ROOT)}:{number}: non-canonical uses mapping syntax"
                )
                reported_syntax_lines.add(number)
            continue
        reference = match.group(1)
        if reference != semantic_reference:
            errors.append(
                f"{path.relative_to(ROOT)}:{number}: uses source disagrees with parsed YAML value"
            )
            continue
        if reference.startswith("./"):
            continue
        if "@" not in reference or not FULL_COMMIT.fullmatch(reference.rsplit("@", 1)[1]):
            errors.append(f"{path.relative_to(ROOT)}:{number}: mutable action ref {reference!r}")
            continue
        action, commit = reference.rsplit("@", 1)
        occurrence = occurrences.get(action, 0) + 1
        occurrences[action] = occurrence
        actions[f"{path.relative_to(ROOT)}:{action}:{occurrence}"] = commit
    return actions, errors


def workflow_paths() -> list[Path]:
    workflows = ROOT / ".github" / "workflows"
    return sorted({*workflows.glob("*.yml"), *workflows.glob("*.yaml")})


def current_actions() -> tuple[dict[str, str], list[str]]:
    actions: dict[str, str] = {}
    errors: list[str] = []
    for path in workflow_paths():
        parsed, parse_errors = parse_actions(path, path.read_text())
        actions.update(parsed)
        errors.extend(parse_errors)
    return actions, errors


def base_actions(ref: str) -> tuple[dict[str, str], list[str]]:
    actions: dict[str, str] = {}
    errors: list[str] = []
    for path in workflow_paths():
        text = git_text(ref, str(path.relative_to(ROOT)))
        if text is None:
            continue
        parsed, parse_errors = parse_actions(path, text)
        actions.update(parsed)
        errors.extend(parse_errors)
    return actions, errors


def validate_actions() -> list[str]:
    return current_actions()[1]


def request_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "uv-dependency-admission/1",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except (OSError, urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"canonical metadata request failed for {url}: {exc}") from exc


def changed_packages(
    current_lock: dict[str, Any],
    base_lock: dict[str, Any] | None,
    *,
    project_package: str,
) -> list[dict[str, Any]]:
    if base_lock is None:
        return []
    base = {package_key(package): package for package in base_lock.get("package", [])}
    return [
        package
        for package in current_lock.get("package", [])
        if package["name"] != project_package and base.get(package_key(package)) != package
    ]


def uv_timestamp(value: str) -> datetime:
    """Normalize canonical timestamps to uv.lock's millisecond precision."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(microsecond=(parsed.microsecond // 1000) * 1000)


def verify_pypi_package(package: dict[str, Any]) -> tuple[list[str], datetime | None]:
    name, version = package_key(package)
    url = f"https://pypi.org/pypi/{urllib.parse.quote(name)}/{urllib.parse.quote(version)}/json"
    try:
        metadata = request_json(url)
    except RuntimeError as exc:
        return [str(exc)], None

    canonical = {
        item["url"]: (
            item.get("digests", {}).get("sha256"),
            item.get("upload_time_iso_8601"),
        )
        for item in metadata.get("urls", [])
    }
    errors: list[str] = []
    uploads: list[datetime] = []
    artifacts = ([package["sdist"]] if "sdist" in package else []) + package.get("wheels", [])
    for artifact in artifacts:
        artifact_url = artifact.get("url", "")
        expected = canonical.get(artifact_url)
        if expected is None:
            errors.append(f"{name} {version}: artifact URL is absent from canonical PyPI metadata")
            continue
        digest, upload_time = expected
        if artifact.get("hash") != f"sha256:{digest}":
            errors.append(
                f"{name} {version}: artifact digest disagrees with canonical PyPI metadata"
            )
        try:
            canonical_time = uv_timestamp(upload_time)
        except (AttributeError, TypeError, ValueError):
            errors.append(f"{name} {version}: canonical PyPI artifact has no valid upload time")
            continue
        try:
            locked_time = uv_timestamp(artifact.get("upload-time", ""))
        except (AttributeError, TypeError, ValueError):
            locked_time = None
        if locked_time != canonical_time:
            errors.append(
                f"{name} {version}: upload timestamp disagrees with canonical PyPI metadata"
            )
        uploads.append(canonical_time)
    return errors, max(uploads, default=None)


def action_changes(base_ref: str) -> tuple[list[tuple[str, str]], list[str]]:
    current, current_errors = current_actions()
    base, base_errors = base_actions(base_ref)
    changed = sorted(
        (key.split(":", 1)[1].rsplit(":", 1)[0], commit)
        for key, commit in current.items()
        if base.get(key) != commit
    )
    return changed, current_errors + base_errors


def verify_github_action(action: str, commit: str) -> tuple[list[str], datetime | None]:
    parts = action.split("/")
    if len(parts) < 2:
        return [f"invalid GitHub Action repository path: {action!r}"], None
    repository = "/".join(parts[:2])
    url = f"https://api.github.com/repos/{repository}/commits/{commit}"
    try:
        metadata = request_json(url)
    except RuntimeError as exc:
        return [str(exc)], None
    if metadata.get("sha") != commit:
        return [f"{action}@{commit}: canonical GitHub commit mismatch"], None
    date = metadata.get("commit", {}).get("committer", {}).get("date")
    try:
        committed = datetime.fromisoformat(date.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return [f"{action}@{commit}: canonical GitHub commit has no valid date"], None
    return [], committed


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


def hotfix_exception(
    event_path: str | None, *, hotfix_label: str = DEFAULT_HOTFIX_LABEL
) -> tuple[bool, str]:
    if not event_path:
        return False, f"add the maintainer-only {hotfix_label!r} label with the required PR fields"
    event = json.loads(Path(event_path).read_text())
    pull_request = event.get("pull_request", {})
    labels = {label.get("name") for label in pull_request.get("labels", [])}
    body = pull_request.get("body") or ""
    if hotfix_label not in labels:
        return False, f"add the maintainer-only {hotfix_label!r} label"
    if not HOTFIX_RATIONALE.search(body):
        return False, "add a non-empty 'Hotfix rationale:' line to the PR body"
    if not HOTFIX_UPSTREAM.search(body):
        return False, "add an 'Upstream: https://…' line to the PR body"
    return True, "maintainer-labeled hotfix with rationale and upstream evidence"


def validate_cooldown(
    current_lock: dict[str, Any],
    base_lock: dict[str, Any] | None,
    *,
    base_ref: str = "origin/main",
    now: datetime,
    event_path: str | None,
    project_package: str = "dewey",
    cooldown: timedelta = timedelta(days=DEFAULT_COOLDOWN_DAYS),
    hotfix_label: str = DEFAULT_HOTFIX_LABEL,
    verify_canonical: bool = False,
    include_actions: bool = False,
) -> list[str]:
    if base_lock is None:
        return ["cooldown enforcement requires the base uv.lock"]

    errors: list[str] = []
    too_fresh: list[tuple[str, str, datetime]] = []
    for package in changed_packages(current_lock, base_lock, project_package=project_package):
        if verify_canonical:
            canonical_errors, uploaded = verify_pypi_package(package)
            errors.extend(canonical_errors)
        else:
            uploaded = latest_upload(package)
        if uploaded is None or now - uploaded < cooldown:
            too_fresh.append((package["name"], package["version"], uploaded or now))

    action_updates: list[tuple[str, str]] = []
    if include_actions:
        action_updates, action_errors = action_changes(base_ref)
        errors.extend(action_errors)
        for action, commit in action_updates:
            if verify_canonical:
                canonical_errors, committed = verify_github_action(action, commit)
                errors.extend(canonical_errors)
            else:
                committed = None
            if committed is None or now - committed < cooldown:
                too_fresh.append((action, commit, committed or now))

    if not too_fresh:
        return errors

    bypassed, reason = hotfix_exception(event_path, hotfix_label=hotfix_label)
    if bypassed:
        print(f"notice: dependency cooldown/Action admission bypassed: {reason}")
        return errors

    for name, version, published in too_fresh:
        age = max(0.0, (now - published).total_seconds() / 86400)
        errors.append(
            f"{name} {version} is only {age:.1f} days old; wait {cooldown.days} days or {reason}"
        )
    return errors


def inventory(
    current_lock: dict[str, Any],
    base_lock: dict[str, Any] | None,
    current_project: dict[str, Any],
    base_project: dict[str, Any] | None,
    *,
    project_package: str,
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
            if key[0] != project_package and current_packages[key] != base_packages[key]
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
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="Validate lock inputs without loading the admitted YAML parser",
    )
    args = parser.parse_args()

    lock = load_toml(ROOT / "uv.lock")
    project = load_toml(ROOT / "pyproject.toml")
    config = admission_config(project)
    base_lock = git_file(args.base_ref, "uv.lock")
    base_project = git_file(args.base_ref, "pyproject.toml")

    report = inventory(
        lock,
        base_lock,
        project,
        base_project,
        project_package=config["project_package"],
    )
    print("\n".join(report))
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a") as handle:
            handle.write("\n".join(report) + "\n")

    errors = validate_lock(lock, project_package=config["project_package"]) + validate_requirements(
        project, lock
    )
    if not args.preflight:
        errors += validate_actions()
    if args.enforce_cooldown:
        errors += validate_cooldown(
            lock,
            base_lock,
            base_ref=args.base_ref,
            now=datetime.now(UTC),
            event_path=os.environ.get("GITHUB_EVENT_PATH"),
            project_package=config["project_package"],
            cooldown=timedelta(days=config["cooldown_days"]),
            hotfix_label=config["hotfix_label"],
            verify_canonical=True,
            include_actions=not args.preflight,
        )
    if errors:
        print("\nDependency admission failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    if args.preflight:
        print("\nDependency lock inputs passed the standard-library preflight.")
    else:
        print("\nDependency structure, artifact hashes, sources, and action pins are admitted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
