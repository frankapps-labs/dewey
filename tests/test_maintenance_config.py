"""
Guards on repository maintenance configuration.

These are not tests of Dewey's behaviour; they are tests of claims. Two kinds of
claim can rot silently and neither shows up in a normal test run:

* **Advertised support.** Every Python classifier must have a blocking matrix lane.
* **Gate strength.** Runtime auditing must block, while development-tool
  advisories remain visible without blocking unrelated product work.

Most assertions are deliberately textual because they protect lines a reviewer reads.
PyYAML also parses every GitHub configuration file so malformed workflow syntax fails CI.
"""

import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
CI = WORKFLOWS / "ci.yml"
DEPENDABOT = REPO_ROOT / ".github" / "dependabot.yml"
DEPENDENCY_POLICY = REPO_ROOT / "docs" / "dependency-admission.md"
DEPENDENCY_CHECKER = REPO_ROOT / "scripts" / "dependency_admission.py"
MAKEFILE = REPO_ROOT / "Makefile"


@pytest.fixture(autouse=True)
def cleanup():
    """Override the DB-backed autouse cleanup: these tests read files only."""
    yield


def job_block(workflow: Path, job: str) -> str:
    """
    Return the lines of one job, from `  <job>:` to the next job at that indent.

    Job-scoped assertions need job-scoped text; searching a whole workflow for
    `continue-on-error` would happily match a different job and prove nothing.
    """
    lines = workflow.read_text().splitlines()
    start = next((i for i, line in enumerate(lines) if line == f"  {job}:"), None)
    assert start is not None, f"{workflow.name} has no job named {job!r}"

    end = len(lines)
    for i in range(start + 1, len(lines)):
        if re.match(r"^ {2}\S", lines[i]):
            end = i
            break
    return "\n".join(lines[start:end])


def pyproject() -> dict:
    return tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())


# --- Advertised Python support ---------------------------------------------


def test_supported_python_versions_are_classified():
    classifiers = pyproject()["project"]["classifiers"]
    for version in ("3.11", "3.12", "3.13", "3.14"):
        assert f"Programming Language :: Python :: {version}" in classifiers


def test_gating_matrix_still_covers_every_supported_version():
    block = job_block(CI, "test")
    versions = re.findall(r'^\s+- python-version: "([^"]+)"$', block, re.MULTILINE)
    assert versions == ["3.11", "3.12", "3.12", "3.13", "3.14"], (
        "the blocking matrix changed; update the classifiers and this guard together"
    )


def test_django_5_2_is_supported_on_python_3_12():
    project = pyproject()["project"]
    assert project["optional-dependencies"]["django"] == ["Django>=5.2.16,<7"]
    assert project["optional-dependencies"]["huey"] == ["huey>=3,<4"]

    project_config = pyproject()
    assert project_config["dependency-groups"]["compat-py312-django52"] == [
        "Django==5.2.16; python_version == '3.12'",
        "huey==3.0.0; python_version == '3.12'",
    ]
    block = job_block(CI, "test")
    assert re.search(
        r'- python-version: "3\.12"\n\s+compatibility-group: "compat-py312-django52"',
        block,
    ), "the Python 3.12 + Django 5.2 admitted lock lane is required"


def test_gating_jobs_are_not_advisory():
    for job in ("checks", "test", "wheel-smoke", "audit"):
        header = "\n".join(job_block(CI, job).splitlines()[:12])
        assert "continue-on-error" not in header, f"{job} must remain a gate"


def test_dependency_installing_jobs_wait_for_admission():
    for job in ("checks", "test", "wheel-smoke"):
        header = "\n".join(job_block(CI, job).splitlines()[:8])
        assert "needs: audit" in header, f"{job} must not install before dependency admission"


def test_execution_paths_use_only_admitted_lock_inputs():
    ci = CI.read_text()
    publish = (WORKFLOWS / "publish.yml").read_text()
    wheel_smoke = (REPO_ROOT / "scripts" / "wheel_smoke.sh").read_text()
    import_matrix = (REPO_ROOT / "scripts" / "optional_import_matrix.sh").read_text()
    sync_env = (REPO_ROOT / "scripts" / "sync_admitted_env.sh").read_text()

    combined = "\n".join((ci, publish, wheel_smoke, import_matrix, sync_env))
    assert "uvx " not in combined
    assert "pip install --quiet --upgrade pip" not in combined
    assert "uv pip install --upgrade" not in combined
    assert "uv build\n" not in combined
    assert "VIRTUAL_ENV=.release-venv uv build --no-build-isolation" in ci
    assert "VIRTUAL_ENV=.release-venv uv build --no-build-isolation" in publish
    assert "sync_admitted_env.sh .release-venv 3.12 group:release" in ci
    assert "sync_admitted_env.sh .release-venv 3.12 group:release" in publish
    assert "--require-hashes" in sync_env
    assert ".venv/bin/ruff" in ci
    assert ".venv/bin/basedpyright" in ci
    assert "uv run ruff" not in ci
    assert "uv run basedpyright" not in ci
    assert "--no-deps --no-build" in wheel_smoke
    assert "--no-deps --no-build" in import_matrix


def test_uv_tool_version_is_pinned_everywhere():
    project = pyproject()
    expected = project["tool"]["dependency-admission"]["uv-version"]
    for path in WORKFLOWS.glob("*.yml"):
        text = path.read_text()
        assert text.count("setup-uv@") == text.count(f'version: "{expected}"')


def test_hotfix_label_or_body_edit_retriggers_admission():
    trigger = CI.read_text().split("permissions:", 1)[0]
    for event in ("labeled", "unlabeled", "edited", "synchronize"):
        assert event in trigger


# --- Dependency audit split ------------------------------------------------


def test_local_dependency_gate_enforces_canonical_cooldown():
    assert "scripts/dependency_admission.py --enforce-cooldown" in MAKEFILE.read_text()


def test_runtime_audit_scope_comes_from_its_own_export():
    """Runtime scope must be a separate export, not a filtered combined report."""
    block = job_block(CI, "audit")
    assert "scripts/dependency_admission.py" in block
    assert "uv lock --check" in block
    assert "--no-dev" in block and "--all-extras" in block
    assert "sync_admitted_env.sh .audit-py311 3.11 group:audit" in block
    assert "sync_admitted_env.sh .audit-py314 3.14 group:audit" in block
    assert "uvx" not in block, "the vulnerability scanner must come from uv.lock"


def test_dev_audit_stays_advisory_and_dev_scoped():
    block = job_block(CI, "audit")
    assert "--only-dev" in block
    assert re.search(r"- name: Audit development tooling\n\s+continue-on-error: true", block)
    assert ".audit-py314/bin/pip-audit" in block


# --- Security workflows ----------------------------------------------------


@pytest.mark.parametrize("name", ["codeql.yml", "scorecard.yml"])
def test_security_workflow_exists_and_holds_no_secrets(name):
    text = (WORKFLOWS / name).read_text()
    assert "secrets." not in text, f"{name} must not consume repository secrets"
    assert "permissions:" in text


def test_codeql_grants_only_the_write_it_needs():
    text = (WORKFLOWS / "codeql.yml").read_text()
    assert "languages: python" in text
    assert "build-mode: none" in text
    block = job_block(WORKFLOWS / "codeql.yml", "analyze")
    writes = set(re.findall(r"^\s+(\S+): write$", block, re.MULTILINE))
    assert writes == {"security-events"}


def test_scorecard_publishes_without_a_stored_token():
    path = WORKFLOWS / "scorecard.yml"
    text = path.read_text()
    assert "permissions: read-all" in text
    triggers = re.findall(r"^  (\w+):", text.split("permissions:")[0], re.MULTILINE)
    assert "pull_request" not in triggers, "fork pull requests must not get these write scopes"
    block = job_block(path, "analysis")
    writes = set(re.findall(r"^\s+(\S+): write$", block, re.MULTILINE))
    # id-token: write is Sigstore keyless publishing — the reason no secret is needed.
    assert writes == {"security-events", "id-token"}
    assert "publish_results: true" in block


def test_ci_workflow_is_read_only_by_default():
    assert re.search(r"^permissions:\n  contents: read$", CI.read_text(), re.MULTILINE)


def test_trusted_publishing_is_preserved():
    """OIDC is isolated to an artifact-only job that never executes project dependencies."""
    path = WORKFLOWS / "publish.yml"
    text = path.read_text()
    build = job_block(path, "build")
    publish = job_block(path, "publish")

    assert "id-token: write" not in build
    assert "VIRTUAL_ENV=.release-venv uv build --no-build-isolation" in build
    assert "sync_admitted_env.sh .release-venv 3.12 group:release" in build
    assert "id-token: write" in publish
    assert "actions/download-artifact@" in publish
    assert "actions/checkout@" not in publish
    assert "setup-python@" not in publish
    assert "setup-uv@" not in publish
    assert "uv " not in publish
    assert "pypa/gh-action-pypi-publish@" in publish
    assert "password" not in text
    assert "PYPI_API_TOKEN" not in text


def test_release_target_tags_only_exact_origin_main():
    text = MAKEFILE.read_text()
    release = text[text.index("release:") : text.index("_check-clean:")]
    release_head = text[text.index("_check-release-head:") : text.index("ci:")]

    assert "_check-release-head" in release
    assert "git fetch --quiet origin main --tags" in release_head
    assert "git rev-parse origin/main" in release_head
    assert 'git tag -a "$$TAG"' in release
    assert 'git push origin "refs/tags/$$TAG"' in release
    assert "git push --tags" not in release
    assert "git push &&" not in release


# --- Dependabot ------------------------------------------------------------


def test_dependabot_covers_both_ecosystems_weekly():
    text = DEPENDABOT.read_text()
    ecosystems = set(re.findall(r'package-ecosystem: "([^"]+)"', text))
    assert ecosystems == {"github-actions", "uv"}
    assert text.count('interval: "weekly"') == 2


def test_dependabot_groups_only_minor_and_patch():
    """Majors stay ungrouped on purpose: they are compatibility decisions."""
    text = DEPENDABOT.read_text()
    assert text.count("groups:") == 2
    assert '"major"' not in text
    for update_type in ('"minor"', '"patch"'):
        assert text.count(update_type) == 2


def test_dependabot_routine_updates_have_a_seven_day_cooldown():
    text = DEPENDABOT.read_text()
    assert text.count("cooldown:") == 2
    assert text.count("default-days: 7") == 2
    assert "ignore:" not in text, "security updates must not be globally ignored"
    assert "insecure-external-code-execution" not in text


def test_manual_hotfix_bypass_is_age_only_and_still_gated():
    text = DEPENDENCY_POLICY.read_text()
    section = text[text.index("## Urgent hotfix bypass") :]
    assert "exception to **age only**" in section
    assert "uv lock --upgrade-package PACKAGE" in section
    assert "make dependency-check" in section
    assert "human approval" in section


def test_dependency_preflight_stays_standard_library_only():
    text = DEPENDENCY_CHECKER.read_text()
    assert "import requests" not in text
    assert "import packaging" not in text

    result = subprocess.run(
        [
            sys.executable,
            "-S",
            str(DEPENDENCY_CHECKER),
            "--preflight",
            "--base-ref",
            "origin/main",
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


# --- Workflow syntax -------------------------------------------------------


def test_workflow_files_are_syntactically_plausible():
    """
    A cheap stand-in for a YAML parser, covering the mistakes that actually happen.

    This one runs everywhere, including where PyYAML is absent, so a malformed
    workflow cannot reach `main` behind a skip.
    """
    for path in sorted(WORKFLOWS.glob("*.yml")):
        text = path.read_text()
        assert "\t" not in text, f"{path.name} contains a tab; YAML forbids tab indentation"
        assert text.endswith("\n"), f"{path.name} has no trailing newline"
        assert re.search(r"^jobs:$", text, re.MULTILINE), f"{path.name} declares no jobs"
        for ref in re.findall(r"uses: (\S+)", text):
            assert re.fullmatch(r"[\w.-]+/[\w.-]+(/[\w.-]+)*@[0-9a-f]{40}", ref), (
                f"{path.name} references {ref!r}, which is not pinned to an immutable SHA"
            )


def test_every_github_yaml_file_parses():
    import yaml

    paths = sorted((REPO_ROOT / ".github").rglob("*.yml"))
    paths += sorted((REPO_ROOT / ".github").rglob("*.yaml"))
    assert paths, ".github has no YAML files; the glob is wrong"
    for path in paths:
        parsed = yaml.safe_load(path.read_text())
        assert isinstance(parsed, dict), f"{path} did not parse as a mapping"
