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
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = REPO_ROOT / ".github" / "workflows"
CI = WORKFLOWS / "ci.yml"
DEPENDABOT = REPO_ROOT / ".github" / "dependabot.yml"
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

    block = job_block(CI, "test")
    assert re.search(
        r'- python-version: "3\.12"\n\s+django-requirement: "Django~=5\.2\.0"',
        block,
    ), "the Python 3.12 + Django 5.2 compatibility lane is required"


def test_gating_jobs_are_not_advisory():
    for job in ("checks", "test", "wheel-smoke", "audit"):
        header = "\n".join(job_block(CI, job).splitlines()[:12])
        assert "continue-on-error" not in header, f"{job} must remain a gate"


# --- Dependency audit split ------------------------------------------------


def test_runtime_audit_scope_comes_from_its_own_export():
    """Runtime scope must be a separate export, not a filtered combined report."""
    block = job_block(CI, "audit")
    assert "--no-dev" in block and "--all-extras" in block
    assert "uvx --python 3.11 pip-audit" in block
    assert "uvx --python 3.14 pip-audit" in block


def test_dev_audit_stays_advisory_and_dev_scoped():
    block = job_block(CI, "audit")
    assert "--only-dev" in block
    assert re.search(r"- name: Audit development tooling\n\s+continue-on-error: true", block)


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
    """Releases authenticate with OIDC. A password or API token would be a regression."""
    text = (WORKFLOWS / "publish.yml").read_text()
    assert "id-token: write" in text
    assert "pypa/gh-action-pypi-publish@" in text
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
