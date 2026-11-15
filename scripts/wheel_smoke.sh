#!/usr/bin/env bash
# Exercise the built wheel the way a consumer gets it: a clean virtualenv, installed
# from dist/, with the source tree deliberately out of reach.
#
# This is the test that catches packaging mistakes no in-repo test can see — a module
# missing from the wheel, migrations not shipped, an extra that does not resolve.
#
# Requires Postgres. With `make wheel-smoke` the compose container provides it.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
DB_URL="${DEWEY_TEST_DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/dewey_test}"
SMOKE_DB="${DEWEY_SMOKE_DATABASE:-dewey_wheel_smoke}"

cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

WHEEL="$(ls -t "$REPO_ROOT"/dist/*.whl 2>/dev/null | head -1 || true)"
if [[ -z "$WHEEL" ]]; then
  echo "No wheel in dist/. Run 'make build' first." >&2
  exit 1
fi
echo "==> Wheel under test: $(basename "$WHEEL")"

echo "==> Creating a hash-locked environment (no access to the source tree)"
"$REPO_ROOT/scripts/sync_admitted_env.sh" "$WORK_DIR/venv" "$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
  extra:sqlalchemy extra:async extra:django group:wheel-smoke >/dev/null
VENV_PY="$WORK_DIR/venv/bin/python"

echo "==> Installing only the local wheel; dependencies are already admitted"
uv pip install --python "$VENV_PY" --no-deps --no-build "$WHEEL" >/dev/null

# Run from a scratch directory so a stray `import dewey` cannot resolve to ./src.
cp "$REPO_ROOT/scripts/wheel_smoke.py" "$WORK_DIR/wheel_smoke.py"
cd "$WORK_DIR"

echo "==> Verifying the import path and version are from the installed package"
EXPECTED_VERSION="$(basename "$WHEEL" | sed -E 's/^dewey-([^-]+)-.*/\1/')"
DEWEY_EXPECTED_VERSION="$EXPECTED_VERSION" "$VENV_PY" - <<'PY'
import os
import pathlib
from importlib.metadata import metadata, version

import dewey

expected = os.environ["DEWEY_EXPECTED_VERSION"]
location = pathlib.Path(dewey.__file__).resolve()
assert "site-packages" in location.parts, f"imported the source tree, not the wheel: {location}"
assert metadata("dewey")["Version"] == expected
assert version("dewey") == expected
assert dewey.__version__ == expected
print(f"    dewey {dewey.__version__} from {location.parent}")
PY

echo "==> Smoke-importing core and each extra"
"$VENV_PY" - <<'PY'
import importlib

for module in [
    "dewey",
    "dewey.cli",
    "dewey.ops",
    "dewey.scheduling",
    "dewey.worker",
    "dewey.core.cron",
    "dewey.sqlalchemy",
    "dewey.sqlalchemy.ops",
    "dewey.sqlalchemy.schedules",
    "dewey.django",
]:
    importlib.import_module(module)
    print(f"    ok {module}")
PY

echo "==> Confirming migrations shipped inside the wheel"
"$VENV_PY" - <<'PY'
from importlib import resources

files = {path.name for path in resources.files("dewey.django.migrations").iterdir()}
expected = {"0001_initial.py", "0008_native_only.py", "0009_schedules.py"}
assert expected <= files, f"migrations missing from the wheel: {sorted(files)}"
for name in sorted(expected):
    print(f"    ok dewey.django.migrations/{name}")
PY

echo "==> Running the end-to-end scenario (Django migrate, one worker, schedules, CLI)"
DEWEY_SMOKE_DB_URL="$DB_URL" DEWEY_SMOKE_DB_NAME="$SMOKE_DB" "$VENV_PY" wheel_smoke.py

echo "==> Installed-wheel smoke passed"
