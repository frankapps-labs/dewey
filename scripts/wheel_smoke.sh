#!/usr/bin/env bash
# Exercise the built wheel the way a consumer gets it: a clean virtualenv, installed
# from dist/, with the source tree deliberately out of reach.
#
# This is the test that catches packaging mistakes no in-repo test can see — a module
# missing from the wheel, migrations not shipped, an extra that does not resolve.
#
# Requires Postgres (and Redis, for the Huey leg). With `make wheel-smoke` the compose
# containers provide both.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d)"
DB_URL="${DEWEY_TEST_DATABASE_URL:-postgresql://postgres:postgres@localhost:5432/dewey_test}"
REDIS_URL="${DEWEY_TEST_REDIS_URL:-redis://localhost:6379/0}"
SMOKE_DB="${DEWEY_SMOKE_DATABASE:-dewey_wheel_smoke}"

cleanup() { rm -rf "$WORK_DIR"; }
trap cleanup EXIT

WHEEL="$(ls -t "$REPO_ROOT"/dist/*.whl 2>/dev/null | head -1 || true)"
if [[ -z "$WHEEL" ]]; then
  echo "No wheel in dist/. Run 'make build' first." >&2
  exit 1
fi
echo "==> Wheel under test: $(basename "$WHEEL")"

echo "==> Creating a clean virtualenv (no access to the source tree)"
python3 -m venv "$WORK_DIR/venv"
VENV_PY="$WORK_DIR/venv/bin/python"
"$VENV_PY" -m pip install --quiet --upgrade pip

echo "==> Installing the wheel with every advertised extra"
"$VENV_PY" -m pip install --quiet "${WHEEL}[sqlalchemy,async,django,huey]" psycopg2-binary redis

# Run from a scratch directory so a stray `import dewey` cannot resolve to ./src.
cp "$REPO_ROOT/scripts/wheel_smoke.py" "$WORK_DIR/wheel_smoke.py"
cd "$WORK_DIR"

echo "==> Verifying the import path really is the installed package"
"$VENV_PY" - <<'PY'
import pathlib
import dewey

location = pathlib.Path(dewey.__file__).resolve()
assert "site-packages" in location.parts, f"imported the source tree, not the wheel: {location}"
print(f"    dewey {dewey.__version__} from {location.parent}")
PY

echo "==> Smoke-importing core and each extra"
"$VENV_PY" - <<'PY'
import importlib

for module in [
    "dewey",
    "dewey.policy",
    "dewey.errors",
    "dewey.serialization",
    "dewey.dispatcher",
    "dewey.listen_sync",
    "dewey.core.states",
    "dewey.core.execution",
    "dewey.adapters",
    "dewey.adapters.huey",
    "dewey.sqlalchemy",
    "dewey.sqlalchemy.dispatch",
    "dewey.django",
]:
    importlib.import_module(module)
    print(f"    ok {module}")
PY

echo "==> Confirming migrations shipped inside the wheel"
"$VENV_PY" - <<'PY'
from importlib import resources

files = {path.name for path in resources.files("dewey.django.migrations").iterdir()}
assert "0001_initial.py" in files, f"migrations missing from the wheel: {sorted(files)}"
print("    ok dewey.django.migrations/0001_initial.py")
PY

echo "==> Running the end-to-end scenario (Django migrate, dispatcher, Huey worker)"
DEWEY_SMOKE_DB_URL="$DB_URL" DEWEY_SMOKE_REDIS_URL="$REDIS_URL" DEWEY_SMOKE_DB_NAME="$SMOKE_DB" \
  "$VENV_PY" wheel_smoke.py

echo "==> Installed-wheel smoke passed"
