#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
wheel=$(find "$root/dist" -maxdepth 1 -name 'dewey-*.whl' -print -quit)
if [[ -z "$wheel" ]]; then
  echo "Build the wheel first: make build" >&2
  exit 2
fi
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

new_env() {
  local name=$1
  shift
  "$root/scripts/sync_admitted_env.sh" "$work/$name" 3.11 "$@" >/dev/null
  uv pip install --python "$work/$name/bin/python" --no-deps --no-build "$wheel" >/dev/null
}

new_env core group:build
"$work/core/bin/python" - <<'PY'
import sys
import dewey
assert "django" not in sys.modules
assert "sqlalchemy" not in sys.modules
PY

new_env sqlalchemy extra:sqlalchemy
"$work/sqlalchemy/bin/python" -c 'import dewey.sqlalchemy'

new_env async extra:sqlalchemy extra:async
"$work/async/bin/python" -c 'import dewey.sqlalchemy.async_executor'

new_env django extra:django
"$work/django/bin/python" - <<'PY'
from django.conf import settings
settings.configure(
    DATABASES={"default": {"ENGINE": "django.db.backends.postgresql", "NAME": "unused"}},
    INSTALLED_APPS=["dewey.django"],
)
import django
django.setup()
from dewey.django import create_task
assert callable(create_task)
PY

echo "Optional import matrix passed."
