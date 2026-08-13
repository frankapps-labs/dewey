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
assert "huey" not in sys.modules
assert "sqlalchemy" not in sys.modules
PY

new_env sqlalchemy extra:sqlalchemy
"$work/sqlalchemy/bin/python" -c 'import dewey.sqlalchemy'

new_env async extra:sqlalchemy extra:async
"$work/async/bin/python" -c 'import dewey.sqlalchemy.async_executor'

new_env django extra:django
"$work/django/bin/python" - <<'PY'
try:
    import dewey.contrib.django_huey
except Exception as exc:
    assert "Huey" in str(exc) or "huey" in str(exc)
else:
    raise AssertionError("django-only contrib import should explain the missing Huey extra")
PY

new_env huey extra:huey
"$work/huey/bin/python" - <<'PY'
try:
    import dewey.contrib.django_huey
except ModuleNotFoundError as exc:
    assert "Django" in str(exc)
else:
    raise AssertionError("huey-only contrib import should explain the missing Django extra")
PY

new_env django_huey extra:django extra:huey
"$work/django_huey/bin/python" - <<'PY'
from huey import MemoryHuey
from django.conf import settings
settings.configure(
    DATABASES={"default": {"ENGINE": "django.db.backends.postgresql", "NAME": "unused"}},
    INSTALLED_APPS=["dewey.django"],
    HUEY=MemoryHuey("import-matrix", immediate=True),
    DEWEY={"DISPATCH": "dewey.contrib.django_huey.dispatch"},
)
import django
django.setup()
from dewey.contrib.django_huey import adapter, dispatch
assert adapter is not None and callable(dispatch)
PY

echo "Optional import matrix passed."
