#!/usr/bin/env bash
# Create an environment exclusively from a hash-locked uv export.
set -euo pipefail

if [[ $# -lt 3 ]]; then
  echo "usage: $0 ENV_DIR PYTHON group:NAME|extra:NAME [...]" >&2
  exit 2
fi

env_dir=$1
python=$2
shift 2
root=$(cd "$(dirname "$0")/.." && pwd)
requirements=$(mktemp)
trap 'rm -f "$requirements"' EXIT

args=(--locked --format requirements-txt --no-emit-project --no-default-groups)
for selector in "$@"; do
  case "$selector" in
    group:*) args+=(--group "${selector#group:}") ;;
    extra:*) args+=(--extra "${selector#extra:}") ;;
    *) echo "invalid selector: $selector" >&2; exit 2 ;;
  esac
done

uv export "${args[@]}" > "$requirements"
rm -rf "$env_dir"
uv venv --python "$python" "$env_dir" >/dev/null
uv pip sync --python "$env_dir/bin/python" --require-hashes --strict "$requirements"

printf 'admitted environment: %s (Python %s; selectors: %s)\n' \
  "$env_dir" "$python" "$*"
