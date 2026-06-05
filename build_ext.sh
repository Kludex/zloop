#!/usr/bin/env bash
# Build the zloop Zig extension against the interpreter that will import it.
# Usage: ./build_ext.sh [path-to-python]   (defaults to the uvicorn venv python)
set -euo pipefail

PY="${1:-/Users/marcelotryle/dev/encode/zloop/.uvicorn-upstream/.venv/bin/python}"
cd "$(dirname "$0")"

read -r INCLUDE SUFFIX < <("$PY" -c \
  'import sysconfig as s; print(s.get_path("platinclude"), s.get_config_var("EXT_SUFFIX"))')

export ZLOOP_PYTHON_INCLUDE="$INCLUDE"
export ZLOOP_EXT_SUFFIX="$SUFFIX"

MODE="${ZLOOP_BUILD_MODE:-ReleaseFast}"
zig build "-Doptimize=$MODE" "$@" 2>/dev/null || zig build "-Doptimize=$MODE"
echo "built zloop/_zloop$SUFFIX against $("$PY" --version) ($INCLUDE)"
