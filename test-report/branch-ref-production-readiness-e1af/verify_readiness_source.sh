#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo 'usage: verify_readiness_source.sh <base-root> <base-commit> <candidate-root> <candidate-commit>' >&2
  exit 2
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$script_dir/verify_selector_readiness.py" "$@"
