#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: source_gate.sh SOURCE_ROOT}
ROOT=$(cd "$ROOT" && pwd)
BASE=cd91b9b90f7f468158b4df154adbed9551eb5d60
HEAD=$(git -C "$ROOT" rev-parse HEAD)
SRC="$ROOT/packages/lix/src"
red=0

emit_red() {
    printf 'RED\t%s\t%s\n' "$1" "$2"
    red=1
}

emit_pass() {
    printf 'PASS\t%s\t%s\n' "$1" "$2"
}

if ! git -C "$ROOT" merge-base --is-ancestor "$BASE" "$HEAD"; then
    emit_red provenance "baseline $BASE is not an ancestor of $HEAD"
else
    emit_pass provenance "baseline=$BASE head=$HEAD"
fi

paths=(
    packages/lix/src/session/checkpoint.rs
    packages/lix/src/sql2/providers/checkpoint.rs
    packages/lix/src/sql2/providers/filesystem_working_diff.rs
    packages/lix/src/sql2/providers/working_diff.rs
)

for path in "${paths[@]}"; do
    if [[ -f "$ROOT/$path" ]]; then
        emit_pass path-present "$path"
    else
        emit_red path-missing "$path"
    fi
done

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
if python3 "$SCRIPT_DIR/structural_source_gate.py" \
    "$ROOT" "$SCRIPT_DIR/source_negative_fixtures"; then
    emit_pass structural-caller-and-materialization-gate "$SCRIPT_DIR"
else
    emit_red structural-caller-and-materialization-gate "$SCRIPT_DIR"
fi

if (( red == 0 )); then
    printf 'GREEN\n'
    exit 0
fi
printf 'RED\n'
exit 1
