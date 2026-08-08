#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: source_gate.sh SOURCE_ROOT}
ROOT=$(cd "$ROOT" && pwd)
BASE=fd2be256d763f17e9f127d4c984e36fba191cb82
HEAD=$(git -C "$ROOT" rev-parse HEAD)
SCRIPT_DIR="$ROOT/test-reports/forktree-stage2-fd2-correction-oracle"
red=0

if git -C "$ROOT" merge-base --is-ancestor "$BASE" "$HEAD"; then
    printf 'PASS\tprovenance\tbase=%s head=%s\n' "$BASE" "$HEAD"
else
    printf 'RED\tprovenance\tbase=%s is not an ancestor of %s\n' "$BASE" "$HEAD"
    red=1
fi

for path in \
    packages/lix/src/sql2/providers/checkpoint.rs \
    packages/lix/src/sql2/providers/filesystem_working_diff.rs \
    packages/lix/src/sql2/providers/working_diff.rs \
    packages/lix/src/sql2/providers/file_history.rs; do
    if [[ -f "$ROOT/$path" ]]; then
        printf 'PASS\tpath-present\t%s\n' "$path"
    else
        printf 'RED\tpath-missing\t%s\n' "$path"
        red=1
    fi
done

if python3 "$SCRIPT_DIR/source_gate.py" "$ROOT"; then
    printf 'PASS\tstructural-source-oracle\t%s\n' "$SCRIPT_DIR"
else
    printf 'RED\tstructural-source-oracle\t%s\n' "$SCRIPT_DIR"
    red=1
fi

if command -v rustfmt >/dev/null 2>&1; then
    while IFS= read -r -d '' file; do
        if rustfmt --check --edition 2021 "$file" >/dev/null 2>&1; then
            printf 'PASS\trustfmt\t%s\n' "${file#$ROOT/}"
        else
            printf 'RED\trustfmt\t%s\n' "${file#$ROOT/}"
            red=1
        fi
    done < <(find "$SCRIPT_DIR/fixtures/readers" -type f -name '*.rs' -print0)
else
    printf 'RED\trustfmt\trustfmt unavailable\n'
    red=1
fi

if (( red == 0 )); then
    printf 'GREEN\n'
    exit 0
fi
printf 'RED\n'
exit 1
