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

for path in "${paths[@]}"; do
    if rg -n -F 'ForkTreeReadFacade::new' "$ROOT/$path" >/dev/null 2>&1; then
        emit_red local-forktree-facade "$path"
    else
        emit_pass no-local-forktree-facade "$path"
    fi
    if rg -n -F 'begin_read(' "$ROOT/$path" >/dev/null 2>&1; then
        emit_red local-begin-read "$path"
    else
        emit_pass no-local-begin-read "$path"
    fi
done

for path in \
    packages/lix/src/session/checkpoint.rs \
    packages/lix/src/sql2/providers/working_diff.rs \
    packages/lix/src/transaction/context.rs; do
    for symbol in BranchHeadControlContext TrackedHeadContext TrackedStateStoreReader \
        TrackedStateContext working_diff_at_head; do
        if rg -n -F "$symbol" "$ROOT/$path" >/dev/null 2>&1; then
            emit_red "legacy-$symbol" "$path"
        else
            emit_pass "no-legacy-$symbol" "$path"
        fi
    done
done

for path in \
    packages/lix/src/sql2/providers/checkpoint.rs \
    packages/lix/src/sql2/providers/filesystem_working_diff.rs \
    packages/lix/src/sql2/providers/working_diff.rs; do
    if rg -n -F 'query_source.forktree_reader' "$ROOT/$path" >/dev/null 2>&1; then
        emit_pass caller-owned-history-source "$path"
    else
        emit_red missing-caller-owned-history-source "$path"
    fi
    if rg -n -e 'query_source\.store' -e 'store: query_source\.store' "$ROOT/$path" >/dev/null 2>&1; then
        emit_red store-extracted-for-history "$path"
    else
        emit_pass no-store-history-extraction "$path"
    fi
done

if rg -n -F 'transaction.forktree_read_facade()' \
    "$ROOT/packages/lix/src/session/checkpoint.rs" >/dev/null 2>&1; then
    emit_pass checkpoint-retained-forktree-view packages/lix/src/session/checkpoint.rs
else
    emit_red missing-checkpoint-retained-forktree-view packages/lix/src/session/checkpoint.rs
fi

for needle in \
    'record.commit_id != reachable.commit.commit_id' \
    'row.commit_id != certified_commit_id' \
    'cycle encountered' \
    'references missing parent' \
    'BlobId::from_hex' \
    'load_bytes_many' \
    'directory parent cycle' \
    'missing from the authenticated history root'; do
    if rg -n -F "$needle" "$SRC/forktree/view.rs" "$SRC/sql2/history_route.rs" \
        "$SRC/sql2/providers/file_history.rs" "$SRC/sql2/providers/filesystem_history_path.rs" \
        "$SRC/sql2/providers/directory_history.rs" >/dev/null 2>&1; then
        emit_pass preserved-historical-check "$needle"
    else
        emit_red missing-historical-check "$needle"
    fi
done

for needle in \
    '|| Some(Vec::new())' \
    'blob_bytes.get(blob_hash).cloned().flatten()'; do
    if rg -n -F "$needle" "$ROOT/packages/lix/src/sql2/providers/file_history.rs" \
        >/dev/null 2>&1; then
        emit_red permissive-file-materialization "$needle"
    else
        emit_pass strict-file-materialization "$needle"
    fi
done

for needle in \
    'exactly one blob reference' \
    'missing authenticated blob payload' \
    'blob reference count'; do
    if rg -n -F "$needle" "$ROOT/packages/lix/src/sql2/providers/file_history.rs" \
        >/dev/null 2>&1; then
        emit_pass required-file-failure-contract "$needle"
    else
        emit_red missing-file-failure-contract "$needle"
    fi
done

if (( red == 0 )); then
    printf 'GREEN\n'
    exit 0
fi
printf 'RED\n'
exit 1
