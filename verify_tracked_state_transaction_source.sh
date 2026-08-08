#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:-}
ANCHOR=${2:-b59e1f11a51153e0a787a81f0f25bf104d150aaf}
LIX="$ROOT/packages/lix/src"

if [[ -z "$ROOT" || ! -e "$ROOT/.git" ]]; then
  echo "usage: verify_tracked_state_transaction_source.sh ROOT [ANCHOR]" >&2
  exit 2
fi
git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" HEAD

DIRECT=(
  transaction/context.rs
  transaction/context/cohort.rs
  transaction/commit.rs
  transaction/staging.rs
  transaction/stale_commit.rs
  transaction/validation.rs
  transaction/types.rs
  transaction/normalization.rs
  session/context.rs
  session/execute.rs
  session/checkpoint.rs
  session/undo_redo.rs
  session/idempotency.rs
  session/merge/analysis.rs
  session/merge/branch.rs
  sql2/providers/checkpoint.rs
  sql2/providers/diff.rs
  sql2/providers/working_diff.rs
  sql2/providers/directory_history.rs
  sql2/providers/file_history.rs
  sql2/providers/filesystem_working_diff.rs
  sql2/providers/entity.rs
  sql2/exec/bound_public_write.rs
  tracked_state/context.rs
  tracked_state/diff.rs
  tracked_state/mod.rs
  tracked_state/types.rs
)

for relative in "${DIRECT[@]}"; do
  test -f "$LIX/$relative" || {
    echo "missing direct-closure path: $relative" >&2
    exit 1
  }
done

declare -A FORBIDDEN=(
  [reader-type]='TrackedStateStoreReader|TrackedStateContext'
  [reader-factory]='tracked_state_reader|with_opening_tracked_reader|tracked_state[.]reader[(]|TrackedHeadContext'
  [branch-control]='BranchHeadControlContext|BranchHeadControlCache'
  [second-authority]='fallback|compatibility|rebuild_reader|reconstruct_reader|cache_as_authority'
)

status=0
for category in "${!FORBIDDEN[@]}"; do
  pattern="${FORBIDDEN[$category]}"
  while IFS= read -r line; do
    echo "forbidden[$category] $line"
    status=1
  done < <(rg -n --no-heading -E "$pattern" "${DIRECT[@]/#/$LIX/}" || true)
done

begin_read_elsewhere=0
for relative in "${DIRECT[@]}"; do
  file="$LIX/$relative"
  count=$(rg -n -c --no-heading 'storage[.]begin_read[(]|begin_read[(]' "$file" || true)
  count=${count:-0}
  if [[ "$relative" != "transaction/context.rs" && "$count" != "0" ]]; then
    echo "unowned begin_read in $relative: $count"
    begin_read_elsewhere=1
  fi
done
if (( begin_read_elsewhere != 0 )); then
  status=1
fi

REQUIRED=(
  CoherentView
  open_coherent_view
  view_id
  PreparedPublication
  into_storage_plan
  prepare_write_set
  '.commit('
  state_point
  state_range
  checkpoint_root
  idempotency
)
for token in "${REQUIRED[@]}"; do
  if ! rg -n --no-heading -F "$token" "${DIRECT[@]/#/$LIX/}" >/dev/null; then
    echo "missing required ForkTree transaction token: $token"
    status=1
  fi
done

for path in \
  tracked_state/context.rs \
  tracked_state/diff.rs \
  tracked_state/mod.rs \
  tracked_state/types.rs; do
  if test -e "$LIX/$path"; then
    echo "legacy tracked-state owner path still present: $path"
    status=1
  fi
done

for path in \
  tracked_state/storage.rs \
  tracked_state/codec.rs \
  tracked_state/tree.rs \
  tracked_state/replacement_part.rs; do
  if test -e "$LIX/$path"; then
    echo "legacy tracked-state storage path still present: $path"
    status=1
  fi
done

if (( status != 0 )); then
  echo "BLOCKED: b59 still contains the tracked-state reader/publication closure" >&2
  exit 1
fi
echo "PASS: ForkTree owns the transaction read/reconcile/transition closure"
