#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: verify_whole_module_source.sh SOURCE_ROOT [ANCHOR_SHA]}
ANCHOR=${2:-b59e1f11a51153e0a787a81f0f25bf104d150aaf}
ROOT=$(cd "$ROOT" && pwd)
SRC="$ROOT/packages/lix/src"
TESTS="$ROOT/packages/lix/tests"
red=0

[[ -d "$SRC" ]] || { echo "missing source root: $SRC" >&2; exit 2; }
head=$(git -C "$ROOT" rev-parse HEAD)
git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" "$head" || {
  echo "RED anchor-not-in-source-lineage: $ANCHOR -> $head"
  exit 1
}
echo "anchor=$ANCHOR"
echo "head=$head"

absent_path() {
  local path="$1"
  if [[ -e "$ROOT/$path" ]]; then
    echo "RED path-restored: $path"
    red=1
  else
    echo "PASS path-absent: $path"
  fi
}

zero() {
  local label="$1"
  local root="$2"
  local needle="$3"
  local found
  found=$(rg -n -F --glob '*.rs' "$needle" "$root" 2>/dev/null || true)
  if [[ -n "$found" ]]; then
    echo "RED $label"
    printf '%s\n' "$found"
    red=1
  else
    echo "PASS $label"
  fi
}

absent_path packages/lix/src/live_state/tracked_head.rs
absent_path packages/lix/src/live_state/tracked_head/hot.rs

for symbol in \
  TrackedHeadContext \
  HotStateTransactionCache \
  TrackedWorkingDiff \
  TrackedWorkingDiffEpoch \
  WorkingDiffIndexCoverage \
  CurrentStateDeltaRef \
  TrackedHeadDeltaRef \
  TRACKED_WORKING_DIFF_MARKER_SPACE \
  TRACKED_WORKING_DIFF_MARKER_NAMESPACE \
  stage_current_state_with_working_diff \
  stage_untracked_generation \
  working_diff_for_control \
  stage_collect_stale_current_state_generations \
  stage_collect_stale_working_diff_indexes; do
  zero "obsolete-production:$symbol" "$SRC" "$symbol"
  zero "obsolete-compiled-tests:$symbol" "$TESTS" "$symbol"
done

zero "old-module-path" "$SRC" 'crate::live_state::tracked_head'
zero "old-module-path-super" "$SRC" 'super::tracked_head'
zero "old-module-path-qualified" "$SRC" 'live_state::tracked_head'
zero "old-live-state-reexport" "$SRC/live_state/mod.rs" tracked_head
zero "old-live-state-field" "$SRC/live_state/context.rs" 'tracked_head:'
zero "sql-direct-old-reader" "$SRC/sql2/providers/working_diff.rs" TrackedHeadContext
zero "sql-fallback-old-reader" "$SRC/sql2/providers/working_diff.rs" TrackedStateContext
zero "sql-working-diff-reader-wrapper" "$SRC/sql2/providers/working_diff.rs" with_opening_tracked_reader
zero "transaction-reader-wrapper" "$SRC/transaction/context.rs" with_opening_tracked_reader
zero "tracked-head-reader-wrapper" "$SRC" 'TrackedHeadContext::new().reader('

for pattern in \
  'TRACKED_WORKING_DIFF_MARKER_SPACE' 'TRACKED_WORKING_DIFF_MARKER_NAMESPACE' \
  'HotStateTransactionCache' 'TrackedWorkingDiffEpoch' \
  'WorkingDiffIndexCoverage' 'CurrentStateDeltaRef' 'TrackedHeadDeltaRef'; do
  zero "obsolete-owner-or-space:$pattern" "$SRC" "$pattern"
done

for path in \
  "$SRC/live_state/tracked_head.rs" "$SRC/live_state/tracked_head/hot.rs"; do
  absent_path "${path#"$ROOT/"}"
done

for path in \
  "$SRC/init.rs" "$SRC/gc.rs" "$SRC/functions/context.rs" \
  "$SRC/functions/state.rs" "$SRC/transaction/context.rs" \
  "$SRC/transaction/schema_resolver.rs" "$SRC/sql2/providers/working_diff.rs" \
  "$SRC/transaction/bench_support.rs" "$SRC/storage_bench.rs" \
  "$SRC/test_support.rs"; do
  if [[ ! -f "$path" ]]; then
    echo "RED missing-cohort-path: ${path#"$ROOT/"}"
    red=1
  else
    echo "PASS cohort-path: ${path#"$ROOT/"}"
  fi
done

if (( red == 0 )); then
  echo GREEN
  exit 0
fi
echo RED
exit 1
