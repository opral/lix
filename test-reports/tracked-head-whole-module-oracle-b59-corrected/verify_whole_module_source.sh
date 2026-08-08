#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: verify_whole_module_source.sh SOURCE_ROOT [ANCHOR_SHA]}
ANCHOR=${2:-b59e1f11a51153e0a787a81f0f25bf104d150aaf}
ROOT=$(cd "$ROOT" && pwd)
SRC="$ROOT/packages/lix/src"
LIX_TESTS="$ROOT/packages/lix/tests"
ENGINE_TESTS="$ROOT/packages/engine-benchmarks"
red=0

[[ -d "$SRC" && -d "$LIX_TESTS" && -d "$ENGINE_TESTS" ]] || {
  echo "RED missing scan root"; exit 2;
}
head=$(git -C "$ROOT" rev-parse HEAD)
git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" "$head" || {
  echo "RED anchor-not-in-source-lineage: $ANCHOR -> $head"; exit 1;
}
echo "anchor=$ANCHOR"
echo "head=$head"

relative() {
  sed "s#^${ROOT%/}/##" | sort
}

absent_path() {
  local path="$1"
  if [[ -e "$ROOT/$path" ]]; then
    echo "RED path-restored: $path"; red=1
  else
    echo "PASS path-absent: $path"
  fi
}

zero() {
  local label="$1" root="$2" needle="$3" found
  found=$(rg -n -F --glob '*.rs' "$needle" "$root" 2>/dev/null | relative || true)
  if [[ -n "$found" ]]; then
    echo "RED $label"
    printf '%s\n' "$found"
    red=1
  else
    echo "PASS $label"
  fi
}

for path in packages/lix/src/live_state/tracked_head.rs packages/lix/src/live_state/tracked_head/hot.rs; do
  absent_path "$path"
done

scan_roots=("$SRC" "$LIX_TESTS" "$ENGINE_TESTS")
for symbol in \
  TrackedHeadContext HotStateTransactionCache TrackedWorkingDiff \
  TrackedWorkingDiffEpoch WorkingDiffIndexCoverage CurrentStateDeltaRef \
  TrackedHeadDeltaRef TRACKED_WORKING_DIFF_MARKER_SPACE \
  TRACKED_WORKING_DIFF_MARKER_NAMESPACE stage_current_state_with_working_diff \
  stage_untracked_generation working_diff_for_control \
  stage_collect_stale_current_state_generations \
  stage_collect_stale_working_diff_indexes; do
  for root in "${scan_roots[@]}"; do
    zero "obsolete:$symbol:${root#"$ROOT/"}" "$root" "$symbol"
  done
done

for needle in \
  'crate::live_state::tracked_head' 'super::tracked_head' \
  'live_state::tracked_head' 'TrackedHeadContext::new' \
  'TrackedHeadContext::reader' 'with_opening_tracked_reader'; do
  for root in "${scan_roots[@]}"; do
    zero "obsolete-path-or-factory:${root#"$ROOT/"}" "$root" "$needle"
  done
done

zero "live-state-reexport" "$SRC/live_state/mod.rs" tracked_head
zero "live-state-field" "$SRC/live_state/context.rs" 'tracked_head:'
zero "sql-direct-old-reader" "$SRC/sql2/providers/working_diff.rs" TrackedHeadContext
zero "sql-fallback-old-reader" "$SRC/sql2/providers/working_diff.rs" TrackedStateContext
zero "transaction-reader-wrapper" "$SRC/transaction/context.rs" with_opening_tracked_reader
zero "merge-reconciliation-wrapper" "$SRC/session/merge/branch.rs" with_opening_tracked_reader

for path in \
  "$SRC/init.rs" "$SRC/gc.rs" "$SRC/functions/context.rs" \
  "$SRC/functions/state.rs" "$SRC/transaction/context.rs" \
  "$SRC/transaction/schema_resolver.rs" "$SRC/sql2/providers/working_diff.rs" \
  "$SRC/transaction/bench_support.rs" "$SRC/storage_bench.rs" \
  "$SRC/test_support.rs" "$SRC/session/merge/branch.rs"; do
  if [[ ! -f "$path" ]]; then
    echo "RED missing-cohort-path: ${path#"$ROOT/"}"; red=1
  else
    echo "PASS cohort-path: ${path#"$ROOT/"}"
  fi
done

changed=$(git -C "$ROOT" diff --name-only "$ANCHOR..$head")
if grep -E '^(packages/lix/src/sql2/providers/entity\.rs|packages/lix/src/live_state/forktree_reader\.rs|packages/lix/src/live_state/entity_columnar\.rs|packages/lix/src/sql2/entity_columnar_layout\.rs)$' <<<"$changed"; then
  echo "RED direct-public-sql-entity-pk-columnar-path-touched"
  red=1
else
  echo "PASS direct-public-sql-entity-pk-columnar-excluded"
fi

if (( red == 0 )); then
  echo GREEN
  exit 0
fi
echo RED
exit 1
