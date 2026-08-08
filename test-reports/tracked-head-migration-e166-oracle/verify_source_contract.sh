#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: verify_source_contract.sh SOURCE_ROOT [ANCHOR_SHA]}
ANCHOR=${2:-e1666edd0b4d814a88d985086ecc5a477b5d32e6}
ROOT=$(cd "$ROOT" && pwd)
SRC="$ROOT/packages/lix/src"
TESTS="$ROOT/packages/lix/tests"

[[ -d "$SRC" ]] || { echo "missing source root: $SRC" >&2; exit 2; }
actual=$(git -C "$ROOT" rev-parse HEAD)
git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" "$actual" || {
  echo "RED anchor: $ANCHOR is not an ancestor of $actual"
  exit 1
}

red=0
echo "anchor=$ANCHOR"
echo "head=$actual"

check_absent_path() {
  local path="$1"
  if [[ -e "$ROOT/$path" ]]; then
    echo "RED path-present: $path"
    red=1
  else
    echo "PASS path-absent: $path"
  fi
}

check_zero() {
  local label="$1"
  local root="$2"
  local needle="$3"
  local found
  found=$(rg -n -F --glob '*.rs' "$needle" "$root" || true)
  if [[ -n "$found" ]]; then
    echo "RED $label"
    printf '%s\n' "$found"
    red=1
  else
    echo "PASS $label"
  fi
}

check_zero_path() {
  local label="$1"
  local path="$2"
  local needle="$3"
  local found
  found=$(rg -n -F --glob '*.rs' "$needle" "$ROOT/$path" || true)
  if [[ -n "$found" ]]; then
    echo "RED $label"
    printf '%s\n' "$found"
    red=1
  else
    echo "PASS $label"
  fi
}

check_absent_path packages/lix/src/live_state/tracked_head.rs
check_absent_path packages/lix/src/live_state/tracked_head/hot.rs

for symbol in \
  TrackedHeadContext \
  HotStateTransactionCache \
  TrackedWorkingDiff \
  TrackedWorkingDiffEpoch \
  WorkingDiffIndexCoverage \
  CurrentStateDeltaRef \
  TrackedHeadDeltaRef \
  TRACKED_WORKING_DIFF_MARKER_ \
  stage_current_state_with_working_diff \
  stage_untracked_generation \
  working_diff_for_control \
  stage_collect_stale_current_state_generations \
  stage_collect_stale_working_diff_indexes; do
  check_zero "obsolete-source:$symbol" "$SRC" "$symbol"
  check_zero "obsolete-test:$symbol" "$TESTS" "$symbol"
done

check_zero_path "old-live-state-reexport" packages/lix/src/live_state/mod.rs tracked_head
check_zero_path "old-sql-working-diff-direct-reader" packages/lix/src/sql2/providers/working_diff.rs TrackedHeadContext
check_zero_path "old-sql-working-diff-fallback-reader" packages/lix/src/sql2/providers/working_diff.rs TrackedStateContext
check_zero_path "live-state-context-field" packages/lix/src/live_state/context.rs 'tracked_head:'

if (( red == 0 )); then
  echo GREEN
  exit 0
fi
echo RED
exit 1
