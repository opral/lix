#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: verify_transaction_migration_source.sh SOURCE_ROOT [ANCHOR_SHA]}
ANCHOR=${2:-413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d}
GATE=${3:-0b9ab47f7ac7da600b215d0b8aa2ab04db6bd36d}
ROOT=$(cd "$ROOT" && pwd)
SRC="$ROOT/packages/lix/src"
TX="$SRC/transaction"
red=0

[[ -d "$TX" ]] || { echo "missing transaction source: $TX" >&2; exit 2; }
head=$(git -C "$ROOT" rev-parse HEAD)
git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" "$head" || {
  echo "RED anchor-not-in-lineage: $ANCHOR -> $head"
  exit 1
}
if ! git -C "$ROOT" cat-file -e "$GATE^{commit}" 2>/dev/null; then
  echo "RED prerequisite-gate-object-missing: $GATE"
  exit 1
fi
echo "anchor=$ANCHOR"
echo "whole_module_gate=$GATE"
echo "head=$head"

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

absent() {
  local path="$1"
  if [[ -e "$ROOT/$path" ]]; then
    echo "RED obsolete-path-present: $path"
    red=1
  else
    echo "PASS obsolete-path-absent: $path"
  fi
}

absent packages/lix/src/live_state/tracked_head.rs
absent packages/lix/src/live_state/tracked_head/hot.rs

for symbol in \
  TrackedHeadContext \
  TrackedWorkingDiff \
  HotStateTransactionCache \
  working_diff_for_control \
  stage_current_state_with_working_diff \
  stage_untracked_generation \
  TRACKED_WORKING_DIFF_MARKER_SPACE \
  TRACKED_WORKING_DIFF_MARKER_NAMESPACE; do
  zero "transaction-legacy:$symbol" "$TX" "$symbol"
done

zero "transaction-legacy-reader" "$TX/context.rs" 'TrackedStateContext::new().reader'
zero "transaction-direct-legacy-reader" "$TX/context.rs" 'tracked_head.reader'
zero "transaction-direct-legacy-writer" "$TX/context.rs" 'tracked_head.writer'
zero "transaction-direct-old-diff" "$TX/context.rs" 'working_diff_for_control'

if [[ -f "$TX/context.rs" ]]; then
  direct_region=$(sed -n '/working_diff_at_head/,/Creates a commit-graph reader/p' "$TX/context.rs" || true)
  if printf '%s\n' "$direct_region" | rg -n 'TrackedHead|TrackedStateContext|tracked_head|tracked_state' >/tmp/tracked_head_tx_region.$$ 2>/dev/null; then
    echo "RED working-diff-caller-closure-legacy-access"
    cat /tmp/tracked_head_tx_region.$$
    red=1
  else
    echo "PASS working-diff-caller-closure-legacy-access"
  fi
  rm -f /tmp/tracked_head_tx_region.$$
fi

if (( red == 0 )); then
  echo GREEN
  exit 0
fi
echo RED
exit 1
