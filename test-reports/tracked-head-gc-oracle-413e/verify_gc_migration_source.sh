#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: verify_gc_migration_source.sh SOURCE_ROOT [ANCHOR] [WHOLE_GATE] [BASE]}
ANCHOR=${2:-413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d}
GATE=${3:-0b9ab47f7ac7da600b215d0b8aa2ab04db6bd36d}
BASE=${4:-}
ROOT=$(cd "$ROOT" && pwd)
SRC="$ROOT/packages/lix/src"
GC="$SRC/gc.rs"
red=0

[[ -f "$GC" ]] || { echo "missing GC source: $GC" >&2; exit 2; }
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

zero_region() {
  local label="$1"
  local start="$2"
  local end="$3"
  shift 3
  local region
  region=$(sed -n "/$start/,/$end/p" "$GC" || true)
  for needle in "$@"; do
    if printf '%s\n' "$region" | rg -n -F "$needle" >/dev/null 2>&1; then
      echo "RED $label:$needle"
      printf '%s\n' "$region" | rg -n -F "$needle"
      red=1
    else
      echo "PASS $label:$needle"
    fi
  done
}

absent_path() {
  local path="$1"
  if [[ -e "$ROOT/$path" ]]; then
    echo "RED obsolete-path-present: $path"
    red=1
  else
    echo "PASS obsolete-path-absent: $path"
  fi
}

absent_path packages/lix/src/live_state/tracked_head.rs
absent_path packages/lix/src/live_state/tracked_head/hot.rs

old_closure=(
  TrackedHeadContext
  tracked_serving_commit_dependencies
  untracked_json_refs
  stage_collect_stale_current_state_generations
  stage_collect_stale_working_diff_indexes
  'tracked_reachability('
  TRACKED_WORKING_DIFF_MARKER_SPACE
  CURRENT_STATE_DATA_PART_SPACE
  CURRENT_STATE_DATA_PART_REFS_SPACE
  TrackedStateContext
)

zero_region root-observation \
  'async fn authenticated_control_commit_reachability' '^async fn ' \
  "${old_closure[@]}"
zero_region native-part-validation \
  'async fn validate_live_native_parts' '^async fn ' \
  "${old_closure[@]}"
zero_region recovery-current-generation \
  'async fn stage_repository_gc_full_recovery' '^async fn ' \
  "${old_closure[@]}"

if [[ -n "$BASE" ]]; then
  while IFS= read -r path; do
    case "$path" in
      packages/lix/src/gc.rs|\
      packages/lix/src/forktree/view.rs|\
      packages/lix/src/forktree/model.rs|\
      packages/lix/src/forktree/state.rs|\
      packages/lix/src/forktree/serving.rs|\
      packages/lix/src/forktree/publication.rs|\
      packages/lix/src/forktree/mod.rs|\
      packages/engine-benchmarks/tests/tracked-head-gc-migration-oracle.rs|\
      packages/lix/tests/tracked-head-gc-migration-oracle.rs|\
      test-reports/tracked-head-gc-oracle-413e/*)
        echo "PASS allowed-path:$path" ;;
      *)
        echo "RED out-of-scope-path:$path"
        red=1 ;;
    esac
  done < <(git -C "$ROOT" diff --name-only "$BASE..$head")
fi

if (( red == 0 )); then
  echo GREEN
  exit 0
fi
echo RED
exit 1
