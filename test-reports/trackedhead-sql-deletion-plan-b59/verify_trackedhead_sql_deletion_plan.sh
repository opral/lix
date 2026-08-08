#!/usr/bin/env bash
set -euo pipefail

if test "$#" -lt 1; then
  printf 'candidate worktree required\n' >&2
  exit 2
fi
ROOT="$1"
ANCHOR="b59e1f11a51153e0a787a81f0f25bf104d150aaf"
ORACLE="1d9c47728377c6ec7d2646704d51f3aadb11c773"
if test "$#" -ge 2; then ANCHOR="$2"; fi
if test "$#" -ge 3; then ORACLE="$3"; fi

cd "$ROOT"
git rev-parse --is-inside-work-tree >/dev/null
git merge-base --is-ancestor "$ANCHOR" HEAD
git merge-base --is-ancestor "$ORACLE" HEAD

status=0
say() { printf '%s\n' "$*"; }
zero() {
  needle="$1"
  tmp="/tmp/trackedhead-plan-residue.$$"
  if rg -n -F --hidden --glob '!target/**' --glob '!*.lock' "$needle" \
      packages/lix/src packages/lix/tests packages/engine-benchmarks >"$tmp" 2>/dev/null
  then
    say "FORBIDDEN $needle"
    cat "$tmp"
    status=1
  fi
  rm -f "$tmp"
}

for path in \
  packages/lix/src/tracked_state/context.rs \
  packages/lix/src/tracked_state/diff.rs \
  packages/lix/src/live_state/tracked_head.rs \
  packages/lix/src/live_state/tracked_head/hot.rs
do
  if test -e "$path"; then say "FORBIDDEN_PATH $path"; status=1; fi
done

for token in \
  TrackedHeadContext HotStateTransactionCache TrackedWorkingDiff \
  TrackedWorkingDiffEpoch WorkingDiffIndexCoverage CurrentStateDeltaRef \
  TrackedHeadDeltaRef TRACKED_WORKING_DIFF_MARKER_SPACE \
  with_opening_tracked_reader load_exact_batch_via_scan_for_test \
  stage_current_state_with_working_diff stage_untracked_generation \
  stage_collect_stale_current_state_generations \
  stage_collect_stale_working_diff_indexes
do
  zero "$token"
done

for token in CoherentView open_coherent_view view_id state_point state_range \
  PreparedPublication into_storage_plan prepare_write_set checkpoint_root generation
do
  if ! rg -n -F --hidden --glob '!target/**' "$token" packages/lix/src >/dev/null; then
    say "MISSING_REQUIRED $token"
    status=1
  fi
done

for path in \
  packages/lix/src/sql2/providers/entity.rs \
  packages/lix/src/sql2/entity_batch.rs \
  packages/lix/src/sql2/entity_columnar_layout.rs
do
  if git diff --name-only "$ANCHOR..HEAD" -- "$path" | rg -q .; then
    say "UNBOUND_SQL_BLOCKER_CHANGED $path"
    status=1
  fi
done

say "anchor=$ANCHOR oracle=$ORACLE head=$(git rev-parse HEAD)"
if test "$status" -eq 0; then
  say "GREEN source deletion contract"
else
  say "RED source deletion contract"
fi
exit "$status"
