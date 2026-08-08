#!/usr/bin/env bash
set -euo pipefail

base="$1"
head="$2"
repo="$3"

cd "$repo"
test "$(git rev-parse "$head^")" = "$(git rev-parse "$base")" ||
  { echo "BLOCKER: successor parent is not exact base" >&2; exit 2; }

allowed='^packages/lix/src/(live_state/(context|derived|reader|types|visibility)|tracked_state/(context|diff|row_materialization|types))\.rs$'
bad_paths=$(git diff --name-only "$base" "$head" -- packages/lix/src |
  grep -Ev "$allowed" || true)
if test -n "$bad_paths"; then
  echo "BLOCKER: changed production paths outside reader closure:" >&2
  echo "$bad_paths" >&2
  exit 2
fi

added=$(git diff --unified=0 "$base" "$head" -- packages/lix/src |
  sed -n '/^+/p' | sed '/^+++/d')

if printf '%s\n' "$added" | rg -n 'begin_read|StorageWriteSet|\.put\(|\.delete\(|\.write\(|selector|epoch|receipt|gc_progress' >/tmp/current-state-reader-source-reds.$$; then
  echo "BLOCKER: added reader code contains a read/write-boundary token:" >&2
  cat /tmp/current-state-reader-source-reds.$$ >&2
  rm -f /tmp/current-state-reader-source-reds.$$
  exit 2
fi

forbidden='BranchHeadControl|BranchHeadControlContext|TrackedHeadContext|CurrentStateDeltaRef|CertifiedCurrentStatePredecessor|TrackedWorkingDiff|EntityColumnarOverlayRow|columnar_row_group|current_state_envelope|scoped_range|mutation_directory|replacement_part|commit_root_rebuild|COMMIT_CHANGE_ID_SPACE|CHANGE_SPACE|COMMIT_SPACE|TRACKED_STATE_TREE_CHUNK_SPACE|CURRENT_STATE_DATA_PART_SPACE|MUTATION_DIRECTORY_NODE_SPACE|StorageSpace::mutable'
if printf '%s\n' "$added" | rg -n "$forbidden" >/tmp/current-state-reader-forbidden.$$; then
  echo "BLOCKER: added source mentions deleted owner/space; classify negative tests explicitly:" >&2
  cat /tmp/current-state-reader-forbidden.$$ >&2
  rm -f /tmp/current-state-reader-forbidden.$$
  exit 2
fi

rm -f /tmp/current-state-reader-source-reds.$$ /tmp/current-state-reader-forbidden.$$
echo "SOURCE_CONTRACT_PASS base=$(git rev-parse "$base") head=$(git rev-parse "$head")"
