#!/usr/bin/env bash
set -euo pipefail

ROOT=${1:?usage: verify_source_contract.sh WORKTREE TARGET_COMMIT}
TARGET=${2:?usage: verify_source_contract.sh WORKTREE TARGET_COMMIT}
cd "$ROOT"
git cat-file -e "${TARGET}^{commit}"

source_at() { git show "${TARGET}:$1"; }
has_source() {
  local path=$1 pattern=$2
  source_at "$path" | rg -F -- "$pattern" >/dev/null
}
count_source() {
  local path=$1 pattern=$2
  local count
  count=$(source_at "$path" | rg -F -c -- "$pattern" || true)
  printf '%s\n' "${count:-0}"
}
extract_function() {
  local path=$1 name=$2
  source_at "$path" | awk -v needle="$name" '
    !started && index($0, needle) { started=1 }
    started && !finished {
      line=$0
      opens=gsub(/\{/, "{", line)
      closes=gsub(/\}/, "}", line)
      depth += opens - closes
      print
      if (started && depth == 0 && opens > 0) finished=1
    }
  '
}
check_present() {
  local path=$1
  if git cat-file -e "${TARGET}:${path}"; then
    echo "ALLOWLIST_PRESENT=${path}"
  else
    echo "RED_MISSING_ALLOWLIST_PATH=${path}"
    red=$((red + 1))
  fi
}
check_required() {
  local path=$1 pattern=$2
  if has_source "$path" "$pattern"; then
    echo "PASS_SOURCE=${pattern}"
  else
    echo "RED_MISSING_SOURCE=${pattern}"
    red=$((red + 1))
  fi
}
check_forbidden() {
  local path=$1 label=$2 pattern=$3
  local count
  count=$(count_source "$path" "$pattern")
  echo "${label}=${count}"
  if (( count > 0 )); then
    echo "RED_FORBIDDEN=${label}"
    red=$((red + 1))
  fi
}

red=0
echo "TARGET=${TARGET}"
echo "TREE=$(git rev-parse "${TARGET}^{tree}")"
echo "ANCHOR_SOURCE_SCOPE=W1b-4 checkpoint/history reconstruction only"
echo "ALLOWLIST_PATHS=5"
check_present packages/lix/src/forktree/view.rs
check_present packages/lix/src/forktree/serving.rs
check_present packages/lix/src/forktree/tests.rs
check_present packages/lix/src/sql2/providers/checkpoint.rs
check_present packages/lix/src/transaction/context.rs

checkpoint_body=$(extract_function packages/lix/src/transaction/context.rs execute_checkpoint_selection)
facade_calls=$(printf '%s\n' "$checkpoint_body" | rg -F -c -- 'forktree_read_facade()' || true)
echo "CHECKPOINT_SELECTION_FACADE_CALLS=${facade_calls}"
if (( facade_calls > 1 )); then
  echo "RED_MULTIPLE_FACADE_CONSTRUCTION=execute_checkpoint_selection"
  red=$((red + 1))
else
  echo "PASS_SINGLE_FACADE_CONSTRUCTION=execute_checkpoint_selection"
fi

check_required packages/lix/src/forktree/view.rs 'pub(crate) struct ForkTreeReadFacade'
check_required packages/lix/src/forktree/view.rs 'checkpoint_history_from_head'
check_required packages/lix/src/forktree/view.rs 'checkpoint_marker_matches_commit'
check_required packages/lix/src/sql2/providers/checkpoint.rs 'ForkTreeReadFacade'

check_forbidden packages/lix/src/sql2/providers/checkpoint.rs CHECKPOINT_PROVIDER_TRACKED_STATE TrackedStateStoreReader
check_forbidden packages/lix/src/sql2/providers/checkpoint.rs CHECKPOINT_PROVIDER_TRACKED_READER tracked_state_reader
check_forbidden packages/lix/src/sql2/providers/checkpoint.rs CHECKPOINT_PROVIDER_BEGIN_READ begin_read
check_forbidden packages/lix/src/sql2/providers/checkpoint.rs CHECKPOINT_PROVIDER_JSON_READER JsonStoreReader
check_forbidden packages/lix/src/forktree/view.rs FORKTREE_VIEW_TRACKED_STATE TrackedStateStoreReader
check_forbidden packages/lix/src/forktree/view.rs FORKTREE_VIEW_JSON_READER JsonStoreReader
check_forbidden packages/lix/src/forktree/view.rs FORKTREE_VIEW_COMMIT_GRAPH_READER commit_graph_reader

view_begin_reads=$(count_source packages/lix/src/forktree/view.rs 'storage.begin_read(')
echo "FORKTREE_VIEW_CANONICAL_STORAGE_BEGIN_READ=${view_begin_reads}"
if (( view_begin_reads > 1 )); then
  echo "RED_FORKTREE_VIEW_MULTIPLE_CANONICAL_BEGIN_READ"
  red=$((red + 1))
else
  echo "PASS_FORKTREE_VIEW_CANONICAL_BEGIN_READ"
fi

legacy_count=$(git grep -n -E 'TrackedStateStoreReader|tracked_state_reader' "${TARGET}" -- packages/lix/src 2>/dev/null | wc -l | tr -d ' ' || true)
echo "WORKSPACE_LEGACY_TRACKED_READER_REFERENCES=${legacy_count}"
if (( legacy_count > 0 )); then
  echo "RED_LEGACY_TRACKED_READER_DELETION_REMAINS_REQUIRED"
  red=$((red + 1))
else
  echo "PASS_LEGACY_TRACKED_READER_ABSENT"
fi

if (( red == 0 )); then
  echo "RESULT=GREEN"
  exit 0
fi
echo "RESULT=EXPECTED_RED"
echo "RED_COUNT=${red}"
exit 1
