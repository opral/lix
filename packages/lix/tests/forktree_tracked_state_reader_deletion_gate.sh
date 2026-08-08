#!/usr/bin/env bash
set -u -o pipefail

# Source-only whole-module deletion gate. The candidate is a disposable
# worktree; this script never edits, formats, compiles, or runs it.
candidate_root="${1:?candidate worktree path required}"
expected_head="${2:?expected candidate commit required}"
expected_tree="${3:?expected candidate tree required}"
oracle_root="$(git rev-parse --show-toplevel)"
src="$candidate_root/packages/lix/src"
probe_dir="$oracle_root/packages/lix/tests"
fail=0

print_first_matches() {
  local text="$1"
  local count=0
  local line
  while IFS= read -r line; do
    printf '%s\n' "$line"
    count=$((count + 1))
    if [[ "$count" -ge 20 ]]; then
      break
    fi
  done <<< "$text"
}

record_forbidden() {
  local pattern="$1"
  local matches count
  matches="$(rg -n --no-heading -g '*.rs' -F "$pattern" "$src" 2>/dev/null || true)"
  if [[ -n "$matches" ]]; then
    count="$(printf '%s\n' "$matches" | awk 'NF { n++ } END { print n + 0 }')"
    printf 'FAIL forbidden=%s count=%s\n%s\n' "$pattern" "$count" "$(print_first_matches "$matches")"
    fail=1
  else
    printf 'PASS absent=%s\n' "$pattern"
  fi
}

record_cohort() {
  local cohort="$1"
  shift
  printf 'COHORT=%s\n' "$cohort"
  for relative in "$@"; do
    local path="$src/$relative"
    local matches count
    if [[ ! -f "$path" ]]; then
      printf 'FILE=%s count=0 status=missing\n' "$relative"
      continue
    fi
    matches="$(rg -n --no-heading -e 'TrackedStateStoreReader|TrackedStateContext|crate::tracked_state|use crate::tracked_state|tracked_state::' "$path" 2>/dev/null || true)"
    if [[ -n "$matches" ]]; then
      count="$(printf '%s\n' "$matches" | awk 'NF { n++ } END { print n + 0 }')"
      printf 'FILE=%s count=%s\n%s\n' "$relative" "$count" "$matches"
    else
      printf 'FILE=%s count=0\n' "$relative"
    fi
  done
}

printf 'ForkTree TrackedStateStoreReader/module deletion gate\n'
actual_head="$(git -C "$candidate_root" rev-parse HEAD 2>/dev/null || true)"
actual_tree="$(git -C "$candidate_root" rev-parse HEAD^{tree} 2>/dev/null || true)"
printf 'CANDIDATE_HEAD=%s\nCANDIDATE_TREE=%s\n' "$actual_head" "$actual_tree"
if [[ "$actual_head" != "$expected_head" ]]; then
  printf 'FAIL head-mismatch expected=%s actual=%s\n' "$expected_head" "$actual_head"
  fail=1
fi
if [[ "$actual_tree" != "$expected_tree" ]]; then
  printf 'FAIL tree-mismatch expected=%s actual=%s\n' "$expected_tree" "$actual_tree"
  fail=1
fi
if [[ ! -d "$src" ]]; then
  printf 'FAIL missing-source-root=%s\n' "$src"
  exit 1
fi

for pattern in \
  'TrackedStateStoreReader' \
  'TrackedStateReaderAdapter' \
  'TrackedStateReaderWrapper' \
  'TrackedStateReaderCompat' \
  'tracked_state_reader_adapter' \
  'tracked_state_reader_wrapper' \
  'tracked_state_reader_compat' \
  'tracked_state_reader_fallback' \
  'tracked_state_reader_migration' \
  'columnar_history_fallback' \
  'tracked_state_compat'; do
  record_forbidden "$pattern"
done

for pattern in \
  'TRACKED_STATE_TREE_CHUNK_SPACE' \
  'TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE' \
  'TRACKED_STATE_CHANGE_LOCATOR_SPACE' \
  'TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE' \
  'TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE' \
  'MUTATION_DIRECTORY_NODE_SPACE' \
  'SCOPED_RANGE_NODE_SPACE' \
  'CURRENT_STATE_DATA_PART_SPACE' \
  'CURRENT_STATE_DATA_PART_REFS_SPACE' \
  'CERTIFIED_ENTITY_BATCH_SPACE' \
  'CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE' \
  'CERTIFIED_ENTITY_BATCH_PAGE_SPACE' \
  'ROW_GROUP_MANIFEST_SPACE' \
  'ROW_GROUP_COLUMN_SPACE' \
  'PACKED_CURRENT_BASE_SPACE' \
  'PACKED_CURRENT_BASE_CONTROL_SPACE' \
  'PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE' \
  'ROOT_CURRENT_BASE_SPACE'; do
  record_forbidden "$pattern"
done

for path in \
  "$src/tracked_state/diff.rs" \
  "$src/tracked_state/diff_id.rs" \
  "$src/tracked_state/merge.rs" \
  "$src/tracked_state/row_materialization.rs"; do
  if [[ -e "$path" ]]; then
    printf 'FAIL reader-only-module-exists=%s\n' "$path"
    fail=1
  else
    printf 'PASS reader-only-module-absent=%s\n' "$path"
  fi
done

for pattern in 'pub(crate) use diff' 'pub(crate) use diff_id' 'pub(crate) use merge' 'pub(crate) use row_materialization'; do
  record_forbidden "$pattern"
done

for required in \
  'open_coherent_view' \
  'state_point' \
  'state_range' \
  'load_commit_topologies' \
  'load_commit_member_records' \
  'validate_commit_topology' \
  'load_branch_head'; do
  if rg -n --no-heading -g '*.rs' -F "$required" "$src/forktree" >/dev/null 2>&1; then
    printf 'PASS required-forktree=%s\n' "$required"
  else
    printf 'FAIL missing-forktree=%s\n' "$required"
    fail=1
  fi
done

# Historical fail-closed prerequisite: derived/history reads must not degrade
# to an empty current-state success while their ForkTree owner is pending.
derived="$src/live_state/derived.rs"
for required in 'request_may_include_derived' 'is_derived_schema' 'fail closed'; do
  if [[ -f "$derived" ]] && rg -n --no-heading -F "$required" "$derived" >/dev/null 2>&1; then
    printf 'PASS historical-fail-closed=%s\n' "$required"
  else
    printf 'FAIL missing-historical-fail-closed=%s\n' "$required"
    fail=1
  fi
done

record_cohort checkpoint \
  checkpoint.rs session/checkpoint.rs sql2/providers/checkpoint.rs
record_cohort history \
  sql2/history_route.rs sql2/providers/change.rs sql2/providers/file_history.rs \
  sql2/providers/directory_history.rs
record_cohort sql_diff \
  sql2/providers/diff.rs sql2/providers/working_diff.rs \
  sql2/providers/filesystem_working_diff.rs
record_cohort merge_analysis \
  session/merge/analysis.rs session/merge/branch.rs session/merge/conflicts.rs \
  session/merge/stats.rs
record_cohort transaction_reconciliation_undo \
  transaction/context.rs session/undo_redo.rs

for probe in \
  "$probe_dir/forktree_tracked_state_forbidden_reader.rs" \
  "$probe_dir/forktree_tracked_state_forbidden_space.rs"; do
  if [[ ! -s "$probe" ]] || ! rg -q -F 'EXPECT_COMPILE_FAIL' "$probe"; then
    printf 'FAIL missing-or-unmarked-probe=%s\n' "$probe"
    fail=1
  else
    printf 'PASS compile-fail-probe-present=%s\n' "$probe"
  fi
done

if [[ "$fail" -ne 0 ]]; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
