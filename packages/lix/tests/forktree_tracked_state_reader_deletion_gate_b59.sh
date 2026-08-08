#!/usr/bin/env bash
set -u -o pipefail

# Test/report-only source gate. It never edits, builds, or runs the candidate.
candidate_root="${1:?candidate worktree path required}"
expected_head="${2:?expected candidate commit required}"
expected_tree="${3:?expected candidate tree required}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
src="$candidate_root/packages/lix/src"
fail=0

first_matches() {
  local text="$1" line count=0
  while IFS= read -r line; do
    printf '%s\n' "$line"
    count=$((count + 1))
    [[ "$count" -ge 20 ]] && break
  done <<< "$text"
}

forbid() {
  local pattern="$1" matches count
  matches="$(rg -n --no-heading -g '*.rs' -F "$pattern" "$src" 2>/dev/null || true)"
  if [[ -n "$matches" ]]; then
    count="$(printf '%s\n' "$matches" | awk 'NF { n++ } END { print n + 0 }')"
    printf 'FAIL forbidden=%s count=%s\n%s\n' "$pattern" "$count" "$(first_matches "$matches")"
    fail=1
  else
    printf 'PASS absent=%s\n' "$pattern"
  fi
}

require() {
  local path="$1" token="$2"
  if [[ -f "$path" ]] && rg -n --no-heading -F "$token" "$path" >/dev/null 2>&1; then
    printf 'PASS required=%s token=%s\n' "${path#"$candidate_root"/}" "$token"
  else
    printf 'FAIL missing-required=%s token=%s\n' "${path#"$candidate_root"/}" "$token"
    fail=1
  fi
}

cohort() {
  local name="$1" relative path matches count
  shift
  printf 'COHORT=%s\n' "$name"
  for relative in "$@"; do
    path="$src/$relative"
    if [[ ! -f "$path" ]]; then
      printf 'FILE=%s status=missing\n' "$relative"
      fail=1
      continue
    fi
    matches="$(rg -n --no-heading -e 'TrackedStateStoreReader|TrackedStateContext|crate::tracked_state|use crate::tracked_state|tracked_state::' "$path" 2>/dev/null || true)"
    count="$(printf '%s\n' "$matches" | awk 'NF { n++ } END { print n + 0 }')"
    printf 'FILE=%s count=%s\n%s\n' "$relative" "$count" "$matches"
  done
}

printf 'ForkTree TrackedStateStoreReader deletion gate, exact b59\n'
actual_head="$(git -C "$candidate_root" rev-parse HEAD 2>/dev/null || true)"
actual_tree="$(git -C "$candidate_root" rev-parse HEAD^{tree} 2>/dev/null || true)"
printf 'CANDIDATE_HEAD=%s\nCANDIDATE_TREE=%s\n' "$actual_head" "$actual_tree"
[[ "$actual_head" == "$expected_head" ]] || { printf 'FAIL head-mismatch\n'; fail=1; }
[[ "$actual_tree" == "$expected_tree" ]] || { printf 'FAIL tree-mismatch\n'; fail=1; }
[[ -d "$src" ]] || { printf 'FAIL missing-source-root=%s\n' "$src"; exit 1; }

for pattern in \
  'TrackedStateStoreReader' 'TrackedStateReaderAdapter' 'TrackedStateReaderWrapper' \
  'TrackedStateReaderCompat' 'tracked_state_reader_adapter' \
  'tracked_state_reader_wrapper' 'tracked_state_reader_compat' \
  'tracked_state_reader_fallback' 'tracked_state_reader_migration' \
  'legacy_tracked_state_reader' 'history_reader_adapter' 'history_reader_wrapper' \
  'history_reader_compat' 'state_reader_adapter' 'state_reader_wrapper' \
  'state_reader_compat' 'columnar_history_fallback' 'tracked_state_compat'; do
  forbid "$pattern"
done

for pattern in \
  'TRACKED_STATE_TREE_CHUNK_SPACE' 'TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE' \
  'TRACKED_STATE_CHANGE_LOCATOR_SPACE' 'TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE' \
  'TRACKED_STATE_COMMIT_MUTATION_INVENTORY_SPACE' 'MUTATION_DIRECTORY_NODE_SPACE' \
  'SCOPED_RANGE_NODE_SPACE' 'CURRENT_STATE_DATA_PART_SPACE' \
  'CURRENT_STATE_DATA_PART_REFS_SPACE' 'CERTIFIED_ENTITY_BATCH_SPACE' \
  'CERTIFIED_ENTITY_BATCH_MANIFEST_SPACE' 'CERTIFIED_ENTITY_BATCH_PAGE_SPACE' \
  'ROW_GROUP_MANIFEST_SPACE' 'ROW_GROUP_COLUMN_SPACE' 'PACKED_CURRENT_BASE_SPACE' \
  'PACKED_CURRENT_BASE_CONTROL_SPACE' 'PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE' \
  'ROOT_CURRENT_BASE_SPACE'; do
  forbid "$pattern"
done

for path in "$src/tracked_state/diff.rs" "$src/tracked_state/diff_id.rs" \
  "$src/tracked_state/merge.rs" "$src/tracked_state/row_materialization.rs"; do
  if [[ -e "$path" ]]; then
    printf 'FAIL reader-only-module-exists=%s\n' "${path#"$candidate_root"/}"
    fail=1
  else
    printf 'PASS reader-only-module-absent=%s\n' "${path#"$candidate_root"/}"
  fi
done
for pattern in 'pub(crate) use diff' 'pub(crate) use diff_id' \
  'pub(crate) use merge' 'pub(crate) use row_materialization'; do
  forbid "$pattern"
done

for token in open_coherent_view state_point state_range load_commit_topologies \
  load_commit_member_records validate_commit_topology load_branch_head; do
  if rg -n --no-heading -g '*.rs' -F "$token" "$src/forktree" >/dev/null 2>&1; then
    printf 'PASS forktree-owner=%s\n' "$token"
  else
    printf 'FAIL missing-forktree-owner=%s\n' "$token"
    fail=1
  fi
done
for token in load_required_commit_catalog_entry 'selected CommitCatalog entry is absent' \
  validate_commit_catalog_identity validate_retained_commit state_point_on_read; do
  require "$src/forktree/serving.rs" "$token"
done
for token in historical_absence_requires_authenticated_commit_and_root \
  historical_missing_commit_catalog_fails_for_point_and_batch \
  historical_missing_state_root_fails_before_empty_result; do
  require "$src/forktree/tests.rs" "$token"
done
for token in request_may_include_derived is_derived_schema 'fail closed'; do
  require "$src/live_state/derived.rs" "$token"
done

require "$src/tracked_state/context.rs" 'pub(crate) fn reader<S>'
factory_count="$(rg -n --no-heading -g '*.rs' -F 'tracked_state.reader(' "$src" "$candidate_root/packages/lix/tests" 2>/dev/null | wc -l | tr -d ' ')"
transaction_count="$(rg -n --no-heading -g '*.rs' -F 'tracked_state_reader(' "$src" "$candidate_root/packages/lix/tests" 2>/dev/null | wc -l | tr -d ' ')"
opening_count="$(rg -n --no-heading -g '*.rs' -F 'with_opening_tracked_reader' "$src" "$candidate_root/packages/lix/tests" 2>/dev/null | wc -l | tr -d ' ')"
printf 'FACTORY_CALLS tracked_state.reader(=%s tracked_state_reader(=%s with_opening_tracked_reader=%s\n' "$factory_count" "$transaction_count" "$opening_count"
[[ "$factory_count" == 0 ]] || { printf 'FAIL tracked-state-reader-factory-calls-remain\n'; fail=1; }
[[ "$transaction_count" == 0 ]] || { printf 'FAIL transaction-reader-factory-calls-remain\n'; fail=1; }
[[ "$opening_count" == 0 ]] || { printf 'FAIL opening-reader-wrappers-remain\n'; fail=1; }

for path in \
  "$candidate_root/packages/lix/Cargo.toml" \
  "$candidate_root/packages/lix/tests/integration/main.rs" \
  "$candidate_root/packages/lix/tests/integration/sql/lix_file_history.rs" \
  "$candidate_root/packages/lix/tests/integration/sql/lix_directory_history.rs" \
  "$candidate_root/packages/lix/tests/integration/sql/diff_commands.rs" \
  "$candidate_root/packages/lix/tests/integration/sql/checkpoint.rs" \
  "$candidate_root/packages/lix/tests/semantic_merge.rs" \
  "$candidate_root/packages/engine-benchmarks/tests/checkpoint_gc_replay_reopen.rs" \
  "$candidate_root/packages/engine-benchmarks/tests/corruption_recovery_qualification.rs" \
  "$candidate_root/packages/engine-benchmarks/benches/tracked_working_diff.rs"; do
  if [[ -f "$path" ]]; then
    printf 'PASS concrete-target=%s\n' "${path#"$candidate_root"/}"
  else
    printf 'FAIL missing-concrete-target=%s\n' "${path#"$candidate_root"/}"
    fail=1
  fi
done

cohort checkpoint checkpoint.rs session/checkpoint.rs sql2/providers/checkpoint.rs
cohort history sql2/history_route.rs sql2/providers/change.rs \
  sql2/providers/file_history.rs sql2/providers/directory_history.rs
cohort sql_diff sql2/providers/diff.rs sql2/providers/working_diff.rs \
  sql2/providers/filesystem_working_diff.rs
cohort merge_analysis session/merge/analysis.rs session/merge/branch.rs \
  session/merge/conflicts.rs session/merge/stats.rs
cohort transaction_reconciliation_undo transaction/context.rs session/undo_redo.rs

for probe in "$script_dir/forktree_tracked_state_forbidden_reader_b59.rs" \
  "$script_dir/forktree_tracked_state_forbidden_space_b59.rs"; do
  if [[ -s "$probe" ]] && rg -q -F 'EXPECT_COMPILE_FAIL' "$probe"; then
    printf 'PASS compile-fail-probe-present=%s\n' "${probe#"$script_dir"/}"
  else
    printf 'FAIL missing-or-unmarked-probe=%s\n' "$probe"
    fail=1
  fi
done

if [[ "$fail" -ne 0 ]]; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
