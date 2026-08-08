#!/usr/bin/env bash
set -u -o pipefail

# TEST/REPORT-ONLY source verifier. It never edits, builds, or runs.
candidate_root="$1"
expected_head="$2"
expected_tree="$3"
script_dir="$(cd -- "$(dirname "$0")" && pwd)"
src="$candidate_root/packages/lix/src"
fail=0

require() {
  path="$1"
  token="$2"
  if test -f "$path" && rg -n --no-heading -F "$token" "$path" >/dev/null 2>&1; then
    printf 'PASS required=%s token=%s\n' "$path" "$token"
  else
    printf 'FAIL missing=%s token=%s\n' "$path" "$token"
    fail=1
  fi
}

printf 'ForkTree tracked-state merge-analysis migration oracle, exact b59\n'
actual_head="$(git -C "$candidate_root" rev-parse HEAD 2>/dev/null || true)"
actual_tree="$(git -C "$candidate_root" rev-parse HEAD^{tree} 2>/dev/null || true)"
printf 'CANDIDATE_HEAD=%s\nCANDIDATE_TREE=%s\n' "$actual_head" "$actual_tree"
test "$actual_head" = "$expected_head" || { printf 'FAIL head-mismatch\n'; fail=1; }
test "$actual_tree" = "$expected_tree" || { printf 'FAIL tree-mismatch\n'; fail=1; }
test -d "$src" || { printf 'FAIL missing-source-root=%s\n' "$src"; exit 1; }

analysis="$src/session/merge/analysis.rs"
branch="$src/session/merge/branch.rs"
tracked_diff="$src/tracked_state/diff.rs"
tracked_merge="$src/tracked_state/merge.rs"
transaction="$src/transaction/context.rs"
conflicts="$src/session/merge/conflicts.rs"
stats="$src/session/merge/stats.rs"

for token in \
  'pub(crate) async fn analyze<S>' 'TrackedStateStoreReader<S>' \
  'TrackedStateDiffRequest::default()' 'diff_commits(&base_commit_id, &source_commit_id' \
  'diff_commits(&base_commit_id, &target_commit_id' 'exclude_internal_checkpoint_markers' \
  'merge_payload_fallback_ids' 'load_change_payloads' 'plan_merge' 'stats_from_plan'; do
  require "$analysis" "$token"
done

for token in merge_branch_preview 'merge_branch(' with_opening_tracked_reader \
  'super::analysis::analyze' branch_ref_reader_on_opening_read \
  commit_graph_reader_on_opening_read forktree_read_facade; do
  require "$branch" "$token"
done
require "$transaction" 'opening_read: SharedStorageAdapterRead'
require "$transaction" 'fn opening_read(&self)'
require "$transaction" 'pub(crate) async fn with_opening_tracked_reader'
require "$transaction" 'self.tracked_state.reader(self.opening_read())'

analysis_calls="$(rg -n --no-heading -F 'super::analysis::analyze' "$branch" | wc -l | tr -d ' ')"
opening_calls="$(rg -n --no-heading -F 'with_opening_tracked_reader' "$branch" | wc -l | tr -d ' ')"
merge_reads="$(rg -n --no-heading -F 'begin_read' "$branch" | wc -l | tr -d ' ')"
printf 'CALL_GRAPH analysis::analyze=%s branch-callbacks=%s merge-branch-begin_read=%s\n' "$analysis_calls" "$opening_calls" "$merge_reads"
test "$analysis_calls" = 2 || { printf 'FAIL expected-two-analysis-callers\n'; fail=1; }
test "$opening_calls" = 2 || { printf 'FAIL expected-two-opening-reader-callers\n'; fail=1; }
test "$merge_reads" = 0 || { printf 'FAIL merge-path-acquires-independent-read\n'; fail=1; }

for token in 'TrackedStateDiffKind::Added' 'TrackedStateDiffKind::Modified' \
  'TrackedStateDiffKind::Removed' same_final_state row_is_live source_change_pick \
  tracked_row_payload_eq sameEntityChanged SameEntityChanged 'Ordering::Less' \
  'Ordering::Greater' 'Ordering::Equal' SortedMergeInputs BTreeMap BTreeSet; do
  if rg -n --no-heading -F "$token" "$tracked_diff" "$tracked_merge" "$conflicts" "$stats" "$branch" >/dev/null 2>&1; then
    printf 'PASS semantic-contract=%s\n' "$token"
  else
    printf 'FAIL missing-semantic-contract=%s\n' "$token"
    fail=1
  fi
done

for token in deleted snapshot metadata tombstone plugin_merge_conflict_groups \
  derived_plugin_blob_conflicts resolve_plugin_merge_conflict_groups \
  plugin_resolution_change_stats plugin_resolution_stats; do
  if rg -n --no-heading -F "$token" "$tracked_diff" "$tracked_merge" "$branch" >/dev/null 2>&1; then
    printf 'PASS retained-state-or-plugin=%s\n' "$token"
  else
    printf 'FAIL missing-retained-state-or-plugin=%s\n' "$token"
    fail=1
  fi
done

for token in 'missing from the map compares unequal' 'return Err(LixError' \
  load_commit_member_records validate_commit_topology \
  'selected CommitCatalog entry is absent' \
  historical_missing_commit_catalog_fails_for_point_and_batch \
  historical_missing_state_root_fails_before_empty_result; do
  if rg -n --no-heading -F "$token" "$src/forktree" "$tracked_merge" >/dev/null 2>&1; then
    printf 'PASS fail-closed=%s\n' "$token"
  else
    printf 'FAIL missing-fail-closed=%s\n' "$token"
    fail=1
  fi
done

reader_count="$(rg -n --no-heading -F 'TrackedStateStoreReader' "$analysis" "$branch" "$transaction" | wc -l | tr -d ' ')"
factory_count="$(rg -n --no-heading -F 'tracked_state.reader(' "$branch" "$transaction" | wc -l | tr -d ' ')"
wrapper_count="$(rg -n --no-heading -F 'with_opening_tracked_reader' "$branch" "$transaction" | wc -l | tr -d ' ')"
printf 'DELETION_FRONTIER TrackedStateStoreReader=%s tracked_state.reader(=%s with_opening_tracked_reader=%s\n' "$reader_count" "$factory_count" "$wrapper_count"
if test "$reader_count" -gt 0 && test "$factory_count" -gt 0 && test "$wrapper_count" -gt 0; then
  printf 'FAIL deletion-frontier-remains TrackedStateStoreReader=%s tracked_state.reader(=%s with_opening_tracked_reader=%s\n' "$reader_count" "$factory_count" "$wrapper_count"
  fail=1
else
  printf 'PASS merge-reader-callback-and-factory-absent\n'
fi

for path in \
  "$candidate_root/packages/lix/tests/semantic_merge.rs" \
  "$candidate_root/packages/lix/tests/integration/branching.rs" \
  "$candidate_root/packages/lix/tests/integration/merge_fuzz.rs" \
  "$candidate_root/packages/engine-benchmarks/benches/tracked_working_diff.rs" \
  "$candidate_root/packages/engine-benchmarks/tests/tracked_state_crud_public_result.rs" \
  "$candidate_root/packages/engine-benchmarks/tests/corruption_recovery_qualification.rs"; do
  if test -f "$path"; then
    printf 'PASS concrete-target=%s\n' "$path"
  else
    printf 'FAIL missing-concrete-target=%s\n' "$path"
    fail=1
  fi
done

model="$script_dir/forktree_tracked_state_merge_analysis_model_b59.rs"
if test -s "$model" && rg -q -F TEST/REPORT-ONLY "$model"; then
  printf 'PASS pure-model=%s\n' "$model"
else
  printf 'FAIL missing-pure-model=%s\n' "$model"
  fail=1
fi

if test "$fail" -ne 0; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
