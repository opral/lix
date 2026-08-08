#!/usr/bin/env bash
set -u -o pipefail

# TEST/REPORT-ONLY source verifier. It never edits, builds, or runs production.
candidate_root="${1:?candidate worktree}"
expected_head="${2:?expected candidate head}"
expected_tree="${3:?expected candidate tree}"
base="b59e1f11a51153e0a787a81f0f25bf104d150aaf"
script_dir="$(cd -- "$(dirname "$0")" && pwd)"
src="$candidate_root/packages/lix/src"
model="$script_dir/forktree_tracked_state_merge_analysis_model_b59_corrected.rs"
fail=0

pass() { printf 'PASS %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; fail=1; }

printf 'ForkTree tracked-state merge-analysis corrected oracle, exact b59 anchor\n'
actual_head="$(git -C "$candidate_root" rev-parse HEAD 2>/dev/null || true)"
actual_tree="$(git -C "$candidate_root" rev-parse HEAD^{tree} 2>/dev/null || true)"
printf 'CANDIDATE_HEAD=%s\nCANDIDATE_TREE=%s\n' "$actual_head" "$actual_tree"
test "$actual_head" = "$expected_head" || fail "head-mismatch expected=$expected_head actual=$actual_head"
test "$actual_tree" = "$expected_tree" || fail "tree-mismatch expected=$expected_tree actual=$actual_tree"
test -d "$src" || { fail "missing-source-root=$src"; exit 1; }

# Artifact integrity: the corrected package itself is test/report-only.
for path in \
  "$script_dir/FORKTREE_TRACKED_STATE_MERGE_ANALYSIS_ORACLE_B59_CORRECTED.md" \
  "$model" \
  "$script_dir/forktree_tracked_state_merge_analysis_oracle_b59_corrected.sh"; do
  test -s "$path" && pass "artifact=$path" || fail "missing-artifact=$path"
done

# Positive model contract. These checks are intentionally semantic, not merely
# a single model-file presence check.
for token in \
  'struct MergeRequest' 'merge_base: CommitRef' 'base: CommitRef' \
  'source: CommitRef' 'target: CommitRef' 'struct MergeIdentities' \
  'generation: u64' 'ObjectKind::Commit' 'ObjectKind::CommitCatalog' \
  'ObjectKind::Root' 'ObjectKind::Member' 'ObjectKind::Payload' \
  'ObjectKind::PluginRegistry' 'ObjectKind::FileOwner' \
  'ReadError::Missing' 'ReadError::WrongKind' 'IdentityMismatch' \
  'GenerationMismatch' 'BindingMismatch' 'MissingTombstone' \
  'enum ChangeKind' 'ChangeKind::Added' 'ChangeKind::Updated' \
  'ChangeKind::Deleted' 'Cell::Null' 'Cell::Tombstone' \
  'semantic_equal' 'plugin_handoffs' 'struct MergeOperation' \
  'RetainedStorageRead' 'ReadEvent' 'assert_one_owner' \
  'wrong_member_kind' 'wrong_payload_kind' 'payload_substitution' \
  'malformed_payload' 'malformed_catalog' 'malformed_root' \
  'disjoint merge succeeds'; do
  if rg -n --no-heading -F "$token" "$model" >/dev/null 2>&1; then
    pass "model-contract=$token"
  else
    fail "model-contract-missing=$token"
  fi
done

# Current semantic source contract and call graph.
analysis="$src/session/merge/analysis.rs"
branch="$src/session/merge/branch.rs"
transaction="$src/transaction/context.rs"
for path in "$analysis" "$branch" "$transaction"; do
  test -f "$path" && pass "source=$path" || fail "missing-source=$path"
done

for token in MergeCommits base_commit_id source_commit_id target_commit_id \
  merge_base forktree_read_facade plugin_merge_conflict_groups \
  load_plugin_registry_at_commit derived_plugin_blob_conflicts \
  resolve_plugin_merge_conflict_groups; do
  if rg -n --no-heading -F "$token" "$analysis" "$branch" >/dev/null 2>&1; then
    pass "semantic-source=$token"
  else
    fail "semantic-source-missing=$token"
  fi
done

for token in load_commit_member_records validate_commit_topology \
  historical_missing_commit_catalog_fails_for_point_and_batch \
  historical_missing_state_root_fails_before_empty_result; do
  if rg -n --no-heading -F "$token" "$src/forktree" >/dev/null 2>&1; then
    pass "fail-closed-source=$token"
  else
    fail "fail-closed-source-missing=$token"
  fi
done

# One-read positive shape: the branch path must use the transaction opening
# read/facade and must not call begin_read itself. The typed model supplies the
# stronger end-to-end read trace.
for token in opening_read forktree_read_facade with_opening_tracked_reader; do
  if rg -n --no-heading -F "$token" "$branch" "$transaction" >/dev/null 2>&1; then
    pass "opening-read-source=$token"
  else
    fail "opening-read-source-missing=$token"
  fi
done
branch_begin_read="$(rg -n --no-heading -F 'begin_read' "$branch" | wc -l | tr -d ' ')"
printf 'MERGE_BRANCH_BEGIN_READ=%s\n' "$branch_begin_read"
test "$branch_begin_read" = 0 && pass "merge-branch-no-independent-begin-read" || fail "merge-branch-independent-begin-read=$branch_begin_read"

# Full merge production closure. This includes the merge implementation,
# transaction opening-read plumbing, ForkTree serving/authentication, and
# plugin/file historical resolution. Legacy authority names are forbidden over
# the whole closure. Cache/fallback/retry checks are scoped to merge-owned
# files, so an unrelated accepted reader-local ForkTree cache is not a false
# positive.
closure=(
  "$src/session/merge/analysis.rs"
  "$src/session/merge/branch.rs"
  "$src/session/merge/conflicts.rs"
  "$src/session/merge/stats.rs"
  "$src/transaction/context.rs"
  "$src/forktree"
  "$src/plugin"
)
printf 'PRODUCTION_CLOSURE=%s\n' "${closure[*]}"
for token in \
  'TrackedStateStoreReader' 'with_opening_tracked_reader' \
  'tracked_state.reader(' 'TrackedStateContext' 'TrackedHead' \
  'BranchHeadControl' 'BranchRefReader' 'merge_reader'; do
  count="$(rg -n --no-heading -F "$token" "${closure[@]}" 2>/dev/null | wc -l | tr -d ' ')"
  printf 'FORBIDDEN_CLOSURE token=%s count=%s\n' "$token" "$count"
  if test "$count" = 0; then
    pass "closure-free=$token"
  else
    fail "closure-residue=$token count=$count"
  fi
done

merge_scope=("$src/session/merge/analysis.rs" "$src/session/merge/branch.rs")
for token in 'StorageAdapterRead' 'begin_read(' 'refresh(' 'retry' 'fallback' \
  'compat' 'Cache' 'cache' 'merge_reader'; do
  count="$(rg -n --no-heading -F "$token" "${merge_scope[@]}" 2>/dev/null | wc -l | tr -d ' ')"
  printf 'MERGE_FORBIDDEN token=%s count=%s\n' "$token" "$count"
  if test "$count" = 0; then
    pass "merge-closure-free=$token"
  else
    fail "merge-closure-residue=$token count=$count"
  fi
done

# The specific merge callback/factory deletion is independently reported so a
# baseline RED can be distinguished from a model/source-contract failure.
reader_count="$(rg -n --no-heading -F 'TrackedStateStoreReader' "$analysis" "$branch" "$transaction" | wc -l | tr -d ' ')"
factory_count="$(rg -n --no-heading -F 'tracked_state.reader(' "$analysis" "$branch" "$transaction" | wc -l | tr -d ' ')"
wrapper_count="$(rg -n --no-heading -F 'with_opening_tracked_reader' "$analysis" "$branch" "$transaction" | wc -l | tr -d ' ')"
printf 'MERGE_DELETION_FRONTIER TrackedStateStoreReader=%s tracked_state.reader(=%s with_opening_tracked_reader=%s\n' "$reader_count" "$factory_count" "$wrapper_count"
if test "$reader_count" = 0 && test "$factory_count" = 0 && test "$wrapper_count" = 0; then
  pass "merge-specific-callback-factory-wrapper-deleted"
else
  fail "merge-specific-callback-factory-wrapper-remains"
fi

if test "$fail" -ne 0; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
