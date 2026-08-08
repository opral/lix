#!/usr/bin/env bash
set -u -o pipefail

# TEST/REPORT-ONLY verifier. It never edits, builds, or runs production.
candidate_root="${1:?candidate worktree}"
expected_head="${2:?expected candidate head}"
expected_tree="${3:?expected candidate tree}"
base="b59e1f11a51153e0a787a81f0f25bf104d150aaf"
script_dir="$(cd -- "$(dirname "$0")" && pwd)"
src="$candidate_root/packages/lix/src"
model="$script_dir/forktree_tracked_state_merge_analysis_workspace_model_b59.rs"
red=0
pass() { printf 'PASS %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; red=1; }

printf 'TrackedState merge-analysis workspace oracle; exact b59 anchor\n'
actual_head="$(git -C "$candidate_root" rev-parse HEAD 2>/dev/null || true)"
actual_tree="$(git -C "$candidate_root" rev-parse HEAD^{tree} 2>/dev/null || true)"
printf 'BASE=%s\nCANDIDATE_HEAD=%s\nCANDIDATE_TREE=%s\n' "$base" "$actual_head" "$actual_tree"
test "$actual_head" = "$expected_head" || fail "head-mismatch expected=$expected_head actual=$actual_head"
test "$actual_tree" = "$expected_tree" || fail "tree-mismatch expected=$expected_tree actual=$actual_tree"
test -d "$src" || { fail "missing-production-root=$src"; exit 1; }

for path in \
  "$script_dir/FORKTREE_TRACKED_STATE_MERGE_ANALYSIS_WORKSPACE_ORACLE_B59.md" \
  "$model" \
  "$script_dir/forktree_tracked_state_merge_analysis_workspace_oracle_b59.sh"; do
  test -s "$path" && pass "artifact=$path" || fail "missing-artifact=$path"
done

for token in \
  'enum ChangeKind' 'ChangeKind::Added' 'ChangeKind::Updated' 'ChangeKind::Deleted' 'ChangeKind::Unchanged' \
  'Cell::Null' 'Cell::Tombstone' 'semantic_equal' 'convergent' 'plugin_handoffs' \
  'CommitRef' 'merge_base' 'base: CommitRef' 'source: CommitRef' 'target: CommitRef' \
  'ObjectKind::CommitCatalog' 'ObjectKind::Root' 'ObjectKind::Member' 'ObjectKind::Payload' \
  'ObjectKind::PluginRegistry' 'ObjectKind::FileOwner' 'ReadError::Missing' 'ReadError::WrongKind' \
  'ReadError::Malformed' 'IdentityMismatch' 'GenerationMismatch' 'ReadError::GenerationMismatch' 'RetainedStorageRead' \
  'ReadEvent' 'assert_one_owner' 'MergeOperation' 'source_plugin' 'wrong_root' 'wrong_catalog' \
  'wrong_registry' 'wrong_member' 'wrong_payload' 'substituted_payload' 'bad_generation'; do
  rg -n --no-heading -F "$token" "$model" >/dev/null 2>&1 \
    && pass "model-contract=$token" || fail "model-contract-missing=$token"
done

# The production source must still expose semantic merge call sites while the
# old reader is present on b59. Future successors retain this positive map.
analysis="$src/session/merge/analysis.rs"
branch="$src/session/merge/branch.rs"
transaction="$src/transaction/context.rs"
for path in "$analysis" "$branch" "$transaction"; do
  test -f "$path" && pass "source=$path" || fail "missing-source=$path"
done
for token in MergeCommits base_commit_id source_commit_id target_commit_id merge_base \
  forktree_read_facade plugin_merge_conflict_groups load_plugin_registry_at_commit \
  derived_plugin_blob_conflicts resolve_plugin_merge_conflict_groups; do
  rg -n --no-heading -F "$token" "$analysis" "$branch" >/dev/null 2>&1 \
    && pass "semantic-source=$token" || fail "semantic-source-missing=$token"
done

# This named fallback helper is an alternate merge authority wherever it is
# found, including tracked_state/context.rs. It is therefore forbidden across
# the entire production workspace, not only in the merge directory.
for token in 'merge_payload_fallback_ids' 'sorted_merge_payload_fallback_ids'; do
  count="$(rg -n --no-heading -F "$token" "$src" 2>/dev/null | wc -l | tr -d ' ')"
  printf 'WORKSPACE_ALTERNATE_AUTHORITY token=%s count=%s\n' "$token" "$count"
  test "$count" = 0 && pass "workspace-alternate-authority-free=$token" || fail "workspace-alternate-authority-residue=$token count=$count"
done

# Workspace-wide legacy residue scan. Every hit is printed and classified.
# Only the following unrelated retained cohorts are allowlisted. Any hit in
# an unlisted path, especially session/merge/**, is forbidden.
allow_unrelated() {
  local path="$1" token="$2" line="$3"
  case "$path:$token" in
    packages/lix/src/checkpoint.rs:*) return 0 ;;
    packages/lix/src/session/checkpoint.rs:*) return 0 ;;
    packages/lix/src/session/undo_redo.rs:*) return 0 ;;
    packages/lix/src/gc.rs:*) return 0 ;;
    packages/lix/src/init.rs:*) return 0 ;;
    packages/lix/src/sql2/providers/file_history.rs:*) return 0 ;;
    packages/lix/src/sql2/providers/filesystem_working_diff.rs:*) return 0 ;;
    packages/lix/src/tracked_state/context.rs:*) return 0 ;;
    packages/lix/src/tracked_state/diff.rs:*) return 0 ;;
    packages/lix/src/tracked_state/mod.rs:TrackedStateStoreReader) return 0 ;;
    packages/lix/src/transaction/context.rs:with_opening_tracked_reader) return 1 ;;
    packages/lix/src/transaction/context.rs:*)
      # Exact b59 merge callback/factory span; transaction helpers outside it
      # remain an explicitly unrelated retained cohort.
      if test "$line" -ge 7390 && test "$line" -le 7413; then return 1; fi
      return 0
      ;;
    packages/lix/src/transaction/context/cohort.rs:*) return 0 ;;
    *) return 1 ;;
  esac
}

for token in 'TrackedStateStoreReader' 'tracked_state.reader(' 'tracked_state_reader' 'with_opening_tracked_reader'; do
  while IFS=: read -r absolute_path line source_line; do
    test -n "$absolute_path" || continue
    rel="${absolute_path#"$candidate_root/"}"
    if allow_unrelated "$rel" "$token" "$line"; then
      printf 'WORKSPACE_RESIDUE token=%s path=%s line=%s class=ALLOWLISTED_UNRELATED\n' "$token" "$rel" "$line"
    else
      printf 'WORKSPACE_RESIDUE token=%s path=%s line=%s class=FORBIDDEN_MERGE\n' "$token" "$rel" "$line"
      red=1
    fi
  done < <(rg -n --no-heading -F "$token" "$src" 2>/dev/null || true)
done

# Merge-owned callback/factory and old plan/fallback authority must disappear;
# this is intentionally RED on b59 and is the primary future-GREEN gate.
# Transaction/context.rs is scanned workspace-wide for the old callback and
# exact fallback helper; generic cache/retry words there belong to unrelated
# transaction cohorts and are not part of the merge-owned scope.
merge_scope=("$src/session/merge/analysis.rs" "$src/session/merge/branch.rs" "$src/tracked_state/merge.rs" "$src/tracked_state/mod.rs")
for token in \
  'TrackedStateStoreReader' 'tracked_state.reader(' 'with_opening_tracked_reader' \
  'merge_payload_fallback_ids' 'TrackedStateDiffRequest' 'TrackedStatePayloadBatch' 'plan_merge' \
  'StorageAdapterRead' 'begin_read(' 'BranchHeadControl' 'BranchRefReader' 'TrackedHead' \
  'TrackedStateContext' 'merge_reader' 'compat' 'fallback' 'retry' 'Cache' 'cache'; do
  count="$(rg -n --no-heading -F "$token" "${merge_scope[@]}" 2>/dev/null | wc -l | tr -d ' ')"
  printf 'MERGE_AUTHORITY token=%s count=%s\n' "$token" "$count"
  if test "$count" = 0; then
    pass "merge-authority-free=$token"
  else
    residual="$(rg -n --no-heading -F "$token" "${merge_scope[@]}" 2>/dev/null | \
      grep -v 'packages/lix/src/session/merge/branch.rs:724:.*actor cache' | \
      grep -v 'packages/lix/src/session/merge/branch.rs:1891:.*retry the merge' || true)"
    if test -z "$residual" && { test "$token" = cache || test "$token" = retry; }; then
      pass "merge-authority-only-allowlisted-prose=$token"
    else
      fail "merge-authority-residue=$token count=$count"
    fi
  fi
done

printf 'MERGE_CALLBACK_FACTORY_FRONTIER=\n'
rg -n --no-heading -F 'with_opening_tracked_reader' "$analysis" "$branch" "$transaction" || true
printf 'MERGE_FALLBACK_FRONTIER=\n'
rg -n --no-heading -F 'merge_payload_fallback_ids' "$analysis" "$src/tracked_state/merge.rs" "$src/tracked_state/mod.rs" || true

if test "$red" -ne 0; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
