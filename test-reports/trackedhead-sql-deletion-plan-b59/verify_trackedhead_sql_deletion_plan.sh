#!/usr/bin/env bash
set -euo pipefail

if test "$#" -ne 0; then
  printf 'this verifier takes no arguments; anchors are pinned\n' >&2
  exit 2
fi
ROOT="$(pwd -P)"
ANCHOR="b59e1f11a51153e0a787a81f0f25bf104d150aaf"
ORACLE="33aa59975808099dfb5e9ca675a1633d713dccf3"
ORACLE_TREE="1ced701e3351af59c48dce75731947dcd1606f3e"
ORACLE_PARENT="1d9c47728377c6ec7d2646704d51f3aadb11c773"
ORACLE_DIFF="31b9374a14846f5e082d193296f6eb33255e667d5775c041a876077fc7952194"
ORACLE_PATCH="d4d96f33fa535171d20e32e1b859ee1b58000cb7"
ORACLE_PACKAGE="f54422520ea2ac7c47427d0e57f95ea6392b990e6e1861a31d6ae7848f509556"
MANIFEST="$ROOT/test-reports/trackedhead-sql-deletion-plan-b59/MANIFEST.json"
PLAN="$ROOT/test-reports/trackedhead-sql-deletion-plan-b59/PLAN.md"
PLAN_CASE_BINDING='24 cases = 6 domains × 4 corruption modes'

cd "$ROOT"
git rev-parse --is-inside-work-tree >/dev/null
git merge-base --is-ancestor "$ANCHOR" HEAD

status=0
say() { printf '%s\n' "$*"; }
metadata() {
  needle="$1"
  if ! rg -n -F -- "$needle" "$MANIFEST" >/dev/null; then
    say "MISSING_ORACLE_METADATA $needle"
    status=1
  fi
}

if ! git cat-file -e "${ORACLE}^{commit}"; then
  say "MISSING_ORACLE_OBJECT $ORACLE"
  status=1
else
  actual_tree="$(git rev-parse "${ORACLE}^{tree}")"
  actual_parent="$(git rev-list --parents -n1 "$ORACLE")"
  if test "$actual_tree" != "$ORACLE_TREE"; then
    say "ORACLE_TREE_MISMATCH expected=$ORACLE_TREE actual=$actual_tree"
    status=1
  fi
  if test "$actual_parent" != "$ORACLE $ORACLE_PARENT"; then
    say "ORACLE_PARENT_MISMATCH expected='$ORACLE $ORACLE_PARENT' actual='$actual_parent'"
    status=1
  fi
fi

metadata '"corruption_oracle_v3"'
metadata "\"commit\": \"$ORACLE\""
metadata "\"tree\": \"$ORACLE_TREE\""
metadata "\"parent\": \"$ORACLE_PARENT\""
metadata "\"full_index_binary_diff\": \"$ORACLE_DIFF\""
metadata "\"stable_patch_id\": \"$ORACLE_PATCH\""
metadata "\"package_sha256sums\": \"$ORACLE_PACKAGE\""
metadata '"case_count": 24'
for domain in StateRoot GlobalSelector BranchSelector CommitCatalog ChangeCatalog CheckpointRoot; do
  metadata "\"$domain\""
done
if rg -n -F -- "$PLAN_CASE_BINDING" "$PLAN" >/dev/null 2>&1; then
  say "PLAN_CASE_BINDING=$PLAN_CASE_BINDING"
else
  say "MISSING_PLAN_CASE_BINDING $PLAN_CASE_BINDING"
  status=1
fi

zero() {
  needle="$1"
  tmp="$ROOT/.trackedhead-plan-residue.$$"
  if rg -n -F --hidden --glob '!target/**' --glob '!*.lock' "$needle" \
      packages/lix/src packages/lix/tests packages/engine-benchmarks \
      2>/dev/null | LC_ALL=C sort >"$tmp"
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
  stage_collect_stale_working_diff_indexes \
  BranchHeadControlContext BranchHeadControlCache stage_branch_head_control \
  BRANCH_HEAD_CONTROL_SPACE MUTATION_REVISION_SPACE \
  TRACKED_MUTATION_REVISION_SPACE load_mutation_revision \
  load_mutation_revision_from_read load_tracked_mutation_revision \
  load_tracked_mutation_revision_from_read stage_mutation_revision \
  stage_tracked_mutation_revision tracked_mutation_revision_precondition \
  TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE load_commit_state_manifest \
  load_commit_state_manifests stage_delete_commit_state_manifest_for_gc \
  stage_resealed_commit_state_manifest_for_test
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
