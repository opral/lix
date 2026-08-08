#!/usr/bin/env bash
set -euo pipefail

root="${1:?repository root required}"
expected_head="${2:?expected head required}"
expected_head="705440f55eccba9e2d55c0951d6a684737005d76"
expected_tree="2b8dcb45a2d06bdda86d0fa5add5ea8c12d18c2d"

cd "$root"
actual_head="$(git rev-parse HEAD)"
actual_tree="$(git rev-parse HEAD^{tree})"
if [[ "$actual_head" != "$expected_head" || "$actual_tree" != "$expected_tree" ]]; then
  echo "TARGET_MISMATCH head=$actual_head tree=$actual_tree" >&2
  exit 2
fi
git diff --check

commit_rs="packages/lix/src/transaction/commit.rs"
context_rs="packages/lix/src/transaction/context.rs"
checkpoint_rs="packages/lix/src/session/checkpoint.rs"

rg -n 'checkpoint publication requires the ForkTree snapshot-root lowering slice' "$commit_rs" >/dev/null
rg -n 'if !prepared\.checkpoint_publications\.is_empty\(\)' "$commit_rs" >/dev/null
echo 'RED-01 checkpoint_publications is rejected before W3 planning'

rg -n 'open_coherent_view_on_read' "$commit_rs" >/dev/null
rg -n 'into_storage_plan' "$commit_rs" >/dev/null
rg -n 'prepare_write_set|commit_at_boundary' "$context_rs" >/dev/null
rg -n 'stage_checkpoint_commit|add_checkpoint_publication' "$context_rs" "$checkpoint_rs" >/dev/null
echo 'CONTROL-01 existing ordinary one-view/plan/prepare/commit route is present'
echo 'CONTROL-02 checkpoint staging and recovery publication intent are present'

if rg -n 'PreparedPublication::commit' packages/lix/src --glob '!**/tests.rs'; then
  echo 'FAIL independent PreparedPublication::commit production seam' >&2
  exit 1
fi
if rg -n 'commit_publication_for_test' packages/lix/src --glob '!**/tests.rs'; then
  echo 'FAIL test-only publication helper leaked outside source tests' >&2
  exit 1
fi
echo 'CONTROL-03 no independent production publication commit seam'

printf 'COUNTS checkpoint_publications='; rg -o 'checkpoint_publications' packages/lix/src | wc -l | tr -d ' '
printf ' recovery_refs='; rg -o 'CheckpointRecoveryRef' packages/lix/src | wc -l | tr -d ' '
printf ' into_storage_plan='; rg -o 'into_storage_plan' packages/lix/src | wc -l | tr -d ' '
printf ' rotations_required=65 suffix_required=64\n'
echo 'ORACLE_STATUS=RED'
exit 1
