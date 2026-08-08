#!/usr/bin/env bash
set -e

repo=${1:?usage: verify_selector_source_contract.sh <repo> [head]}
head=${2:-HEAD}
cd "$repo"

expected_head=705440f55eccba9e2d55c0951d6a684737005d76
expected_tree=2b8dcb45a2d06bdda86d0fa5add5ea8c12d18c2d
test "$(git rev-parse "$head")" = "$expected_head" || {
  echo "BLOCKER wrong anchor head"; exit 2;
}
test "$(git rev-parse "$head^{tree}")" = "$expected_tree" || {
  echo "BLOCKER wrong anchor tree"; exit 2;
}

model=$(git show "$head:packages/lix/src/forktree/model.rs")
view=$(git show "$head:packages/lix/src/forktree/view.rs")
publication=$(git show "$head:packages/lix/src/forktree/publication.rs")
gc=$(git show "$head:packages/lix/src/gc.rs")
create=$(git show "$head:packages/lix/src/session/create_branch.rs")
switch=$(git show "$head:packages/lix/src/session/switch_branch.rs")
stage_rows=$(git show "$head:packages/lix/src/branch/stage_rows.rs")
undo=$(git show "$head:packages/lix/src/session/undo_redo.rs")

echo "TARGET head=$expected_head tree=$expected_tree"
echo "CONTRACT_SHA256=ff784043429f563fb01a29c42eecc90a939f7ce8ac7926d9db07a0f13313da24"

printf '%s\n' "$model" | rg -q 'struct GlobalSelectorV1' &&
  printf '%s\n' "$model" | rg -q 'struct BranchSelectorV1' &&
  printf '%s\n' "$model" | rg -q 'authenticated_body' &&
  echo 'PASS-01 authenticated GlobalSelectorV1/BranchSelectorV1 codecs exist'

printf '%s\n' "$view" | rg -q 'StorageAdapterReadScope.*begin_read|begin_read\(ReadOptions::default\(\)\)' &&
  printf '%s\n' "$view" | rg -q 'get_many' &&
  printf '%s\n' "$view" | rg -q 'GlobalSelectorV1::decode' &&
  printf '%s\n' "$view" | rg -q 'BranchSelectorV1::decode' &&
  printf '%s\n' "$view" | rg -q 'branch selector key does not match' &&
  echo 'PASS-02 one retained read authenticates both selectors and branch identity'

printf '%s\n' "$publication" | rg -q 'next_global: view\.global_selector\(\)\.rotated' &&
  printf '%s\n' "$publication" | rg -q 'raw_branch_selector' &&
  printf '%s\n' "$publication" | rg -q 'KeyValueEquals' &&
  printf '%s\n' "$publication" | rg -q 'writes\.put\(' &&
  printf '%s\n' "$publication" | rg -q 'global_selector_key\(\)\.to_vec' &&
  printf '%s\n' "$publication" | rg -q 'delete_branch_selector' &&
  echo 'PASS-03 publication has global epoch and branch raw-byte CAS in one plan'

printf '%s\n' "$gc" | rg -q 'chronology_roots' &&
  printf '%s\n' "$gc" | rg -q 'serving_checkpoint_commit_id' &&
  printf '%s\n' "$gc" | rg -q 'cycle in its retained undo interval' &&
  printf '%s\n' "$gc" | rg -q 'references missing commit' &&
  echo 'PASS-04 chronology roots and checkpoint floor are separately validated'

red=0
if printf '%s\n' "$create" | rg -q 'branch_ref_stage_row'; then
  echo 'RED-01 create_branch still writes legacy branch-ref rows instead of BranchSelectorV1 publication'
  red=1
fi
if printf '%s\n' "$switch" | rg -q 'branch_ref_reader|workspace_branch_stage_row'; then
  echo 'RED-02 switch_branch still reads/writes legacy BranchRef/workspace rows'
  red=1
fi
if printf '%s\n' "$stage_rows" | rg -q 'branch_ref_tombstone_row'; then
  echo 'RED-03 branch deletion/retirement still exposes a legacy branch-ref tombstone writer'
  red=1
fi
if printf '%s\n' "$undo" | rg -q 'UNDO_REDO_MARKER_SCHEMA_KEY|lix_undo_redo_marker'; then
  echo 'RED-04 undo/redo still persists a marker authority not proven equivalent to snapshot selectors'
  red=1
fi

if test "$red" -eq 0; then
  echo 'SOURCE_STATUS=NOT_RED (integration residue changed; inspect before accepting)'
  exit 1
fi
echo 'SOURCE_STATUS=RED'
exit 1
