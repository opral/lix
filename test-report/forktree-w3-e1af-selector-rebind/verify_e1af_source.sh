#!/bin/sh
set -eu

repo=${1:?usage: verify_e1af_source.sh <git-worktree>}
commit=e1af471b9ab0f598dafa7c2ddec7867667c81740
parent=b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
tree=bfa0d271a723da8250ab76ada16fda90926f1099
parent_tree=4477c83b246bddac09cd972564bd4ccd67f90f7b

test "$(git -C "$repo" rev-parse "$commit^{commit}")" = "$commit"
test "$(git -C "$repo" show -s --format=%T "$commit")" = "$tree"
test "$(git -C "$repo" show -s --format=%T "$parent")" = "$parent_tree"
test "$(git -C "$repo" show -s --format=%P "$commit")" = "$parent"

count() {
    pattern=$1
    (git -C "$repo" grep -n -i -E "$pattern" "$commit" -- 'packages/lix/src/**/*.rs' || true) | wc -l | tr -d ' '
}

expect() {
    label=$1
    actual=$2
    expected=$3
    test "$actual" = "$expected"
    printf '%s\t%s\n' "$label" "$actual"
}

expect legacy_control_generation "$(count 'BranchHeadControl|TrackedHead|current.?generation')" 58
expect checkpoint_history "$(count 'checkpoint|recovery|snapshot.?pin|undo|redo')" 1139
expect snapshot_pin "$(count 'snapshot.?pin')" 16
expect selector_epoch "$(count 'GlobalSelectorV1|BranchSelectorV1|global.?epoch|selector')" 770
expect mutation_revision "$(count 'stage_branch_head_control|branch_head_control_precondition|stage_mutation_revision|MUTATION_REVISION_SPACE|TRACKED_MUTATION_REVISION_SPACE')" 24

git -C "$repo" grep -q 'GlobalSelectorV1' "$commit" -- 'packages/lix/src/**/*.rs'
git -C "$repo" grep -q 'BranchSelectorV1' "$commit" -- 'packages/lix/src/**/*.rs'
git -C "$repo" grep -q 'PreparedPublication' "$commit" -- 'packages/lix/src/**/*.rs'

printf 'PASS e1af_source_calibration\n'
