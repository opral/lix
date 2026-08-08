#!/bin/sh
set -eu

repo=${1:?usage: verify_e1af_rebind.sh <e1af-worktree>}
commit=e1af471b9ab0f598dafa7c2ddec7867667c81740
parent=b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
tree=bfa0d271a723da8250ab76ada16fda90926f1099
parent_tree=4477c83b246bddac09cd972564bd4ccd67f90f7b

test "$(git -C "$repo" rev-parse "$commit^{commit}")" = "$commit"
test "$(git -C "$repo" show -s --format=%T "$commit")" = "$tree"
test "$(git -C "$repo" show -s --format=%P "$commit")" = "$parent"
test "$(git -C "$repo" show -s --format=%T "$parent")" = "$parent_tree"

grep -q '^RED 168 forbidden production residues$' SOURCE_RED.log
grep -q 'da2df9406124f627f28f53bb37dc7d3216dc2396ffadeccf68199ac95c56f846' README.md
grep -q 'OBJECT_SPACE' README.md
grep -q 'SELECTOR_SPACE' README.md
grep -q '65 total' README.md
grep -q 'advance_gc' README.md
grep -q 'abort_corrupt_gc' README.md
grep -q 'commit_progress' README.md

printf 'PASS e1af_w5_r7_rebind_expected_red\n'
