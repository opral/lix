#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo=${1:?usage: verify_e1af_rebind.sh <repo> [target] [anchor]}
target=${2:-e1af471b9ab0f598dafa7c2ddec7867667c81740}
anchor=${3:-e1af471b9ab0f598dafa7c2ddec7867667c81740}
commit=e1af471b9ab0f598dafa7c2ddec7867667c81740
parent=b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
tree=bfa0d271a723da8250ab76ada16fda90926f1099
parent_tree=4477c83b246bddac09cd972564bd4ccd67f90f7b

target_commit=$(git -C "$repo" rev-parse "$target^{commit}")
anchor_commit=$(git -C "$repo" rev-parse "$anchor^{commit}")
test "$anchor_commit" = "$commit"
git -C "$repo" merge-base --is-ancestor "$anchor_commit" "$target_commit"
test "$(git -C "$repo" show -s --format=%T "$commit")" = "$tree"
test "$(git -C "$repo" show -s --format=%P "$commit")" = "$parent"
test "$(git -C "$repo" show -s --format=%T "$parent")" = "$parent_tree"

if [ "$target_commit" = "$commit" ]; then
  test "$(sha256sum "$script_dir/SOURCE_RED.log" | awk '{print $1}')" = da2df9406124f627f28f53bb37dc7d3216dc2396ffadeccf68199ac95c56f846
  grep -q '^RED 168 forbidden production residues$' "$script_dir/SOURCE_RED.log"
  printf '%s\n' "ANCHOR PASS target=$target_commit anchor=$anchor_commit"
  printf '%s\n' 'RED 168 forbidden production residues'
  exit 1
fi

exec node "$script_dir/verify_w5_r7_structure.mjs" "$repo" "$target_commit" "$anchor_commit"
