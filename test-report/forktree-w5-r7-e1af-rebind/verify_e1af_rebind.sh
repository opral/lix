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
test "$(git -C "$repo" show -s --format=%T "$commit")" = "$tree"
test "$(git -C "$repo" show -s --format=%P "$commit")" = "$parent"
test "$(git -C "$repo" show -s --format=%T "$parent")" = "$parent_tree"

changed_source=$(git -C "$repo" diff --name-only "$anchor_commit" "$target_commit" -- packages/lix/src)
while IFS= read -r path; do
  [ -z "$path" ] && continue
  case "$path" in
    packages/lix/src/forktree/*|packages/lix/src/gc.rs|packages/lix/src/session/gc.rs|packages/lix/src/session/checkpoint.rs|packages/lix/src/session/media_upload.rs|packages/lix/src/engine.rs|packages/lix/src/binary_cas/*|packages/lix/src/transaction/context.rs) ;;
    *) echo "RED-SCOPE forbidden production path: $path"; exit 1 ;;
  esac
done <<EOF
$changed_source
EOF

if [ "$target_commit" = "$commit" ]; then
  test "$(sha256sum "$script_dir/SOURCE_RED.log" | awk '{print $1}')" = da2df9406124f627f28f53bb37dc7d3216dc2396ffadeccf68199ac95c56f846
  grep -q '^RED 168 forbidden production residues$' "$script_dir/SOURCE_RED.log"
  printf '%s\n' "ANCHOR PASS target=$target_commit anchor=$anchor_commit"
  printf '%s\n' 'RED 168 forbidden production residues'
  exit 1
fi

for required in OBJECT_SPACE SELECTOR_SPACE CoherentView PreparedPublication; do
  if ! git -C "$repo" grep -q -F -- "$required" "$target_commit" -- packages/lix/src; then
    echo "RED-MISSING required owner symbol: $required"
    exit 1
  fi
done

forbidden='CHECKPOINT_RECOVERY_REF_SPACE|CHECKPOINT_GC_STATE_SPACE|GC_REACHABILITY_(DELTA|QUEUE)|GC_TREE_SWEEP_|StorageSpace::mutable|StorageSpaceId|BranchRefReader|BranchHeadControl|CachingBranchRefReader|BranchRefFallback|SecondBranchAuthority|DualSelectorAuthority|LegacyGc|LegacyGC|legacy_gc|fallback_gc|retry_gc'
hits=$(git -C "$repo" grep -n -E -- "$forbidden" "$target_commit" -- packages/lix/src || true)
if [ -n "$hits" ]; then
  count=$(printf '%s\n' "$hits" | sed '/^$/d' | wc -l | tr -d ' ')
  printf '%s\n' "RED $count forbidden production residues"
  exit 1
fi

printf '%s\n' "GREEN candidate source authority gate target=$target_commit anchor=$anchor_commit"
