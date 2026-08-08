#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo 'usage: verify_readiness_source.sh <e1af-worktree>' >&2
  exit 2
fi
repo=$1
commit=e1af471b9ab0f598dafa7c2ddec7867667c81740
tree=bfa0d271a723da8250ab76ada16fda90926f1099
parent=b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
parent_tree=4477c83b246bddac09cd972564bd4ccd67f90f7b

test "$(git -C "$repo" rev-parse "$commit^{commit}")" = "$commit"
test "$(git -C "$repo" show -s --format=%T "$commit")" = "$tree"
test "$(git -C "$repo" show -s --format=%T "$parent")" = "$parent_tree"
test "$(git -C "$repo" show -s --format=%P "$commit")" = "$parent"

calibration=$(mktemp /tmp/branch-ref-readiness-calibration.XXXXXX)
set +e
sh "$repo/test-report/forktree-w3-e1af-selector-rebind/verify_e1af_source.sh" "$repo" >"$calibration" 2>&1
calibration_rc=$?
set -e
test "$calibration_rc" = 0
cat "$calibration"
test "$(sha256sum "$calibration" | cut -d' ' -f1)" = \
  ef6077659dca998b3a4030f19d61434fb4bb97c0f491c738851f4bdfad553c9e

grep -qx $'legacy_control_generation\t58' "$calibration"
grep -qx $'checkpoint_history\t1139' "$calibration"
grep -qx $'snapshot_pin\t16' "$calibration"
grep -qx $'selector_epoch\t770' "$calibration"
grep -qx $'mutation_revision\t24' "$calibration"

count() {
  local pattern=$1
  (git -C "$repo" grep -n -E "$pattern" "$commit" -- packages/lix/src || true) | wc -l | tr -d ' '
}

legacy_control=$(count 'BranchHeadControl|BranchRefReader|TrackedHead|current.?generation')
legacy_spaces=$(count 'MUTATION_REVISION_SPACE|TRACKED_MUTATION_REVISION_SPACE|BRANCH_REF_SCHEMA_KEY')
cache_or_fallback=$(count 'BranchHeadControlCache|CachingBranchRefReader|fallback_branch_ref|BranchRefFallback|SecondBranchAuthority|DualSelectorAuthority')
required_owner=$(count 'GlobalSelectorV1|BranchSelectorV1|PreparedPublication|CoherentView')

printf 'legacy_control_hits=%s\n' "$legacy_control"
printf 'legacy_space_hits=%s\n' "$legacy_spaces"
printf 'cache_fallback_dual_hits=%s\n' "$cache_or_fallback"
printf 'required_owner_hits=%s\n' "$required_owner"

for path in \
  packages/lix/src/branch/refs.rs \
  packages/lix/src/branch/context.rs \
  packages/lix/src/branch/stage_rows.rs \
  packages/lix/src/sql2/branch_ref.rs; do
  if git -C "$repo" cat-file -e "$commit:$path" 2>/dev/null; then
    printf 'legacy_path_present=%s\n' "$path"
  fi
done

for token in GlobalSelectorV1 BranchSelectorV1 PreparedPublication CoherentView; do
  git -C "$repo" grep -q -F "$token" "$commit" -- packages/lix/src
done

echo 'READINESS RED e1af legacy selector/control ownership remains'
echo 'RED is expected until the sole ForkTree selector/read/publication owner replaces all listed residues.'
exit 1
