#!/usr/bin/env bash
set -u -o pipefail

# Dormant negative compiler gate. It is run only after a future candidate has
# compiled a Lix rlib; success of either probe is a hard failure.
candidate_root="${1:?candidate worktree path required}"
dependency_dir="${2:?candidate dependency directory required}"
lix_rlib="${3:?candidate Lix rlib path required}"
oracle_root="$(git rev-parse --show-toplevel)"
probe_dir="$oracle_root/packages/lix/tests"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/forktree-branch-head-control-compile-fail.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
fail=0

printf 'ForkTree BranchHeadControl compile-fail probes\n'
printf 'CANDIDATE=%s\nRLIB=%s\n' "$candidate_root" "$lix_rlib"

for probe in \
  "$probe_dir/forktree_branch_head_control_forbidden_api.rs" \
  "$probe_dir/forktree_branch_head_control_forbidden_space.rs"; do
  name="$(basename "$probe" .rs)"
  set +e
  rustc --edition=2021 \
    --extern "lix=$lix_rlib" \
    -L "dependency=$dependency_dir" \
    "$probe" -o "$tmp/$name" >"$tmp/$name.log" 2>&1
  status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    printf 'FAIL probe-compiled=%s\n' "$probe"
    fail=1
  elif rg -q 'unresolved import|cannot find|not found in' "$tmp/$name.log"; then
    printf 'PASS compile-failed=%s\n' "$probe"
  else
    printf 'FAIL unexpected-compiler-error=%s\n' "$probe"
    sed -n '1,20p' "$tmp/$name.log"
    fail=1
  fi
done

if [[ "$fail" -ne 0 ]]; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
