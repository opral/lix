#!/usr/bin/env bash
set -u -o pipefail

# Dormant negative compiler gate. Run only after a future candidate has a Lix
# rlib; a successful deleted-reader/space import is a hard failure.
candidate_root="${1:?candidate worktree path required}"
dependency_dir="${2:?candidate dependency directory required}"
lix_rlib="${3:?candidate Lix rlib path required}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/forktree-tracked-state-compile-fail.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
fail=0

printf 'ForkTree tracked-state reader compile-fail probes, exact b59 contract\n'
printf 'CANDIDATE=%s\nRLIB=%s\n' "$candidate_root" "$lix_rlib"
for probe in "$script_dir/forktree_tracked_state_forbidden_reader_b59.rs" \
  "$script_dir/forktree_tracked_state_forbidden_space_b59.rs"; do
  name="$(basename "$probe" .rs)"
  set +e
  rustc --edition=2021 --extern "lix=$lix_rlib" \
    -L "dependency=$dependency_dir" "$probe" -o "$tmp/$name" \
    >"$tmp/$name.log" 2>&1
  status=$?
  set -e
  if [[ "$status" -eq 0 ]]; then
    printf 'FAIL probe-compiled=%s\n' "${probe#"$script_dir"/}"
    fail=1
  elif rg -q 'unresolved import|cannot find|not found in' "$tmp/$name.log"; then
    printf 'PASS compile-failed=%s\n' "${probe#"$script_dir"/}"
  else
    printf 'FAIL unexpected-compiler-error=%s\n' "${probe#"$script_dir"/}"
    sed -n '1,20p' "$tmp/$name.log"
    fail=1
  fi
done

if [[ "$fail" -ne 0 ]]; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
