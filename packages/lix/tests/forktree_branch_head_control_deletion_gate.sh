#!/usr/bin/env bash
set -u -o pipefail

# Source-only hard-cut gate. The candidate is supplied as a disposable
# worktree; this script never edits or builds it.
candidate_root="${1:?candidate worktree path required}"
expected_head="${2:?expected candidate commit required}"
expected_tree="${3:?expected candidate tree required}"
oracle_root="$(git rev-parse --show-toplevel)"
src="$candidate_root/packages/lix/src"
script_dir="$oracle_root/packages/lix/tests"
fail=0

print_first_matches() {
  local text="$1"
  local count=0
  local line
  while IFS= read -r line; do
    printf '%s\n' "$line"
    count=$((count + 1))
    if [[ "$count" -ge 20 ]]; then
      break
    fi
  done <<< "$text"
}

record_forbidden() {
  local pattern="$1"
  local matches count
  matches="$(rg -n --no-heading -g '*.rs' -F "$pattern" "$src" 2>/dev/null || true)"
  if [[ -n "$matches" ]]; then
    count="$(printf '%s\n' "$matches" | awk 'NF { n++ } END { print n + 0 }')"
    printf 'FAIL forbidden=%s count=%s\n%s\n' "$pattern" "$count" "$(print_first_matches "$matches")"
    fail=1
  else
    printf 'PASS absent=%s\n' "$pattern"
  fi
}

printf 'ForkTree BranchHeadControl deletion/compile-fail gate\n'
actual_head="$(git -C "$candidate_root" rev-parse HEAD 2>/dev/null || true)"
actual_tree="$(git -C "$candidate_root" rev-parse HEAD^{tree} 2>/dev/null || true)"
printf 'CANDIDATE_HEAD=%s\nCANDIDATE_TREE=%s\n' "$actual_head" "$actual_tree"
if [[ "$actual_head" != "$expected_head" ]]; then
  printf 'FAIL head-mismatch expected=%s actual=%s\n' "$expected_head" "$actual_head"
  fail=1
fi
if [[ "$actual_tree" != "$expected_tree" ]]; then
  printf 'FAIL tree-mismatch expected=%s actual=%s\n' "$expected_tree" "$actual_tree"
  fail=1
fi
if [[ ! -d "$src" ]]; then
  printf 'FAIL missing-source-root=%s\n' "$src"
  exit 1
fi

forbidden=(
  'BranchHeadControl'
  'BranchHeadControlCache'
  'BranchHeadControlContext'
  'BranchHeadControlObservation'
  'BranchHeadTrackedReachability'
  'branch_head_control'
  'stage_branch_head_control'
  'stage_delete_branch_head_control'
  'branch_head_control_precondition'
  'untracked_lifecycle_generation'
  'BRANCH_HEAD_CONTROL_SPACE'
  'BRANCH_HEAD_CONTROL_NAMESPACE'
  'BRANCH_HEAD_CONTROL_'
  'BranchHeadControlAdapter'
  'BranchHeadControlWrapper'
  'BranchHeadControlFacade'
  'BranchHeadControlCompat'
  'branch_head_control_adapter'
  'branch_head_control_wrapper'
  'branch_head_control_compat'
  'legacy_branch_head_control'
  'branch_head_control_fallback'
  'branch_head_control_migration'
)
for pattern in "${forbidden[@]}"; do
  record_forbidden "$pattern"
done

required=(
  'GlobalSelectorV1'
  'BranchSelectorV1'
  'PreparedPublication'
  'open_coherent_view'
  'SELECTOR_SPACE'
  'advance_gc'
)
for pattern in "${required[@]}"; do
  if rg -n --no-heading -g '*.rs' -F "$pattern" "$src/forktree" >/dev/null 2>&1; then
    printf 'PASS required=%s\n' "$pattern"
  else
    printf 'FAIL missing-required=%s\n' "$pattern"
    fail=1
  fi
done

if [[ -e "$src/branch/control.rs" ]]; then
  printf 'FAIL old-control-module=%s\n' "$src/branch/control.rs"
  fail=1
else
  printf 'PASS old-control-module-absent\n'
fi

for probe in \
  "$script_dir/forktree_branch_head_control_forbidden_api.rs" \
  "$script_dir/forktree_branch_head_control_forbidden_space.rs"; do
  if [[ ! -s "$probe" ]] || ! rg -q -F 'EXPECT_COMPILE_FAIL' "$probe"; then
    printf 'FAIL missing-or-unmarked-probe=%s\n' "$probe"
    fail=1
  else
    printf 'PASS compile-fail-probe-present=%s\n' "$probe"
  fi
done

if [[ "$fail" -ne 0 ]]; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
