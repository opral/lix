#!/usr/bin/env bash
set -u -o pipefail

root="$(git rev-parse --show-toplevel)"
src="$root/packages/lix/src"
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

forbidden=(
  'BranchHeadControl'
  'BranchHeadControlCache'
  'BranchHeadControlContext'
  'BranchHeadControlObservation'
  'BranchHeadTrackedReachability'
  'branch_head_control'
  'stage_branch_head_control'
  'stage_delete_branch_head_control'
  'untracked_lifecycle_generation'
  'BRANCH_HEAD_CONTROL_SPACE'
  'BRANCH_HEAD_CONTROL_NAMESPACE'
)

printf 'ForkTree BranchHeadControl hard-cut source gate\n'
printf 'HEAD=%s\n' "$(git rev-parse HEAD)"
printf 'TREE=%s\n' "$(git rev-parse HEAD^{tree})"

for pattern in "${forbidden[@]}"; do
  matches="$(rg -n --no-heading -g '*.rs' -F "$pattern" "$src" 2>/dev/null || true)"
  if [[ -n "$matches" ]]; then
    printf 'FAIL forbidden=%s\n%s\n' "$pattern" "$(print_first_matches "$matches")"
    fail=1
  else
    printf 'PASS absent=%s\n' "$pattern"
  fi
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
  printf 'FAIL old-control-path-exists=%s\n' "$src/branch/control.rs"
  fail=1
else
  printf 'PASS old-control-path-absent\n'
fi

if [[ "$fail" -ne 0 ]]; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
