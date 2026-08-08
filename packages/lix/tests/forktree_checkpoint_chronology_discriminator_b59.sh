#!/usr/bin/env bash
set -u -o pipefail

# Independent test/report-only discriminator. It never edits, builds, or runs
# the candidate. The only compilation performed is the dependency-free oracle
# in forktree_checkpoint_chronology_vectors_b59.rs.

candidate_root="${1:?candidate worktree path required}"
expected_head="${2:?candidate head required}"
expected_tree="${3:?candidate tree required}"
expected_parent="${4:?candidate parent required}"
lib_log="${5:-}"
tests_log="${6:-}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
src="$candidate_root/packages/lix/src"
fail=0

pass() { printf 'PASS %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1"; fail=1; }

forbid_in() {
  local relative="$1" pattern="$2" path="$candidate_root/$relative" matches
  if [[ ! -f "$path" ]]; then
    fail "missing-file=$relative"
    return
  fi
  matches="$(rg -n --no-heading -F "$pattern" "$path" 2>/dev/null || true)"
  if [[ -n "$matches" ]]; then
    printf 'FAIL forbidden=%s file=%s\n%s\n' "$pattern" "$relative" "$matches"
    fail=1
  else
    pass "absent=$pattern file=$relative"
  fi
}

require_in() {
  local relative="$1" pattern="$2" path="$candidate_root/$relative"
  if [[ -f "$path" ]] && rg -n --no-heading -F "$pattern" "$path" >/dev/null 2>&1; then
    pass "required=$pattern file=$relative"
  else
    printf 'FAIL missing-required=%s file=%s\n' "$pattern" "$relative"
    fail=1
  fi
}

forbid_regex_in() {
  local relative="$1" pattern="$2" path="$candidate_root/$relative" matches
  if [[ ! -f "$path" ]]; then
    fail "missing-file=$relative"
    return
  fi
  matches="$(rg -n --no-heading -e "$pattern" "$path" 2>/dev/null || true)"
  if [[ -n "$matches" ]]; then
    printf 'FAIL forbidden-regex=%s file=%s\n%s\n' "$pattern" "$relative" "$matches"
    fail=1
  else
    pass "absent-regex=$pattern file=$relative"
  fi
}

account_log() {
  local label="$1" path="$2" summary errors warnings error_hash warning_hash
  if [[ ! -s "$path" ]]; then
    printf 'FAIL missing-compiler-log=%s\n' "$path"
    fail=1
    return
  fi
  summary="$(rg 'error: could not compile `lix` \((lib|lib test)\) due to [0-9]+ previous errors; [0-9]+ warnings emitted' "$path" || true)"
  printf 'COMPILER_LOG=%s SHA256=' "$label"
  sha256sum "$path" | awk '{print $1}'
  printf '%s\n' "$summary"
  errors="$(printf '%s\n' "$summary" | sed -n 's/.*due to \([0-9][0-9]*\) previous errors.*/\1/p' | paste -sd, -)"
  warnings="$(printf '%s\n' "$summary" | sed -n 's/.*; \([0-9][0-9]*\) warnings emitted.*/\1/p' | paste -sd, -)"
  printf 'COMPILER_COUNTS=%s errors=%s warnings=%s\n' "$label" "${errors:-missing}" "${warnings:-missing}"
  error_hash="$(rg 'error\[E[0-9]+\]:' "$path" | sed -E 's/:[0-9]+:[0-9]+: error/:: error/' | sort | sha256sum | awk '{print $1}')"
  warning_hash="$(rg '^[^ ]+:[0-9]+:[0-9]+: warning:' "$path" | sed -E 's/:[0-9]+:[0-9]+: warning:/:: warning:/' | sort | sha256sum | awk '{print $1}')"
  printf 'COMPILER_NORMALIZED=%s errors=%s warnings=%s\n' "$label" "$error_hash" "$warning_hash"
}

actual_head="$(git -C "$candidate_root" rev-parse HEAD 2>/dev/null || true)"
actual_tree="$(git -C "$candidate_root" rev-parse HEAD^{tree} 2>/dev/null || true)"
actual_parent="$(git -C "$candidate_root" rev-parse HEAD^ 2>/dev/null || true)"
printf 'HEAD=%s\nTREE=%s\nPARENT=%s\n' "$actual_head" "$actual_tree" "$actual_parent"
[[ "$actual_head" == "$expected_head" ]] || fail "head-mismatch"
[[ "$actual_tree" == "$expected_tree" ]] || fail "tree-mismatch"
[[ "$actual_parent" == "$expected_parent" ]] || fail "parent-mismatch"

fixture="$script_dir/forktree_checkpoint_chronology_vectors_b59.tsv"
oracle="$script_dir/forktree_checkpoint_chronology_vectors_b59.rs"
if [[ ! -s "$fixture" || ! -s "$oracle" ]]; then
  fail "missing-independent-oracle"
else
  pass "oracle-artifacts-present"
fi
grep -F 'checkpoint_to_ordinary|branch-A|commit-R|commit-C,commit-D|commit-O|commit-C,commit-D|commit-R|commit-O' "$fixture" >/dev/null \
  && pass "fixture-exact-marker-and-implicit-root-vector" \
  || fail "fixture-exact-marker-and-implicit-root-vector"

oracle_dir="$(mktemp -d)"
trap 'rm -rf "$oracle_dir"' EXIT
if rustc --edition=2021 --test "$oracle" -o "$oracle_dir/oracle" \
    && "$oracle_dir/oracle" --nocapture; then
  pass "standalone-chronology-oracle"
else
  fail "standalone-chronology-oracle"
fi

route_files=(
  packages/lix/src/sql2/history_route.rs
  packages/lix/src/sql2/context.rs
  packages/lix/src/sql2/mod.rs
)
provider_files=(
  packages/lix/src/sql2/providers/checkpoint.rs
  packages/lix/src/sql2/providers/file_history.rs
  packages/lix/src/sql2/providers/directory_history.rs
  packages/lix/src/sql2/providers/diff.rs
  packages/lix/src/sql2/providers/filesystem_working_diff.rs
)

for relative in "${route_files[@]}" "${provider_files[@]}"; do
  for pattern in \
    CertifiedHistoryStoreReader CertifiedHistoryReader certified_history_reader \
    TrackedStateScanRequest TrackedStateReadColumns TrackedStateStoreReader \
    TrackedStateContext 'tracked_state.reader(' 'tracked_state_reader(' \
    certified_request; do
    forbid_in "$relative" "$pattern"
  done
done

# No second snapshot or local chronology walker may remain in the checkpoint
# owner. The successor must call an authenticated ForkTree-owned chronology
# method over the already-owned read.
forbid_regex_in packages/lix/src/checkpoint.rs 'fn[[:space:]]+checkpoint_history'
forbid_in packages/lix/src/checkpoint.rs checkpoint_history_for_branch_forktree
forbid_in packages/lix/src/checkpoint.rs checkpoint_marker_from_rows
forbid_in packages/lix/src/checkpoint.rs scan_state_rows_at_commit
forbid_in packages/lix/src/sql2/providers/checkpoint.rs checkpoint_history_for_branch_forktree
forbid_in packages/lix/src/sql2/providers/filesystem_working_diff.rs checkpoint_history_for_branch_forktree

for relative in "${provider_files[@]}"; do
  forbid_in "$relative" 'begin_read('
done
forbid_in packages/lix/src/sql2/history_route.rs 'begin_read('

owner_matches="$(rg -n --no-heading -i -g '*.rs' \
  -e 'checkpoint_(history|marker|chronology|baseline)|historical_checkpoint' \
  "$src/forktree" 2>/dev/null || true)"
if [[ -n "$owner_matches" ]]; then
  printf 'FORKTREE_OWNER_CANDIDATES\n%s\n' "$owner_matches"
  pass "forktree-owns-checkpoint-seam"
else
  fail "missing-forktree-owned-checkpoint-seam"
fi
for relative in packages/lix/src/sql2/providers/checkpoint.rs \
  packages/lix/src/sql2/providers/filesystem_working_diff.rs; do
  if rg -n --no-heading -i -e 'checkpoint_(history|marker|chronology|baseline)|historical_checkpoint' \
      "$candidate_root/$relative" >/dev/null 2>&1; then
    pass "provider-binds-forktree-checkpoint-seam file=$relative"
  else
    fail "provider-missing-forktree-checkpoint-seam file=$relative"
  fi
done

for token in TrackedStateStoreReader TrackedHeadContext BranchHeadControlContext \
  BranchHeadControlCache stage_branch_head_control \
  branch_head_control_precondition untracked_lifecycle_generation; do
  count="$(rg -n --no-heading --glob '*.rs' -F "$token" "$src" 2>/dev/null | wc -l | tr -d ' ')"
  printf 'RESIDUAL_COUNT token=%s count=%s\n' "$token" "$count"
done

if [[ -n "$lib_log" ]]; then account_log library "$lib_log"; fi
if [[ -n "$tests_log" ]]; then account_log library-tests "$tests_log"; fi

if [[ "$fail" -ne 0 ]]; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
