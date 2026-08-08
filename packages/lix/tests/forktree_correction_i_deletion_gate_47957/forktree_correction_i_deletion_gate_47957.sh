#!/usr/bin/env bash
set -euo pipefail

# Test/report-only static gate. It never edits, builds, or runs the candidate.
# Arguments:
#   candidate_root expected_head expected_tree expected_parent
#   base_lib_log base_tests_log candidate_lib_log candidate_tests_log

candidate_root="${1:?candidate root required}"
expected_head="${2:?expected head required}"
expected_tree="${3:?expected tree required}"
expected_parent="${4:?expected parent required}"
base_lib_log="${5:?base library log required}"
base_tests_log="${6:?base test-aware log required}"
candidate_lib_log="${7:?candidate library log required}"
candidate_tests_log="${8:?candidate test-aware log required}"
package_rel="packages/lix/tests/forktree_correction_i_deletion_gate_47957"
src="$candidate_root/packages/lix/src"
failures=0
route_files=(
  packages/lix/src/checkpoint.rs
  packages/lix/src/sql2/providers/working_diff.rs
  packages/lix/src/sql2/providers/checkpoint.rs
  packages/lix/src/sql2/providers/filesystem_working_diff.rs
  packages/lix/src/sql2/history_route.rs
  packages/lix/src/sql2/context.rs
  packages/lix/src/sql2/mod.rs
)

pass() { printf 'PASS %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1"; failures=$((failures + 1)); }

absent() {
  local relative="$1"
  local pattern="$2"
  local path="$candidate_root/$relative"
  if [[ ! -f "$path" ]]; then
    fail "missing-file=$relative"
    return
  fi
  if rg -n --no-heading -F "$pattern" "$path" >/tmp/correction-i-matches 2>/dev/null; then
    printf 'FAIL forbidden=%s file=%s\n' "$pattern" "$relative"
    sed -n '1,20p' /tmp/correction-i-matches
    failures=$((failures + 1))
  else
    pass "absent=$pattern file=$relative"
  fi
}

absent_regex() {
  local relative="$1"
  local pattern="$2"
  local path="$candidate_root/$relative"
  if [[ ! -f "$path" ]]; then
    fail "missing-file=$relative"
    return
  fi
  if rg -n --no-heading -e "$pattern" "$path" >/tmp/correction-i-matches 2>/dev/null; then
    printf 'FAIL forbidden-regex=%s file=%s\n' "$pattern" "$relative"
    sed -n '1,20p' /tmp/correction-i-matches
    failures=$((failures + 1))
  else
    pass "absent-regex=$pattern file=$relative"
  fi
}

absent_production() {
  local relative="$1"
  local pattern="$2"
  local path="$candidate_root/$relative"
  if [[ ! -f "$path" ]]; then
    fail "missing-file=$relative"
    return
  fi
  if awk '/^#[[:space:]]*\[cfg\(test\)\]/{exit} {print}' "$path" |
      rg -n --no-heading -F "$pattern" >/tmp/correction-i-matches 2>/dev/null; then
    printf 'FAIL forbidden-production=%s file=%s\n' "$pattern" "$relative"
    sed -n '1,20p' /tmp/correction-i-matches
    failures=$((failures + 1))
  else
    pass "absent-production=$pattern file=$relative"
  fi
}

required_regex() {
  local relative="$1"
  local pattern="$2"
  local path="$candidate_root/$relative"
  if [[ -f "$path" ]] && rg -n --no-heading -e "$pattern" "$path" >/dev/null 2>&1; then
    pass "required-regex=$pattern file=$relative"
  else
    fail "missing-required-regex=$pattern file=$relative"
  fi
}

count_token() {
  local label="$1"
  local pattern="$2"
  local path="$3"
  local count
  count=$(rg -o -F "$pattern" "$path" 2>/dev/null | wc -l | tr -d ' ')
  printf 'RESIDUAL_COUNT token=%s count=%s path=%s\n' "$label" "$count" "${path#"$candidate_root/"}"
  printf '%s\n' "$count"
}

check_baseline_token() {
  local label="$1"
  local pattern="$2"
  local expected="$3"
  local count delta
  count=$(rg -o -F "$pattern" "$src" 2>/dev/null | wc -l | tr -d ' ')
  delta=$((count - expected))
  printf 'REVERSE_DEP token=%s baseline=%s current=%s delta=%+d\n' "$label" "$expected" "$count" "$delta"
  if ((delta > 0)); then
    fail "reverse-dependency-increase=$label baseline=$expected current=$count"
  else
    pass "reverse-dependency-nonincreasing=$label delta=$delta"
  fi
}

check_route_baseline_token() {
  local label="$1"
  local pattern="$2"
  local expected="$3"
  local count delta
  local absolute_route_files=()
  for file in "${route_files[@]}"; do
    absolute_route_files+=("$candidate_root/$file")
  done
  count=$(awk '
    FNR == 1 {next_file=0}
    /^#[[:space:]]*\[cfg\(test\)\]/{next_file=1}
    !next_file{print FILENAME ":" $0}
  ' "${absolute_route_files[@]}" | { rg -o -i -F "$pattern" || true; } | wc -l | tr -d ' ')
  delta=$((count - expected))
  printf 'ROUTE_DEP token=%s baseline=%s current=%s delta=%+d\n' "$label" "$expected" "$count" "$delta"
  if ((delta > 0)); then
    fail "route-dependency-increase=$label baseline=$expected current=$count"
  else
    pass "route-dependency-nonincreasing=$label delta=$delta"
  fi
}

normalize_errors() {
  rg 'error\[E[0-9]+\]:' "$1" |
    sed -E 's/:[0-9]+:[0-9]+: error/:: error/' | sort
}

normalize_warnings() {
  rg '^[^ ]+:[0-9]+:[0-9]+: warning:' "$1" |
    sed -E 's/:[0-9]+:[0-9]+: warning:/:: warning:/' | sort
}

no_added_diagnostics() {
  local label="$1"
  local normalizer="$2"
  local base="$3"
  local candidate="$4"
  local added
  added=$(comm -13 <($normalizer "$base") <($normalizer "$candidate") || true)
  if [[ -n "$added" ]]; then
    printf 'FAIL compiler-added-%s\n%s\n' "$label" "$added"
    failures=$((failures + 1))
  else
    pass "compiler-no-added-$label"
  fi
}

check_frontier() {
  local label="$1"
  local log="$2"
  local maximum_errors="$3"
  local maximum_warnings="$4"
  if [[ ! -s "$log" ]]; then
    fail "missing-compiler-log=$label"
    return
  fi
  local summary
  summary=$(rg 'error: could not compile `lix` \((lib|lib test)\) due to [0-9]+ previous errors; [0-9]+ warnings emitted' "$log" || true)
  printf 'COMPILER_LOG=%s SHA256=' "$label"
  sha256sum "$log" | awk '{print $1}'
  printf '%s\n' "$summary"
  local line
  if [[ "$label" == library ]]; then
    line=$(grep -m1 'error: could not compile `lix` (lib)' <<<"$summary" || true)
  else
    line=$(grep -m1 'error: could not compile `lix` (lib test)' <<<"$summary" || true)
  fi
  local actual_errors actual_warnings
  actual_errors=$(sed -n 's/.*due to \([0-9][0-9]*\) previous errors.*/\1/p' <<<"$line")
  actual_warnings=$(sed -n 's/.*; \([0-9][0-9]*\) warnings emitted.*/\1/p' <<<"$line")
  if [[ -n "$actual_errors" && -n "$actual_warnings" ]] &&
     ((actual_errors <= maximum_errors && actual_warnings <= maximum_warnings)); then
    pass "compiler-frontier=$label ${actual_errors}/${actual_warnings} (max ${maximum_errors}/${maximum_warnings})"
  else
    fail "compiler-frontier=$label actual=${actual_errors:-missing}/${actual_warnings:-missing} max=${maximum_errors}/${maximum_warnings}"
  fi
}

printf 'CORRECTION_I_HEAD=%s\n' "$(git -C "$candidate_root" rev-parse HEAD)"
printf 'CORRECTION_I_TREE=%s\n' "$(git -C "$candidate_root" rev-parse HEAD^{tree})"
printf 'CORRECTION_I_PARENT=%s\n' "$(git -C "$candidate_root" rev-parse HEAD^)"

[[ "$(git -C "$candidate_root" rev-parse HEAD)" == "$expected_head" ]] || fail head-mismatch
[[ "$(git -C "$candidate_root" rev-parse HEAD^{tree})" == "$expected_tree" ]] || fail tree-mismatch
[[ "$(git -C "$candidate_root" rev-parse HEAD^)" == "$expected_parent" ]] || fail parent-mismatch
[[ -z "$(git -C "$candidate_root" status --porcelain)" ]] && pass worktree-clean || fail worktree-dirty

mapfile -t changed_paths < <(git -C "$candidate_root" diff --name-only "$expected_parent..$expected_head")
if ((${#changed_paths[@]} == 0)); then
  fail no-acceptance-package-paths
else
  package_only=1
  for path in "${changed_paths[@]}"; do
    [[ "$path" == "$package_rel/"* ]] || package_only=0
  done
  if ((package_only)); then
    pass "test-report-only-scope paths=${#changed_paths[@]}"
  else
    printf 'FAIL production-or-out-of-scope-path\n%s\n' "${changed_paths[*]}"
    failures=$((failures + 1))
  fi
fi

oracle="$candidate_root/$package_rel/correction_i_marker_oracle.rs"
if rustc --edition=2021 --test "$oracle" -o /tmp/correction-i-marker-oracle &&
   /tmp/correction-i-marker-oracle --nocapture; then
  pass marker-root-oracle
else
  fail marker-root-oracle
fi

# Function-scoped proof: token presence is insufficient because two providers
# can carry identically named fields while binding different read instances,
# or call a name-only fake chronology seam.  The executable verifier checks
# the actual provider binding/call body and runs positive plus three negative
# fixtures (distinct view, fresh acquisition, fake seam).
structural_proof="$candidate_root/$package_rel/structural_view_proof.py"
if python3 "$structural_proof" "$candidate_root" \
    --fixture-root "$candidate_root/$package_rel/fixtures/positive" \
    --fixture-root "$candidate_root/$package_rel/fixtures/distinct_views" \
    --fixture-root "$candidate_root/$package_rel/fixtures/independent_acquisition" \
    --fixture-root "$candidate_root/$package_rel/fixtures/fake_seam"; then
  pass structural-shared-view-proof
else
  fail structural-shared-view-proof
fi

# Exactly one retained ForkTree-owned chronology seam is required. The name is
# intentionally contract-level: a successor may use either chronology or
# checkpoint wording, but it must be a production function under forktree.
if rg -n -i '(^|[[:space:]])(pub\([^)]*\)[[:space:]]+)?(async[[:space:]]+)?fn[[:space:]]+[A-Za-z0-9_]*(checkpoint|chronology)[A-Za-z0-9_]*' \
    "$src/forktree" -g '*.rs' --glob '!tests.rs' >/tmp/correction-i-seam 2>/dev/null; then
  seam_count=$(wc -l < /tmp/correction-i-seam | tr -d ' ')
  [[ "$seam_count" == 1 ]] && pass "one-forktree-chronology-seam=$seam_count" || {
    printf 'FAIL fork-tree-chronology-seam-count=%s\n' "$seam_count"
    sed -n '1,20p' /tmp/correction-i-seam
    failures=$((failures + 1))
  }
else
  fail missing-forktree-chronology-seam
fi

absent packages/lix/src/checkpoint.rs TrackedStateStoreReader
absent packages/lix/src/checkpoint.rs checkpoint_history_from_head
absent packages/lix/src/checkpoint.rs checkpoint_history_for_branch
absent_regex packages/lix/src/checkpoint.rs 'fn[[:space:]]+(checkpoint_history|checkpoint_chronology|[A-Za-z0-9_]*chronology[A-Za-z0-9_]*)'

forbidden_working_diff=(
  TrackedStateContext
  TrackedStateStoreReader
  TrackedStateScanRequest
  tracked_state.reader
  tracked_state_reader
  latest_checkpoint_for_branch
  checkpoint_history_from_head
)
for token in "${forbidden_working_diff[@]}"; do
  absent packages/lix/src/sql2/providers/working_diff.rs "$token"
done

provider_files=(
  packages/lix/src/sql2/providers/checkpoint.rs
  packages/lix/src/sql2/providers/file_history.rs
  packages/lix/src/sql2/providers/directory_history.rs
  packages/lix/src/sql2/providers/diff.rs
  packages/lix/src/sql2/providers/filesystem_working_diff.rs
  packages/lix/src/sql2/history_route.rs
)
for file in "${provider_files[@]}"; do
  for token in CertifiedHistoryStoreReader CertifiedHistoryReader certified_history_reader TrackedStateScanRequest TrackedStateReadColumns TrackedStateStoreReader TrackedStateContext tracked_state_reader; do
    absent "$file" "$token"
  done
  absent_regex "$file" 'deferred.*(checkpoint|chronology)|fallback.*(checkpoint|chronology)'
  absent_production "$file" 'begin_read('
done

absent_production packages/lix/src/sql2/providers/checkpoint.rs 'ForkTreeReadFacade::new'
absent_production packages/lix/src/sql2/providers/filesystem_working_diff.rs 'ForkTreeReadFacade::new'

# Direct route scan: compatibility/fallback/authority words are checked in
# production text rather than inferred from a seam-name count.
for file in "${route_files[@]}"; do
  for token in fallback compat compatibility legacy; do
    absent_production "$file" "$token"
  done
done

# Structural shared-view identity proof. Both providers must bind the exact
# same retained reader field from the incoming HistoryQuerySource.
for file in packages/lix/src/sql2/providers/checkpoint.rs packages/lix/src/sql2/providers/filesystem_working_diff.rs; do
  required_regex "$file" 'forktree_reader:[[:space:]]+crate::forktree::ForkTreeReadFacade<S>'
  required_regex "$file" 'forktree_reader:[[:space:]]+query_source\.forktree_reader\.clone\(\)'
  required_regex "$file" 'forktree_reader[^\n]*(checkpoint|chronology)|checkpoint|chronology[^\n]*forktree_reader'
  absent_production "$file" 'begin_read('
  absent_production "$file" 'ForkTreeReadFacade::new'
done

required_regex packages/lix/src/sql2/providers/checkpoint.rs '(checkpoint|chronology)'
required_regex packages/lix/src/sql2/providers/checkpoint.rs 'forktree_reader|coherent_view|ForkTreeReadFacade|checkpoint_(history|chronology)'
required_regex packages/lix/src/sql2/providers/filesystem_working_diff.rs '(checkpoint|chronology)'
required_regex packages/lix/src/sql2/providers/filesystem_working_diff.rs 'forktree_reader|coherent_view|ForkTreeReadFacade|checkpoint_(history|chronology)'

for file in packages/lix/src/sql2/history_route.rs packages/lix/src/sql2/context.rs packages/lix/src/sql2/mod.rs; do
  for token in CertifiedHistoryStoreReader CertifiedHistoryReader certified_history_reader TrackedStateScanRequest TrackedStateReadColumns TrackedStateStoreReader TrackedStateContext tracked_state_reader; do
    absent "$file" "$token"
  done
done
absent_production packages/lix/src/sql2/history_route.rs 'ForkTreeReadFacade::new'

check_frontier library "$candidate_lib_log" 139 9
check_frontier library-tests "$candidate_tests_log" 382 16
no_added_diagnostics library-errors normalize_errors "$base_lib_log" "$candidate_lib_log"
no_added_diagnostics tests-errors normalize_errors "$base_tests_log" "$candidate_tests_log"
no_added_diagnostics library-warnings normalize_warnings "$base_lib_log" "$candidate_lib_log"
no_added_diagnostics tests-warnings normalize_warnings "$base_tests_log" "$candidate_tests_log"

baseline_tokens=(
  TrackedStateStoreReader TrackedStateContext TrackedStateScanRequest
  TrackedStateReadColumns TrackedStateWriter TrackedHeadContext
  TrackedWorkingDiff TrackedWorkingDiffEpoch WorkingDiffIndexCoverage
  BranchHeadControl BranchHeadControlContext BranchHeadControlCache
  stage_branch_head_control branch_head_control_precondition
  checkpoint_history_for_branch checkpoint_history_from_head
  checkpoint_history_from_checkpoint is_checkpoint_commit
  latest_checkpoint_for_branch scan_state_rows_at_commit
)
baseline_counts=(17 143 40 15 6 34 4 2 13 86 35 13 32 6 1 7 6 4 3 11)
for i in "${!baseline_tokens[@]}"; do
  check_baseline_token "${baseline_tokens[$i]}" "${baseline_tokens[$i]}" "${baseline_counts[$i]}"
done

route_tokens=(fallback compat compatibility legacy authority authoritative writer)
route_counts=(1 0 0 0 1 0 3)
for i in "${!route_tokens[@]}"; do
  check_route_baseline_token "${route_tokens[$i]}" "${route_tokens[$i]}" "${route_counts[$i]}"
done

if ((failures == 0)); then
  printf 'RESULT=PASS\n'
else
  printf 'RESULT=RED failures=%s\n' "$failures"
  exit 1
fi
