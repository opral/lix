#!/usr/bin/env bash
set -u -o pipefail

# TEST/REPORT-ONLY source verifier. It never edits, builds, or runs Lix.
root="${1:?candidate worktree}"
expected_head="${2:?expected report-package head}"
expected_tree="${3:?expected report-package tree}"
frontier="47957d30ae7c16c89c3c523feea23e2f98461fed"
oracle="103e7fe29c60bcd675cee57f8a69986c133366a3"
report="packages/lix/tests/FORKTREE_MERGE_ANALYSIS_DELETION_PLAN_479.md"
script="packages/lix/tests/forktree_merge_analysis_deletion_plan_479.sh"
red=0
pass() { printf 'PASS %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; red=1; }

actual_head="$(git -C "$root" rev-parse HEAD 2>/dev/null || true)"
actual_tree="$(git -C "$root" rev-parse HEAD^{tree} 2>/dev/null || true)"
printf 'MERGE_ANALYSIS_DELETION_PLAN_479\nHEAD=%s\nTREE=%s\n' "$actual_head" "$actual_tree"
test "$actual_head" = "$expected_head" && pass "frontier-head=$actual_head" || fail "frontier-head"
test "$actual_tree" = "$expected_tree" && pass "frontier-tree=$actual_tree" || fail "frontier-tree"
test "$(git -C "$root" rev-parse "$frontier^1" 2>/dev/null || true)" = "39b12568f86d02ec81327cb672b7ef5f7e936448" && pass frontier-parent || fail frontier-parent
for object in "$frontier" "$oracle"; do git -C "$root" cat-file -e "$object^{commit}" 2>/dev/null && pass "object=$object" || fail "missing-object=$object"; done

changed="$(git -C "$root" diff --name-only "$frontier" "$actual_head" 2>/dev/null || true)"
bad_changed="$(printf '%s\n' "$changed" | rg -v -x -e "$report" -e "$script" || true)"
test -z "$bad_changed" && pass report-only-diff || fail "unexpected-diff=$bad_changed"
test -s "$root/$report" && pass "artifact=$report" || fail "missing-artifact=$report"
test -s "$root/$script" && pass "artifact=$script" || fail "missing-artifact=$script"

src="$root/packages/lix/src"
analysis="$src/session/merge/analysis.rs"
branch="$src/session/merge/branch.rs"
transaction="$src/transaction/context.rs"
for path in "$analysis" "$branch" "$transaction" "$src/forktree/serving.rs" "$src/forktree/view.rs"; do test -f "$path" && pass "source=$path" || fail "missing-source=$path"; done

# Positive baseline map: the immutable frontier still contains the legacy
# closure. A future production successor must invert these checks.
for token in with_opening_tracked_reader merge_payload_fallback_ids plan_merge CommitTopologyReader scan_state_rows_at_commit load_state_rows_at_commit derived_plugin_blob_conflicts plugin_merge_conflict_groups; do rg -n --no-heading -F "$token" "$src" >/dev/null 2>&1 && pass "frontier-map=$token" || fail "frontier-map-missing=$token"; done
merge_calls="$(rg -n --no-heading -F with_opening_tracked_reader "$branch" | wc -l | tr -d ' ')"
test "$merge_calls" = 2 && pass merge-callback-callers-2 || fail "merge-callback-callers=$merge_calls"

# Retained cohorts are evidence for the deletion boundary, not merge-owned
# paths that the future slice may remove.
for path in "$src/checkpoint.rs" "$src/session/checkpoint.rs" "$src/session/undo_redo.rs" "$src/sql2/providers/file_history.rs" "$src/sql2/providers/filesystem_working_diff.rs" "$src/tracked_state/diff.rs"; do test -f "$path" && pass "retained-cohort=$path" || fail "missing-retained-cohort=$path"; done

printf 'BASELINE_DELETION_GATE=RED_UNTIL_FORKTREE_SUCCESSOR\n'
printf 'APPROVED_ORACLE=103e7fe29c60bcd675cee57f8a69986c133366a3\n'
if test "$red" -ne 0; then printf 'RESULT=RED\n'; exit 1; fi
printf 'RESULT=GREEN_REPORT_BOUND\n'
