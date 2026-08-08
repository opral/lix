#!/usr/bin/env bash
set -u -o pipefail

# TEST/REPORT-ONLY verifier. No production source, build, adapter, or runtime.
root="${1:?candidate root}"
expected_head="${2:?expected head}"
expected_tree="${3:?expected tree}"
base="1d9c47728377c6ec7d2646704d51f3aadb11c773"
dir="$(cd -- "$(dirname "$0")" && pwd)"
v2_model="$root/test-reports/tracked-head-whole-module-oracle-b59-corrected/whole_module_contract_model.rs"
v3_model="$dir/authority_corruption_matrix_model.rs"
red=0
pass() { printf 'PASS %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; red=1; }

actual_head="$(git -C "$root" rev-parse HEAD 2>/dev/null || true)"
actual_tree="$(git -C "$root" rev-parse HEAD^{tree} 2>/dev/null || true)"
printf 'BASE=%s\nHEAD=%s\nTREE=%s\n' "$base" "$actual_head" "$actual_tree"
test "$actual_head" = "$expected_head" || fail "head-mismatch"
test "$actual_tree" = "$expected_tree" || fail "tree-mismatch"
test -s "$v2_model" && pass "v2-model-present" || fail "missing-v2-model"
test -s "$v3_model" && pass "v3-model-present" || fail "missing-v3-model"

# Calibration discriminator: v2's corrupt() mutates only state_root. This is
# intentionally accepted as RED evidence and never as v3 coverage.
v2_corrupt="$(sed -n '/fn corrupt(&mut self, kind: Corruption)/,/^    }$/p' "$v2_model" 2>/dev/null || true)"
if printf '%s\n' "$v2_corrupt" | rg -q 'self\.state_root' \
  && ! printf '%s\n' "$v2_corrupt" | rg -q 'global_selector|branch_selector|catalog_root|checkpoint_root'; then
  pass "v2-state-root-only-matrix-rejected"
else
  fail "v2-state-root-only-calibration-not-discriminated"
fi

for token in \
  'Domain::GlobalSelector' 'Domain::BranchSelector' 'Domain::StateRoot' \
  'Domain::CatalogRoot' 'Domain::CheckpointRoot' 'AuthoritySlot::ALL' \
  'Corruption::ALL' 'Corruption::Malformed' 'Corruption::Missing' \
  'Corruption::WrongKind' 'Corruption::IdentitySubstitution' \
  'fn corrupt(&mut self, slot: AuthoritySlot' 'for slot in AuthoritySlot::ALL' \
  'for kind in Corruption::ALL' 'retained_reads: 1' 'retained_views: 1' \
  'plans: 0' 'prepared_writes: 0' 'commits: 0' 'selector_rotations: 0' \
  'assert_eq!(cases, 5 * 4)' 'healthy_view_is_one_read_and_zero_durable_work'; do
  if rg -n --no-heading -F "$token" "$v3_model" >/dev/null 2>&1; then
    pass "v3-contract=$token"
  else
    fail "v3-contract-missing=$token"
  fi
done

# The successor is test/report-only. Any production or v2 mutation is a hard
# scope failure; only this v3 package may differ from v2.
while IFS= read -r path; do
  case "$path" in
    test-reports/tracked-head-whole-module-oracle-b59-corrected-v3/*) : ;;
    *) fail "out-of-scope-path=$path" ;;
  esac
done < <(git -C "$root" diff --name-only "$base..$actual_head")

if test "$red" -ne 0; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
