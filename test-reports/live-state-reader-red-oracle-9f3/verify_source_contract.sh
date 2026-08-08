#!/usr/bin/env bash
set -euo pipefail

repo=${1:?usage: verify_source_contract.sh <repo> [head]}
head=${2:-HEAD}
cd "$repo"

expected_head=9f3c703e953440cde1d60b1511467c4337648c8f
expected_tree=51a0026c0c3eced6fdaa5e5ed4824111377f086c
actual_head=$(git rev-parse "$head")
actual_tree=$(git rev-parse "$head^{tree}")
test "$actual_head" = "$expected_head" || {
  echo "BLOCKER wrong head: expected=$expected_head actual=$actual_head"; exit 2;
}
test "$actual_tree" = "$expected_tree" || {
  echo "BLOCKER wrong tree: expected=$expected_tree actual=$actual_tree"; exit 2;
}

reader=$(git show "$head:packages/lix/src/live_state/forktree_reader.rs")
context=$(git show "$head:packages/lix/src/live_state/context.rs")

echo "TARGET head=$actual_head tree=$actual_tree"

# Existing fail-closed controls are retained. These are controls, not red
# findings, and their absence would turn this oracle into a different blocker.
printf '%s\n' "$reader" | rg -q 'request\.filter\.untracked == Some\(true\)' || {
  echo "BLOCKER explicit untracked guard missing"; exit 2;
}
printf '%s\n' "$reader" | rg -q 'filter\.constraints\.is_empty\(\)' || {
  echo "BLOCKER constraint guard missing"; exit 2;
}
printf '%s\n' "$reader" | rg -q 'LiveStateRowFilter::All' || {
  echo "BLOCKER row-shape guard missing"; exit 2;
}
echo "CONTROL-01 explicit untracked/constraint/rows=None guard is present"

red=0
if printf '%s\n' "$reader" | rg -q 'schema_keys\.iter\(\).*schema == &key\.schema_key|schema == &key\.schema_key'; then
  echo "RED-01 derived/history schema reaches current-state filtering and may return Ok(empty)"
  red=1
fi
if ! printf '%s\n' "$reader" | rg -q 'is_derived_schema|request_may_include_derived'; then
  echo "RED-02 mixed/complex schema request has no boundary rejection and may return partial/empty current rows"
  red=1
fi

exact=$(printf '%s\n' "$context" | sed -n '/pub(crate) async fn load_exact_batch(/,/^    pub(crate) async fn scan_tracked_batch(/p')
if printf '%s\n' "$exact" | rg -q 'self\.scan_batch\(&request\.row_scan_request\(row\)\)'; then
  echo "RED-03 load_exact_batch lowers derived rows through scan_batch and maps empty to None"
  red=1
fi
if printf '%s\n' "$exact" | rg -q 'scan_scope\('; then
  echo "RED-04 load_exact_batch transitively acquires BranchHeadControlContext through scan_scope"
  red=1
fi
if printf '%s\n' "$exact" | rg -q 'tracked_head' &&
   printf '%s\n' "$exact" | rg -q 'load_projected_live_batch_refs_for_domain'; then
  echo "RED-05 load_exact_batch directly acquires TrackedHead through load_projected_live_batch_refs_for_domain"
  red=1
fi

if test "$red" -eq 0; then
  echo "ORACLE_STATUS=NOT_RED (source shape changed; inspect before accepting)"
  exit 1
fi
echo "ORACLE_STATUS=RED"
exit 1
