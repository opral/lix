#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
base="b59e1f11a51153e0a787a81f0f25bf104d150aaf"
tree="700fd04d21bc40c05425c9fc9e10d65c9e1eda24"
package="test-reports/forktree-duckdb-olap-b59-rebind"

git -C "$repo_root" merge-base --is-ancestor "$base" HEAD || {
  echo "HEAD is not descended from the exact b59 binding" >&2
  exit 2
}
[[ "$(git -C "$repo_root" rev-parse "$base^{tree}")" == "$tree" ]] || {
  echo "b59 tree mismatch" >&2
  exit 2
}

changed="$(git -C "$repo_root" diff --name-only "$base..HEAD")"
if [[ -n "$changed" ]] && grep -Ev "^$package/" <<<"$changed"; then
  echo "production or out-of-package change detected" >&2
  exit 2
fi

manifest="$repo_root/$package/MANIFEST.json"
results="$repo_root/$package/RESULTS.csv"
matrix="$repo_root/$package/CORRUPTION_MATRIX.md"
model="$repo_root/$package/corruption_matrix_model.rs"
report="$repo_root/$package/CORRECTION_REPORT.md"
jq -e '
  .status == "test-report-only-unrun" and
  .runtime_claims == false and
  .current_main_performance == false and
  .b59_runtime_cells_completed == false and
  .inherited_measurements_are_b59 == false and
  .fixture.row_counts == [10000, 50000, 500000] and
  .fixture.query_count == 9 and
  .thresholds.target_improvement_percent_at_least == 10 and
  .thresholds.critical_guardrail_regression_percent_at_most == 5 and
  .corruption_contract.target_classes == ["global_selector", "branch_selector", "state_root", "catalog_root", "checkpoint_root"] and
  .corruption_contract.adapter_domains == ["global_selector", "branch_selector", "state_root", "catalog_root", "checkpoint_root"] and
  .corruption_contract.corruption_classes == ["malformed", "missing", "wrong_kind", "identity_substitution"] and
  .corruption_contract.valid_absence_distinct == true and
  .corruption_contract.typed_failures == true and
  .corruption_contract.unchanged_authority_fingerprint == true and
  .corruption_contract.zero_durable_work == true
' "$manifest" >/dev/null

test -f "$matrix" && test -f "$model" && test -f "$report" || {
  echo "corruption matrix/model/report missing" >&2
  exit 2
}
(cd "$(dirname "$matrix")" && sha256sum -c SHA256SUMS >/dev/null) || {
  echo "artifact hash manifest mismatch" >&2
  exit 2
}

for marker in \
  "global selector" "branch selector" "state/root object" \
  "MalformedGlobalSelector" "MissingGlobalSelector" \
  "WrongGlobalSelectorKind" "GlobalSelectorIdentityMismatch" \
  "MalformedBranchSelector" "MissingBranchSelector" \
  "WrongBranchSelectorKind" "BranchSelectorIdentityMismatch" \
  "MalformedStateRoot" "MissingStateRoot" "WrongStateRootKind" \
  "StateRootIdentityMismatch" "MalformedCatalogRoot" "MissingCatalogRoot" \
  "WrongCatalogRootKind" "CatalogRootIdentityMismatch" "MalformedCheckpointRoot" \
  "MissingCheckpointRoot" "WrongCheckpointRootKind" "CheckpointRootIdentityMismatch" \
  "ValidAbsence" "fingerprint"; do
  grep -Fq "$marker" "$matrix" || {
    echo "corruption matrix marker missing: $marker" >&2
    exit 2
  }
done

expected_header='source_kind,source_ref,source_sha256,b59_cell_status,setup_excluded,rows,query,historical_forktree_rocks_us,historical_forktree_slate_us,historical_duckdb_us,historical_rocks_over_duck,historical_slate_over_duck,result_digest,reopen_digest,verified,setup_wall_ns,query_wall_ns,query_cpu_ns,alloc_bytes,rss_peak_bytes,backend_reads,backend_read_keys,backend_read_bytes,backend_writes,backend_write_bytes,physical_read_objects,physical_read_bytes,physical_write_objects,physical_write_bytes,publication_count,selector_cas,epoch_cas,vc_reads,vc_writes,oltp_calls,filesystem_calls,cold_reopen'
[[ "$(head -n 1 "$results")" == "$expected_header" ]] || {
  echo "RESULTS.csv schema mismatch" >&2
  exit 2
}
[[ "$(wc -l < "$results")" -eq 28 ]] || {
  echo "expected header plus 27 inherited result rows" >&2
  exit 2
}
awk -F, '
  NR == 1 { next }
  NF != 37 ||
  $1 != "historical-cd76-timing-only" ||
  $2 != "origin/codex/olap-duckdb-comparator-2a0" ||
  $3 != "20f6b010fa770b3a24e69cf7e13a44cda4977d0b3ee3b705dcc49c95e56b3f99" ||
  $4 != "UNRUN" || $5 != "TRUE" { exit 1 }
  { for (i = 13; i <= 37; i++) if ($i != "UNRUN") exit 1 }
' "$results" || {
  echo "RESULTS.csv contains relabeled b59 values or malformed rows" >&2
  exit 2
}
for query in pk_point pk_range narrow_scan wide_scan filtered_scan group_by order_limit simple_join column_projection; do
  [[ "$(awk -F, -v query="$query" 'NR > 1 && $7 == query { count++ } END { print count + 0 }' "$results")" -eq 3 ]] || {
    echo "expected three historical rows for query: $query" >&2
    exit 2
  }
done
[[ "$(wc -l < "$repo_root/$package/RAW_SHA256SUMS")" -eq 9 ]] || {
  echo "expected nine inherited raw-log hashes" >&2
  exit 2
}
git -C "$repo_root" diff --check
echo "GREEN exact b59 report-only binding"
echo "GREEN inherited input explicitly not relabeled as b59 runtime"
echo "GREEN 27 fixture rows, nine raw-log hashes, 10%/5% thresholds"
