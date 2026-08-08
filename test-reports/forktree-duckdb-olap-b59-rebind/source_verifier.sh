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
jq -e '
  .status == "test-report-only-unrun" and
  .runtime_claims == false and
  .current_main_performance == false and
  .b59_runtime_cells_completed == false and
  .inherited_measurements_are_b59 == false and
  .fixture.row_counts == [10000, 50000, 500000] and
  .fixture.query_count == 9 and
  .thresholds.target_improvement_percent_at_least == 10 and
  .thresholds.critical_guardrail_regression_percent_at_most == 5
' "$manifest" >/dev/null

[[ "$(wc -l < "$repo_root/$package/RESULTS.csv")" -eq 28 ]] || {
  echo "expected header plus 27 inherited result rows" >&2
  exit 2
}
[[ "$(wc -l < "$repo_root/$package/RAW_SHA256SUMS")" -eq 9 ]] || {
  echo "expected nine inherited raw-log hashes" >&2
  exit 2
}
git -C "$repo_root" diff --check
echo "GREEN exact b59 report-only binding"
echo "GREEN inherited input explicitly not relabeled as b59 runtime"
echo "GREEN 27 fixture rows, nine raw-log hashes, 10%/5% thresholds"
