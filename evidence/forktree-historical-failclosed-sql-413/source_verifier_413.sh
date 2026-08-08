#!/usr/bin/env bash
set -euo pipefail

# Source-only binding verifier for the exact SQL frontier. It intentionally
# proves RED: 413 still has the historical missing-CommitCatalog ambiguity.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source_commit="413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d"
expected_tree="820fe560da3bbd2b00b788b0b1759c409048cd6e"
actual_tree="$(git -C "$repo_root" rev-parse "$source_commit^{tree}")"
if [[ "$actual_tree" != "$expected_tree" ]]; then
  echo "SOURCE PROVENANCE ERROR: 413 tree is $actual_tree, expected $expected_tree" >&2
  exit 2
fi

source_blob() { git -C "$repo_root" show "$source_commit:$1"; }
serving="$(source_blob packages/lix/src/forktree/serving.rs)"
view="$(source_blob packages/lix/src/forktree/view.rs)"
reader="$(source_blob packages/lix/src/live_state/forktree_reader.rs)"
entity="$(source_blob packages/lix/src/sql2/entity_batch.rs)"
context="$(source_blob packages/lix/src/live_state/context.rs)"
point_body="$(sed -n '668,719p' <<<"$serving")"
batch_body="$(sed -n '288,323p' <<<"$view")"

require() {
  local label="$1" haystack="$2" needle="$3"
  if ! grep -Fq "$needle" <<<"$haystack"; then
    echo "SOURCE CHECK ERROR: missing $label: $needle" >&2
    exit 2
  fi
  echo "GREEN $label"
}

require "historical point function" "$point_body" 'pub(crate) async fn load_state_value_at_commit<R>'
require "authenticated repository root" "$point_body" 'let repository = load_repository_root(read).await?;'
require "missing catalog lookup" "$point_body" 'lookup_on_read('
require "413 still returns absence for missing catalog" "$point_body" 'return Ok(None);'
require "retained commit validation" "$point_body" 'validate_retained_commit('
require "state lookup uses passed read" "$point_body" 'state_point_on_read('
require "historical batch function" "$batch_body" 'pub(crate) async fn load_state_rows_at_commit('
require "batch maps point result" "$batch_body" 'rows.push(value.map'
require "current reader rejects derived/history" "$reader" 'current ForkTree reader does not serve derived or history schemas'
require "current exact reader opens coherent view" "$reader" 'open_coherent_view_on_read(read, branch_id).await?'
require "SQL projection uses canonical reader" "$entity" 'LiveStateReader, LiveStateScanRequest'
require "SQL projection calls one scan" "$entity" '.scan_batch(request)'

if grep -Eq 'begin_read|retry|fallback|cache' <<<"$point_body$batch_body"; then
  echo "SOURCE CHECK ERROR: historical point/batch body contains a second-read/fallback route" >&2
  exit 2
fi
echo "GREEN historical point/batch body has no begin_read/retry/fallback/cache"

if grep -Fq 'scan_direct_entity_rows' <<<"$context" ||
   grep -Fq 'scan_direct_entity_snapshots' <<<"$context" ||
   grep -Fq 'scan_direct_entity_primary_keys' <<<"$context"; then
  echo "SOURCE CHECK ERROR: deleted direct SQL helper reappeared" >&2
  exit 2
fi
echo "GREEN 413 SQL context has no deleted direct helper symbols"

cat <<'EOF'
RED 413 historical point/scan contract
  valid commit + valid root + absent key => authenticated absence
  missing CommitCatalog entry           => STILL Ok(None), indistinguishable
  missing/wrong-kind/malformed root     => typed object/decode error path
  null/tombstone/value                  => distinct StateCell lowering
  point/batch read lifetime              => passed read, no historical retry/fallback/cache
EOF

if [[ "${1:-}" == "--expect-red" ]]; then
  echo "EXPECTED RED: 413 does not fail closed for a missing CommitCatalog commit"
  exit 0
fi
echo "RED: 413 requires the narrow historical correction" >&2
exit 1
