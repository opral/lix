#!/usr/bin/env bash
set -euo pipefail

# Source-only baseline and migration-boundary verifier. It never builds or
# runs production code. It proves that this package changed no production path
# and that b59 exposes the historical authority and provider surfaces that a
# caller migration must bind to one retained ForkTree read.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
base="b59e1f11a51153e0a787a81f0f25bf104d150aaf"
expected_tree="700fd04d21bc40c05425c9fc9e10d65c9e1eda24"
actual_tree="$(git -C "$repo_root" rev-parse "$base^{tree}")"
[[ "$actual_tree" == "$expected_tree" ]] || { echo "BASE TREE MISMATCH" >&2; exit 2; }

changed="$(git -C "$repo_root" diff --name-only "$base..HEAD")"
if [[ -n "$changed" ]] && grep -Ev '^test-reports/forktree-historical-provider-acceptance-b59/' <<<"$changed"; then
  echo "PRODUCTION OR OUT-OF-PACKAGE CHANGE DETECTED" >&2
  exit 2
fi
echo "GREEN package has no production change"

serving="$repo_root/packages/lix/src/forktree/serving.rs"
view="$repo_root/packages/lix/src/forktree/view.rs"
providers="$repo_root/packages/lix/src/sql2/providers"

require() {
  local label="$1" file="$2" needle="$3"
  grep -Fq "$needle" "$file" || { echo "MISSING $label: $needle" >&2; exit 2; }
  echo "GREEN $label"
}

require "required CommitCatalog resolver" "$serving" 'async fn load_required_commit_catalog_entry<R>'
require "missing catalog is corruption" "$serving" 'selected CommitCatalog entry is absent'
require "historical point uses required resolver" "$serving" 'load_required_commit_catalog_entry(read, repository.commit_catalog_root, commit_id)'
require "retained closure validation" "$serving" 'validate_retained_commit('
require "same-read point lookup" "$serving" 'state_point_on_read('
require "historical batch lowering" "$view" 'load_state_rows_at_commit'
require "batch propagates point error" "$view" 'await?;'
require "file history surface" "$providers/file_history.rs" 'lix_file_history'
require "directory history surface" "$providers/directory_history.rs" 'lix_directory_history'
require "diff surface" "$providers/diff.rs" 'diff_commits('
require "checkpoint history/marker selection" "$providers/checkpoint.rs" 'checkpoint_history_for_branch('
require "checkpoint markers excluded from diff" "$providers/diff.rs" 'CHECKPOINT_MARKER_SCHEMA_KEY'
require "filesystem file/directory working diff" "$providers/filesystem_working_diff.rs" 'FilesystemWorkingDiffKind::Directory'
require "history anchor routing" "$repo_root/packages/lix/src/sql2/history_route.rs" 'default_to_as_of_commit_id'

point_body="$(sed -n '673,715p' "$serving")"
batch_body="$(sed -n '288,323p' "$view")"
if grep -Eq 'begin_read|retry|fallback|cache' <<<"$point_body$batch_body"; then
  echo "HISTORICAL PATH CONTAINS SECOND READ/RETRY/FALLBACK/CACHE" >&2
  exit 2
fi
echo "GREEN historical point/batch bodies use one passed read"

if grep -Fq 'scan_direct_entity_' "$repo_root/packages/lix/src/sql2/entity_batch.rs"; then
  echo "DELETED DIRECT ENTITY READER RESIDUE" >&2
  exit 2
fi
echo "GREEN no deleted direct entity reader symbol"

cat <<'EOF'
BASELINE CONTRACT BOUND
  valid authenticated commit/root + absent key => None/empty
  missing/malformed/wrong-kind/mismatched commit/root => typed error
  file/directory history, diff, checkpoint, working-diff callers must use
  one retained ForkTree read and preserve public ordering, limits, identity,
  null/tombstone, marker, reopen, and error semantics.
EOF
