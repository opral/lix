#!/usr/bin/env bash
set -euo pipefail

# Source-only Cut B oracle. It intentionally does not compile or open an
# adapter. The d6b predecessor is expected to fail because the reader cut has
# not landed yet.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

fail=0
primary=(packages/lix/src/filesystem/read.rs packages/lix/src/plugin/registry.rs)
require_present() {
  local pattern="$1" path="$2"
  if ! rg -q --fixed-strings "$pattern" "$path"; then
    echo "MISSING Cut B owner/facade symbol: $pattern in $path" >&2
    fail=1
  fi
}
require_absent() {
  local pattern="$1" path="$2"
  if rg -q --fixed-strings "$pattern" "$path"; then
    echo "LEGACY Cut B reader residue: $pattern in $path" >&2
    fail=1
  fi
}

require_present "BlobId" "${primary[0]}"
require_present "BlobId" "${primary[1]}"
require_present "filesystem_schema_keys" "${primary[0]}"
require_present "PLUGIN_REGISTRY_KEY" "${primary[1]}"

for path in "${primary[@]}"; do
  require_absent "TrackedHeadContext" "$path"
  require_absent "TrackedStateStoreReader" "$path"
  require_absent "TrackedStateScanRequest" "$path"
  require_absent "TrackedStateFilter" "$path"
  require_absent "TrackedStateReadColumns" "$path"
  require_absent "scan_live_batches_for_controls" "$path"
  require_absent "load_projected_batch_at_commit" "$path"
  require_absent "load_retained_commit_snapshots_for_schemas" "$path"
  require_absent "BranchHeadControl" "$path"
  require_absent "BinaryCasContext" "$path"
  require_absent "BINARY_CAS_" "$path"
  require_absent "StorageSpace::mutable" "$path"
done

if (( fail != 0 )); then
  echo "Cut B filesystem/plugin reader source gate: FAIL (expected on d6b/predecessor)" >&2
  exit 1
fi
echo "Cut B filesystem/plugin reader source gate: PASS"
