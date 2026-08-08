#!/usr/bin/env bash
set -euo pipefail

# Source-only Cut B oracle. It never compiles, opens an adapter, writes a
# repository, or invokes a production reader. It also owns the rebased
# successor path policy: filesystem/plugin reader plumbing is permitted, while
# scalar/W2, writer, GC, CAS/storage, selector, and compatibility widening is
# rejected before any symbol check.
root="${CUT_B_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$root"

fail=0
base="${CUT_B_BASE:-${1:-}}"
head="${CUT_B_HEAD:-${2:-HEAD}}"
artifact_prefix="test-reports/stage2-filesystem-plugin-reader-705/"
allowed_production=(
  packages/lix/src/filesystem/read.rs
  packages/lix/src/filesystem/mod.rs
  packages/lix/src/plugin/registry.rs
  packages/lix/src/plugin/mod.rs
  packages/lix/src/sql2/providers/file.rs
  packages/lix/src/session/merge/branch.rs
)
primary=(
  packages/lix/src/filesystem/read.rs
  packages/lix/src/plugin/registry.rs
)

if [[ -n "$base" ]]; then
  if ! git rev-parse --verify --quiet "${base}^{commit}" >/dev/null; then
    echo "Cut B path policy: base is not a commit: $base" >&2
    exit 2
  fi
  if ! git rev-parse --verify --quiet "${head}^{commit}" >/dev/null; then
    echo "Cut B path policy: head is not a commit: $head" >&2
    exit 2
  fi
  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    case "$path" in
      "${artifact_prefix}"*)
        ;;
      packages/lix/src/filesystem/read.rs|\
      packages/lix/src/filesystem/mod.rs|\
      packages/lix/src/plugin/registry.rs|\
      packages/lix/src/plugin/mod.rs|\
      packages/lix/src/sql2/providers/file.rs|\
      packages/lix/src/session/merge/branch.rs)
        ;;
      *)
        case "$path" in
          *scalar*|*Scalar*|*entity*|*Entity*|*w2*|*W2*)
            category="scalar/entity/W2"
            ;;
          *writer*|*Writer*|*transaction*|*Transaction*|*publication*|*Publication*)
            category="transaction/publication/writer"
            ;;
          *gc*|*GC*)
            category="GC orchestration"
            ;;
          *cas*|*CAS*|*storage*|*Storage*)
            category="CAS/storage"
            ;;
          *selector*|*Selector*|*forktree*|*ForkTree*)
            category="selector/ForkTree owner"
            ;;
          *)
            category="outside Cut B reader allowlist"
            ;;
        esac
        echo "FORBIDDEN Cut B widening [$category]: $path" >&2
        fail=1
        ;;
    esac
  done < <(git diff --name-only --diff-filter=ACMRTUXB "$base" "$head")
fi

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
require_present "FilesystemIndex" "${primary[0]}"
require_present "load_plugin_registry_at_commit" "${primary[1]}"
require_present "CoherentView" "${primary[0]}"
require_present "CoherentView" "${primary[1]}"

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
  require_absent "load_commit_state_manifest" "$path"
  require_absent "load_change_record_by_id" "$path"
  require_absent "scan_change_records_from_commit_deltas" "$path"
  require_absent "StorageSpace::mutable" "$path"
done

if (( fail != 0 )); then
  echo "Cut B filesystem/plugin reader source gate: FAIL (expected on exact 705/predecessor)" >&2
  exit 1
fi
echo "Cut B filesystem/plugin reader source gate: PASS"
