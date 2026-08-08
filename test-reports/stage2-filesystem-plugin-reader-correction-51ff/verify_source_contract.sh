#!/usr/bin/env bash
set -euo pipefail

# Test/report-only Cut B correction oracle. No compile, adapter, write, or
# production mutation is performed. The source gate is intentionally RED on
# the immutable 51ff control and becomes eligible only after all raw-view,
# empty-masking, and forbidden-path discriminators disappear.
root="${CUT_B_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$root"

fail=0
base="${CUT_B_BASE:-${1:-}}"
head="${CUT_B_HEAD:-${2:-HEAD}}"
artifact_prefix="test-reports/stage2-filesystem-plugin-reader-correction-51ff/"
primary=(
  packages/lix/src/filesystem/read.rs
  packages/lix/src/plugin/registry.rs
)
allowed=(
  packages/lix/src/filesystem/read.rs
  packages/lix/src/filesystem/mod.rs
  packages/lix/src/plugin/registry.rs
  packages/lix/src/plugin/mod.rs
  packages/lix/src/session/merge/branch.rs
  packages/lix/src/forktree/mod.rs
  packages/lix/src/forktree/serving.rs
  packages/lix/src/forktree/state.rs
  packages/lix/src/forktree/view.rs
  packages/lix/src/live_state/forktree_reader.rs
  packages/lix/src/live_state/mod.rs
  packages/lix/src/tracked_state/context.rs
)

require_present() {
  local pattern="$1" path="$2"
  if ! rg -q --fixed-strings "$pattern" "$path"; then
    echo "MISSING Cut B correction symbol: $pattern in $path" >&2
    fail=1
  fi
}

require_absent() {
  local pattern="$1" path="$2"
  if rg -q --fixed-strings "$pattern" "$path"; then
    echo "RED Cut B correction residue: $pattern in $path" >&2
    fail=1
  fi
}

is_allowed_path() {
  local candidate="$1" allowed_path
  for allowed_path in "${allowed[@]}"; do
    [[ "$candidate" == "$allowed_path" ]] && return 0
  done
  return 1
}

if [[ -n "$base" ]]; then
  git rev-parse --verify --quiet "${base}^{commit}" >/dev/null || {
    echo "Cut B correction path policy: invalid base $base" >&2
    exit 2
  }
  git rev-parse --verify --quiet "${head}^{commit}" >/dev/null || {
    echo "Cut B correction path policy: invalid head $head" >&2
    exit 2
  }
  while IFS= read -r path; do
    [[ -n "$path" ]] || continue
    if [[ "$path" == "$artifact_prefix"* ]]; then
      continue
    fi
    if ! is_allowed_path "$path"; then
      case "$path" in
        *gc.rs|*session/gc.rs|*reachability.rs|*publication.rs|*selector*)
          category="GC/selector/publication algorithm" ;;
        *transaction*|*writer*|*commit.rs)
          category="writer/transaction" ;;
        *scalar*|*entity*|*w2*|*W2*|*w3*|*W3*|*w4*|*W4*|*w5*|*W5*)
          category="scalar/W2/W3/W4/W5" ;;
        *binary_cas*|*storage*|*Storage*)
          category="CAS/storage authority" ;;
        *)
          category="outside read-facade allowlist" ;;
      esac
      echo "FORBIDDEN Cut B correction path [$category]: $path" >&2
      fail=1
      continue
    fi
    if [[ "$path" == "packages/lix/src/tracked_state/context.rs" ]]; then
      cleanup_diff="$(git diff --unified=0 "$base" "$head" -- "$path")"
      if ! grep -q -- "^-.*pub(crate) fn store" <<<"$cleanup_diff"; then
        echo "FORBIDDEN tracked_state/context.rs change is not raw-store deletion: $path" >&2
        fail=1
      fi
      if grep -Eq '^\+[^+].*(store\(\)|StorageAdapterRead|begin_read)' <<<"$cleanup_diff"; then
        echo "FORBIDDEN tracked-state read/store addition: $path" >&2
        fail=1
      fi
    fi
  done < <(git diff --name-only --diff-filter=ACMRTUXB "$base" "$head")

  additions="$(git diff --unified=0 "$base" "$head" -- 'packages/lix/src')"
  for forbidden in begin_write StorageWriteSet PreparedPublication stage_reclaimable_upload_receipts advance_gc StorageSpace::mutable; do
    if grep -Eq "^\+[^+].*${forbidden}" <<<"$additions"; then
      echo "FORBIDDEN Cut B correction source addition: $forbidden" >&2
      fail=1
    fi
  done
fi

require_present "BlobId" "${primary[0]}"
require_present "BlobId" "${primary[1]}"
require_present "filesystem_schema_keys" "${primary[0]}"
require_present "PLUGIN_REGISTRY_KEY" "${primary[1]}"
require_present "CoherentView" "${primary[0]}"
require_present "CoherentView" "${primary[1]}"
require_present "from_optional_snapshot" "${primary[1]}"

for path in "${primary[@]}"; do
  require_absent "scan_forktree_branch" "$path"
  require_absent "open_coherent_view_on_read(store" "$path"
  require_absent "store: &S" "$path"
  require_absent "S: crate::storage_adapter::StorageAdapterRead" "$path"
  require_absent "load_commit_member_records(store" "$path"
  require_absent "load_state_value_at_commit(store" "$path"
  require_absent "unwrap_or_default()" "$path"
done

require_absent "pub(crate) fn store(&self) -> &S" packages/lix/src/tracked_state/context.rs
require_absent "reader.store()" packages/lix/src/session/merge/branch.rs
require_absent "None | Some(StateCell::Null | StateCell::Tombstone) => None" packages/lix/src/plugin/registry.rs
require_absent "load_commit_member_records(store" packages/lix/src/filesystem/read.rs
require_absent "load_state_value_at_commit(store" packages/lix/src/plugin/registry.rs

if (( fail != 0 )); then
  echo "Cut B correction source gate: FAIL (expected on exact 51ff control)" >&2
  exit 1
fi
echo "Cut B correction source gate: PASS"
