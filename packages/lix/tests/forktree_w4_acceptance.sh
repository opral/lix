#!/usr/bin/env bash
set -euo pipefail

# Source-only W4 residue gate. It intentionally does not compile or open a
# storage adapter. A pre-W4 tree is expected to fail this gate.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$root"

fail=0
require_present() {
  local pattern="$1" file="$2"
  if ! rg -q --fixed-strings "$pattern" "$file"; then
    echo "MISSING required W4 owner: $pattern in $file" >&2
    fail=1
  fi
}
require_absent() {
  local pattern="$1" path="$2"
  if rg -q --fixed-strings "$pattern" "$path"; then
    echo "LEGACY W4 residue: $pattern in $path" >&2
    fail=1
  fi
}

pub="packages/lix/src/forktree/publication.rs"
media="packages/lix/src/session/media_upload.rs"
commit="packages/lix/src/transaction/commit.rs"

require_present "publish_new_upload" "$pub"
require_present "stage_upload_part" "$pub"
require_present "stage_upload_progress" "$pub"
require_present "stage_receipt_tree_edit" "$pub"
require_present "publish_completed_upload" "$pub"
require_present "into_storage_plan" "$pub"

require_absent "UPLOAD_STATE_SPACE" packages/lix/src
require_absent "UPLOAD_MANIFEST_LEAF_SPACE" packages/lix/src
require_absent "struct UploadState" "$media"
require_absent "struct UploadManifestLeaf" "$media"
require_absent "struct UploadComplete" "$media"
require_absent "stage_atomic_cas_publication(" "$media"
require_absent "execute_fast_lix_file_prepared_path_write" "$media"
require_absent "binary_cas::kv" packages/lix/src
require_absent "stage_fixed_part" "$media"
require_absent "stage_fixed_manifest" "$media"

if rg -q --fixed-strings "file payload publication requires the ForkTree receipt/manifest lowering slice" "$commit"; then
  echo "LEGACY W4 rejection still present: file content is not lowered" >&2
  fail=1
fi

if (( fail != 0 )); then
  echo "W4 source gate: FAIL (expected on pre-W4/compiler-red anchor)" >&2
  exit 1
fi
echo "W4 source gate: PASS"
