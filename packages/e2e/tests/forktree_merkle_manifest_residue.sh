#!/usr/bin/env bash
set -euo pipefail

# Source gate for the composed Merkle reader/writer closure. The gate is
# deliberately aware that UploadPartV1's ordered chunk list is a transient
# receipt, not a durable BlobManifest authority.

root=${1:?usage: $0 <lix-repository-root>}
cd "$root"

model=packages/lix/src/forktree/model.rs
merkle=packages/lix/src/forktree/merkle.rs
reachability=packages/lix/src/forktree/reachability.rs
tests=packages/lix/src/forktree/tests.rs

for path in "$model" "$merkle" "$reachability" "$tests"; do
  test -f "$path" || { echo "missing required path: $path" >&2; exit 1; }
done

manifest_section=$(awk '
  /pub\(crate\) struct BlobManifestV1/ { capture=1 }
  capture { print }
  /pub\(crate\) struct UploadPartV1/ { exit }
' "$model")
if grep -Eq 'ordered_chunks: Vec<BlobChunkRefV1>|content_digest: \[u8; 32\]|from_authenticated_chunks' <<<"$manifest_section"; then
  echo "flat BlobManifestV1 authority remains" >&2
  exit 1
fi

if grep -Eq 'BlobId::from_content|BlobId::from_chunks|content_digest' "$merkle"; then
  echo "legacy flat identity residue remains in Merkle closure" >&2
  exit 1
fi

manifest_reachability=$(awk '
  /ObjectDomain::BlobManifest =>/ { capture=1 }
  capture { print }
  /ObjectDomain::BlobMerkleLeafV1/ { exit }
' "$reachability")
if grep -Eq 'ordered_chunks|content_digest|BlobId::from_content|BlobId::from_chunks' <<<"$manifest_reachability"; then
  echo "flat manifest edge residue remains in BlobManifest reachability" >&2
  exit 1
fi

# Every BlobManifestV1 fixture must use the canonical root constructor/build;
# this catches multiline legacy literals without rejecting UploadPartV1.
awk '
  /BlobManifestV1[[:space:]]*\{/ { in_manifest=1; bad=0; start=NR }
  in_manifest && /ordered_chunks[[:space:]]*:/ { bad=1 }
  in_manifest && /content_digest[[:space:]]*:/ { bad=1 }
  in_manifest && /\}/ {
    if (bad) { print "legacy flat fixture near line " start > "/dev/stderr"; exit 1 }
    in_manifest=0
  }
' "$tests"

# The Merkle closure must not reintroduce a whole-base witness or legacy
# constructor.  R4-owned production paths are supplied to the same gate after
# composition; this package does not edit or pre-judge those callers.
for path in "$merkle" "$reachability"; do
  if grep -Eq 'full_base_sha|base_sha256|from_authenticated_chunks|BlobId::from_content|BlobId::from_chunks' "$path"; then
    echo "legacy full-base/BlobId constructor residue in $path" >&2
    exit 1
  fi
done

# Final production composition: file blob readers/publication may retain
# UploadPartV1's receipt-local ordered chunks, but no durable BlobManifest
# traversal, flat identity builder, whole-base witness, or retired splice
# authenticator may remain.
production_closure=(
  packages/lix/src/forktree/blob.rs
  packages/lix/src/forktree/publication.rs
  packages/lix/src/forktree/view.rs
  packages/lix/src/transaction/commit.rs
  packages/lix/src/transaction/context.rs
  packages/lix/src/transaction/staging.rs
  packages/lix/src/transaction/types.rs
  packages/lix/src/session/execute.rs
  packages/lix/src/binary_cas/types.rs
  packages/lix/src/sql2/providers/file.rs
  packages/lix/src/sql2/providers/file_history.rs
)
for path in "${production_closure[@]}"; do
  test -f "$path" || { echo "missing production closure path: $path" >&2; exit 1; }
done
if grep -En 'manifest\.ordered_chunks|manifest\.content_digest|from_authenticated_chunks|authenticate_blob_for_splice|CanonicalBlobIdBuilder|base_sha256\(|result_sha256\(' "${production_closure[@]}"; then
  echo "legacy flat blob authority remains in production composition" >&2
  exit 1
fi

if grep -REn 'BlobId::from_content|BlobId::from_chunks|BlobId::from_single_chunk|fixed manifest v3' packages/lix/src; then
  echo "retired flat/fixed BlobId constructor remains in the production workspace" >&2
  exit 1
fi

echo "PASS: canonical Merkle manifest/root fixture and reachability closure"
