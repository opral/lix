#!/usr/bin/env bash
set -u

repo=${1:?repository path required}
candidate=${2:?candidate SHA required}
expected=e1af471b9ab0f598dafa7c2ddec7867667c81740
if [[ "$candidate" != "$expected" ]]; then
  echo "WRONG-CANDIDATE expected=$expected actual=$candidate" >&2
  exit 2
fi
git -C "$repo" cat-file -e "$candidate^{commit}" || exit 2

red=0
finding() {
  red=$((red + 1))
  printf 'RED-%02d %s\n' "$red" "$1"
}
present() {
  local path=$1 pattern=$2 label=$3
  if git -C "$repo" grep -q -E "$pattern" "$candidate" -- "$path"; then
    finding "$label path=$path pattern=$pattern"
  fi
}

echo "W4_SOURCE_GATE verdict=RED candidate=$candidate"
present packages/lix/src/transaction/commit.rs 'reject_not_yet_lowered_cohorts|file_content_writes' 'file-content cohort is not lowered'
present packages/lix/src/transaction/context.rs 'stage_atomic_cas_publication' 'legacy independent CAS publication seam'
present packages/lix/src/sql2/providers/file.rs 'execute_fast_lix_file_prepared_path_write' 'prepared-CAS file bridge'
present packages/lix/src/session/media_upload.rs 'UPLOAD_STATE_SPACE|UPLOAD_MANIFEST_LEAF_SPACE|stage_atomic_cas_publication' 'legacy upload spaces/finalizer'
present packages/lix/src/binary_cas/context.rs 'ExistingChunkAwareBinaryCasWriter|stage_fixed_part|stage_fixed_manifest|stage_file_payload' 'legacy Binary CAS writer'
present packages/lix/src/binary_cas/context.rs 'binary_cas::kv' 'stale missing Binary CAS KV owner reference'
present packages/lix/src/storage_bench.rs 'BINARY_CAS_(CHUNK|MANIFEST)_SPACE' 'legacy Binary CAS benchmark owner'
present packages/lix/src/session/media_upload.rs 'StorageSpace::mutable|StorageSpaceId\(' 'raw legacy upload space constructor'

if ! git -C "$repo" cat-file -e "$candidate:packages/lix/src/binary_cas/kv.rs" 2>/dev/null; then
  finding 'binary_cas/kv.rs is absent while binary_cas::kv references remain'
fi

echo "W4_SOURCE_GATE findings=$red"
if (( red == 0 )); then
  echo 'UNEXPECTED GREEN: legacy W4 authorities were not detected'
  exit 3
fi
exit 1
