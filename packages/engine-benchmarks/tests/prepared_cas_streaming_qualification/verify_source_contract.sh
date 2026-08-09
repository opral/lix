#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:?usage: verify_source_contract.sh REPO_ROOT BASE_COMMIT}
base=${2:?usage: verify_source_contract.sh REPO_ROOT BASE_COMMIT}
cd "$repo_root"

mapfile -t changed < <(git diff --name-only "$base" HEAD)
for path in "${changed[@]}"; do
    case "$path" in
        packages/engine-benchmarks/tests/prepared_cas_streaming_qualification/*) ;;
        packages/engine-benchmarks/tests/prepared_cas_streaming_adapter.rs) ;;
        packages/engine-benchmarks/Cargo.toml) ;;
        *) echo "forbidden non-test path changed: $path" >&2; exit 1 ;;
    esac
done

source=packages/lix/src/transaction/types.rs
receipt=packages/lix/src/binary_cas/types.rs
commit=packages/lix/src/transaction/commit.rs
staging=packages/lix/src/transaction/staging.rs
for needle in 'struct BlobWriteReceipt' 'enum FileContent' 'PreparedCas' 'prepared_cas_receipt' 'file_content_writes'; do
    rg -q "$needle" "$source" "$receipt" "$commit" "$staging" || {
        echo "missing prepared-CAS source seam: $needle" >&2
        exit 1
    }
done

if git diff --quiet "$base" HEAD -- packages/lix packages/cli packages/engine packages/rs-sdk; then
    :
else
    echo 'production source changed by the oracle package' >&2
    exit 1
fi

echo "source contract PASS: ${#changed[@]} test/report paths only"
