#!/usr/bin/env bash
set -euo pipefail

candidate=${1:?usage: run_adapter_qualification.sh CANDIDATE_ROOT TARGET_DIR RESULTS_DIR}
target=${2:?usage: run_adapter_qualification.sh CANDIDATE_ROOT TARGET_DIR RESULTS_DIR}
results=${3:?usage: run_adapter_qualification.sh CANDIDATE_ROOT TARGET_DIR RESULTS_DIR}
test_prefix=${PREPARED_CAS_TEST_PREFIX:-prepared_cas_streaming}
files=${PREPARED_CAS_FILES:-65}
payload_bytes=${PREPARED_CAS_PAYLOAD_BYTES:-1048576}
page_sizes=${PREPARED_CAS_PAGE_SIZES:-64}

mkdir -p "$results"
: > "$results/public-adapter-results.tsv"
common_env=(
    CARGO_TARGET_DIR="$target"
    PREPARED_CAS_FILES="$files"
    PREPARED_CAS_PAYLOAD_BYTES="$payload_bytes"
    PREPARED_CAS_PAGE_SIZES="$page_sizes"
    PREPARED_CAS_RESULT_DIR="$results"
    CARGO_BUILD_JOBS=2
)

run_adapter() {
    local adapter=$1
    local log="$results/${adapter}.log"
    echo "[prepared-cas] adapter=$adapter files=$files payload_bytes=$payload_bytes pages=$page_sizes"
    (
        cd "$candidate"
        env "${common_env[@]}" \
            timeout 1200 \
            cargo test -p lix_benchmarks --test prepared_cas_streaming_adapter \
            --features 'storage-benches slatedb prepared-cas-observability' "${test_prefix}_${adapter}" \
            -- --exact --nocapture --test-threads=1
    ) 2>&1 | tee "$log"
}

# These are public adapter tests in the same test-only package. A missing test
# is a hard stop, not a fallback to the standalone model.
run_adapter memory
run_adapter rocksdb
run_adapter slatedb

"$(dirname "$0")/verify_public_adapter_results.sh" \
    "$results/public-adapter-results.tsv" "$files" "$payload_bytes"
