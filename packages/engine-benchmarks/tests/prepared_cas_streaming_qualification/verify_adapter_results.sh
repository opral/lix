#!/usr/bin/env bash
set -euo pipefail

csv=${1:?usage: verify_adapter_results.sh RESULTS_CSV FILES PAYLOAD_BYTES}
files=${2:?usage: verify_adapter_results.sh RESULTS_CSV FILES PAYLOAD_BYTES}
payload_bytes=${3:?usage: verify_adapter_results.sh RESULTS_CSV FILES PAYLOAD_BYTES}
expected_payload=$((files * payload_bytes))

test -s "$csv" || {
    echo "missing result CSV: $csv" >&2
    exit 1
}

expected_header='adapter,page_size,files,payload_bytes,peak_transaction_retained_payload_bytes,peak_file_content_writes_payload_bytes,peak_file_content_writes_metadata_bytes,prepared_receipt_bytes,prepared_object_payload_bytes,prepared_object_metadata_bytes,semantic_markers,semantic_commits,tree_digest,plugin_digest,semantic_digest,rollback_visible_rows,rollback_selectors,rollback_markers,orphan_reclaimed_bytes,corruption_failures'
header=$(head -n 1 "$csv")
test "$header" = "$expected_header" || {
    echo "result schema mismatch" >&2
    echo "expected: $expected_header" >&2
    echo "actual:   $header" >&2
    exit 1
}

awk -F, -v expected_files="$files" -v expected_payload="$expected_payload" '
NR == 1 { next }
NF != 20 { print "wrong result field count at line " NR > "/dev/stderr"; bad=1; next }
$1 !~ /^(memory|rocksdb|slatedb)$/ { print "unknown adapter at line " NR > "/dev/stderr"; bad=1 }
$2 !~ /^(1|8|32|64)$/ { print "unexpected page size at line " NR > "/dev/stderr"; bad=1 }
$3 != expected_files || $4 != expected_payload { print "fixture mismatch at line " NR > "/dev/stderr"; bad=1 }
$5 >= expected_payload { print "retained payload is O(total) at line " NR > "/dev/stderr"; bad=1 }
$6 != 0 { print "file_content_writes retains payload at line " NR > "/dev/stderr"; bad=1 }
$11 != 1 || $12 != 1 { print "marker/commit atomicity mismatch at line " NR > "/dev/stderr"; bad=1 }
$16 != 0 || $17 != 0 || $18 != 0 { print "rollback published visible state at line " NR > "/dev/stderr"; bad=1 }
END {
    if (NR != 13) { print "expected 12 adapter/page rows" > "/dev/stderr"; bad=1 }
    if (bad) exit 1
}' "$csv"

echo "prepared-CAS result contract PASS: $csv"
