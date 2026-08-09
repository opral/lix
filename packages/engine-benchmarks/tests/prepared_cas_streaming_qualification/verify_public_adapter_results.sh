#!/usr/bin/env bash
set -euo pipefail

tsv=${1:?usage: verify_public_adapter_results.sh RESULTS_TSV FILES PAYLOAD_BYTES}
files=${2:?usage: verify_public_adapter_results.sh RESULTS_TSV FILES PAYLOAD_BYTES}
payload_bytes=${3:?usage: verify_public_adapter_results.sh RESULTS_TSV FILES PAYLOAD_BYTES}

test -s "$tsv" || { echo "missing public adapter result file: $tsv" >&2; exit 1; }
expected=$((files * payload_bytes))
awk -F '\t' -v expected_files="$files" -v expected_payload="$expected" '
NR == 1 {
  if ($0 != "adapter\tfiles\tpayload_bytes\tsemantic_commits\tmarker_rows\ttree_digest\trollback_visible_rows\tmalformed_chunk_failures\tcold_reopen\tstrict_retained_bytes\torphan_reclamation") {
    print "public adapter result schema mismatch" > "/dev/stderr"; bad=1
  }
  next
}
NF != 11 { print "wrong public result field count at line " NR > "/dev/stderr"; bad=1; next }
$1 !~ /^(memory|rocksdb|slatedb)$/ { print "unknown adapter at line " NR > "/dev/stderr"; bad=1 }
$2 != expected_files || $3 != expected_payload { print "fixture mismatch at line " NR > "/dev/stderr"; bad=1 }
$4 != 1 || $5 != 1 { print "semantic marker/commit mismatch at line " NR > "/dev/stderr"; bad=1 }
$7 != 0 || $8 != 1 || $9 != "true" { print "observable semantic gate failed at line " NR > "/dev/stderr"; bad=1 }
$10 == "UNOBSERVED" { print "strict retained-byte counter is unobserved at line " NR > "/dev/stderr"; strict=1 }
$11 == "UNOBSERVED" { print "orphan reclamation counter is unobserved at line " NR > "/dev/stderr"; strict=1 }
END {
  if (NR != 4) { print "expected three adapter rows" > "/dev/stderr"; bad=1 }
  if (strict) { print "public semantic adapter cells passed, strict owner counters remain BLOCKED" > "/dev/stderr"; bad=1 }
  if (bad) exit 1
}' "$tsv"

echo "public prepared-CAS adapter contract PASS: $tsv"
