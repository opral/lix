#!/usr/bin/env bash
set -euo pipefail

tsv=${1:?usage: verify_public_adapter_results.sh RESULTS_TSV FILES PAYLOAD_BYTES}
files=${2:?usage: verify_public_adapter_results.sh RESULTS_TSV FILES PAYLOAD_BYTES}
payload_bytes=${3:?usage: verify_public_adapter_results.sh RESULTS_TSV FILES PAYLOAD_BYTES}

test -s "$tsv" || { echo "missing public adapter result file: $tsv" >&2; exit 1; }
expected=$((files * payload_bytes))
awk -F '\t' -v expected_files="$files" -v expected_payload="$expected" '
NR == 1 {
  if ($0 != "adapter\tfiles\tpayload_bytes\tsemantic_commits\tmarker_rows\ttree_digest\trollback_visible_rows\tmalformed_chunk_failures\tcold_reopen\tcurrent_transaction_resident_payload_bytes\tpeak_transaction_resident_payload_bytes\tpage_bytes_before_flush\tpage_bytes_after_flush\tprepared_receipt_metadata_bytes\tfinal_transaction_payload_bytes\tunreferenced_object_ids\tunreferenced_object_bytes\treachable_object_ids\treachable_object_bytes\torphan_object_ids\torphan_object_bytes\treclaimed_object_ids\treclaimed_object_bytes\tcorrupted_receipts_rejected") {
    print "public adapter result schema mismatch" > "/dev/stderr"; bad=1
  }
  next
}
NF != 24 { print "wrong public result field count at line " NR > "/dev/stderr"; bad=1; next }
$1 !~ /^(memory|rocksdb|slatedb)$/ { print "unknown adapter at line " NR > "/dev/stderr"; bad=1 }
$2 != expected_files || $3 != expected_payload { print "fixture mismatch at line " NR > "/dev/stderr"; bad=1 }
$4 != 1 || $5 != 1 { print "semantic marker/commit mismatch at line " NR > "/dev/stderr"; bad=1 }
$7 != 0 || $8 != 1 || $9 != "true" { print "observable semantic gate failed at line " NR > "/dev/stderr"; bad=1 }
$10 !~ /^[0-9]+$/ || $10 != 0 { print "current retained payload is not zero at line " NR > "/dev/stderr"; bad=1 }
$11 !~ /^[0-9]+$/ || $11 <= 0 || $11 >= expected_payload { print "peak retained payload is not bounded at line " NR > "/dev/stderr"; bad=1 }
$12 !~ /^[0-9]+$/ || $12 <= 0 { print "page bytes before flush missing at line " NR > "/dev/stderr"; bad=1 }
$13 !~ /^[0-9]+$/ || $13 != 0 { print "page bytes after flush not zero at line " NR > "/dev/stderr"; bad=1 }
$14 !~ /^[0-9]+$/ || $14 <= 0 { print "receipt metadata missing at line " NR > "/dev/stderr"; bad=1 }
$15 !~ /^[0-9]+$/ || $15 != 0 { print "final transaction retained payload is nonzero at line " NR > "/dev/stderr"; bad=1 }
$16 !~ /^[0-9]+$/ || $16 <= 0 || $17 !~ /^[0-9]+$/ || $17 <= 0 { print "unreferenced object accounting missing at line " NR > "/dev/stderr"; bad=1 }
$18 !~ /^[0-9]+$/ || $18 <= 0 || $19 !~ /^[0-9]+$/ || $19 <= 0 { print "reachable object accounting missing at line " NR > "/dev/stderr"; bad=1 }
$20 !~ /^[0-9]+$/ || $20 <= 0 || $21 !~ /^[0-9]+$/ || $21 <= 0 { print "orphan accounting missing at line " NR > "/dev/stderr"; bad=1 }
$22 !~ /^[0-9]+$/ || $22 <= 0 || $23 !~ /^[0-9]+$/ || $23 <= 0 { print "reclamation accounting missing at line " NR > "/dev/stderr"; bad=1 }
$24 !~ /^[0-9]+$/ || $24 <= 0 { print "corruption rejection accounting missing at line " NR > "/dev/stderr"; bad=1 }
END {
  if (NR != 4) { print "expected three adapter rows" > "/dev/stderr"; bad=1 }
  if (bad) exit 1
}' "$tsv"

echo "public prepared-CAS adapter contract PASS: $tsv"
