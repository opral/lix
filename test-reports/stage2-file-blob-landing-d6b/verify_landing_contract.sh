#!/usr/bin/env bash
set -euo pipefail

# Report/package-only gate. It verifies package scope and required contract
# terms. It does not compile, open adapters, or run a benchmark.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

dir="test-reports/stage2-file-blob-landing-d6b"
report="$dir/MINIMUM_FILE_BLOB_LANDING_ACCEPTANCE.md"
manifest="$dir/LANDING_MANIFEST.json"
fail=0

for path in "$report" "$manifest" "$dir/verify_landing_contract.sh"; do
  test -f "$path" || { echo "missing package file: $path" >&2; fail=1; }
done

for phrase in \
  "one retained" \
  "plugin-parsed semantic rows" \
  "64 MiB / 64 canonical 1 MiB" \
  "same-size manifest" \
  "RocksDB first" \
  "SlateDB second" \
  "512 MiB payloads" \
  "comparator" \
  "W5 owns"; do
  if ! rg -q --fixed-strings "$phrase" "$report"; then
    echo "missing required landing contract phrase: $phrase" >&2
    fail=1
  fi
done

if git diff --name-only d6b2690afc0fc6a0acccd5c4bef4c171a7aa7768 HEAD \
    | rg -v '^test-reports/stage2-file-blob-landing-d6b/' \
    | rg -q .; then
  echo "production/unscoped file changed in report-only package" >&2
  fail=1
fi

if (( fail != 0 )); then
  echo "minimum file/blob landing package gate: FAIL" >&2
  exit 1
fi
echo "minimum file/blob landing package gate: PASS (report/source scope only)"
