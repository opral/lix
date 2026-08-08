#!/usr/bin/env bash
set -euo pipefail

source_checkout="${1:?usage: run_oracle.sh <exact-b484-checkout>}"
package_dir="$(cd "$(dirname "$0")" && pwd)"
source_log="$(mktemp)"
trap 'rm -f "$source_log"' EXIT

set +e
python3 "$package_dir/source_gate.py" "$source_checkout" >"$source_log"
source_status=$?
set -e

test "$source_status" -eq 1
for expected in \
  ABSENT_TO_EMPTY_FALLBACK \
  ZERO_BLOBREF_NOT_DISTINGUISHED \
  DESCRIPTOR_ENTITYPK_FILEID_BINDING \
  DIRECTORY_FILEID_NULL_BINDING \
  DESCRIPTOR_TOMBSTONE_PAYLOAD \
  DIRECTORY_TOMBSTONE_PAYLOAD \
  PLUGIN_OWNER_TOMBSTONE_PAYLOAD \
  COMPOSITE_PK_FIRST_COMPONENT \
  CONFLICTING_SOURCE_DEDUP; do
  grep -qx "RED=$expected" "$source_log"
done
grep -qx 'WORKING_DIFF_POSITIVE_CONTROL=PASS' "$source_log"
grep -qx 'SOURCE_STATUS=BLOCKED_EXPECTED_RED' "$source_log"
cat "$source_log"

python3 "$package_dir/model_oracle.py"
echo 'ORACLE_STATUS=GREEN_EXPECTED_RED_SOURCE_CALIBRATION'
