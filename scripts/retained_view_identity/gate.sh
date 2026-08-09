#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 || $# -gt 5 ]]; then
  echo "usage: $0 REPO BASE_REF CANDIDATE_REF WORK_DIR [expected-source=red|green]" >&2
  exit 2
fi

repo=$1
base=$2
candidate=$3
work_dir=$4
expected_source=${5:-green}

mkdir -p "$work_dir"
model_bin="$work_dir/retained_view_identity_model"
model_log="$work_dir/model.log"
source_log="$work_dir/source-gate.log"

rustc --edition 2024 -D warnings \
  "$repo/scripts/retained_view_identity/model.rs" \
  -o "$model_bin"
"$model_bin" | tee "$model_log"
model_status=${PIPESTATUS[0]}
test "$model_status" -eq 0

set +e
python3 "$repo/scripts/retained_view_identity/source_gate.py" \
  --repo "$repo" --base "$base" --candidate "$candidate" \
  | tee "$source_log"
source_status=${PIPESTATUS[0]}
set -e

if [[ "$expected_source" == red ]]; then
  test "$source_status" -eq 1
else
  test "$expected_source" == green
  test "$source_status" -eq 0
fi

echo "MODEL_STATUS=0"
echo "SOURCE_STATUS=$source_status EXPECTED_SOURCE=$expected_source"
sha256sum "$model_bin" "$model_log" "$source_log"
