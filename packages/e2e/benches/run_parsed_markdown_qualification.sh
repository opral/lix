#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "usage: $0 <benchmark-binary> <output-directory> [warmups=3] [samples=10]" >&2
  exit 2
fi

binary=$(realpath "$1")
output=$(realpath -m "$2")
warmups=${3:-3}
samples=${4:-10}
mkdir -p "$output"

for backend in rocksdb slatedb; do
  : >"$output/${backend}-warmup.jsonl"
  : >"$output/${backend}-measured.jsonl"
  : >"$output/${backend}-stderr.log"
  : >"$output/${backend}-time.log"

  for ((sample = 1; sample <= warmups + samples; sample++)); do
    database=$(mktemp -d "$output/${backend}-db.XXXXXX")
    if ((sample <= warmups)); then
      phase=warmup
      log="$output/${backend}-warmup.jsonl"
    else
      phase=measured
      log="$output/${backend}-measured.jsonl"
    fi
    echo "sample=$sample phase=$phase backend=$backend" >>"$output/${backend}-stderr.log"
    /usr/bin/time -v -a -o "$output/${backend}-time.log" \
      timeout 1200s "$binary" "$backend" "$database" \
      >>"$log" 2>>"$output/${backend}-stderr.log"
    rm -rf -- "$database"
  done
done

python3 "$(dirname "$0")/summarize_parsed_markdown_qualification.py" "$output"
