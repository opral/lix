#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <binary> <encoding-label> <fresh-output-root>" >&2
  exit 2
fi

binary=$1
encoding=$2
root=$3
[[ -x "$binary" ]] || { echo "binary is not executable: $binary" >&2; exit 2; }
[[ ! -e "$root" ]] || { echo "output root must be absent: $root" >&2; exit 2; }
mkdir -p "$root/databases" "$root/logs"

sha256sum "$binary" > "$root/binary.sha256"
{
  echo "encoding=$encoding"
  echo "binary=$binary"
  echo "warmups=${LIX_JSONB_CELL_WARMUPS:-3}"
  echo "samples=${LIX_JSONB_CELL_SAMPLES:-11}"
  uname -a
} > "$root/provenance.txt"

for backend in rocksdb slatedb; do
  for shape in absent sparse dense; do
    for rows in 1000 10000 50000; do
      for changes in 1 100 1%; do
        cell="${backend}-${shape}-n${rows}-d${changes//%/pct}"
        timeout 1200 env \
          LIX_JSONB_CELL_BACKEND="$backend" \
          LIX_JSONB_CELL_PATH="$root/databases/$cell" \
          LIX_JSONB_CELL_ROWS="$rows" \
          LIX_JSONB_CELL_CHANGES="$changes" \
          LIX_JSONB_CELL_SHAPE="$shape" \
          LIX_JSONB_CELL_WARMUPS="${LIX_JSONB_CELL_WARMUPS:-3}" \
          LIX_JSONB_CELL_SAMPLES="${LIX_JSONB_CELL_SAMPLES:-11}" \
          "$binary" | tee "$root/logs/$cell.log"
      done
    done
  done
done

find "$root/logs" -type f -print0 | sort -z | xargs -0 sha256sum > "$root/logs.sha256"
