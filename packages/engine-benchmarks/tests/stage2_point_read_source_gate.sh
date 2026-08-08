#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 ENTRYPOINT.rs TRANSITIVE_READ_HELPER.rs..." >&2
  exit 64
fi

entry=$1
shift
files=("$entry" "$@")

for file in "${files[@]}"; do
  [[ -f "$file" ]] || { echo "missing source file: $file" >&2; exit 65; }
done

begin_reads=$(rg -o '\.begin_read\s*\(' "${files[@]}" | wc -l)
if [[ $begin_reads -ne 1 ]]; then
  echo "BLOCKER: transitive point-read closure has $begin_reads begin_read calls, expected exactly 1" >&2
  exit 1
fi

entry_begin_reads=$(rg -o '\.begin_read\s*\(' "$entry" | wc -l)
if [[ $entry_begin_reads -ne 1 ]]; then
  echo "BLOCKER: point-read entrypoint must own the sole begin_read" >&2
  exit 1
fi

helper_begin_reads=$(rg -o '\.begin_read\s*\(' "$@" | wc -l || true)
if [[ $helper_begin_reads -ne 0 ]]; then
  echo "BLOCKER: a transitive point-read helper opens a replacement snapshot" >&2
  exit 1
fi

source=$(printf '%s\n' "${files[@]}" | xargs sed -n '1,$p')
for token in StorageRead selector catalog tree object auth; do
  if ! grep -qi "$token" <<<"$source"; then
    echo "BLOCKER: transitive closure does not visibly bind '$token'" >&2
    exit 1
  fi
done

if grep -Eq '(^|[^[:alnum:]_])(unwrap_or|unwrap_or_else|ok\(\)|\.ok\(\)|fallback|rebuild)' <<<"$source"; then
  echo "BLOCKER: point-read source contains a corruption-suppressing fallback token" >&2
  exit 1
fi

echo "stage2_point_read_source_gate,begin_reads=1,helpers=$#,storage_read_threaded=true,authority_tokens=true,fallback_tokens=false,pass=true"
