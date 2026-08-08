#!/usr/bin/env bash
set -euo pipefail

repo=${1:?usage: verify_commit_record_fail_closed.sh <worktree>}
file="$repo/packages/lix/src/sql2/providers/change.rs"

if [[ ! -f "$file" ]]; then
  echo "BLOCKER: missing sql2/providers/change.rs at $file"
  exit 2
fi

# Negative control: immutable 1f742 must fail until every enumerated
# CommitRecord is required and authenticated before row assembly/truncation.
window=$(sed -n '188,210p' "$file")
if printf '%s\n' "$window" | rg -q 'load_commit_records' &&
   printf '%s\n' "$window" | rg -q '\.flatten\(\)'; then
  echo "BLOCKER: CommitRecord scan flattens missing records in scan_changelog_changes"
  printf '%s\n' "$window"
  exit 2
fi

echo "SOURCE_CONTROL_PASS: no flattening of CommitRecord scan in inspected window"
