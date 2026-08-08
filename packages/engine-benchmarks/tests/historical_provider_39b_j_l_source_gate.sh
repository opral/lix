#!/usr/bin/env bash
set -euo pipefail

# J-L source gate. It is intentionally independent of Cargo and is RED on
# exact 47957. H/I remains frozen in the predecessor artifact and is checked
# as an inherited gate.

ROOT=${1:-$(git rev-parse --show-toplevel)}
cd "$ROOT"

file_history=packages/lix/src/sql2/providers/file_history.rs
history_route=packages/lix/src/sql2/history_route.rs
path_resolver=packages/lix/src/sql2/providers/filesystem_history_path.rs
findings=()
record() { findings+=("$1"); }

set +e
hi_output=$(bash packages/engine-benchmarks/tests/historical_provider_39b_source_gate.sh "$ROOT" 2>&1)
hi_status=$?
set -e
if (( hi_status != 0 )); then
  record 'inherited H/I source gate is RED'
fi

# J: required file/directory/blob rows accept missing/NULL payloads in both
# entry and observed-row parsers, which also admits tombstone-shaped records.
if git grep -nF 'let Some(snapshot_content) = entry.change.snapshot_content.as_deref() else' -- "$file_history" >/dev/null; then
  record 'required file/directory/blob entry payload missing-NULL-tombstone path'
fi
if git grep -nF 'let Some(snapshot_content) = row.snapshot_content() else' -- "$file_history" >/dev/null; then
  record 'required file/directory/blob observed payload missing-NULL-tombstone path'
fi

# K: certified row identity mismatch is currently a continue/skip branch.
if git grep -nF 'row.commit_id != certified_commit_id' -- "$history_route" >/dev/null \
  && git grep -nF 'continue;' -- "$history_route" >/dev/null; then
  record 'certified-row commit_id mismatch is skipped instead of errored'
fi

# L: path corruption is currently represented as Option/None, including a
# cycle and a missing parent, rather than a typed failure.
if git grep -nF ') -> Option<String>' -- "$path_resolver" >/dev/null; then
  record 'filesystem path resolver returns Option instead of typed error'
fi
if git grep -nF 'if !visiting.insert(directory_id.to_string())' -- "$path_resolver" >/dev/null \
  && git grep -nF 'cache.insert(directory_id.to_string(), None)' -- "$path_resolver" >/dev/null; then
  record 'filesystem path cycle becomes None instead of typed failure'
fi
if git grep -nF 'resolve_observed_directory_path(parent_id' -- "$path_resolver" >/dev/null; then
  record 'filesystem path missing parent propagates None instead of typed failure'
fi

if ((${#findings[@]} == 0)); then
  echo 'J_L_SOURCE_GATE=GREEN'
  exit 0
fi

echo 'J_L_SOURCE_GATE=RED'
printf 'FINDING=%s\n' "${findings[@]}"
printf 'INHERITED_HI_STATUS=%s\n' "$hi_status"
exit 1
