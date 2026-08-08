#!/usr/bin/env bash
set -euo pipefail

# Static source gate for the historical-provider correction. It is deliberately
# independent of Cargo and exits RED on the blocked 47957 anchor.

ROOT=${1:-$(git rev-parse --show-toplevel)}
cd "$ROOT"

checkpoint=packages/lix/src/sql2/providers/checkpoint.rs
working_diff=packages/lix/src/sql2/providers/filesystem_working_diff.rs
history_route=packages/lix/src/sql2/history_route.rs
history_scope=(
  packages/lix/src/sql2/history_route.rs
  packages/lix/src/sql2/context.rs
  packages/lix/src/sql2/providers/checkpoint.rs
  packages/lix/src/sql2/providers/diff.rs
  packages/lix/src/sql2/providers/directory_history.rs
  packages/lix/src/sql2/providers/file_history.rs
  packages/lix/src/sql2/providers/filesystem_working_diff.rs
)

findings=()
record() { findings+=("$1"); }

if git grep -nF 'let reachable_nodes = if metadata_projection.commit_created_at' -- "$history_route" >/dev/null; then
  record 'projection-dependent reachable_nodes'
fi

if git grep -nF 'checkpoint history is deferred' -- "$checkpoint" >/dev/null; then
  record 'typed checkpoint chronology deferral'
fi
if git grep -nF 'filesystem working-diff checkpoint baseline is deferred' -- "$working_diff" >/dev/null; then
  record 'typed filesystem working-diff deferral'
fi

for token in TrackedStateScanRequest certified_history_reader CertifiedHistoryStoreReader TrackedStateStoreReader; do
  if git grep -nF "$token" -- "${history_scope[@]}" >/dev/null; then
    record "legacy or duplicate history authority: $token"
  fi
done

for path in "$checkpoint" "$working_diff"; do
  if ! git grep -nF 'ForkTreeReadFacade' -- "$path" >/dev/null; then
    record "missing ForkTree chronology owner: $path"
  fi
  if git grep -nE 'begin_read\(|storage_read\(' -- "$path" >/dev/null; then
    record "raw/second read acquisition: $path"
  fi
  count=$(git grep -nF 'ForkTreeReadFacade::new' -- "$path" | wc -l || true)
  if (( count > 1 )); then
    record "multiple ForkTree view acquisitions: $path ($count)"
  fi
done

if ! git grep -nF 'record.parent_commit_ids.is_empty()' -- packages/lix/src/checkpoint.rs >/dev/null; then
  record 'missing implicit root checkpoint rule'
fi
if ! git grep -nF 'marker == Some(record.commit_id)' -- packages/lix/src/checkpoint.rs >/dev/null; then
  record 'missing exact marker-to-walked-commit rule'
fi

if ((${#findings[@]} == 0)); then
  echo 'SOURCE_GATE=GREEN'
  exit 0
fi

echo 'SOURCE_GATE=RED'
printf 'FINDING=%s\n' "${findings[@]}"
exit 1
