#!/usr/bin/env bash
set -euo pipefail

# TEST/REPORT-ONLY source verifier. It calibrates RED on 413: checkpoint and
# history reconstruction still enter TrackedStateStoreReader/legacy graph
# paths instead of one retained ForkTree view.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source_commit="413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d"
source_tree="820fe560da3bbd2b00b788b0b1759c409048cd6e"
prerequisite_commit="97a7116d00206954b581cf43937cc5db6c23f30b"
prerequisite_tree="457a3919903169ca1edd2fe81df8b81e70b06d37"
oracle_commit="448624a557bca2c341f4a1820b79222a5691613a"

actual_tree="$(git -C "$repo_root" rev-parse "$source_commit^{tree}")"
[[ "$actual_tree" == "$source_tree" ]] || {
  echo "SOURCE PROVENANCE ERROR: 413 tree $actual_tree != $source_tree" >&2
  exit 2
}
actual_prereq_tree="$(git -C "$repo_root" rev-parse "$prerequisite_commit^{tree}")"
[[ "$actual_prereq_tree" == "$prerequisite_tree" ]] || {
  echo "PREREQUISITE PROVENANCE ERROR: 97a tree $actual_prereq_tree != $prerequisite_tree" >&2
  exit 2
}
[[ "$(git -C "$repo_root" rev-parse "$prerequisite_commit^")" == "$oracle_commit" ]] || {
  echo "PREREQUISITE ANCESTRY ERROR: 97a is not a child of 448624a" >&2
  exit 2
}

source_blob() { git -C "$repo_root" show "$source_commit:$1"; }
checkpoint="$(source_blob packages/lix/src/checkpoint.rs)"
session_checkpoint="$(source_blob packages/lix/src/session/checkpoint.rs)"
sql_checkpoint="$(source_blob packages/lix/src/sql2/providers/checkpoint.rs)"
working_diff="$(source_blob packages/lix/src/sql2/providers/working_diff.rs)"
filesystem_diff="$(source_blob packages/lix/src/sql2/providers/filesystem_working_diff.rs)"
file_history="$(source_blob packages/lix/src/sql2/providers/file_history.rs)"
directory_history="$(source_blob packages/lix/src/sql2/providers/directory_history.rs)"
undo_redo="$(source_blob packages/lix/src/session/undo_redo.rs)"
merge_branch="$(source_blob packages/lix/src/session/merge/branch.rs)"
transaction="$(source_blob packages/lix/src/transaction/context.rs)"
tracked="$(source_blob packages/lix/src/tracked_state/context.rs)"
gc="$(source_blob packages/lix/src/gc.rs)"

require() {
  local label="$1" haystack="$2" needle="$3"
  if ! grep -Fq "$needle" <<<"$haystack"; then
    echo "SOURCE CHECK ERROR: missing $label: $needle" >&2
    exit 2
  fi
  echo "GREEN $label"
}

# Bind the previous historical prerequisite first. Its expected RED is part of
# this oracle, not a production build or adapter test.
bash "$repo_root/evidence/forktree-historical-failclosed-sql-413/source_verifier_413.sh" --expect-red

require "checkpoint legacy reader import" "$checkpoint" 'TrackedStateStoreReader'
require "latest checkpoint legacy signature" "$checkpoint" 'tracked: &mut TrackedStateStoreReader<S>'
require "checkpoint marker point read" "$checkpoint" 'load_projected_batch_at_commit('
require "first-parent checkpoint walk" "$checkpoint" 'commit.parent_commit_ids.first().copied()'
require "history uses separate graph reader" "$checkpoint" 'reader: &mut dyn CommitGraphReader'
require "history loads graph node" "$checkpoint" 'reader.load_node(&commit_id).await?'

require "session checkpoint opens legacy reader" "$session_checkpoint" 'transaction.tracked_state_reader().await'
require "SQL checkpoint opens legacy reader" "$sql_checkpoint" 'TrackedStateContext::new().reader(store)'
require "working diff checkpoint lookup" "$working_diff" 'latest_checkpoint_for_branch('
require "filesystem diff legacy reader" "$filesystem_diff" 'TrackedStateStoreReader'
require "file history legacy reader" "$file_history" 'TrackedStateStoreReader'
require "directory history legacy scan" "$directory_history" 'scan_batch_at_commit('
require "undo marker legacy reader" "$undo_redo" 'transaction.tracked_state_reader().await'
require "merge base graph authority" "$merge_branch" 'reader.merge_base('
require "transaction legacy reader factory" "$transaction" 'pub(crate) async fn tracked_state_reader('
require "transaction opening legacy callback" "$transaction" 'with_opening_tracked_reader'
require "tracked historical scan API" "$tracked" 'pub(crate) async fn scan_batch_at_commit('
require "tracked reader caches" "$tracked" 'point_value_cache: HashMap'
require "recovery ref stores recovered head" "$gc" 'recovered_head_commit_id: CommitId'
require "recovery ref stores checkpoint" "$gc" 'checkpoint_commit_id: CommitId'
require "GC includes recovery roots" "$gc" 'load_recovery_refs(store)'
require "GC includes queue checkpoint roots" "$gc" 'collect_all_reachability_checkpoint_roots(store, queue)'

cat <<'EOF'
RED 413 checkpoint/history reconstruction migration
  latest checkpoint lookup        => TrackedStateStoreReader point read
  chronology/history              => separate CommitGraphReader first-parent walk
  SQL checkpoint/history          => legacy TrackedStateContext reader
  undo/redo marker reads          => transaction-scoped legacy reader
  historical missing commit/root  => inherited 97a fail-closed prerequisite RED
  one retained ForkTree view      => not yet the production call graph
  65 rotations/GC/branch merge    => future adapter oracle required
EOF

if [[ "${1:-}" == "--expect-red" ]]; then
  echo "EXPECTED RED: 413 retains checkpoint/history legacy readers"
  exit 0
fi
echo "RED: 413 checkpoint/history migration is not yet complete" >&2
exit 1
