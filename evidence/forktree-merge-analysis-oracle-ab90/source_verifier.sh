#!/usr/bin/env bash
set -euo pipefail

# TEST/REPORT-ONLY verifier. It calibrates RED on ab90: merge analysis still
# owns its historical diff/plan through TrackedStateStoreReader even though
# merge/branch.rs already uses the ForkTree facade for plugin rows.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source_commit="ab90fc51e148611f5fdacde173dd6789ab22ab88"
source_tree="5bcf259918f86e5b439c1bc50a3e198f87826adc"
prerequisite_commit="97a7116d00206954b581cf43937cc5db6c23f30b"
prerequisite_tree="457a3919903169ca1edd2fe81df8b81e70b06d37"
historical_oracle="448624a557bca2c341f4a1820b79222a5691613a"

[[ "$(git -C "$repo_root" rev-parse "$source_commit^{tree}")" == "$source_tree" ]] || {
  echo "SOURCE PROVENANCE ERROR: ab90 tree mismatch" >&2
  exit 2
}
[[ "$(git -C "$repo_root" rev-parse "$prerequisite_commit^{tree}")" == "$prerequisite_tree" ]] || {
  echo "PREREQUISITE PROVENANCE ERROR: 97a tree mismatch" >&2
  exit 2
}
[[ "$(git -C "$repo_root" rev-parse "$prerequisite_commit^")" == "$historical_oracle" ]] || {
  echo "PREREQUISITE ANCESTRY ERROR: 97a is not a child of 448624a" >&2
  exit 2
}

source_blob() { git -C "$repo_root" show "$source_commit:$1"; }
analysis="$(source_blob packages/lix/src/session/merge/analysis.rs)"
branch="$(source_blob packages/lix/src/session/merge/branch.rs)"
facade="$(source_blob packages/lix/src/forktree/view.rs)"
context="$(source_blob packages/lix/src/transaction/context.rs)"
tracked="$(source_blob packages/lix/src/tracked_state/context.rs)"
merge="$(source_blob packages/lix/src/tracked_state/merge.rs)"
diff="$(source_blob packages/lix/src/tracked_state/diff.rs)"
plugin="$(source_blob packages/lix/src/plugin/registry.rs)"

require() {
  local label="$1" haystack="$2" needle="$3"
  if ! grep -Fq "$needle" <<<"$haystack"; then
    echo "SOURCE CHECK ERROR: missing $label: $needle" >&2
    exit 2
  fi
  echo "GREEN $label"
}

# The historical point/root distinction is an inherited prerequisite and must
# remain visible as RED until its production correction is independently fixed.
bash "$repo_root/evidence/forktree-merge-analysis-oracle-ab90/historical_failclosed_prerequisite.sh" --expect-red

require "merge analysis legacy reader import" "$analysis" 'TrackedStateStoreReader'
require "merge analysis diff source" "$analysis" '.diff_commits(&base_commit_id, &source_commit_id'
require "merge analysis diff target" "$analysis" '.diff_commits(&base_commit_id, &target_commit_id'
require "merge analysis fallback payload load" "$analysis" 'reader.load_change_payloads(&fallback_ids)'
require "merge plan primitive" "$analysis" 'plan_merge(&target_diff, &source_diff, &payloads)'
require "merge base from opening graph reader" "$branch" 'commit_graph_reader_on_opening_read()'
require "analysis through opening legacy reader" "$branch" 'with_opening_tracked_reader'
require "branch uses ForkTree facade" "$branch" 'transaction.forktree_read_facade()'
require "historical base rows use facade" "$branch" '.load_state_rows_at_commit(&analysis.commits.base_commit_id.to_string()'
require "plugin registry historical path" "$branch" 'load_plugin_registry_at_commit(facade'
require "facade has historical rows" "$facade" 'pub(crate) async fn load_state_rows_at_commit('
require "transaction facade owns opening read" "$context" 'ForkTreeReadFacade::new(self.opening_read())'
require "tracked diff primitive remains" "$tracked" 'pub(crate) async fn diff_commits('
require "tracked plan primitive remains" "$tracked" 'pub(crate) async fn plan_merge('
require "tracked merge algorithm remains" "$merge" 'pub(crate) fn plan_merge('
require "tracked diff algorithm remains" "$diff" 'pub(crate) async fn diff_commits<S>'
require "plugin registry loader" "$plugin" 'pub(crate) async fn load_plugin_registry_at_commit'

cat <<'EOF'
RED ab90 merge-analysis migration
  merge base chronology       => opening CommitGraphReader, not typed ForkTree owner
  target/source historical   => TrackedStateStoreReader::diff_commits
  payload fallback            => legacy reader::load_change_payloads
  merge plan                  => tracked-state facade/primitive call chain
  plugin rows/registry        => ForkTree facade already used, must remain same read
  missing commit/root         => inherited 97a prerequisite RED
  retained read               => same opening read object, but no single ForkTree merge owner
EOF

if [[ "${1:-}" == "--expect-red" ]]; then
  echo "EXPECTED RED: ab90 merge analysis retains TrackedStateStoreReader"
  exit 0
fi
echo "RED: ab90 merge-analysis migration is not complete" >&2
exit 1
