#!/usr/bin/env bash
set -euo pipefail

# This verifier is intentionally source-only. It proves the e166 behavior
# that the pure model must reject; it does not compile or open an adapter.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source_commit="e1666edd0b4d814a88d985086ecc5a477b5d32e6"
expected_tree="c680bd7e7f7b70cd784676515839af2dcbbc7917"

actual_tree="$(git -C "$repo_root" rev-parse "$source_commit^{tree}")"
if [[ "$actual_tree" != "$expected_tree" ]]; then
  echo "SOURCE PROVENANCE ERROR: e166 tree is $actual_tree, expected $expected_tree" >&2
  exit 2
fi

source_blob() {
  git -C "$repo_root" show "$source_commit:$1"
}

serving="$(source_blob packages/lix/src/forktree/serving.rs)"
view="$(source_blob packages/lix/src/forktree/view.rs)"

point_body="$(sed -n '668,719p' <<<"$serving")"
exact_body="$(sed -n '284,323p' <<<"$view")"
state_point_body="$(sed -n '1235,1261p' <<<"$serving")"
root_body="$(sed -n '756,777p' <<<"$serving")"
object_body="$(sed -n '490,508p' <<<"$view")"

require() {
  local label="$1"
  local haystack="$2"
  local needle="$3"
  if ! grep -Fq "$needle" <<<"$haystack"; then
    echo "SOURCE CHECK ERROR: missing $label: $needle" >&2
    exit 2
  fi
  echo "GREEN $label"
}

require "point API result shape" "$point_body" 'Result<Option<(StateValue, StateSource)>'
require "missing catalog branch" "$point_body" 'else {'
require "missing catalog returns absence" "$point_body" 'return Ok(None);'
require "exact batch maps point absence" "$exact_body" 'rows.push(value.map'
require "valid missing-key absence" "$state_point_body" 'return Ok(None);'
require "required selector/root bytes" "$root_body" 'required_full_value('
require "global selector authentication" "$root_body" 'GlobalSelectorV1::decode(&raw)?'
require "repository root authentication" "$root_body" 'RepositoryRootV1::decode(selector.repository_root, &bytes)?'
require "commit envelope authentication" "$point_body" 'CommitObjectV1::decode(entry.commit_object_id, &bytes)?'
require "object absence is an error" "$object_body" 'object {id} is absent'
require "typed state-cell split" "$exact_body" 'StateCell::Tombstone'
require "typed state-cell null" "$exact_body" 'StateCell::Null'
require "typed state-cell value" "$exact_body" 'StateCell::Value(snapshot)'

for body_name in point_body exact_body state_point_body; do
  body="${!body_name}"
  if grep -Eq 'begin_read|retry|fallback|cache' <<<"$body"; then
    echo "SOURCE CHECK ERROR: $body_name contains a second-read/retry/fallback/cache route" >&2
    exit 2
  fi
done
echo "GREEN one retained read / no fallback / no retry / no cache in point path"

cat <<'EOF'
RED e166 historical point contract
  valid commit + root + absent key       => source can return authenticated absence
  missing CommitCatalog commit            => indistinguishable from that absence
  missing root object                     => source decoder/object load errors
  wrong-kind/substituted root             => source decoder/tree validation errors
  malformed catalog/root                  => source decoder errors
  valid tombstone/null/value              => exact batch maps distinct cells
  read lifetime                           => one passed read; no retry/fallback/cache
EOF

if [[ "${1:-}" == "--expect-red" ]]; then
  echo "EXPECTED RED: missing CommitCatalog and missing key share Option::None"
  exit 0
fi
echo "RED: e166 does not fail closed for a missing CommitCatalog commit" >&2
exit 1
