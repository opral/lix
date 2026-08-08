#!/usr/bin/env bash
set -euo pipefail

# Self-contained source binding for immutable prerequisite 97a. It calibrates
# the known RED without requiring a production build or another worktree.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source_commit="413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d"
expected_tree="820fe560da3bbd2b00b788b0b1759c409048cd6e"
[[ "$(git -C "$repo_root" rev-parse "$source_commit^{tree}")" == "$expected_tree" ]] || {
  echo "SOURCE PROVENANCE ERROR: prerequisite source tree mismatch" >&2
  exit 2
}
serving="$(git -C "$repo_root" show "$source_commit:packages/lix/src/forktree/serving.rs")"
view="$(git -C "$repo_root" show "$source_commit:packages/lix/src/forktree/view.rs")"
point="$(sed -n '668,719p' <<<"$serving")"
batch="$(sed -n '288,323p' <<<"$view")"
grep -Fq 'return Ok(None);' <<<"$point"
grep -Fq 'rows.push(value.map' <<<"$batch"
cat <<'EOF'
RED historical prerequisite
  missing CommitCatalog entry => Ok(None)
  valid absent key           => also None/empty historical slot
EOF
if [[ "${1:-}" == "--expect-red" ]]; then
  echo "EXPECTED RED: historical point/root fail-closed correction remains required"
  exit 0
fi
exit 1
