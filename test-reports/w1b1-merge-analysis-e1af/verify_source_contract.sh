#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <git-worktree> <target-commit-or-ref> [anchor-commit]" >&2
  exit 2
}

[[ $# -ge 2 && $# -le 3 ]] || usage
root=$1
target=$2
anchor=${3:-e1af471b9ab0f598dafa7c2ddec7867667c81740}

git -C "$root" rev-parse --is-inside-work-tree >/dev/null
target_commit=$(git -C "$root" rev-parse "$target^{commit}")
anchor_commit=$(git -C "$root" rev-parse "$anchor^{commit}")

if [[ "$anchor_commit" != e1af471b9ab0f598dafa7c2ddec7867667c81740 ]]; then
  echo "BLOCKER anchor is not exact e1af: $anchor_commit" >&2
  exit 2
fi

source_at() {
  git -C "$root" show "$target_commit:$1"
}

has_source() {
  local path=$1 pattern=$2
  source_at "$path" | rg -- "$pattern" >/dev/null
}

changed_source=$(git -C "$root" diff --name-only "$anchor_commit" "$target_commit" -- packages/lix/src)
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  case "$path" in
    packages/lix/src/session/merge/analysis.rs|packages/lix/src/session/merge/branch.rs|packages/lix/src/transaction/context.rs|packages/lix/src/tracked_state/diff.rs|packages/lix/src/forktree/view.rs|packages/lix/src/forktree/serving.rs|packages/lix/src/forktree/tests.rs) ;;
    *) echo "RED-SCOPE forbidden production path: $path"; exit 1 ;;
  esac
done <<< "$changed_source"

echo "ANCHOR PASS target=$target_commit anchor=$anchor_commit"
echo "SCOPE PASS changed_source=${changed_source:-<none>}"

reds=0
red() {
  reds=$((reds + 1))
  echo "RED-$reds $1"
}

if has_source packages/lix/src/session/merge/analysis.rs 'TrackedStateStoreReader'; then
  red 'merge analysis still accepts TrackedStateStoreReader'
fi
if has_source packages/lix/src/session/merge/analysis.rs 'diff_commits'; then
  red 'merge analysis still routes through legacy diff_commits'
fi
if has_source packages/lix/src/session/merge/branch.rs 'with_opening_tracked_reader'; then
  red 'merge branch still enters with_opening_tracked_reader'
fi
if has_source packages/lix/src/transaction/context.rs 'with_opening_tracked_reader'; then
  red 'transaction context still exposes merge callback plumbing'
fi
if has_source packages/lix/src/tracked_state/diff.rs 'TrackedStateStoreReader'; then
  red 'legacy tracked-state diff owner remains reachable'
fi

if ! has_source packages/lix/src/session/merge/branch.rs 'branch_ref_reader_on_opening_read'; then
  red 'merge branch lacks opening-read branch-ref anchor'
fi
if ! has_source packages/lix/src/session/merge/branch.rs 'commit_graph_reader_on_opening_read'; then
  red 'merge branch lacks opening-read topology anchor'
fi
if ! has_source packages/lix/src/session/merge/branch.rs 'forktree_read_facade'; then
  red 'merge branch lacks ForkTree facade anchor'
fi

if (( reds > 0 )); then
  echo "EXPECTED-RED predicates=$reds target=$target_commit"
  exit 1
fi

echo "GREEN merge-analysis reader deletion predicates pass"
