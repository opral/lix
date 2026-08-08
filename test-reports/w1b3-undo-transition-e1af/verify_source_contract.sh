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
  source_at "$path" | rg -F -- "$pattern" >/dev/null
}

changed_source=$(git -C "$root" diff --name-only "$anchor_commit" "$target_commit" -- packages/lix/src)
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  case "$path" in
    packages/lix/src/session/undo_redo.rs|packages/lix/src/transaction/context.rs|packages/lix/src/forktree/view.rs|packages/lix/src/forktree/serving.rs|packages/lix/src/forktree/tests.rs) ;;
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

if has_source packages/lix/src/session/undo_redo.rs 'tracked_state_reader()'; then
  red 'undo/redo still opens or uses the legacy tracked-state reader'
fi
if has_source packages/lix/src/session/undo_redo.rs 'commit_graph_reader()'; then
  red 'undo/redo still opens a fresh commit-graph reader'
fi
if ! has_source packages/lix/src/session/undo_redo.rs 'forktree_read_facade'; then
  red 'undo/redo lacks the retained ForkTree facade anchor'
fi
if has_source packages/lix/src/transaction/context.rs 'execute_tracked_state_transition' &&
   has_source packages/lix/src/transaction/context.rs 'tracked_state_reader().await'; then
  red 'typed transitions still reload through the legacy tracked-state reader'
fi

if ! has_source packages/lix/src/session/undo_redo.rs 'async fn undo_in_transaction'; then
  red 'undo state-machine entry anchor is missing'
fi
if ! has_source packages/lix/src/session/undo_redo.rs 'async fn redo_in_transaction'; then
  red 'redo state-machine entry anchor is missing'
fi
if ! has_source packages/lix/src/session/undo_redo.rs 'apply_state_diff'; then
  red 'inverse/replay transition anchor is missing'
fi
if ! has_source packages/lix/src/transaction/context.rs 'execute_typed_state_transitions'; then
  red 'typed atomic staging anchor is missing'
fi
if ! has_source packages/lix/src/session/undo_redo.rs 'CHECKPOINT_MARKER_SCHEMA_KEY'; then
  red 'checkpoint-floor marker anchor is missing'
fi

if (( reds > 0 )); then
  echo "EXPECTED-RED predicates=$reds target=$target_commit"
  exit 1
fi

echo "GREEN undo/redo and typed-transition reader deletion predicates pass"
