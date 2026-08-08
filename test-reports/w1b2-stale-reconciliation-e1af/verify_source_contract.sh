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
    packages/lix/src/transaction/context.rs|packages/lix/src/transaction/context/cohort.rs|packages/lix/src/transaction/stale_commit.rs|packages/lix/src/forktree/view.rs|packages/lix/src/forktree/serving.rs|packages/lix/src/forktree/tests.rs) ;;
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

if has_source packages/lix/src/transaction/context.rs 'reconcile_stale_disjoint_writes' &&
   has_source packages/lix/src/transaction/context.rs 'self.tracked_state.reader(read)'; then
  red 'stale disjoint reconciliation still owns a legacy tracked-state reader'
fi
if has_source packages/lix/src/transaction/context.rs 'reconcile_stale_plugin_writes' &&
   has_source packages/lix/src/transaction/context.rs 'self.tracked_state.reader(read)'; then
  red 'stale plugin reconciliation still owns a legacy tracked-state reader'
fi
if has_source packages/lix/src/transaction/context/cohort.rs 'reconcile_cohort_files' &&
   has_source packages/lix/src/transaction/context/cohort.rs 'tracked_state.reader'; then
  red 'cohort reconciliation still owns a legacy tracked-state reader'
fi
if has_source packages/lix/src/transaction/context/cohort.rs 'load_projected_batch_at_commit'; then
  red 'cohort owner/version discovery still uses legacy projected batch loading'
fi
if has_source packages/lix/src/transaction/context.rs 'reconcile_stale_plugin_writes' &&
   has_source packages/lix/src/transaction/context.rs 'load_projected_batch_at_commit'; then
  red 'plugin owner/version/revision discovery still uses legacy projected batch loading'
fi

if ! has_source packages/lix/src/transaction/context.rs 'forktree_read_facade'; then
  red 'transaction context lacks retained ForkTree facade anchor'
fi
if ! has_source packages/lix/src/transaction/stale_commit.rs 'classify_stale_commit'; then
  red 'pure stale-overlap classifier anchor is missing'
fi
if ! has_source packages/lix/src/transaction/context.rs 'commit_prepared'; then
  red 'commit-time reconciliation boundary anchor is missing'
fi
if ! has_source packages/lix/src/transaction/context.rs 'resolve_plugin_conflicts'; then
  red 'deterministic plugin resolver anchor is missing'
fi

if (( reds > 0 )); then
  echo "EXPECTED-RED predicates=$reds target=$target_commit"
  exit 1
fi

echo "GREEN stale reconciliation reader deletion predicates pass"
