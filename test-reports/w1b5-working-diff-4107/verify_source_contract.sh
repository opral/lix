#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <git-worktree> <target-commit-or-ref> [anchor-commit]" >&2
  exit 2
}

[[ $# -ge 2 && $# -le 3 ]] || usage
root=$1
target=$2
anchor=${3:-4107bef177c00694574b4fc65d6bb209239ee877}
here=$(cd "$(dirname "$0")" && pwd)

git -C "$root" rev-parse --is-inside-work-tree >/dev/null
target_commit=$(git -C "$root" rev-parse "$target^{commit}")
anchor_commit=$(git -C "$root" rev-parse "$anchor^{commit}")
if [[ "$anchor_commit" != 4107bef177c00694574b4fc65d6bb209239ee877 ]]; then
  echo "BLOCKER anchor is not exact 4107: $anchor_commit" >&2
  exit 2
fi

python3 "$here/structural_gate.py" --self-test --fixtures "$here/fixtures"

changed_source=$(git -C "$root" diff --name-only "$anchor_commit" "$target_commit" -- packages/lix/src)
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  case "$path" in
    packages/lix/src/forktree/view.rs|packages/lix/src/forktree/serving.rs|packages/lix/src/forktree/tests.rs|packages/lix/src/sql2/context.rs|packages/lix/src/sql2/providers/working_diff.rs|packages/lix/src/sql2/providers/filesystem_working_diff.rs|packages/lix/src/sql2/providers/checkpoint.rs|packages/lix/src/session/checkpoint.rs|packages/lix/src/session/context.rs|packages/lix/src/filesystem/read.rs|packages/lix/src/live_state/forktree_reader.rs) ;;
    *) echo "RED-SCOPE forbidden production path: $path"; exit 1 ;;
  esac
done <<< "$changed_source"

echo "ANCHOR PASS target=$target_commit anchor=$anchor_commit"
echo "SCOPE PASS changed_source=${changed_source:-<none>}"

set +e
python3 "$here/structural_gate.py" --root "$root" --target "$target_commit" --fixtures "$here/fixtures"
structural_status=$?
set -e
if (( structural_status != 0 )); then
  echo "EXPECTED-RED target=$target_commit"
  exit 1
fi
echo "GREEN W1b-5 provider/chronology/source contract"
