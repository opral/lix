#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 REPOSITORY_ROOT ANCHOR_COMMIT" >&2
  exit 2
fi

repo=$1
anchor=$2
expected_anchor=e1af471b9ab0f598dafa7c2ddec7867667c81740
expected_tree=bfa0d271a723da8250ab76ada16fda90926f1099
package_rel=packages/lix/tests/w1a_historical_reader_readiness
src_root="$repo/packages/lix/src"
route="$src_root/sql2/history_route.rs"
context="$src_root/sql2/context.rs"

test "$(git -C "$repo" rev-parse "$anchor^{commit}")" = "$expected_anchor"
test "$(git -C "$repo" rev-parse "$anchor^{tree}")" = "$expected_tree"

mapfile -t w1a_paths < <(cat "$repo/$package_rel/W1A_PRODUCTION_ALLOWLIST.tsv")
while IFS= read -r changed; do
  [[ -z "$changed" ]] && continue
  if ! printf '%s\n' "${w1a_paths[@]}" | grep -Fxq "$changed"; then
    echo "RED-SCOPE unexpected production path: $changed"
    exit 1
  fi
done < <(git -C "$repo" diff --name-only "$anchor..HEAD" -- packages/lix/src | sort)

red_count=0
red() {
  red_count=$((red_count + 1))
  printf 'RED-%02d %s\n' "$red_count" "$1"
}

present() {
  local label=$1
  local pattern=$2
  local file=$3
  if rg -n -F -- "$pattern" "$file" >/dev/null; then
    red "$label: $file contains '$pattern'"
  else
    printf 'PASS %s absent: %s\n' "$label" "$pattern"
  fi
}

present "legacy raw HistoryQuerySource.store" 'pub(crate) store:' "$context"
present "legacy raw HistoryQuerySource.json_reader" 'pub(crate) json_reader:' "$context"
present "legacy storage-bearing common helper" 'pub(crate) async fn load_history_entries' "$route"
present "legacy CommitGraphReader common route" 'CommitGraphReader' "$route"

for file in \
  "$src_root/sql2/providers/entity_history.rs" \
  "$src_root/sql2/providers/directory_history.rs" \
  "$src_root/sql2/providers/file_history.rs"; do
  present "provider CommitGraphReader" 'CommitGraphReader' "$file"
  present "provider load_history_entries" 'load_history_entries' "$file"
done

present "file history parent graph helper" 'load_history_commit_parents' "$src_root/sql2/providers/file_history.rs"
present "directory history parent graph helper" 'load_history_commit_parents' "$src_root/sql2/providers/directory_history.rs"
present "raw file history source store" 'query_source.store' "$src_root/sql2/providers/file_history.rs"
present "raw file history JSON reader" 'query_source.json_reader' "$src_root/sql2/providers/file_history.rs"

for file in \
  "$src_root/sql2/context.rs" \
  "$src_root/sql2/history_route.rs" \
  "$src_root/sql2/providers/entity_history.rs" \
  "$src_root/sql2/providers/directory_history.rs" \
  "$src_root/sql2/providers/file_history.rs"; do
  if ! rg -n -F -- 'ForkTreeReadFacade' "$file" >/dev/null; then
    red "missing ForkTree owner reference: $file"
  else
    printf 'PASS ForkTree owner reference: %s\n' "$file"
  fi
done

if rg -n -F -- 'ForkTreeReadFacade' \
  "$src_root/sql2/providers/entity_history.rs" \
  "$src_root/sql2/providers/directory_history.rs" \
  "$src_root/sql2/providers/file_history.rs" >/dev/null; then
  printf 'PASS existing typed ForkTree observed-state path present\n'
else
  red 'typed ForkTree observed-state path absent from filesystem history providers'
fi

printf 'SUMMARY expected-red-findings=%d\n' "$red_count"
if (( red_count == 0 )); then
  echo 'ERROR expected e1af RED calibration did not find legacy W1a boundaries' >&2
  exit 2
fi
exit 1
