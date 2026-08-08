#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 --baseline|--successor /absolute/repository/path" >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
mode=$1
root=$2
[[ $mode == --baseline || $mode == --successor ]] || usage
[[ -d "$root/.git" || -f "$root/.git" ]] || { echo "REJECT: not a git worktree: $root"; exit 2; }

tx="$root/packages/lix/src/transaction/context.rs"
staging="$root/packages/lix/src/transaction/staging.rs"
types="$root/packages/lix/src/transaction/types.rs"
undo="$root/packages/lix/src/session/undo_redo.rs"
change="$root/packages/lix/src/sql2/providers/change.rs"
view="$root/packages/lix/src/forktree/view.rs"
for file in "$tx" "$staging" "$types" "$undo" "$change" "$view"; do
  [[ -f "$file" ]] || { echo "REJECT: missing source path $file"; exit 2; }
done

has() { rg -F -q -- "$1" "$2"; }
all_source="$root/packages/lix/src"
extract_fn() {
  local name=$1 file=$2
  perl -0777 -ne "if(/(async fn ${name}\\b.*?)(?=\\n    (?:async )?fn |\\n\\})/s){print \$1}" "$file"
}

if [[ $mode == --baseline ]]; then
  failures=0
  expect_red() {
    local label=$1 needle=$2 file=$3
    if has "$needle" "$file"; then
      echo "RED $label: $needle at $file"
    else
      echo "MISSING RED $label: expected $needle in $file"
      failures=$((failures + 1))
    fi
  }
  expect_red "old change loader" "crate::tracked_state::load_change_record_by_id" "$tx"
  expect_red "old replay metadata" "crate::tracked_state::load_commit_delta_replay_metadata" "$tx"
  expect_red "old replay scope" "crate::tracked_state::CommitDeltaReplacementScope" "$tx"
  expect_red "legacy undo reader" "tracked_state_reader()" "$undo"
  expect_red "raw replay read" "StorageReadOptions::default()" "$tx"
  expect_red "raw replay argument" "StorageAdapterRead + ?Sized" "$tx"
  apply_body=$(extract_fn execute_apply_or_revert "$tx")
  if grep -Fq "crate::tracked_state::load_change_record_by_id" <<<"$apply_body"; then
    echo "RED exact apply caller: old change loader"
  else
    echo "MISSING RED exact apply caller: old change loader"
    failures=$((failures + 1))
  fi
  if grep -Fq "StorageReadOptions::default()" <<<"$apply_body"; then
    echo "RED exact apply caller: raw read"
  else
    echo "MISSING RED exact apply caller: raw read"
    failures=$((failures + 1))
  fi
  if (( failures != 0 )); then
    echo "BASELINE SOURCE GATE INTERNAL ERROR: $failures expected red predicates absent"
    exit 2
  fi
  echo "BASELINE SOURCE GATE: RED as expected; fd2 is not an accepted bridge"
  exit 1
fi

failures=0
reject_if_present() {
  local label=$1 needle=$2 file=$3
  if has "$needle" "$file"; then
    echo "REJECT $label: forbidden '$needle' in $file"
    failures=$((failures + 1))
  fi
}
require() {
  local label=$1 needle=$2 file=$3
  if has "$needle" "$file"; then
    echo "PASS $label: $needle"
  else
    echo "REJECT $label: missing '$needle' in $file"
    failures=$((failures + 1))
  fi
}

require "transaction uses caller facade" "forktree_read_facade" "$tx"
require "ForkTree semantic owner exists" "load_required_commit_record" "$view"
require "ForkTree member owner exists" "load_commit_member_records" "$view"

for needle in \
  "crate::tracked_state::load_change_record_by_id" \
  "crate::tracked_state::load_commit_delta_replay_metadata" \
  "crate::tracked_state::CommitDeltaReplacementScope" \
  "TrackedStateContext::new" \
  "tracked_state_reader()" \
  "StorageAdapterRead + ?Sized"; do
  reject_if_present "legacy replay authority" "$needle" "$tx"
done
reject_if_present "legacy undo reader" "tracked_state_reader()" "$undo"
reject_if_present "deleted changelog space" "COMMIT_CHANGE_ID_SPACE" "$change"
reject_if_present "deleted changelog loader" "crate::tracked_state::" "$change"

apply_body=$(extract_fn execute_apply_or_revert "$tx")
if grep -Fq "forktree_read_facade" <<<"$apply_body"; then
  echo "PASS exact apply caller: caller-owned ForkTree facade"
else
  echo "REJECT exact apply caller: missing caller-owned ForkTree facade"
  failures=$((failures + 1))
fi
if grep -Eq 'begin_read|StorageReadOptions|SharedStorageAdapterRead|StorageAdapterRead' <<<"$apply_body"; then
  echo "REJECT exact apply caller: raw/second read"
  failures=$((failures + 1))
else
  echo "PASS exact apply caller: no raw/second read"
fi
if grep -Fq "load_change_records" <<<"$apply_body"; then
  echo "REJECT exact apply caller: legacy fallback loader"
  failures=$((failures + 1))
else
  echo "PASS exact apply caller: no legacy fallback loader"
fi

# The replay helper must not take a raw adapter or create a read. This check
# intentionally binds the exact helper signature, not merely a symbol count.
helper=$(extract_fn opening_parent_complete_lifecycle_created_at "$tx")
if [[ -z "$helper" ]]; then
  echo "REJECT replay helper: opening_parent_complete_lifecycle_created_at not found"
  failures=$((failures + 1))
else
  if grep -Eq 'ForkTreeReadFacade|CoherentView' <<<"$helper"; then
    echo "PASS replay helper: typed ForkTree view argument"
  else
    echo "REJECT replay helper: no typed ForkTree view argument"
    failures=$((failures + 1))
  fi
  if grep -Eq 'StorageAdapterRead|begin_read|tracked_state::' <<<"$helper"; then
    echo "REJECT replay helper: raw/legacy read path"
    failures=$((failures + 1))
  fi
fi

# No new independent publication authority may appear anywhere in production.
for needle in \
  "PreparedPublication::commit" \
  "ForkTree::begin_write" \
  "COMMIT_CHANGE_ID_SPACE"; do
  if has "$needle" "$all_source"; then
    echo "REJECT production second authority: $needle"
    failures=$((failures + 1))
  fi
done

if (( failures != 0 )); then
  echo "SUCCESSOR SOURCE GATE: BLOCKED ($failures findings)"
  exit 1
fi
echo "SUCCESSOR SOURCE GATE: GREEN"
