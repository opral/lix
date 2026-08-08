#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 WORKTREE TARGET [EXACT_E1AF_ANCHOR]" >&2
  echo "       $0 --fixture FIXTURE_ROOT" >&2
  exit 2
}

[[ $# -ge 1 && $# -le 3 ]] || usage
EXACT_E1AF=e1af471b9ab0f598dafa7c2ddec7867667c81740

fixture=0
if [[ ${1:-} == --fixture ]]; then
  [[ $# -eq 2 ]] || usage
  fixture=1
  root=$2
  target=fixture
  anchor=fixture
  [[ -d "$root" ]] || { echo "BLOCKER fixture root is absent: $root" >&2; exit 2; }
else
  [[ $# -ge 2 ]] || usage
  root=$1
  target=$2
  anchor=${3:-$EXACT_E1AF}
  git -C "$root" rev-parse --is-inside-work-tree >/dev/null
  target=$(git -C "$root" rev-parse "$target^{commit}")
  anchor=$(git -C "$root" rev-parse "$anchor^{commit}")
  [[ "$anchor" == "$EXACT_E1AF" ]] || {
    echo "BLOCKER anchor is not exact e1af: $anchor" >&2
    exit 2
  }
fi

source_at() {
  if (( fixture )); then
    cat "$root/$1"
  else
    git -C "$root" show "$target:$1"
  fi
}

has_source() {
  local path=$1 pattern=$2
  source_at "$path" | rg -F -- "$pattern" >/dev/null
}

extract_function() {
  local path=$1 name=$2
  source_at "$path" | awk -v needle="fn $name" '
    !started && index($0, needle) { started=1 }
    started && !finished {
      line=$0
      opens=gsub(/\{/, "{", line)
      closes=gsub(/\}/, "}", line)
      depth += opens - closes
      print
      if (depth == 0 && closes > 0) finished=1
    }
  '
}

function_has() {
  local path=$1 name=$2 pattern=$3
  extract_function "$path" "$name" | rg -F -- "$pattern" >/dev/null
}

function_index() {
  local path=$1 name=$2 pattern=$3 index=0 line
  while IFS= read -r line; do
    index=$((index + 1))
    if [[ "$line" == *"$pattern"* ]]; then
      echo "$index"
      return 0
    fi
  done < <(extract_function "$path" "$name")
  echo 0
}

scope_paths() {
  if (( fixture )); then
    find "$root/packages/lix/src" -type f -printf 'packages/lix/src/%P\n' 2>/dev/null | sort
  else
    git -C "$root" diff --name-only "$anchor" "$target" -- packages/lix/src
  fi
}

bad_scope_path() {
  case "$1" in
    packages/lix/src/transaction/context.rs|packages/lix/src/transaction/context/cohort.rs|packages/lix/src/transaction/stale_commit.rs|packages/lix/src/forktree/view.rs|packages/lix/src/forktree/serving.rs|packages/lix/src/forktree/tests.rs) return 1 ;;
    *) return 0 ;;
  esac
}

bad_scope=$(scope_paths | awk '
  $0 == "packages/lix/src/transaction/context.rs" ||
  $0 == "packages/lix/src/transaction/context/cohort.rs" ||
  $0 == "packages/lix/src/transaction/stale_commit.rs" ||
  $0 == "packages/lix/src/forktree/view.rs" ||
  $0 == "packages/lix/src/forktree/serving.rs" ||
  $0 == "packages/lix/src/forktree/tests.rs" { next }
  { print; exit }
')
if [[ -n "$bad_scope" ]]; then
  echo "RED-SCOPE forbidden production path: $bad_scope"
  exit 1
fi

if (( fixture )); then
  echo "FIXTURE SCOPE PASS allowlist=6"
else
  changed_source=$(scope_paths)
  echo "ANCHOR PASS target=$target anchor=$anchor"
  echo "SCOPE PASS changed_source=${changed_source:-<none>}"
fi

if (( ! fixture )); then
  reds=0
  red() {
    reds=$((reds + 1))
    echo "RED-$reds $1"
  }
  if function_has packages/lix/src/transaction/context.rs reconcile_stale_disjoint_writes 'self.tracked_state.reader(read)'; then
    red 'stale disjoint reconciliation still owns a legacy tracked-state reader'
  fi
  if function_has packages/lix/src/transaction/context.rs reconcile_stale_plugin_writes 'self.tracked_state.reader(read)'; then
    red 'stale plugin reconciliation still owns a legacy tracked-state reader'
  fi
  if function_has packages/lix/src/transaction/context/cohort.rs reconcile_cohort_files 'tracked_state.reader'; then
    red 'cohort reconciliation still owns a legacy tracked-state reader'
  fi
  if function_has packages/lix/src/transaction/context/cohort.rs load_cohort_plugin_groups 'load_projected_batch_at_commit'; then
    red 'cohort owner/version discovery still uses legacy projected batch loading'
  fi
  if function_has packages/lix/src/transaction/context.rs reconcile_stale_plugin_writes 'load_projected_batch_at_commit'; then
    red 'plugin owner/version/revision discovery still uses legacy projected batch loading'
  fi
  if (( reds > 0 )); then
    echo "EXPECTED-RED predicates=$reds target=$target"
    exit 1
  fi
fi

green_errors=0
green_error() {
  green_errors=$((green_errors + 1))
  echo "RED-STRUCTURE-$green_errors $1"
}
context=packages/lix/src/transaction/context.rs
cohort=packages/lix/src/transaction/context/cohort.rs
view=packages/lix/src/forktree/view.rs
stale=packages/lix/src/transaction/stale_commit.rs
commit_body=$(extract_function "$context" commit_prepared)
commit_facades=$(printf '%s\n' "$commit_body" | rg -F -c -- 'forktree_read_facade' || true)
[[ "${commit_facades:-0}" == 1 ]] || green_error 'commit_prepared must construct exactly one opening-read facade'
if [[ "$commit_body" == *'begin_read('* || "$commit_body" == *'read_store('* || "$commit_body" == *'.clone()'* ]]; then
  green_error 'commit_prepared contains a second read, raw extraction, or facade clone'
fi
check_reader_function() {
  local path=$1 name=$2 body facade_call
  body=$(extract_function "$path" "$name")
  [[ -n "$body" ]] || { green_error "missing function $name"; return; }
  [[ "$body" == *'facade'* ]] || green_error "$name does not receive/use an operation facade"
  facade_call=$(printf '%s\n' "$body" | rg -F -c -- 'facade.' || true)
  [[ "${facade_call:-0}" -gt 0 ]] || green_error "$name has no argument-aware facade operation"
  [[ "$body" != *'tracked_state.reader'* ]] || green_error "$name still constructs tracked-state reader"
  [[ "$body" != *'load_projected_batch_at_commit'* ]] || green_error "$name still uses projected legacy batch"
  [[ "$body" != *'begin_read('* ]] || green_error "$name begins a nested storage read"
  [[ "$body" != *'.clone()'* ]] || green_error "$name clones a read/facade"
}
check_reader_function "$context" reconcile_stale_disjoint_writes
check_reader_function "$context" reconcile_stale_plugin_writes
check_reader_function "$cohort" reconcile_cohort_files
check_reader_function "$cohort" load_cohort_plugin_groups

check_forbidden_path() {
  local path=$1
  if source_at "$path" | rg -n -P -- 'TrackedStateStoreReader|tracked_state_reader|JsonStoreReader|HistoryQuerySource|load_projected_batch_at_commit|begin_read\(|fallback_|retry_|reader_cache|alternate_authority|StorageAdapterRead|read_store\(|raw_storage_read' >/dev/null; then
    green_error "forbidden legacy/second-authority symbol in $path"
  fi
}
check_forbidden_path "$context"
check_forbidden_path "$cohort"
check_forbidden_path "$stale"
check_forbidden_path "$view"
check_forbidden_path packages/lix/src/forktree/serving.rs
check_forbidden_path packages/lix/src/forktree/tests.rs

require_function_pattern() {
  local path=$1 name=$2 pattern=$3 label=$4
  if function_has "$path" "$name" "$pattern"; then
    echo "PASS_FUNCTION=$label"
  else
    green_error "$label"
  fi
}
require_function_pattern "$context" reconcile_stale_plugin_writes authenticate_owner_registry 'owner/registry authentication'
auth_line=$(function_index "$context" reconcile_stale_plugin_writes authenticate_owner_registry)
idempotency_line=$(function_index "$context" reconcile_stale_plugin_writes idempotency_keys)
terminal_idempotency_line=$(function_index "$context" reconcile_stale_plugin_writes 'Outcome::Idempotent')
if (( auth_line == 0 || idempotency_line == 0 || terminal_idempotency_line == 0 || auth_line >= idempotency_line || idempotency_line >= terminal_idempotency_line )); then
  green_error 'authentication must precede idempotency replay and terminal success'
fi
require_function_pattern "$context" reconcile_stale_plugin_writes sort_by_key 'deterministic multi-write ordering'
require_function_pattern "$context" reconcile_stale_plugin_writes rank 'write rank binding'
require_function_pattern "$context" reconcile_stale_plugin_writes validate_complete_plan 'complete-plan validation'
require_function_pattern "$view" load_owner_proof load_owner_proof 'owner proof operation'
require_function_pattern "$view" load_registry_proof load_registry_proof 'registry proof operation'
require_function_pattern "$view" load_semantic_row load_semantic_row 'semantic row operation'
if ! has_source "$view" 'pub(crate) struct ForkTreeReadFacade'; then
  green_error 'ForkTreeReadFacade declaration is absent'
fi
if ! function_has "$stale" classify_stale_commit 'return'; then
  green_error 'pure stale classifier is absent'
fi

if (( green_errors > 0 )); then
  echo "RESULT=RED structural_predicates=$green_errors"
  exit 1
fi
echo "RESULT=GREEN candidate-parametric structural predicates pass"
