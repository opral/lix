#!/usr/bin/env bash
set -u -o pipefail

candidate=${1:?usage: $0 <candidate-worktree> [base-commit]}
base=${2:-fd2be256d763f17e9f127d4c984e36fba191cb82}
candidate=$(cd "$candidate" && pwd)
head=$(git -C "$candidate" rev-parse HEAD 2>/dev/null || true)

if [[ -z "$head" ]]; then
  echo "ERROR: candidate is not a git worktree: $candidate"
  exit 2
fi

if ! git -C "$candidate" cat-file -e "$base^{commit}" 2>/dev/null; then
  echo "ERROR: base commit is unavailable: $base"
  exit 2
fi

if ! git -C "$candidate" merge-base --is-ancestor "$base" "$head"; then
  echo "ERROR: candidate is not a descendant of base: base=$base head=$head"
  exit 2
fi

root="$candidate/packages/lix/src"
provider="$root/sql2/providers/change.rs"
context="$root/sql2/context.rs"
package_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cases="$package_dir/SQL_CHANGE_READER_CASES.tsv"
fixtures="$package_dir/fixtures"

red=0
red_line() {
  printf 'RED-%s %s\n' "$1" "$2"
  red=1
}
pass_line() {
  printf 'PASS-%s %s\n' "$1" "$2"
}
contains() {
  local pattern=$1 file=$2
  rg -q -- "$pattern" "$file" 2>/dev/null
}
count_matches() {
  local pattern=$1 file=$2
  rg -o -- "$pattern" "$file" 2>/dev/null | wc -l | tr -d ' '
}
line_number() {
  local pattern=$1 file=$2
  rg -n -- "$pattern" "$file" 2>/dev/null | head -n1 | cut -d: -f1
}

if [[ ! -f "$provider" || ! -f "$context" ]]; then
  red_line 01 "required SQL changelog source files are absent"
else
  pass_line 01 "SQL changelog provider/context files exist"
fi

changed=$(git -C "$candidate" diff --name-only "$base" "$head")
allowed_path() {
  case "$1" in
    packages/lix/src/sql2/providers/change.rs|\
    packages/lix/src/sql2/context.rs|\
    packages/lix/src/session/context.rs|\
    packages/lix/src/transaction/context.rs|\
    packages/lix/src/forktree/view.rs|\
    packages/lix/src/forktree/serving.rs|\
    packages/lix/src/forktree/mod.rs|\
    test-reports/stage2-sql-change-reader-fd2/*) return 0 ;;
    *) return 1 ;;
  esac
}
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  if ! allowed_path "$path"; then
    red_line 02 "changed path outside the SQL changelog read-facade closure: $path"
  fi
done <<< "$changed"
if [[ "$red" -eq 0 ]]; then
  pass_line 02 "changed-path fence contains only the declared read-facade closure"
fi

if [[ -f "$provider" ]]; then
  forbidden=(
    'tracked_state::'
    'COMMIT_CHANGE_ID_SPACE'
    'PointReadPlan'
    'StorageKey'
    'StorageProjectedValue'
    'StorageGetOptions'
    'ChangelogContext'
    'ChangelogReader'
    'CommitGraphContext'
    'query_source\.store'
    'store\.clone\('
    '\.flatten\('
    'filter_map\('
    'begin_read'
  )
  for token in "${forbidden[@]}"; do
    if contains "$token" "$provider"; then
      red_line 03 "legacy/raw/fallback token remains in provider: $token"
    fi
  done

  if contains 'scan_changelog_changes\(&query_source\.forktree_reader,' "$provider"; then
    pass_line 04 "scan receives the operation-owned ForkTree reader by reference"
  else
    red_line 04 "scan does not receive &query_source.forktree_reader"
  fi
  if contains 'load_exact_change\(&query_source\.forktree_reader,' "$provider"; then
    pass_line 05 "exact lookup receives the same operation-owned ForkTree reader"
  else
    red_line 05 "exact lookup does not receive &query_source.forktree_reader"
  fi
  if contains 'ForkTreeReadFacade' "$provider" && contains 'ForkTreeReadFacade' "$context"; then
    pass_line 06 "provider and source boundary name the ForkTree facade"
  else
    red_line 06 "ForkTree facade is not the provider/source boundary"
  fi
  if awk '
    /struct ChangelogQuerySource/ { seen=1 }
    seen && /forktree_reader/ { found=1; exit 0 }
    seen && /^}/ { exit 1 }
    END { if (!seen || !found) exit 1 }
  ' "$context"; then
    pass_line 07 "ChangelogQuerySource carries the retained ForkTree reader"
  else
    red_line 07 "ChangelogQuerySource has no retained ForkTree reader field"
  fi
  if contains 'require_commit_records' "$provider" && \
     contains 'records\.len\(\) != expected_commit_ids\.len\(\)' "$provider" && \
     contains 'record\.commit_id != expected_commit_id' "$provider"; then
    pass_line 08 "commit-record length and ordered embedded identity checks are present"
  else
    red_line 08 "commit-record enumeration is not length/identity checked"
  fi

  duplicate_line=$(line_number '[Dd]uplicate' "$provider")
  set_line=$(line_number 'BTreeSet|HashSet' "$provider")
  sort_line=$(line_number 'sort_by_key' "$provider")
  truncate_line=$(line_number 'truncate' "$provider")
  if [[ -n "$duplicate_line" && -n "$set_line" && -n "$truncate_line" && \
        "$duplicate_line" -lt "$truncate_line" && "$set_line" -lt "$truncate_line" ]]; then
    pass_line 09 "duplicate logical IDs are rejected before output/limit"
  else
    red_line 09 "no source-proven duplicate-ID rejection before limit"
  fi
  if [[ -n "$sort_line" && -n "$truncate_line" && "$sort_line" -lt "$truncate_line" ]]; then
    pass_line 10 "canonical ordering precedes SQL limit"
  else
    red_line 10 "SQL limit is not source-ordered after canonical sort"
  fi
else
  for n in 03 04 05 06 07 08 09 10; do
    red_line "$n" "provider unavailable for source checks"
  done
fi

if [[ -f "$cases" && -d "$fixtures" ]]; then
  case_ok=1
  while IFS=$'\t' read -r id class fixture expected; do
    [[ -z "$id" || "$id" == "id" ]] && continue
    fixture_path="$fixtures/$fixture"
    if [[ ! -f "$fixture_path" ]] || \
       ! contains '"case"' "$fixture_path" || \
       ! contains '"expected"' "$fixture_path" || \
       ! contains "$expected" "$fixture_path"; then
      red_line 11 "fixture contract mismatch: $id"
      case_ok=0
    fi
  done < "$cases"
  if [[ "$case_ok" -eq 1 ]]; then
    pass_line 11 "all discriminating positive/negative fixtures are present"
  fi
else
  red_line 11 "case table or fixture directory is absent"
fi

if [[ "$red" -eq 0 ]]; then
  echo "SOURCE-CONTRACT PASS: candidate satisfies the SQL changelog closure"
  exit 0
fi
echo "SOURCE-CONTRACT RED: expected on compiler-red fd2; no implementation is claimed"
exit 1
