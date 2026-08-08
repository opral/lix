#!/usr/bin/env bash
set -euo pipefail

# Static future-candidate gate. Exact b59 is intentionally RED: its defining
# BranchHead control owner is absent while old consumers remain. The scanner
# covers production, compiled tests, benchmarks, aliases/reexports, wrappers,
# caches, fallback writers, and second-authority code under packages.
ROOT=${1:?usage: verify_branch_ref_whole_closure.sh ROOT [ANCHOR]}
ANCHOR=${2:-b59e1f11a51153e0a787a81f0f25bf104d150aaf}
ROOT=$(cd "$ROOT" && pwd)
ORACLE_REL=packages/lix/tests/branch_ref_whole_closure_oracle_b59
# The ripgrep roots below are `$ROOT/packages`, so their glob paths are
# relative to `packages/`, not to the repository root. Keeping both forms
# explicit makes the exclusion deterministic and prevents this oracle's own
# manifest/report text from changing the residue calibration.
ORACLE_RG_REL=lix/tests/branch_ref_whole_closure_oracle_b59
ORACLE_RG_EXCLUDE="!**/$ORACLE_RG_REL/**"

if ! git -C "$ROOT" merge-base --is-ancestor "$ANCHOR" HEAD; then
  echo "BLOCKED: candidate is not descended from $ANCHOR" >&2
  exit 2
fi

SRC="$ROOT/packages/lix/src"
[[ -d "$SRC" ]] || { echo "MISSING source root" >&2; exit 1; }

LEGACY=(
  BranchHeadControl BranchHeadControlContext BranchHeadControlCache
  BranchHeadTrackedReachability branch_head_control_precondition
  stage_branch_head_control stage_untracked_generation
  untracked_lifecycle_generation next_current_state_revision
  BranchRefReader CachingBranchRefReader BranchRefContext BranchRefStoreReader
  branch_ref_stage_row branch_ref_tombstone_row BRANCH_REF_SCHEMA_KEY
)
REQUIRED=(
  GlobalSelectorV1 BranchSelectorV1 CoherentView ForkTreeReadFacade StorageRead PreparedPublication
  open_coherent_view_on_read 'from_branch_view' 'from_global_epoch'
  SELECTOR_SPACE global_selector_key branch_selector_key
)

is_code_file() {
  case "$1" in
    *.rs|*.toml|*.json|*.js|*.mjs|*.ts) return 0 ;;
    *) return 1 ;;
  esac
}

all_sources=()
while IFS= read -r file; do
  rel=${file#"$ROOT/"}
  [[ "$rel" == "$ORACLE_REL"/* ]] && continue
  is_code_file "$file" || continue
  all_sources+=("$file")
done < <(find "$ROOT/packages" -type f -not -path '*/target/*' -not -path '*/.git/*' | sort)

all=""
for file in "${all_sources[@]}"; do
  all+="$(cat "$file")"
  all+=$'\n'
done

missing=()
for token in "${REQUIRED[@]}"; do
  grep -Fq "$token" <<<"$all" || missing+=("$token")
done

legacy_hits=()
for token in "${LEGACY[@]}"; do
  while IFS= read -r line; do
    legacy_hits+=("${line#"$ROOT/"}")
  done < <(
    rg -n -F "$token" "$ROOT/packages" \
      -g '*.rs' -g '*.toml' -g '*.json' -g '*.js' -g '*.mjs' -g '*.ts' \
      -g '!target/**' -g "$ORACLE_RG_EXCLUDE" | sort || true
  )
done

# Explicit second-authority and wrapper spellings are scanned independently of
# the legacy type list so a successor cannot hide them behind a new alias.
forbidden_patterns=(
  fallback_branch_ref
  legacy_branch_ref
  branch_ref_fallback
  raw_branch_ref
  branch_ref_cache
  SecondBranchAuthority
  DualBranchAuthority
  BranchRefFallback
  BranchHeadFallback
  branch_head_control_cache
  branch_ref_reader_cache
  BranchRefAuthority
  BranchRefWriter
  SecondSelectorAuthority
  DualSelectorAuthority
  raw_selector_authority
)
for pattern in "${forbidden_patterns[@]}"; do
  while IFS= read -r line; do
    legacy_hits+=("${line#"$ROOT/"}")
  done < <(
    rg -n -F "$pattern" "$ROOT/packages" \
      -g '*.rs' -g '*.toml' -g '*.json' -g '*.js' -g '*.mjs' -g '*.ts' \
      -g '!target/**' -g "$ORACLE_RG_EXCLUDE" | sort || true
  )
done

old_paths=()
for rel in \
  packages/lix/src/branch/refs.rs \
  packages/lix/src/branch/context.rs \
  packages/lix/src/branch/stage_rows.rs \
  packages/lix/src/sql2/branch_ref.rs; do
  [[ -e "$ROOT/$rel" ]] && old_paths+=("$rel")
done

authority=()
while IFS= read -r line; do
  authority+=("${line#"$ROOT/"}")
done < <(
  rg -n 'BRANCH_REF_SCHEMA_KEY|branch_ref_stage_row|branch_ref_tombstone_row|stage_branch_head_control|branch_head_control_precondition|BranchHeadControl|BranchRefReader|CachingBranchRefReader' \
    "$ROOT/packages" -g '*.rs' -g '*.toml' -g '*.json' -g '*.js' -g '*.mjs' -g '*.ts' \
    -g '!target/**' -g "$ORACLE_RG_EXCLUDE" | sort || true
)

projection_files=()
while IFS= read -r file; do
  if rg -l -F 'lix_branch_ref' "$file" >/dev/null 2>&1; then
    projection_files+=("${file#"$ROOT/"}")
  fi
done < <(printf '%s\n' "${all_sources[@]}" | sort)

derived_only_files=(
  packages/lix/src/schema/builtin/lix_branch_ref.json
  packages/lix/src/schema/builtin/lix_branch_descriptor.json
  packages/lix/src/schema/builtin/mod.rs
  packages/lix/src/sql2/bind/table.rs
  packages/lix/src/sql2/catalog/registry.rs
  packages/lix/src/sql2/catalog/entity_surface.rs
  packages/lix/src/sql2/read_only.rs
  packages/lix/src/engine.rs
)
is_derived_only_path() {
  local rel=$1
  [[ "$rel" == packages/lix/tests/* || "$rel" == packages/engine-benchmarks/* || "$rel" == packages/rs-sdk-tests/* ]] && return 0
  for allowed in "${derived_only_files[@]}"; do
    [[ "$rel" == "$allowed" ]] && return 0
  done
  return 1
}
non_derived_projection=()
for rel in "${projection_files[@]}"; do
  is_derived_only_path "$rel" || non_derived_projection+=("$rel")
done

categories=(
  'reader:branch refs, SQL session, directory/file providers, branch scope'
  'writer:init, functions, transaction, create/switch/delete, merge, checkpoint'
  'gc:gc roots, reachability, preconditions, checkpoint retention'
  'live-state:live_state context/cache/generation consumers'
  'sql:providers, projections, bind/read-only surfaces, reexports'
  'fixtures:packages/lix/tests and test_support'
  'benchmarks:packages/engine-benchmarks and storage_bench'
  'aliases:module reexports and compatibility wrappers'
  'spaces:branch ref schema/stage rows and declared storage spaces'
)

echo "branch-ref-whole-closure root=$ROOT"
echo "branch-ref-whole-closure anchor=$ANCHOR"
echo "required-missing=${#missing[@]}"
printf '  missing=%s\n' "${missing[*]:-none}"
echo "legacy-residue=${#legacy_hits[@]}"
printf '  residue=%s\n' "${legacy_hits[*]:-none}"
echo "old-closure-paths=${#old_paths[@]}"
printf '  paths=%s\n' "${old_paths[*]:-none}"
echo "lix-branch-ref-occurrence-files=${#projection_files[@]}"
printf '  projection-files=%s\n' "${projection_files[*]:-none}"
echo "non-derived-lix-branch-ref-files=${#non_derived_projection[@]}"
printf '  non-derived=%s\n' "${non_derived_projection[*]:-none}"
echo "authority-use-lines=${#authority[@]}"
printf '  authority=%s\n' "${authority[*]:-none}"
for category in "${categories[@]}"; do
  echo "inventory-$category"
done

if ((${#missing[@]} || ${#legacy_hits[@]} || ${#old_paths[@]} || ${#authority[@]} || ${#non_derived_projection[@]})); then
  echo "RED BranchHead/BranchRef whole-closure deletion boundary"
  exit 1
fi
echo "GREEN BranchHead/BranchRef whole-closure deletion boundary"
