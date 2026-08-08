#!/usr/bin/env bash
set -u -o pipefail

root="$(git rev-parse --show-toplevel)"
src="$root/packages/lix/src"
fail=0

check_absent() {
  local label="$1"
  local pattern="$2"
  shift 2
  local scopes=("$@")
  local matches
  matches="$(rg -n --no-heading -g '*.rs' -e "$pattern" "${scopes[@]}" 2>/dev/null || true)"
  if [[ -n "$matches" ]]; then
    printf 'FAIL %s\n%s\n' "$label" "$(printf '%s\n' "$matches" | head -12)"
    fail=1
  else
    printf 'PASS %s\n' "$label"
  fi
}

check_paths_absent() {
  local label="$1"
  shift
  local path
  local found=0
  for path in "$@"; do
    if [[ -e "$root/$path" ]]; then
      printf 'FAIL %s: %s exists\n' "$label" "$path"
      found=1
    fi
  done
  if [[ "$found" -eq 0 ]]; then
    printf 'PASS %s\n' "$label"
  else
    fail=1
  fi
}

check_present() {
  local label="$1"
  local pattern="$2"
  local scope="$3"
  if rg -n --no-heading -g '*.rs' -e "$pattern" "$scope" >/dev/null 2>&1; then
    printf 'PASS %s\n' "$label"
  else
    printf 'FAIL %s: required pattern absent: %s\n' "$label" "$pattern"
    fail=1
  fi
}

printf 'Stage2 scalar SQL source gate\n'
printf 'HEAD=%s\n' "$(git rev-parse HEAD)"
printf 'TREE=%s\n' "$(git rev-parse 'HEAD^{tree}')"

check_paths_absent 'legacy columnar owner paths' \
  packages/lix/src/columnar_row_group.rs \
  packages/lix/src/live_state/entity_columnar.rs \
  packages/lix/src/live_state/entity_columnar_cache.rs \
  packages/lix/src/live_state/entity_decoded_column_cache.rs \
  packages/lix/src/sql2/entity_columnar_layout.rs

check_absent 'legacy columnar symbols' \
  'columnar_row_group|RowGroupManifest|RowGroupSetId|RowGroupScalar|EncodedRowGroupSet|load_row_group_|stage_row_group_|ROW_GROUP_|plan_entity_columnar_scan|load_entity_columnar_group|EntityColumnarOverlayRow|ColumnarBaseCoordinate|EntityColumnarScanLayout|EntityDecodedColumnCache|EntityColumnarShadowMaskCache' \
  "$src"

check_absent 'deleted current-layout owner symbols' \
  'BranchHeadControl(Context)?|tracked_head|current_state_envelope|commit_root_rebuild|scoped_range|tracked_state::(codec|storage|tree)' \
  "$src/live_state" "$src/sql2" "$src/transaction"

check_absent 'new scalar provider columnar branch' \
  'plan_entity_columnar_scan|entity_columnar|RowGroup|Columnar' \
  "$src/sql2/providers/entity.rs"

check_present 'ForkTree coherent view' \
  'struct CoherentView|open_coherent_view' \
  "$src/forktree"
check_present 'ForkTree point/range readers' \
  'state_point|state_range' \
  "$src/forktree"
check_present 'SQL provider canonical reader seam' \
  'CoherentView|state_point|state_range|open_coherent_view' \
  "$src/sql2/providers/entity.rs"

if [[ "$fail" -ne 0 ]]; then
  printf 'RESULT=RED\n'
  exit 1
fi
printf 'RESULT=GREEN\n'
