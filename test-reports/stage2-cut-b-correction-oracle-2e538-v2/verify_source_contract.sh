#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:?usage: verify_source_contract.sh REPO_ROOT BASE HEAD}
base=${2:?usage: verify_source_contract.sh REPO_ROOT BASE HEAD}
head=${3:?usage: verify_source_contract.sh REPO_ROOT BASE HEAD}
cd "$repo_root"

source_root=packages/lix/src
findings=0
say() { printf '%s\n' "$*"; }
pass() { printf 'PASS %s\n' "$*"; }
fail() {
  findings=$((findings + 1))
  printf 'BLOCKER-%02d %s\n' "$findings" "$*"
}
contains() {
  local haystack=$1 needle=$2
  grep -Fq -- "$needle" <<<"$haystack"
}
span_until_marker() {
  local path=$1 start=$2 end=$3
  awk -v start="$start" -v end="$end" '
    index($0, start) { active = 1 }
    active { print }
    active && NR > 1 && index($0, end) { exit }
  ' "$path"
}

say "CUT_B_CORRECTION_ORACLE v2"
say "repo=$(git rev-parse --show-toplevel)"
say "base=$base"
say "head=$head"
say "head_tree=$(git rev-parse "$head^{tree}")"
say "production_anchor=2e5389265d0495728325efe43d7eb6d9ad715aa0"
say "h4_report_sha256=a9a13f5f58410e779d8494f288f9dafbecf69f6e5a0c2c984b63f813c1a7eb7b"

for object in "$base^{commit}" "$head^{commit}"; do
  if ! git rev-parse --verify "$object" >/dev/null 2>&1; then
    fail "revision is not a commit: $object"
  fi
done

model=test-reports/stage2-cut-b-correction-oracle-2e538-v2/cut_b_discriminators.rs
if [[ -f "$model" ]]; then
  pass "standalone positive/negative discriminator model exists"
else
  fail "standalone discriminator model is absent"
fi
for method in \
  branch_descriptors_share_one_read_but_cursors_cannot_cross \
  missing_registry_is_not_bootstrap_empty \
  same_size_remapped_blob_ref_fails; do
  if rg -n --fixed-strings "$method" "$model" >/dev/null 2>&1; then
    pass "model contains discriminator: $method"
  else
    fail "model is missing discriminator: $method"
  fi
done

say "-- exact production path allowlist relative to 2e538 --"
allowed_paths=(
  "$source_root/filesystem/read.rs"
  "$source_root/forktree/mod.rs"
  "$source_root/forktree/view.rs"
  "$source_root/live_state/forktree_reader.rs"
  "$source_root/live_state/mod.rs"
  "$source_root/plugin/registry.rs"
  "$source_root/session/merge/branch.rs"
  "$source_root/tracked_state/context.rs"
)
is_allowed_path() {
  local candidate=$1 allowed
  for allowed in "${allowed_paths[@]}"; do
    [[ "$candidate" == "$allowed" ]] && return 0
  done
  return 1
}
changed_paths=$(git diff --name-only "$base" "$head" -- "$source_root")
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  if is_allowed_path "$path"; then
    pass "production path allowed: $path"
  else
    fail "production path outside exact Cut B allowlist: $path"
  fi
done <<< "$changed_paths"
if [[ -z "$changed_paths" ]]; then
  say "no production paths changed in this red anchor calibration"
fi
context_numstat=$(git diff --numstat "$base" "$head" -- "$source_root/tracked_state/context.rs")
if [[ -n "$context_numstat" ]]; then
  context_adds=${context_numstat%%$'\t'*}
  if [[ "$context_adds" == "0" || "$context_adds" == "-" ]]; then
    pass "tracked_state/context.rs changes are deletion-only"
  else
    fail "tracked_state/context.rs adds code; only deletion is allowed"
  fi
fi

production_diff=$(git diff --unified=0 "$base" "$head" -- "$source_root")
for forbidden in \
  begin_write PreparedPublication stage_reclaimable_upload_receipts advance_gc \
  StorageSpace::mutable transaction cache compat fallback migration; do
  if grep -Fq "+$forbidden" <<< "$production_diff"; then
    fail "production diff adds forbidden writer/authority token: $forbidden"
  else
    pass "production diff does not add forbidden token: $forbidden"
  fi
done
if git diff --name-only "$base" "$head" -- "$source_root" | rg -n \
  '(^|/)(gc|reachability|publication|transaction|binary_cas|storage|selector)(/|\.)' >/dev/null; then
  fail "production diff widens into forbidden GC/writer/selector/CAS/storage path"
else
  pass "production diff has no forbidden GC/writer/selector/CAS/storage path"
fi

for path in \
  "$source_root/filesystem/read.rs" \
  "$source_root/plugin/registry.rs" \
  "$source_root/session/merge/branch.rs" \
  "$source_root/forktree/view.rs" \
  "$source_root/live_state/forktree_reader.rs" \
  "$source_root/tracked_state/context.rs"; do
  if [[ -f "$path" ]]; then
    pass "required source path exists: $path"
  else
    fail "required source path is absent: $path"
  fi
done

plugin_registry=$(span_until_marker "$source_root/plugin/registry.rs" \
  'pub(crate) async fn load_plugin_registry_at_commit' '/// Re-derives every WASM')
plugin_roots=$(span_until_marker "$source_root/plugin/registry.rs" \
  'pub(crate) async fn collect_gc_wasm_blob_roots' 'fn extend_registry_wasm_roots')
filesystem_roots=$(span_until_marker "$source_root/filesystem/read.rs" \
  'pub(crate) async fn collect_gc_binary_blob_roots' 'fn blob_id_from_snapshot')
blob_helper=$(span_until_marker "$source_root/filesystem/read.rs" \
  'fn blob_id_from_snapshot' '#[derive(Debug, Clone)]')
facade=$(span_until_marker "$source_root/forktree/view.rs" \
  'pub(crate) struct ForkTreeReadFacade' 'pub(crate) async fn open_coherent_view')
branch_method=$(span_until_marker "$source_root/forktree/view.rs" \
  'pub(crate) async fn branch(' 'pub(crate) async fn load_commit_member_records')
raw_scan=$(span_until_marker "$source_root/live_state/forktree_reader.rs" \
  'pub(crate) async fn scan_branch' 'pub(crate) async fn scan_view')

say "-- historical plugin/merge ownership --"
if [[ -z "$plugin_registry" ]]; then
  fail "load_plugin_registry_at_commit function is absent"
else
  if contains "$plugin_registry" TrackedStateStoreReader; then
    fail "historical plugin loader accepts TrackedStateStoreReader"
  else
    pass "historical plugin loader does not accept TrackedStateStoreReader"
  fi
  if contains "$plugin_registry" load_projected_batch_at_commit; then
    fail "historical plugin loader uses projected tracked-state loading"
  else
    pass "historical plugin loader avoids projected tracked-state loading"
  fi
  if ! contains "$plugin_registry" ForkTreeReadFacade &&
     ! contains "$plugin_registry" load_state_value_at_commit; then
    fail "historical plugin loader has no typed ForkTree owner method"
  else
    pass "historical plugin loader names a typed ForkTree owner method"
  fi
fi
if rg -n --fixed-strings 'load_plugin_registry_at_commit(reader' \
  "$source_root/session/merge/branch.rs"; then
  fail "merge historical plugin path calls the old reader loader"
else
  pass "merge historical plugin path has no old reader loader call"
fi

say "-- raw scan/open boundary --"
if [[ -n "$raw_scan" ]]; then
  fail "raw scan_branch(&S) remains usable"
else
  pass "raw scan_branch entry is absent"
fi
if rg -n --fixed-strings 'open_coherent_view_on_read(store' \
  "$source_root/live_state/forktree_reader.rs"; then
  fail "live-state Cut B consumer opens a view from raw store"
else
  pass "live-state Cut B consumers have no raw store view acquisition"
fi
for method in scan_view scan_untracked_view; do
  if rg -n --fixed-strings "$method" "$source_root/live_state/forktree_reader.rs" >/dev/null; then
    pass "typed entry exists: $method"
  else
    fail "typed entry is absent: $method"
  fi
done

say "-- one opaque owner and cursor identity --"
if [[ -z "$facade" ]]; then
  fail "ForkTreeReadFacade is absent"
else
  if contains "$facade" 'read: &' || contains "$facade" 'from_retained_read(read:'; then
    fail "facade stores/accepts arbitrary raw &R"
  else
    pass "facade has no arbitrary raw &R field or constructor"
  fi
  if contains "$facade" CoherentView; then
    pass "facade is visibly bound to a retained CoherentView"
  else
    fail "facade is not visibly bound to a retained CoherentView"
  fi
  if contains "$facade" 'fn read(' || contains "$facade" 'fn into_read'; then
    fail "facade exposes/extracts retained read"
  else
    pass "facade exposes no retained read extraction"
  fi
  for method in branch load_commit_member_records load_state_value_at_commit load_json_slot; do
    if contains "$facade" "$method"; then
      pass "facade has typed operation: $method"
    else
      fail "facade lacks typed operation: $method"
    fi
  done
fi
if [[ -z "$branch_method" ]]; then
  fail "facade branch method is absent"
else
  for forbidden in begin_read refresh 'open_coherent_view_on_read(self' 'fn clone' replacement; do
    if contains "$branch_method" "$forbidden"; then
      fail "branch() contains forbidden refresh/replacement operation: $forbidden"
    else
      pass "branch() contains no forbidden operation: $forbidden"
    fi
  done
  if contains "$branch_method" view_id || contains "$branch_method" view_instance_id; then
    pass "branch descriptor carries a view/descriptor identity"
  else
    fail "branch descriptor has no visible cursor identity binding"
  fi
  if contains "$branch_method" InvalidCursor || contains "$branch_method" validate_resume_key; then
    pass "branch/cursor path has fail-closed identity validation"
  else
    fail "branch/cursor path lacks fail-closed identity validation"
  fi
fi

say "-- root ownership and fail-closed discriminators --"
for label in "filesystem binary roots" "plugin WASM roots"; do
  if [[ "$label" == "filesystem binary roots" ]]; then
    span=$filesystem_roots
  else
    span=$plugin_roots
  fi
  if contains "$span" 'owner: &O' || contains "$span" 'from_retained_read(owner)'; then
    fail "$label accepts/builds a facade from arbitrary raw owner"
  else
    pass "$label has no raw-owner facade construction"
  fi
done
if contains "$filesystem_roots" load_commit_member_records &&
   contains "$filesystem_roots" load_json_slot &&
   contains "$filesystem_roots" ok_or_else; then
  pass "filesystem retained roots fail closed on missing members/JSON"
else
  fail "filesystem retained roots lack typed missing-root failure"
fi
if contains "$plugin_roots" load_state_value_at_commit &&
   contains "$plugin_roots" ok_or_else; then
  pass "plugin retained roots fail closed on missing state"
else
  fail "plugin retained roots lack typed missing-state failure"
fi
if contains "$plugin_roots" rows.is_empty &&
   (contains "$plugin_roots" bootstrap || contains "$plugin_roots" explicit); then
  pass "plugin roots distinguish explicit bootstrap-empty from missing registry"
else
  fail "plugin roots allow absent selected registry to become empty success"
fi
if rg -n --fixed-strings unwrap_or_default \
  "$source_root/filesystem/read.rs" "$source_root/plugin/registry.rs"; then
  fail "root consumer erases missing/corrupt historical state with unwrap_or_default"
else
  pass "root consumers do not use unwrap_or_default"
fi
if [[ -z "$blob_helper" ]] ||
   ! contains "$blob_helper" row ||
   ! contains "$blob_helper" snapshot.id ||
   ! contains "$blob_helper" blob_hash ||
   ! contains "$blob_helper" size; then
  fail "BlobRef helper does not bind row identity, blob_hash, and size"
else
  pass "BlobRef helper binds row identity, blob_hash, and size"
fi

say "-- raw reader accessor --"
if rg -n --fixed-strings 'pub(crate) fn store(&self)' \
  "$source_root/tracked_state/context.rs"; then
  fail "TrackedStateStoreReader raw store accessor remains"
else
  pass "TrackedStateStoreReader raw store accessor is absent"
fi

say "findings=$findings"
if (( findings != 0 )); then
  say "RESULT=RED"
  exit 1
fi
say "RESULT=GREEN"
