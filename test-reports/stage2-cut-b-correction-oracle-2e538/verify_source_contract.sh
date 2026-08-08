#!/usr/bin/env bash
set -euo pipefail

repo_root=${1:?usage: verify_source_contract.sh REPO_ROOT BASE HEAD}
base=${2:?usage: verify_source_contract.sh REPO_ROOT BASE HEAD}
head=${3:?usage: verify_source_contract.sh REPO_ROOT BASE HEAD}

cd "$repo_root"
source_root=packages/lix/src
findings=0

say() { printf '%s\n' "$*"; }
fail() {
  findings=$((findings + 1))
  printf 'BLOCKER-%02d %s\n' "$findings" "$*"
}
pass() { printf 'PASS %s\n' "$*"; }

require_path() {
  local path=$1
  if [[ ! -f "$path" ]]; then
    fail "required source path is absent: $path"
  else
    pass "required source path exists: $path"
  fi
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

say "CUT_B_CORRECTION_ORACLE v1"
say "repo=$(git rev-parse --show-toplevel)"
say "base=$base"
say "head=$head"
say "head_tree=$(git rev-parse "$head^{tree}")"
say "anchor_expected=2e5389265d0495728325efe43d7eb6d9ad715aa0"

if ! git rev-parse --verify "$base^{commit}" >/dev/null 2>&1; then
  fail "base is not a commit: $base"
fi
if ! git rev-parse --verify "$head^{commit}" >/dev/null 2>&1; then
  fail "head is not a commit: $head"
fi

for path in \
  "$source_root/filesystem/read.rs" \
  "$source_root/plugin/registry.rs" \
  "$source_root/session/merge/branch.rs" \
  "$source_root/forktree/view.rs" \
  "$source_root/live_state/forktree_reader.rs" \
  "$source_root/tracked_state/context.rs"; do
  require_path "$path"
done

plugin_registry=$(span_until_marker "$source_root/plugin/registry.rs" \
  'pub(crate) async fn load_plugin_registry_at_commit' '/// Re-derives every WASM')
plugin_roots=$(span_until_marker "$source_root/plugin/registry.rs" \
  'pub(crate) async fn collect_gc_wasm_blob_roots' 'fn extend_registry_wasm_roots')
filesystem_roots=$(span_until_marker "$source_root/filesystem/read.rs" \
  'pub(crate) async fn collect_gc_binary_blob_roots' 'fn blob_id_from_snapshot')
filesystem_blob_helper=$(span_until_marker "$source_root/filesystem/read.rs" \
  'fn blob_id_from_snapshot' '#[derive(Debug, Clone)]')
facade=$(span_until_marker "$source_root/forktree/view.rs" \
  'pub(crate) struct ForkTreeReadFacade' 'pub(crate) async fn open_coherent_view')
raw_scan=$(span_until_marker "$source_root/live_state/forktree_reader.rs" \
  'pub(crate) async fn scan_branch' 'pub(crate) async fn scan_view')

say "-- historical plugin registry owner --"
if [[ -z "$plugin_registry" ]]; then
  fail "load_plugin_registry_at_commit function was not found"
else
  if contains "$plugin_registry" 'TrackedStateStoreReader'; then
    fail "historical plugin registry still accepts TrackedStateStoreReader"
  else
    pass "historical plugin registry does not mention TrackedStateStoreReader"
  fi
  if contains "$plugin_registry" 'load_projected_batch_at_commit'; then
    fail "historical plugin registry still uses projected tracked-state loading"
  else
    pass "historical plugin registry avoids projected tracked-state loading"
  fi
  if ! contains "$plugin_registry" 'ForkTreeReadFacade' &&
     ! contains "$plugin_registry" 'load_state_value_at_commit'; then
    fail "historical plugin registry has no typed ForkTree historical owner method"
  else
    pass "historical plugin registry names a typed ForkTree historical owner method"
  fi
fi

say "-- merge historical plugin callsites --"
if rg -n --fixed-strings 'load_plugin_registry_at_commit(reader' \
  "$source_root/session/merge/branch.rs"; then
  fail "merge historical plugin path passes TrackedStateStoreReader to the old loader"
else
  pass "merge historical plugin path has no old-reader loader call"
fi
if rg -n --fixed-strings 'reader.store()' \
  "$source_root/session/merge/branch.rs" "$source_root/plugin/registry.rs" "$source_root/filesystem/read.rs"; then
  fail "Cut B consumer extracts a raw storage reader through reader.store()"
else
  pass "Cut B consumers do not extract reader.store()"
fi

say "-- raw scan/view entry boundary --"
if [[ -n "$raw_scan" ]]; then
  fail "raw scan_branch(&S) remains an independently usable Cut B entry point"
else
  pass "raw scan_branch entry is absent"
fi
if rg -n --fixed-strings 'open_coherent_view_on_read(store' \
  "$source_root/live_state/forktree_reader.rs"; then
  fail "live-state reader still opens a view from an arbitrary raw store"
else
  pass "live-state reader has no raw store-to-view acquisition"
fi
if ! rg -n --fixed-strings 'scan_view' "$source_root/live_state/forktree_reader.rs" >/dev/null; then
  fail "typed scan_view entry is absent"
else
  pass "typed scan_view entry exists"
fi
if ! rg -n --fixed-strings 'scan_untracked_view' "$source_root/live_state/forktree_reader.rs" >/dev/null; then
  fail "typed scan_untracked_view entry is absent"
else
  pass "typed scan_untracked_view entry exists"
fi

say "-- opaque retained-read owner --"
if [[ -z "$facade" ]]; then
  fail "ForkTreeReadFacade is absent"
else
  if contains "$facade" 'read: &' || contains "$facade" 'from_retained_read(read:'; then
    fail "ForkTreeReadFacade can be constructed around arbitrary raw &R"
  else
    pass "ForkTreeReadFacade has no arbitrary raw &R constructor/field"
  fi
  if contains "$facade" 'open_coherent_view_on_read(self.read'; then
    fail "facade branch wrapper creates a detached view from a raw field"
  else
    pass "facade branch wrapper does not create a detached raw-read view"
  fi
  if contains "$facade" 'fn read(' || contains "$facade" 'fn into_read'; then
    fail "facade exposes/extracts its retained read"
  else
    pass "facade exposes no retained-read extraction"
  fi
  for method in \
    'load_commit_member_records' \
    'load_state_value_at_commit' \
    'load_json_slot' \
    'branch'; do
    if contains "$facade" "$method"; then
      pass "facade has typed operation: $method"
    else
      fail "facade is missing typed operation: $method"
    fi
  done
  if ! contains "$facade" 'CoherentView'; then
    fail "facade is not visibly bound to CoherentView ownership"
  else
    pass "facade is visibly bound to CoherentView ownership"
  fi
fi

say "-- filesystem and plugin GC root consumers --"
for label in "filesystem binary roots" "plugin WASM roots"; do
  if [[ "$label" == "filesystem binary roots" ]]; then
    span=$filesystem_roots
  else
    span=$plugin_roots
  fi
  if [[ -z "$span" ]]; then
    fail "$label collector was not found"
    continue
  fi
  if contains "$span" 'owner: &O' || contains "$span" 'from_retained_read(owner)'; then
    fail "$label collector accepts/builds a facade from arbitrary raw owner"
  else
    pass "$label collector has no raw-owner facade construction"
  fi
done
if contains "$filesystem_roots" 'load_commit_member_records' &&
   contains "$filesystem_roots" 'load_json_slot' &&
   contains "$filesystem_roots" 'ok_or_else'; then
  pass "filesystem retained roots require member records/JSON and fail closed"
else
  fail "filesystem retained roots do not prove typed missing-root failure"
fi
if contains "$plugin_roots" 'load_state_value_at_commit' &&
   contains "$plugin_roots" 'ok_or_else'; then
  pass "plugin retained roots require typed state and fail closed"
else
  fail "plugin retained roots do not prove typed missing-root failure"
fi
if contains "$plugin_roots" 'rows.is_empty()' &&
   (contains "$plugin_roots" 'bootstrap' || contains "$plugin_roots" 'explicit'); then
  pass "plugin current roots distinguish authenticated bootstrap-empty from missing registry"
else
  fail "plugin current roots allow zero registry rows to become unqualified empty success"
fi
if rg -n --fixed-strings 'unwrap_or_default' \
  "$source_root/filesystem/read.rs" "$source_root/plugin/registry.rs"; then
  fail "Cut B root consumer erases missing/corrupt historical state with unwrap_or_default"
else
  pass "Cut B root consumers do not use unwrap_or_default"
fi
if [[ -z "$filesystem_blob_helper" ]] ||
   ! contains "$filesystem_blob_helper" 'row' ||
   ! contains "$filesystem_blob_helper" 'snapshot.id'; then
  fail "filesystem BlobRef extraction authenticates blob_hash without binding semantic row identity"
else
  pass "filesystem BlobRef extraction binds JSON identity to the semantic row"
fi

say "-- deleted raw-reader accessor --"
if rg -n --fixed-strings 'pub(crate) fn store(&self)' \
  "$source_root/tracked_state/context.rs"; then
  fail "TrackedStateStoreReader raw store accessor remains"
else
  pass "TrackedStateStoreReader raw store accessor is absent"
fi

say "-- compatibility/authority guard --"
for forbidden in \
  'begin_write' \
  'PreparedPublication' \
  'stage_reclaimable_upload_receipts' \
  'StorageSpace::mutable' \
  'advance_gc'; do
  if git diff --unified=0 "$base" "$head" -- "$source_root" | grep -Fq "+$forbidden"; then
    fail "production diff adds forbidden authority/writer token: $forbidden"
  else
    pass "production diff does not add forbidden token: $forbidden"
  fi
done
if git diff --name-only "$base" "$head" -- "$source_root" | rg -n \
  '(^|/)(gc|reachability|publication|transaction|binary_cas|selector)(/|\.)' >/dev/null; then
  fail "production diff widens outside Cut B reader/facade paths"
else
  pass "production diff has no GC/writer/selector/CAS path"
fi

say "findings=$findings"
if (( findings != 0 )); then
  say "RESULT=RED"
  exit 1
fi
say "RESULT=GREEN"
