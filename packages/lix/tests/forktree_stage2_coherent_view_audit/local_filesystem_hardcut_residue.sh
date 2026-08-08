#!/usr/bin/env bash
set -euo pipefail

root=${1:?usage: local_filesystem_hardcut_residue.sh <checkout> [baseline|candidate]}
mode=${2:-candidate}
cd "$root"

search_paths=(packages/js-sdk packages/local-filesystem packages/rs-sdk-tests Cargo.toml)
findings=$(mktemp)
trap 'rm -f "$findings"' EXIT

record() {
  local label=$1
  local pattern=$2
  shift 2
  rg -n --hidden --glob '!target/**' --glob '!.git/**' "$pattern" "$@" \
    | sed "s#^#${label}\t#" >>"$findings" || true
}

# Removed public JS/type/binding surface.
record js_options 'LocalFilesystemOptions|lixDir|syncAllFiles' packages/js-sdk
record js_sync_api 'importPaths|syncDiskToLix|importFilesystemPaths' packages/js-sdk
record js_object_constructor 'constructor\(options: LocalFilesystemOptions\)|new LocalFilesystem\(\{' packages/js-sdk

# Removed public Rust/native surface. Private supervisor synchronization is allowed.
record rust_options 'LocalFilesystemOpenOptions|open_with_options|open_with_options_and_wasm_runtime' packages/local-filesystem packages/rs-sdk-tests packages/js-sdk/native
record rust_public_sync 'pub async fn (import_paths|sync_disk_to_lix)' packages/local-filesystem
record native_option_fields 'lix_dir:|sync_all_files:|lix_dir,|sync_all_files,' packages/js-sdk/native

sort -o "$findings" "$findings"
printf 'mode=%s\n' "$mode"
printf 'finding_count=%s\n' "$(wc -l <"$findings")"
sha256sum "$findings"
cat "$findings"

watcher_constructors=$( (rg -n 'new_debouncer_opt::<.*RecommendedWatcher' packages/local-filesystem/src || true) | wc -l)
supervisor_owners=$( (rg -n '^struct FilesystemSupervisorInner' packages/local-filesystem/src || true) | wc -l)
worker_owners=$( (rg -n 'lix-sdk-filesystem-sync' packages/local-filesystem/src || true) | wc -l)
js_watcher_owners=$( (rg -n 'chokidar|fs\.watch|watchFile|new (RecommendedWatcher|Debouncer)' packages/js-sdk/src packages/js-sdk/native 2>/dev/null || true) | wc -l)
printf 'watcher_constructors=%s\n' "$watcher_constructors"
printf 'supervisor_owners=%s\n' "$supervisor_owners"
printf 'worker_owners=%s\n' "$worker_owners"
printf 'js_watcher_owners=%s\n' "$js_watcher_owners"

if [[ "$mode" == candidate ]]; then
  [[ ! -s "$findings" ]]
  [[ "$watcher_constructors" == 1 ]]
  [[ "$supervisor_owners" == 1 ]]
  [[ "$worker_owners" == 1 ]]
  [[ "$js_watcher_owners" == 0 ]]
fi
