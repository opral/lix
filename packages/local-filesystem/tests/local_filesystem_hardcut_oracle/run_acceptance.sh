#!/usr/bin/env bash
set -euo pipefail

repo=${1:?usage: run_acceptance.sh REPO CARGO_TARGET_DIR}
target=${2:?usage: run_acceptance.sh REPO CARGO_TARGET_DIR}
oracle="$repo/packages/local-filesystem/tests/local_filesystem_hardcut_oracle"

python3 "$oracle/source_gate.py" "$repo" candidate

env CARGO_TARGET_DIR="$target" CARGO_BUILD_JOBS=2 RUSTFLAGS='-D warnings' \
  cargo build --manifest-path "$repo/Cargo.toml" -p lix-storage-filesystem
rlib=$(find "$target/debug/deps" -maxdepth 1 -type f -name 'liblix_storage_filesystem-*.rlib' -printf '%T@ %p\n' \
  | sort -nr | head -1 | cut -d' ' -f2-)
test -n "$rlib"

rustc --edition=2024 -D warnings \
  -L "dependency=$target/debug/deps" \
  --extern "lix_storage_filesystem=$rlib" \
  "$oracle/positive.rs" -o "$target/local-filesystem-hardcut-positive"

negative_stderr=$(mktemp)
trap 'rm -f "$negative_stderr"' EXIT
if rustc --edition=2024 -D warnings \
  -L "dependency=$target/debug/deps" \
  --extern "lix_storage_filesystem=$rlib" \
  "$oracle/negative.rs" -o "$target/local-filesystem-hardcut-negative" \
  2>"$negative_stderr"
then
  echo 'removed Rust LocalFilesystem API unexpectedly compiled' >&2
  exit 1
fi
for symbol in \
  LocalFilesystemOpenOptions \
  open_with_options \
  open_with_options_and_wasm_runtime \
  import_paths \
  sync_disk_to_lix
do
  rg -q "$symbol" "$negative_stderr"
done

env CARGO_TARGET_DIR="$target" CARGO_BUILD_JOBS=2 RUSTFLAGS='-D warnings' \
  cargo test --manifest-path "$repo/Cargo.toml" -p lix-storage-filesystem \
  --test local_filesystem_hardcut_oracle -- --nocapture --test-threads=1

test -d "$repo/packages/js-sdk/node_modules"
npm --prefix "$repo/packages/js-sdk" run typecheck
npm --prefix "$repo/packages/js-sdk" run build:native
npm --prefix "$repo/packages/js-sdk" run build:ts
npm --prefix "$repo/packages/js-sdk" exec -- \
  vitest run src/local-filesystem-hardcut-oracle.test.ts

cargo fmt --manifest-path "$repo/Cargo.toml" --all -- --check
git -C "$repo" diff --check
