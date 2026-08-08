#!/usr/bin/env bash
set -u -o pipefail

# Test/report-only W0 probe runner. It deliberately runs against the actual
# package sources; it never edits the repository and does not start an adapter.
ROOT=${1:?usage: $0 <repo-root> [target-dir]}
TARGET=${2:-"$(mktemp -d /tmp/lix-w0-probes.XXXXXX)"}
TIMEOUT_SECONDS=${W0_TIMEOUT_SECONDS:-1200}
PROBES="$ROOT/packages/engine-benchmarks/tests/forktree_w0_compile_probes"
WORK="$TARGET/work"
mkdir -p "$WORK"
status=0

run_success() {
  local label=$1
  shift
  echo "PROBE $label EXPECT_SUCCESS"
  if timeout "${TIMEOUT_SECONDS}s" "$@"; then
    echo "PROBE $label PASS"
  else
    echo "PROBE $label RED: expected success did not compile"
    status=1
  fi
}

run_failure() {
  local label=$1
  shift
  echo "PROBE $label EXPECT_FAILURE"
  if timeout "${TIMEOUT_SECONDS}s" "$@"; then
    echo "PROBE $label RED: forbidden API compiled"
    status=1
  else
    echo "PROBE $label PASS: forbidden API rejected"
  fi
}

make_probe_crate() {
  local name=$1
  local source=$2
  local dir="$WORK/$name"
  mkdir -p "$dir/src"
  cp "$source" "$dir/src/main.rs"
  {
    echo '[package]'
    echo "name = \"w0_$name\""
    echo 'version = "0.0.0"'
    echo 'edition = "2024"'
    echo '[dependencies]'
    printf 'lix = { path = "%s/packages/lix" }\n' "$ROOT"
  } >"$dir/Cargo.toml"
  printf '%s\n' "$dir/Cargo.toml"
}

POSITIVE_MANIFEST=$(make_probe_crate positive_descriptor "$PROBES/positive_descriptor.rs")
run_success rust-positive-descriptor \
  env CARGO_TARGET_DIR="$TARGET/cargo-positive" \
  cargo check --manifest-path "$POSITIVE_MANIFEST" --quiet

run_success rust-positive-oracle \
  env CARGO_TARGET_DIR="$TARGET/cargo-oracle" \
  cargo test --manifest-path "$ROOT/packages/engine-benchmarks/Cargo.toml" \
    --test forktree_w0_storage_boundary_oracle --no-run --quiet

for probe in negative_raw_space negative_columnar_owner negative_tracked_changelog negative_binary_cas_owner; do
  manifest=$(make_probe_crate "$probe" "$PROBES/$probe.rs")
  run_failure "rust-$probe" \
    env CARGO_TARGET_DIR="$TARGET/cargo-negative-$probe" \
    cargo check --manifest-path "$manifest" --quiet
done

TSC_BIN=${TSC:-$(command -v tsc || true)}
if [[ -z "$TSC_BIN" ]]; then
  echo "PROBE ts-native UNAVAILABLE: tsc not found"
  status=1
else
  run_failure ts-native \
    "$TSC_BIN" --noEmit --strict --module NodeNext --moduleResolution NodeNext \
    --target ES2024 --skipLibCheck "$PROBES/negative_native_exports.ts"
fi

echo "PROBE native-rust-exports EXPECT_ABSENT"
if rg -n --glob '*.rs' \
  'openLocalFilesystem|importFilesystemPaths|syncDiskToLix|LocalFilesystemOpenOptions' \
  "$ROOT/packages/js-sdk/native"; then
  echo "PROBE native-rust-exports RED: removed native exports remain"
  status=1
else
  echo "PROBE native-rust-exports PASS"
fi

exit "$status"
