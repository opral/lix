#!/usr/bin/env bash
set -u -o pipefail

# Test/report-only W0 probe runner. It deliberately runs against the actual
# package sources; it never edits the repository and does not start an adapter.
ROOT=${1:?usage: $0 <repo-root> [target-dir]}
TARGET=${2:-"$(mktemp -d /tmp/lix-w0-probes.XXXXXX)"}
TIMEOUT_SECONDS=${W0_TIMEOUT_SECONDS:-1200}
PROBES="$ROOT/packages/engine-benchmarks/tests/forktree_w0_compile_probes"
WORK="$TARGET/work"
LOGS="$TARGET/logs"
mkdir -p "$WORK"
mkdir -p "$LOGS"
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
  local expected_code=$2
  local expected_token=$3
  shift 3
  echo "PROBE $label EXPECT_FAILURE"
  local log="$LOGS/$label.log"
  if timeout "${TIMEOUT_SECONDS}s" "$@" >"$log" 2>&1; then
    echo "PROBE $label RED: forbidden API compiled"
    status=1
  elif ! rg -q -- "$expected_code" "$log"; then
    echo "PROBE $label RED: exit was nonzero but expected diagnostics were absent"
    echo "PROBE $label LOG_SHA256=$(sha256sum "$log" | cut -d' ' -f1)"
    status=1
  else
    local token
    local missing_token=0
    IFS=',' read -r -a expected_tokens <<< "$expected_token"
    for token in "${expected_tokens[@]}"; do
      if ! rg -F -q -- "$token" "$log"; then
        missing_token=1
        break
      fi
    done
    if ((missing_token)); then
      echo "PROBE $label RED: expected diagnostic token set was absent"
      echo "PROBE $label LOG_SHA256=$(sha256sum "$log" | cut -d' ' -f1)"
      status=1
    else
      echo "PROBE $label PASS: diagnostic $expected_code/$expected_token"
    fi
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
    printf 'lix = { path = "%s/packages/lix", features = ["storage-benches"] }\n' "$ROOT"
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

run_failure rust-negative-raw-space E0423 SpaceId \
  env CARGO_TARGET_DIR="$TARGET/cargo-negative-raw-space" \
  cargo check --manifest-path "$(make_probe_crate negative_raw_space "$PROBES/negative_raw_space.rs")" --quiet

run_failure rust-negative-columnar-owner E0599 load_columnar_row_group \
  env CARGO_TARGET_DIR="$TARGET/cargo-negative-columnar-owner" \
  cargo check --manifest-path "$(make_probe_crate negative_columnar_owner "$PROBES/negative_columnar_owner.rs")" --quiet

run_failure rust-negative-tracked-changelog E0599 load_commit_state_manifest,load_tracked_state,load_branch_head_control \
  env CARGO_TARGET_DIR="$TARGET/cargo-negative-tracked-changelog" \
  cargo check --manifest-path "$(make_probe_crate negative_tracked_changelog "$PROBES/negative_tracked_changelog.rs")" --quiet

run_failure rust-negative-binary-cas-owner E0599 load_binary_cas_manifest \
  env CARGO_TARGET_DIR="$TARGET/cargo-negative-binary-cas-owner" \
  cargo check --manifest-path "$(make_probe_crate negative_binary_cas_owner "$PROBES/negative_binary_cas_owner.rs")" --quiet

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
