#!/usr/bin/env bash
set -euo pipefail

root=${1:-.}
expected_head=${2:-}
target_dir=${CARGO_TARGET_DIR:-/tmp/lix-no-json-current-state-gate-target}

args=(--root "$root")
if [[ -n "$expected_head" ]]; then
  args+=(--expect-head "$expected_head")
fi

python3 "$root/scripts/acceptance/no_json_current_state_gate.py" "${args[@]}"

env CARGO_TARGET_DIR="$target_dir" \
  cargo check --manifest-path "$root/Cargo.toml" --workspace --all-targets --all-features
env CARGO_TARGET_DIR="$target_dir" \
  cargo test --manifest-path "$root/Cargo.toml" -p lix --test schema_v1_public_smoke \
  --all-features -- --nocapture
env CARGO_TARGET_DIR="$target_dir" \
  cargo test --manifest-path "$root/Cargo.toml" -p lix --lib --all-features \
  immutable_objects_and_typed_state_codecs_fail_closed -- --nocapture
env CARGO_TARGET_DIR="$target_dir" \
  cargo test --manifest-path "$root/Cargo.toml" -p lix --lib --all-features \
  current_state_pack_round_trips_and_rejects_identity_substitution -- --nocapture
env CARGO_TARGET_DIR="$target_dir" \
  cargo test --manifest-path "$root/Cargo.toml" -p lix_e2e \
  --test no_json_current_state_acceptance --features 'storage-benches slatedb' -- --nocapture
