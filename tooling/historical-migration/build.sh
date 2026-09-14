#!/usr/bin/env bash
set -euo pipefail
package_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
base_revision=6e1efebb3953e8e4e5945fd9a9269a69803970c8
checkout=${1:?Usage: build.sh NEW_ISOLATED_CHECKOUT [GIT_SOURCE]}
source_repo=${2:-$(git -C "$package_dir" rev-parse --show-toplevel)}
(cd "$package_dir" && sha256sum --check bridge.patch.sha256)
if [[ -e "$checkout" ]]; then
  echo 'Refusing existing checkout; use a new isolated directory.' >&2
  exit 1
fi
git clone --no-hardlinks --no-checkout "$source_repo" "$checkout"
git -C "$checkout" checkout --detach "$base_revision"
git -C "$checkout" apply --check "$package_dir/bridge.patch"
git -C "$checkout" apply "$package_dir/bridge.patch"
cargo build --locked --manifest-path "$checkout/Cargo.toml" -p lix-storage-slatedb --example historical_bridge
binary="${CARGO_TARGET_DIR:-$checkout/target}/debug/examples/historical_bridge"
sha256sum "$binary" > "$binary.sha256"
cut -d ' ' -f 1 "$package_dir/bridge.patch.sha256" > "$binary.patch-sha256"
printf 'Built pinned bridge in %s; invoke run.sh with its binary path.\n' "$checkout"
