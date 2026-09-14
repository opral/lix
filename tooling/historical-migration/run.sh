#!/usr/bin/env bash
set -euo pipefail
package_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
(cd "$package_dir" && sha256sum --check bridge.patch.sha256 >&2)
binary=${1:?Usage: run.sh PINNED_BINARY COPIED_PHYSICAL_DIR REPORT --isolated-copy [--verify-only]}
shift
export LIX_HISTORICAL_PATCH_SHA256
LIX_HISTORICAL_PATCH_SHA256=$(cut -d ' ' -f 1 "$package_dir/bridge.patch.sha256")
test "$(cat "$binary.patch-sha256")" = "$LIX_HISTORICAL_PATCH_SHA256"
sha256sum --check "$binary.sha256" >&2
exec "$binary" "$@"
