#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
parent=$(git -C "$root" rev-parse HEAD^)
test "$parent" = "4b385e4fcca973d5a6ea9e344164c63e790ac2c0"
anchor=$(git -C "$root" merge-base HEAD b484e20d845aee3f8137bfa3496f9b3cd0e8cd35)
test "$anchor" = "b484e20d845aee3f8137bfa3496f9b3cd0e8cd35"
package_dir="$root/test-report/forktree-stage2-fd2-file-history-correction-oracle"

PYTHONDONTWRITEBYTECODE=1 python3 "$root/test-report/forktree-stage2-fd2-file-history-correction-oracle/oracle.py" \
  --root "$root" --mode all

git -C "$root" diff --check HEAD^ HEAD

(cd "$package_dir" && sha256sum -c SHA256SUMS)

fmt_log=$(mktemp)
trap 'rm -f "$fmt_log"' EXIT
set +e
cargo fmt --all -- --check >"$fmt_log" 2>&1
fmt_status=$?
set -e
test "$fmt_status" -ne 0
printf 'PASS b484_fmt_red exit=%s\n' "$fmt_status"
printf 'fmt_red_sha256 '
sha256sum "$fmt_log" | awk '{print $1}'
