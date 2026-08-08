#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
parent=$(git -C "$root" rev-parse HEAD^)
test "$parent" = "1b8134f7bc02802c203853a3f71dbbee639b6932"
anchor=$(git -C "$root" merge-base HEAD b484e20d845aee3f8137bfa3496f9b3cd0e8cd35)
test "$anchor" = "b484e20d845aee3f8137bfa3496f9b3cd0e8cd35"
package_dir="$root/test-report/forktree-stage2-fd2-file-history-correction-oracle"
binding="$package_dir/ADDITIVE_CLOSURE_BINDING.md"
test -f "$binding"
grep -q '9cd14f684205f21f76f0504871fd00ed2d5eea07' "$binding"
grep -q '6f74dbaa54574e6e94dec6f758c1a6d2047225d7f7bfe31e1f50582f2426e832' "$binding"

changed=$(git -C "$root" diff --name-only HEAD^ HEAD)
while IFS= read -r path; do
  [ -z "$path" ] && continue
  case "$path" in
    test-report/forktree-stage2-fd2-file-history-correction-oracle/*) ;;
    *) echo "RED package path outside oracle directory: $path" >&2; exit 1 ;;
  esac
done <<EOF
$changed
EOF

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
