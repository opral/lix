#!/usr/bin/env bash
set -euo pipefail

# Normalize one scanner stream. The transformation is intentionally narrow:
# no sorting, count rewriting, path-list filtering, or stdout/stderr merge.
input=${1:?usage: normalize_branch_ref_scan.sh INPUT OUTPUT}
output=${2:?usage: normalize_branch_ref_scan.sh INPUT OUTPUT}

LC_ALL=C sed \
  -e 's/\r$//' \
  -e 's/[[:blank:]]\+$//' \
  -e 's#^branch-ref-whole-closure root=.*$#branch-ref-whole-closure root=<ROOT>#' \
  "$input" > "$output"
