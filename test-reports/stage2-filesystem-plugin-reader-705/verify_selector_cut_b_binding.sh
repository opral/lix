#!/usr/bin/env bash
set -euo pipefail

# Report-only provenance/path oracle. It never compiles, opens an adapter,
# invokes a reader, writes storage, or mutates production. It verifies that a
# successor remains an artifact-only child of the exact Cut B package.
root="${SELECTOR_CUT_B_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$root"

cut_b="c80bafbed5545b7768ac3a8dd4ed2ee9d3dacef4"
cut_b_parent="705440f55eccba9e2d55c0951d6a684737005d76"
cut_b_tree="18913425e9ce29b1c821837e04339458e200d397"
artifact_prefix="test-reports/stage2-filesystem-plugin-reader-705/"
fail=0

if [[ "$(git rev-parse --verify "${cut_b}^{commit}")" != "$cut_b" ]]; then
  echo "binding: exact Cut B head is unavailable" >&2
  fail=1
fi
if [[ "$(git rev-parse --verify "${cut_b}^{tree}")" != "$cut_b_tree" ]]; then
  echo "binding: exact Cut B tree mismatch" >&2
  fail=1
fi
if [[ "$(git rev-parse --verify "${cut_b}^1")" != "$cut_b_parent" ]]; then
  echo "binding: exact Cut B parent mismatch" >&2
  fail=1
fi

while IFS= read -r path; do
  [[ -n "$path" ]] || continue
  case "$path" in
    "${artifact_prefix}SELECTOR_INVENTORY_BINDING.md"|\
    "${artifact_prefix}SELECTOR_READINESS_BINDING.json"|\
    "${artifact_prefix}verify_selector_cut_b_binding.sh")
      ;;
    *)
      echo "binding: successor changed pre-existing Cut B content or production: $path" >&2
      fail=1
      ;;
  esac
done < <(git diff --name-only --diff-filter=ACMRTUXB "$cut_b" HEAD)

if rg -n --glob '*.rs' 'GlobalSelectorV1|BranchSelectorV1|SELECTOR_SPACE' \
  packages/lix/src/filesystem packages/lix/src/plugin \
  packages/lix/src/sql2/providers/file.rs packages/lix/src/session/merge/branch.rs \
  >/dev/null 2>&1; then
  echo "binding: selector implementation text found in Cut B reader surface" >&2
  fail=1
fi

for required in \
  'ff784043429f563fb01a29c42eecc90a939f7ce8ac7926d9db07a0f13313da24' \
  'c80bafbed5545b7768ac3a8dd4ed2ee9d3dacef4' \
  '7f467eb3192c8964c9f25f62ff1a2cd78b280dc3'; do
  if ! rg -q --fixed-strings "$required" \
    "${artifact_prefix}SELECTOR_INVENTORY_BINDING.md" \
    "${artifact_prefix}SELECTOR_READINESS_BINDING.json"; then
    echo "binding: missing immutable identity $required" >&2
    fail=1
  fi
done

if (( fail != 0 )); then
  echo "selector/Cut B binding: FAIL" >&2
  exit 1
fi
echo "selector/Cut B binding: PASS (artifact-only successor; no production/runtime)"
