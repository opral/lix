#!/usr/bin/env bash
set -euo pipefail

B59_ROOT=${1:?usage: verify_calibration.sh B59_ROOT CANDIDATE_ROOT}
CANDIDATE_ROOT=${2:?usage: verify_calibration.sh B59_ROOT CANDIDATE_ROOT}
ANCHOR=b59e1f11a51153e0a787a81f0f25bf104d150aaf
CANDIDATE=9f45f77955317b8dd64fadb049d85c33ca109bf4
CONTRACT_DIR=$(cd "$(dirname "$0")" && pwd)
SCANNER="$CANDIDATE_ROOT/packages/lix/tests/branch_ref_whole_closure_oracle_b59/verify_branch_ref_whole_closure.sh"
NORMALIZER="$CONTRACT_DIR/normalize_branch_ref_scan.sh"
EXPECTED_NORMALIZED_SHA=026fcd6b7aaa9afd8341fdca6451962d4addd5aedef63724b6f90d50b8b573bb
EXPECTED_STDERR_SHA=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

[[ "$(git -C "$B59_ROOT" rev-parse HEAD)" == "$ANCHOR" ]]
[[ "$(git -C "$CANDIDATE_ROOT" rev-parse HEAD)" == "$CANDIDATE" ]]
[[ "$(git -C "$B59_ROOT" rev-parse HEAD^{tree})" == "700fd04d21bc40c05425c9fc9e10d65c9e1eda24" ]]
[[ "$(git -C "$CANDIDATE_ROOT" rev-parse HEAD^{tree})" == "c38c4d60c74bf70994378029ad9e286a83cf2d69" ]]
[[ -x "$SCANNER" || -f "$SCANNER" ]]

run_one() {
  local label=$1 root=$2 dir rc stdout_norm stderr_norm combined
  dir=$(mktemp -d "/tmp/branch-ref-calibration-${label}.XXXXXX")
  set +e
  bash "$SCANNER" "$root" "$ANCHOR" >"$dir/stdout" 2>"$dir/stderr"
  rc=$?
  set -e
  [[ "$rc" == 1 ]]
  bash "$NORMALIZER" "$dir/stdout" "$dir/stdout.normalized"
  bash "$NORMALIZER" "$dir/stderr" "$dir/stderr.normalized"
  {
    printf '%s\n' '[stdout]'
    cat "$dir/stdout.normalized"
    printf '%s\n' '[stderr]'
    cat "$dir/stderr.normalized"
  } >"$dir/normalized"
  [[ "$(sha256sum "$dir/normalized" | cut -d' ' -f1)" == "$EXPECTED_NORMALIZED_SHA" ]]
  [[ "$(sha256sum "$dir/stderr" | cut -d' ' -f1)" == "$EXPECTED_STDERR_SHA" ]]
  printf '%s raw-stdout-sha256=' "$label"
  sha256sum "$dir/stdout" | cut -d' ' -f1
  printf '%s raw-stderr-sha256=' "$label"
  sha256sum "$dir/stderr" | cut -d' ' -f1
  printf '%s normalized-sha256=' "$label"
  sha256sum "$dir/normalized" | cut -d' ' -f1
  rg -q '^required-missing=0$' "$dir/stdout"
  rg -q '^legacy-residue=460$' "$dir/stdout"
  rg -q '^old-closure-paths=4$' "$dir/stdout"
  rg -q '^lix-branch-ref-occurrence-files=15$' "$dir/stdout"
  rg -q '^non-derived-lix-branch-ref-files=4$' "$dir/stdout"
  rg -q '^authority-use-lines=331$' "$dir/stdout"
}

run_one b59 "$B59_ROOT"
run_one candidate "$CANDIDATE_ROOT"
echo 'canonical calibration: PASS'
