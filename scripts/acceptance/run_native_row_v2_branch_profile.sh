#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <candidate-root> <output-directory>" >&2
  exit 2
fi

candidate_root=$(realpath "$1")
output_dir=$(mkdir -p "$2" && realpath "$2")
head=$(git -C "$candidate_root" rev-parse HEAD)
tree=$(git -C "$candidate_root" rev-parse 'HEAD^{tree}')

python3 "$candidate_root/scripts/acceptance/native_row_v2_acceptance.py" \
  --root "$candidate_root" --expect-head "$head" \
  --output "$output_dir/source-authority.json"

# These candidate-side controls inspect private selector/root/object identities.
# Their names are part of the v2 acceptance contract, not optional smoke tests.
for test_name in \
  native_row_v2_rejects_v1_transplant_truncation_and_graft \
  native_row_v2_branch_creation_shares_local_root_without_row_rewrite \
  native_row_v2_branch_sharing_survives_cold_reopen
do
  cargo test --manifest-path "$candidate_root/Cargo.toml" -p lix --lib --all-features \
    "$test_name" -- --exact --nocapture 2>&1 | tee "$output_dir/$test_name.log"
done

# Existing public benchmark reports per-space rows/key/value bytes and settled
# backend objects. branch_noop and branches_10 expose any O(N) row/pack rewrite.
for backend in rocksdb slatedb; do
  LIX_BRANCH_SHARING_ROWS=1000,10000,50000 \
  LIX_BRANCH_SHARING_SCENARIOS=branch_noop,branches_10 \
  LIX_BRANCH_SHARING_BACKENDS="$backend" \
  LIX_BRANCH_SHARING_SETTLE_MS=250 \
  cargo bench --manifest-path "$candidate_root/Cargo.toml" -p lix_e2e \
    --features storage-benches,slatedb --bench branch_storage_sharing 2>&1 \
    | tee "$output_dir/branch-sharing-$backend.log"
done

python3 - "$output_dir" "$head" "$tree" <<'PY'
import json, re, sys
from pathlib import Path

root, head, tree = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
records = []
for path in sorted(root.glob("branch-sharing-*.log")):
    for line in path.read_text().splitlines():
        if not line.startswith("branch_sharing,"):
            continue
        fields = dict(part.split("=", 1) for part in line.split(",")[1:] if "=" in part)
        records.append(fields)
if len(records) != 12:
    raise SystemExit(f"expected 12 branch profile rows, found {len(records)}")

failures = []
for backend in ("rocksdb", "slatedb"):
    for scenario in ("branch_noop", "branches_10"):
        cells = sorted(
            (r for r in records if r["backend"] == backend and r["scenario"] == scenario),
            key=lambda r: int(r["rows"]),
        )
        logical = [int(r["delta_logical_rows"]) for r in cells]
        objects = [int(r["after_physical_objects"]) - int(r["base_physical_objects"]) for r in cells]
        # Branch creation may add topology proportional to branch count, never N.
        if max(logical) - min(logical) != 0:
            failures.append(f"{backend}/{scenario}: logical row delta varies with N: {logical}")
        if max(objects) - min(objects) > (12 if scenario == "branch_noop" else 120):
            failures.append(f"{backend}/{scenario}: physical object delta varies with N: {objects}")

summary = {
    "head": head,
    "tree": tree,
    "verdict": "APPROVE" if not failures else "BLOCK",
    "contract": {
        "complexity": "branch creation O(1) in inherited state rows N",
        "measurements": "state/current-pack logical rewrites, settled objects, backend-derived bytes",
        "private_controls": "identical child/grandchild local roots and cold reopen",
    },
    "records": records,
    "failures": failures,
}
(root / "branch-profile-summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
print(json.dumps(summary, indent=2, sort_keys=True))
if failures:
    raise SystemExit(1)
PY
