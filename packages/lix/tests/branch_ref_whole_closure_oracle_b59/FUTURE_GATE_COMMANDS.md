# Dormant future commands

These commands are acceptance instructions for a compile-green successor.
They are not run against b59. Use isolated target/evidence directories and a
fresh adapter path per cell.

```sh
set -euo pipefail
ROOT=/path/to/compile-green-successor
ANCHOR=b59e1f11a51153e0a787a81f0f25bf104d150aaf
ORACLE="$ROOT/packages/lix/tests/branch_ref_whole_closure_oracle_b59"
TARGET=/root/repos/target-branch-ref-whole-closure
EVIDENCE=/root/repos/evidence-branch-ref-whole-closure
mkdir -p "$EVIDENCE"

bash "$ORACLE/verify_branch_ref_whole_closure.sh" "$ROOT" "$ANCHOR" \
  >"$EVIDENCE/source.log" 2>&1
rustc --edition=2021 --test -D warnings \
  "$ORACLE/branch_ref_whole_closure_model.rs" \
  -o "$EVIDENCE/model"
"$EVIDENCE/model" --nocapture --test-threads=1 \
  >"$EVIDENCE/model.log" 2>&1

cargo fmt --all -- --check
git -C "$ROOT" diff --check
CARGO_TARGET_DIR="$TARGET" CARGO_BUILD_JOBS=2 \
  cargo clippy -p lix_benchmarks --test branch_ref_whole_closure_oracle \
  --features 'storage-benches slatedb' -- -D warnings
CARGO_TARGET_DIR="$TARGET" CARGO_BUILD_JOBS=2 \
  cargo test -p lix_benchmarks --test branch_ref_whole_closure_oracle \
  --features 'storage-benches slatedb' --no-run
```

The candidate-side Cargo test must exercise the same contract as the corrected
model; the frozen package itself deliberately does not wire a production or
Cargo target on compiler-red b59.

After compile/no-run green, run one backend cell at a time, in this order:

```sh
for backend in memory rocksdb slatedb; do
  CARGO_TARGET_DIR="$TARGET" CARGO_BUILD_JOBS=2 \
    timeout 1200 cargo test -p lix_benchmarks \
    --test branch_ref_whole_closure_oracle \
    --features 'storage-benches slatedb' -- \
    "${backend}_branch_ref_whole_closure" --exact --nocapture \
    --test-threads=1
done
```

Each backend must verify one retained-view read and one-commit writes for
create, switch, advance, delete, retire, undo, redo, checkpoint, and GC. For
create, the retained read must be acquired before publication and its exact
nonzero read ID must be accepted; zero, released, or root-mismatched read IDs
must fail before a backend write. It
must measure no read-side writes, exact selector CAS failures, and distinguish
same-owner stale CAS from unrelated-owner CAS without a backend write. Every
result must include the authenticated global/branch root, generations,
canonical selector bytes, catalog root, and owner identities. The active read
and cold reopen must require the catalog-root object to exist and authenticate
its canonical object ID, `selector_catalog` kind, and `selector:global`
back-edge; missing, substituted, wrong-kind, and wrong-back-edge catalog
objects must fail closed. Cold reopen must preserve those fingerprints and
reject a global selector epoch gap, same-size selector substitutions, wrong
owner, wrong catalog, missing root, and cycles.
It must separately assert that a forged or stale `lix_branch_ref` projection
cannot change the selected branch root and that a second-authority publication
is rejected before any selector rotation. State fingerprints must include
active branch, histories, object set, live-object set, in-flight allocations,
and per-branch selector fingerprints.
