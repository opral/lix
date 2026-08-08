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

The candidate-side Cargo test must exercise the same contract as the model;
the frozen package itself deliberately does not wire a production or Cargo
target on compiler-red b59.

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

Each backend must verify one-view reads and one-commit writes for create,
switch, delete, undo, redo, checkpoint, and GC. It must measure no read-side
writes, exact selector CAS failures, cold reopen parity, final-reference
reclamation, and fail-closed malformed/cycle/missing-root cases. It must
separately assert that a forged or stale `lix_branch_ref` projection cannot
change the selected branch root. State fingerprints must include active
branch, histories, object set, live-object set, and in-flight allocation set;
reopen must reject a global selector epoch gap.
