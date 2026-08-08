# Dormant first-runnable readiness commands

These are recipes only. Cargo and adapter commands remain unrun for this
report-only package. Each cell is independently isolated and hard-capped at
1200 seconds; stop on the first compile, source, semantic, corruption, or
authority failure.

## Static gates

~~~sh
bash test-report/branch-ref-production-readiness-e1af/verify_readiness_source.sh \
  <base-root> <base-commit> <candidate-root> <candidate-commit>
cargo fmt --all -- --check
git diff --check <base-commit> <candidate-commit>
~~~

The four identities are mandatory. The verifier checks both commit/tree
identities, ancestry, selector-specific normalized deltas, complete legacy
path deletion, and the approved v4 tree. It must be run against the exact
candidate commit, not a mutable worktree or the e1af calibration alone.

The candidate source gate must be GREEN only after all legacy symbols/spaces,
flat-row writers/projections, caches, fallbacks, and second authorities are
compiler-deleted. It also rejects `lix_branch_ref` outside the derived-only
allowlist and tests/benchmarks. No allowlist suppression changes the authority
contract.

## Standalone model (not production)

This model-only gate is intentionally separate from Cargo and adapter runtime:

```sh
rustc --edition=2021 --test -D warnings \
  test-report/branch-ref-production-readiness-e1af/branch_ref_selector_readiness_model.rs \
  -o /tmp/branch-ref-selector-readiness-model
/tmp/branch-ref-selector-readiness-model --nocapture --test-threads=1
```

The package records a standalone warnings-denied model compile/run and makes
no production or adapter runtime claim. The model's seven tests cover
one-read/one-commit authority,
create/switch/advance/delete/retire/cold reopen, stale versus unrelated
owners, malformed key/root/cycle/epoch, no fallback/dual authority, retained
view GC, and empty undo/redo no-ops.

## Adapter order

Use the same focused fixture and exact semantic target in this order:
Memory, then RocksDB, then SlateDB. A failure stops the sequence.

~~~sh
CARGO_TARGET_DIR=<memory-target> CARGO_BUILD_JOBS=2 \
  timeout 1200 cargo test -p lix --lib <branch_ref_readiness_test> -- \
  --exact --nocapture --test-threads=1

CARGO_TARGET_DIR=<rocks-target> CARGO_BUILD_JOBS=2 \
  timeout 1200 cargo test -p lix --features storage-benches --lib \
  <branch_ref_readiness_test> -- --exact --nocapture --test-threads=1

CARGO_TARGET_DIR=<slate-target> CARGO_BUILD_JOBS=2 \
  timeout 1200 cargo test -p lix --features 'storage-benches slatedb' --lib \
  <branch_ref_readiness_test> -- --exact --nocapture --test-threads=1
~~~

The focused target must prove one read/view and one publication/commit for
create/switch/advance/delete/retire/checkpoint/GC, stale versus unrelated
owners, rollback/savepoint/idempotency, cold reopen/corruption, retained-root
GC, and unsupported zero-write cohorts. Measure no broad matrix until the
focused correctness gate is green.
