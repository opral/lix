# ForkTree Stage-1 typed-owner application oracle

## Provenance and scope

- Production base: `138b55e1de90806c380ad27b2b349f4c66a1387f`
- Production base tree: `26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`
- Scope: test-only oracle and feature-gated test bridge for the unwired Stage-1 owner.
- Stage 2 remains absent. No SQL, Storage/StorageRead, adapter, scan, or production serving caller is changed.
- The obsolete `0876d74a763bee7935234a7b0095f435d05b3e54` oracle is excluded. This oracle does not name `OBJECT_SPACE`, invoke `discover_sweep_plan` or `apply_sweep_plan`, release a raw snapshot pin, construct a raw publication, or forge a sweep plan.

## Owner boundary

Fixture bootstrap is implemented inside the sealed ForkTree publication owner. It validates canonical object envelopes, object domains and IDs, selected state ownership, selector generations, branch-ref edges, commit/change catalog keys, and every catalog back-edge before an exact absent-selector atomic commit. The external harness receives only case names and typed result strings.

Reclamation uses only `advance_gc(GcBudget::default())`, including its persisted bounded mark/queue/continuation state and owner-produced sweep batches. Snapshot retirement uses the typed catalog-retirement operation. Persisted corruption is never introduced through a raw mutation API; the corruption case verifies authenticated bytes and then proves in-memory substitution, wrong-domain decoding, and selector corruption fail closed while the selected persisted graph remains valid.

## Deterministic cases

1. `state_catalog`: typed value/NULL/tombstone state cells; coherent global+branch point/range precedence; path-copy update; unified CommitCatalog/ChangeCatalog exact and ordered resume; view-bound cursor rejection; commit-member and branch-ref back-edges.
2. `upload_gc`: typed receipt tree and upload selector; completion moves receipt reachability to file-state reachability atomically; abort; bounded reclamation; cold reopen.
3. `shared_final`: two valid manifests share one authenticated chunk; first-reference retirement preserves it and final-reference retirement reclaims it.
4. `retained_races`: checkpoint/recovery/undo roots; typed catalog/root retirement; an old `StorageRead` retains the old root while new reads observe retirement; both GC/publication orders; stale publication rejection and retry; cold reopen.
5. `corruption`: content-hash, object-domain, and selector failures are fail-closed without exposing a raw persisted mutation capability.

## Local qualification

The following gates passed on this exact source before freezing:

```text
CARGO_TARGET_DIR=/root/repos/forktree-stage1-app-oracle-target CARGO_BUILD_JOBS=2 cargo check -p lix --features storage-benches --lib
CARGO_TARGET_DIR=/root/repos/forktree-stage1-app-oracle-target CARGO_BUILD_JOBS=2 cargo test -p lix --lib --features storage-benches typed_application_oracle_memory -- --nocapture --test-threads=1
CARGO_TARGET_DIR=/root/repos/forktree-stage1-app-oracle-target CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test forktree_stage1_application_oracle --no-run
CARGO_TARGET_DIR=/root/repos/forktree-stage1-app-oracle-target cargo test -p lix_tests --test forktree_stage1_application_oracle forktree_stage1_application_memory -- --exact --nocapture --test-threads=1
CARGO_TARGET_DIR=/root/repos/forktree-stage1-app-oracle-target CARGO_BUILD_JOBS=2 cargo clippy -p lix --features storage-benches --lib -- -D warnings
CARGO_TARGET_DIR=/root/repos/forktree-stage1-app-oracle-target CARGO_BUILD_JOBS=2 cargo clippy -p lix_tests --test forktree_stage1_application_oracle -- -D warnings
cargo test -p lix forktree::tests --lib -j2 -- --test-threads=1
cargo test -p lix --test integration sealed_owner_violations_are_empty -- --nocapture
cargo fmt --all -- --check
git diff --check
```

Results: Memory application oracle 5/5 cases with reopen passed; ForkTree owner tests 24/24 passed; sealed-owner test passed; the external RocksDB/SlateDB harness compiled; canonical warnings-denied Clippy, formatting, and diff checks passed. RocksDB and SlateDB runtime execution is intentionally delegated to independent hosts and is not claimed by this local package.

## Independent bounded invocation

From a clean checkout of the immutable transport ref, use a fresh target directory:

```text
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test forktree_stage1_application_oracle --no-run
CARGO_TARGET_DIR=<isolated-target> cargo test -p lix_tests --test forktree_stage1_application_oracle forktree_stage1_application_rocksdb -- --exact --nocapture --test-threads=1
CARGO_TARGET_DIR=<isolated-target> cargo test -p lix_tests --test forktree_stage1_application_oracle forktree_stage1_application_slatedb -- --exact --nocapture --test-threads=1
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 cargo clippy -p lix --features storage-benches --lib -- -D warnings
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 cargo clippy -p lix_tests --test forktree_stage1_application_oracle -- -D warnings
cargo fmt --all -- --check
git diff --check
```

Each adapter test runs the five deterministic cases serially, drops the adapter, reopens the same database, and verifies the authenticated selected state and retained/final-release outcomes. No semantic adaptation is permitted during independent execution.
