# BranchHeadControl semantic oracle — future execution contract

Status: frozen TEST/REPORT-ONLY. These commands are intentionally not run on
b59. No adapter target exists on the b59 compiler-red frontier; execution is
admitted only after the immutable successor compiles.

All cells are capped at 20 minutes. Use /root/projects, never /tmp, for
database/object-store state and raw JSON evidence.

## Static/source gate

    node /root/repos/lix-evidence/branch-head-control-b59-oracle/verify_branch_head_control_residue.mjs \
      --root "$CANDIDATE_CHECKOUT"

The command must exit 0. It must report zero legacy control/ref/cache symbols
and retain the authenticated GlobalSelectorV1/BranchSelectorV1 and
PreparedPublication surface. The exact b59 control is expected to exit 1.

## Pure model gate

    set -eu
    model_target=/root/projects/branch-head-control-b59-oracle/model-test
    rustc --edition=2021 --test -D warnings \
      /root/repos/lix-evidence/branch-head-control-b59-oracle/branch_head_control_model.rs \
      -o "$model_target"
    timeout 20m "$model_target" --exact tests::init_create_switch_delete_and_owner_conflicts_are_atomic
    timeout 20m "$model_target" --exact tests::undo_redo_checkpoint_and_sequence_are_deterministic
    timeout 20m "$model_target" --exact tests::gc_and_publication_both_orders_preserve_roots_and_reject_stale_gc
    timeout 20m "$model_target" --exact tests::malformed_missing_and_wrong_kind_selectors_fail_closed

The model is pure and dependency-free. Its sequence is an observational
monotonic publication order derived from successful selector publications,
not a second persisted authority or a requested format field.

## Adapter gate

The future test-only target name is forktree_branch_head_control_acceptance.
It must expose one identical scenario set for all adapters and emit canonical
JSON records with semantic digest, selector generations, outcomes, and backend
counters. Do not run this target until the package no-run and warnings-denied
Clippy checks pass on the exact immutable successor.

### Memory

    timeout 20m env CARGO_TARGET_DIR=/root/projects/branch-head-control-memory-target \
      CARGO_BUILD_JOBS=2 cargo test -p lix_benchmarks \
      --test forktree_branch_head_control_acceptance \
      --features storage-benches -- --exact memory_all_scenarios --nocapture

### RocksDB

    timeout 20m env CARGO_TARGET_DIR=/root/projects/branch-head-control-rocks-target \
      CARGO_BUILD_JOBS=2 BRANCH_HEAD_CONTROL_DB=/root/projects/branch-head-control-rocks-db \
      cargo test -p lix_benchmarks \
      --test forktree_branch_head_control_acceptance \
      --features storage-benches -- --exact rocksdb_all_scenarios --nocapture

### SlateDB

    timeout 20m env CARGO_TARGET_DIR=/root/projects/branch-head-control-slate-target \
      CARGO_BUILD_JOBS=2 BRANCH_HEAD_CONTROL_DB=/root/projects/branch-head-control-slate-db \
      cargo test -p lix_benchmarks \
      --test forktree_branch_head_control_acceptance \
      --features storage-benches,slatedb -- --exact slatedb_all_scenarios --nocapture

Each adapter run must cover:

    init -> close -> cold reopen
    create/switch/delete branch
    global+branch selector identity and generation fences
    unrelated branch publications coexist; same-owner stale publication rejects
    deterministic global sequence/epoch monotonicity
    undo/redo and checkpoint undo floor
    branch-first and GC-first races
    malformed, missing, wrong-kind selector/control fail-closed cases
    zero partial publication after every rejected operation

The counter record must separate begin reads/writes, point gets, scans, keys,
logical rows, logical bytes, physical objects, backend bytes, retries/CAS
rejects, writes/deletes, peak RSS/allocations, and immediate/settled disk.
Correctness compares exact state digests before and after every rejected or
successful operation; no current-main performance claim is permitted.
