# Future adapter commands for the 413-bound correction

These are frozen commands only. They were not run in this report-only task;
there was no production build or adapter matrix. Every cell is serialized,
has a 20-minute cap, and stops on the first failure.

The correction must add one focused test target named
`forktree_historical_fail_closed` under `packages/engine-benchmarks/tests/`.
The target must use public `Storage`/`StorageRead` behavior and report the
retained-read identity and operation counters without exposing raw storage
spaces.

```bash
ORACLE_ROOT=/root/repos/lix-evidence/forktree-historical-failclosed-sql-413
ORACLE_TMP="$ORACLE_ROOT/tmp"
ORACLE_TARGET="$ORACLE_ROOT/target"
mkdir -p "$ORACLE_TMP"

# Source binding (the expected RED is the current status of 413).
bash evidence/forktree-historical-failclosed-sql-413/source_verifier_413.sh --expect-red

# Reuse the inherited pure model from the immutable 448 oracle.
rustc --edition 2021 --test \
  evidence/forktree-historical-failclosed-e166/model.rs \
  -o "$ORACLE_TMP/historical-failclosed-model"
"$ORACLE_TMP/historical-failclosed-model" --nocapture

# Memory: point + scan, authenticated absence, all corruption substitutions,
# null/tombstone/value, one retained read and no fallback/retry/cache.
timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_historical_fail_closed \
  --features storage-benches \
  historical_point_scan_fail_closed_memory -- --exact --nocapture --test-threads=1

# RocksDB: identical cases through flush/drop/cold-reopen.
timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_historical_fail_closed \
  --features storage-benches \
  historical_point_scan_fail_closed_rocksdb -- --exact --nocapture --test-threads=1

# SlateDB: identical cases through flush/drop/cold-reopen.
timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_historical_fail_closed \
  --features storage-benches,slatedb \
  historical_point_scan_fail_closed_slatedb -- --exact --nocapture --test-threads=1
```

Each adapter test must execute, in the same retained read where applicable:

1. publish a valid commit/root and query a genuinely absent key; accept only
   authenticated absence;
2. remove the selected CommitCatalog member; require corruption/error for both
   point and scan, never an empty result;
3. remove or substitute the selected root with missing, wrong-kind, and
   malformed bytes; require corruption/error;
4. publish and read valid null, tombstone, and value cells, preserving their
   distinct semantics and tombstone filtering;
5. flush, drop, reopen, and repeat the same point/scan assertions;
6. assert no second `StorageRead`, retry, fallback, cache, legacy reader, or
   permissive empty-success event.

The future correction is green only if a valid commit/root plus absent key is
the sole empty/None case. The final scan assertion is mandatory because a
point-only fix can still let a missing commit/root become an empty SQL scan.
