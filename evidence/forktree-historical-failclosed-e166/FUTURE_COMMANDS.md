# Future adapter qualification commands

These commands are a frozen future contract, not commands run against e166.
They assume the candidate adds one test target named
`forktree_historical_fail_closed` under `packages/engine-benchmarks/tests/`
and exposes the public `Storage`/`StorageRead` path without raw storage-space
access.

Every cell is serialized, has a 20-minute cap, and must stop on the first
failure. The test must report the retained read identity and operation count;
it must not retry, reopen, consult a legacy reader, or use a cache to turn a
missing commit into an empty batch.

```bash
ORACLE_TMP=/root/repos/lix-evidence/forktree-historical-failclosed-e166/tmp
ORACLE_TARGET=/root/repos/lix-evidence/forktree-historical-failclosed-e166/target
mkdir -p "$ORACLE_TMP"

# Pure model only; no repository build is required for the source RED gate.
rustc --edition 2021 --test \
  evidence/forktree-historical-failclosed-e166/model.rs \
  -o "$ORACLE_TMP/historical-failclosed-model"
"$ORACLE_TMP/historical-failclosed-model" --nocapture

# Memory: valid absence, all corruption substitutions, cell distinctions,
# one-retained-read and no-fallback assertions.
timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_historical_fail_closed \
  --features storage-benches \
  historical_point_scan_fail_closed_memory -- --exact --nocapture --test-threads=1

# RocksDB: identical oracle and cold reopen/recovery proof.
timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_historical_fail_closed \
  --features storage-benches \
  historical_point_scan_fail_closed_rocksdb -- --exact --nocapture --test-threads=1

# SlateDB: identical oracle and cold reopen/recovery proof.
timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_historical_fail_closed \
  --features storage-benches,slatedb \
  historical_point_scan_fail_closed_slatedb -- --exact --nocapture --test-threads=1
```

Required cases per adapter:

1. Publish a valid commit/root and query a key that is genuinely absent:
   success with an authenticated absence.
2. Remove only the CommitCatalog member: corruption/error, never an empty
   row set.
3. Remove the selected root object: corruption/error.
4. Substitute a same-sized wrong-kind root or malformed catalog/root bytes:
   corruption/error.
5. Publish valid tombstone, JSON null, and JSON value cells and assert they
   remain distinct through flush, drop, reopen, and scan/point paths.
6. Assert every operation uses the same retained read identity and that no
   retry, fallback, second reader, or cache counter is observed.

The final adapter gate must include a scan, not only a point lookup: an
authenticated commit with a valid root and an absent filtered key may be
empty; a missing/corrupt commit/root must fail before returning that empty
result.
