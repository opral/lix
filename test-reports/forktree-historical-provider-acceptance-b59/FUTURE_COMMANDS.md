# Future Memory → RocksDB → SlateDB commands

These commands are frozen for the first production historical-provider caller
migration. They are not run by this package. Every cell is serialized and
must stop at 20 minutes or at the first failure.

The future test target must be one test-only adapter harness named
`forktree_historical_provider_acceptance` under
`packages/engine-benchmarks/tests/`. It must use the public SQL/session path
and a caller-owned `StorageRead`/ForkTree facade. Test fixture construction may
use existing internal authenticated fixture helpers, but the production read
path may not access raw legacy spaces or add a compatibility reader.

```bash
ORACLE_ROOT=/root/repos/lix-evidence/forktree-historical-provider-b59
ORACLE_TARGET="$ORACLE_ROOT/target"
MODEL_TMP="$ORACLE_ROOT/model"
mkdir -p "$ORACLE_ROOT" "$MODEL_TMP"

bash test-reports/forktree-historical-provider-acceptance-b59/source_verifier.sh
rustc --edition 2021 --test \
  test-reports/forktree-historical-provider-acceptance-b59/model.rs \
  -o "$MODEL_TMP/historical-provider-model"
"$MODEL_TMP/historical-provider-model" --nocapture

timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks \
  --test forktree_historical_provider_acceptance \
  --features storage-benches \
  historical_provider_semantics_memory -- --exact --nocapture --test-threads=1

timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks \
  --test forktree_historical_provider_acceptance \
  --features storage-benches,rocksdb \
  historical_provider_semantics_rocksdb -- --exact --nocapture --test-threads=1

timeout 20m env CARGO_TARGET_DIR="$ORACLE_TARGET" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks \
  --test forktree_historical_provider_acceptance \
  --features storage-benches,slatedb \
  historical_provider_semantics_slatedb -- --exact --nocapture --test-threads=1
```

Each adapter test must execute the following cases through the five surfaces:

1. Valid CommitCatalog/CommitObject/root plus a genuinely absent requested
   identity: accept only `None`/empty for that identity.
2. Remove the selected CommitCatalog entry; require typed error for point and
   scan before any empty filtered result.
3. Remove, same-size substitute, wrong-kind substitute, and malformed the
   selected commit/root; require typed error.
4. Read value, JSON null, included tombstone, and filtered tombstone; verify
   exact `content`/`is_deleted` distinctions.
5. Exercise file and directory rows with the same textual ID; verify domains
   remain separate. Exercise rename, add, remove, descendant directory change,
   and unchanged unrelated rows.
6. Exercise exact history anchors, depth/branch filters, projections, zero and
   boundary LIMITs. Verify file/directory history order and source-change
   ordering, diff before/after IDs and marker filtering, checkpoint marker
   selection/depth, and working-diff before/after paths.
7. Flush, drop, cold reopen, and repeat the authority/error/absence and public
   result assertions on the same fixture.
8. Instrument the operation to assert one retained read identity, zero second
   `begin_read`, zero retry, zero cache substitution, and zero legacy/fallback
   reader events.

The candidate is not eligible for approval if a missing authority is converted
to an empty result, if a valid absence errors, if a file and directory collide,
if LIMIT is applied before grouping/order, or if reopen changes any public
row/error/order result.
