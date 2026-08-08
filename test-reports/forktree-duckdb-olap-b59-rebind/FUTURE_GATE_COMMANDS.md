# Dormant b59 OLAP comparator commands

No command here has been run. These are transportable future forms only. Each
cell must use a fresh isolated path, exact candidate binary/source hashes, and
a 20-minute cap.

## Static and model gates

```bash
ROOT=/path/to/exact-b59-candidate
ORACLE="$ROOT/test-reports/forktree-duckdb-olap-b59-rebind"
test "$(git -C "$ROOT" rev-parse HEAD)" = b59e1f11a51153e0a787a81f0f25bf104d150aaf
test "$(git -C "$ROOT" rev-parse HEAD^{tree})" = 700fd04d21bc40c05425c9fc9e10d65c9e1eda24
sha256sum "$ORACLE"/{README.md,MANIFEST.json,QUERY_CONTRACT.md,RESULTS.csv,RAW_SHA256SUMS,CORRUPTION_MATRIX.md,corruption_matrix_model.rs,CORRECTION_REPORT.md,FUTURE_GATE_COMMANDS.md,source_verifier.sh}
rustc --edition=2021 --test "$ORACLE/corruption_matrix_model.rs" \
  -o /tmp/forktree-duckdb-corruption-model
timeout 20m /tmp/forktree-duckdb-corruption-model --nocapture --test-threads=1
```

The future harness must verify one authenticated coherent ForkTree read per
query, provider planning/materialization attribution, zero OLTP/VC/filesystem
mutation counters, exact result digest, and cold-reopen equality before any
timed optimization claim.

It must run the named selector/root/object corruption matrix and the catalog
and checkpoint extensions before timing. Save typed failure, before/after
authority fingerprint, zero-work counters, and valid-absence results.

## Ordered adapter cells

Run the pure model and pinned DuckDB control first. Then run Memory, RocksDB,
and SlateDB in this order for each size/query group; stop on the first failure:

```bash
timeout 20m <exact-harness> model 10000 all 1
timeout 20m <exact-harness> duckdb 10000 all 3
timeout 20m <exact-harness> memory 10000 all 1
timeout 20m <exact-harness> rocksdb 10000 all 3
timeout 20m <exact-harness> slatedb 10000 all 3

timeout 20m <exact-harness> rocksdb 50000 all 3
timeout 20m <exact-harness> slatedb 50000 all 3

timeout 20m <exact-harness> rocksdb 500000 all 3
timeout 20m <exact-harness> slatedb 500000 all 3
```

The harness must use the exact nine-query order and fixture in
`QUERY_CONTRACT.md`, report warm/cold/model digests, and preserve the raw
DuckDB allocation scope (`Rust output bridge only`). It must not report these
inherited `RESULTS.csv` medians as b59 measurements.

## Acceptance thresholds

An optimization candidate needs at least 10% improvement in its targeted
provider/materialization metric on both RocksDB and SlateDB, or an explicitly
documented major resource win. Every OLTP, VC/history/branch/checkpoint,
filesystem/file, OLAP, digest, reopen, backend, RSS, and settled-disk
guardrail must remain within +5%. Any query write, publication, selector/epoch
CAS, fallback, second authority/read, corruption acceptance, or digest mismatch
is a blocker independent of speed.

Current-main performance is explicitly excluded; comparison may only be made
against an exact candidate-specific baseline that is separately frozen.
