# SQLite OLTP rebind report — `0e543716`

Status: **BLOCKED/UNRUN at target compatibility**, test/report-only.

## Immutable source binding

The requested newer model is exact commit
`0e543716b0a89d377015ce8cdb1579cad70408ff`, tree
`b0589d9037a8b3a530d252150360ce9f5648577c`, parent
`cd76d29406ed7e00711a5b5ba9c40da537524dd3`, with parent-to-head full-index
binary diff SHA-256
`b6f7046c017196beff03aa79016ec686df7070621a1025e292e3e604e0ceee65`.
The stable patch ID is
`d587a183973eb31ce01593d3919f925dcdd29f45`.
Its four changed paths are benchmark/evidence paths only:

1. `benchmarks/forktree_duckdb_comparator/BATCHING_EXPERIMENT.md`
2. `benchmarks/forktree_duckdb_comparator/BATCHING_RAW_SHA256SUMS`
3. `benchmarks/forktree_duckdb_comparator/BATCHING_RESULTS.csv`
4. `packages/engine-benchmarks/benches/forktree_replacement/olap_datafusion.rs`

The model is runnable for read-only OLAP/DataFusion queries. It does not
expose the SQLite OLTP target required by this package: CRUD, point-read
batches, DML `RETURNING`, UPSERT, savepoint/rollback, stale-writer conflicts,
file-row mutations, or a transaction-owned one-plan/one-commit publication.
Fixture setup writes and branch setup are not substitutes for those semantics.

## Cell status

No Memory, SQLite, ForkTree, RocksDB, or SlateDB SQLite-OLTP cell ran. No
benchmark, compile, adapter, or runtime claim is made. The dormant commands in
`FUTURE_GATE_COMMANDS.md` remain gated on a future explicit test-only OLTP
adapter over this model. The unrelated 0e OLAP command shape is documented but
was not run here.

The full OLTP vector remains frozen: point/range reads, INSERT/UPDATE/DELETE
RETURNING, UPSERT, savepoint rollback, mixed transactions, overlay precedence,
historical corruption/fail-closed, cold reopen, and backend/resource counters.
Any future run must prove exact semantic digests, one coherent read, one
transaction publication, zero fallback/cache/legacy counters, and the stated
RocksDB/SlateDB guardrails before measuring performance.

The preceding b59 package is preserved unchanged in its historical parent
ref; this successor does not mutate it or any production path.
