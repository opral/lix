# Optimization Log 10: Untracked-State CRUD

Goal: make `packages/engine/src/untracked_state` fast for durable local overlay
CRUD without moving the workload through tracked-state, changelog, or SQL2.

Untracked state is the storage path for local rows excluded from commit
membership. It still needs the same disciplined scorecard as Log 7, with the
physical-layout focus from Log 8 and the isolated benchmark ownership from Log
9.

## Scope

In scope:

```text
untracked_state row key layout
untracked_state row codec
untracked_state point lookup
untracked_state scan/filter/projection behavior
untracked_state write/update/delete batching
backend get/scan/write interaction used by untracked_state
SQLite and RocksDB behavior for the same untracked workload
```

Out of scope:

```text
tracked_state commit/delta layout
commit_store history and merge behavior
SQL2 planning or provider overhead
live_state overlay composition above untracked_state
```

Rule:

```text
If a profile points above untracked_state, record it as a follow-up. If it
points below untracked_state, optimize the shared storage/backend primitive only
when both SQLite and RocksDB can use the new shape cleanly.
```

## Benchmark Surface

Benchmark target:

```text
packages/engine/benches/untracked_state_crud/main.rs
```

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud
```

Focused smoke command:

```sh
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/.*/smoke'
```

Logical I/O report:

```sh
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
```

The I/O report measures logical backend KV request/result payload accounting
after fixture setup is reset out of the counters. It reports read calls,
returned/read rows, request key bytes for point reads, returned key/value bytes
for scans, write batches, puts, deletes, and logical write bytes. It does not
measure OS-level disk pages, filesystem cache behavior, WAL flushes, RocksDB
compaction bytes, or keys examined but not returned by a backend scan.

Groups:

```text
untracked_state_crud/raw_sqlite/smoke
untracked_state_crud/raw_sqlite/real_workload
untracked_state_crud/lix_sqlite/smoke
untracked_state_crud/lix_sqlite/real_workload
untracked_state_crud/lix_rocksdb/smoke
untracked_state_crud/lix_rocksdb/real_workload
```

Rows:

```text
insert_all_rows/{1k,10k}
select_all_rows/{1k,10k}
select_keys_only/{1k,10k}
select_one_by_pk/{1k,10k}
select_all_by_pk/{1k,10k}
update_all_rows/{1k,10k}
update_one_by_pk/{1k,10k}
delete_all_rows/{1k,10k}
delete_one_by_pk/{1k,10k}
```

`raw_sqlite` omits `select_keys_only` and `select_all_by_pk`; those are
untracked-state API diagnostics rather than plain-table CRUD rows.

## Fixture

The suite owns a copied fixture so benchmark changes do not leak across other
benchmark targets:

```text
packages/engine/benches/untracked_state_crud/pnpm-lock.fixture.json
```

```text
sizes:
  smoke = 1,000 rows
  real_workload = 10,000 rows
shape:
  source = copied pnpm-lock JSON fixture flattened into JSON-pointer rows
  version_id = bench-version
  schema_key = json_pointer
  entity_id = JSON pointer path
  file_id = NULL in Lix untracked state, empty text in raw SQLite reference
  snapshot_content = {"path": path, "value": JSON node value}
  metadata = NULL
  global = false
```

The raw SQLite reference stores the equivalent identity and payload fields in a
single `WITHOUT ROWID` table keyed by
`(version_id, schema_key, entity_id, file_id)`.

The Lix write rows use prepared fixtures and write-only benchmark helpers, so
insert/update/delete timings do not include post-write verification scans.

Implementation note:

```text
The copied pnpm-lock fixture includes the root JSON pointer, represented as an
empty path in snapshots. Untracked-state entity identities cannot contain an
empty primary-key value, so the benchmark maps only that internal entity id to
"/" while preserving snapshot_content.path = "".
```

## Smoke Baseline: 2026-05-11

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/.*/smoke'
```

Fixture:

```text
rows: 1,000
source: packages/engine/benches/untracked_state_crud/pnpm-lock.fixture.json
```

| operation          |       Raw SQLite |       Lix SQLite |      Lix RocksDB |
| ------------------ | ---------------: | ---------------: | ---------------: |
| `insert_all_rows`  | 2.3917-2.4331 ms | 8.6009-8.7247 ms | 4.0452-4.0974 ms |
| `select_all_rows`  | 3.5049-3.5782 ms | 5.4745-5.9177 ms | 1.6078-1.6459 ms |
| `select_keys_only` |                - | 5.1347-5.2920 ms | 1.6006-1.6339 ms |
| `select_one_by_pk` | 3.4179-3.4651 ms | 4.3376-4.5237 ms | 792.61-810.26 µs |
| `select_all_by_pk` |                - | 9.8336-11.246 ms | 3.1604-3.2220 ms |
| `update_all_rows`  | 5.4680-5.7347 ms | 7.9466-9.5611 ms | 3.3160-3.3757 ms |
| `update_one_by_pk` | 3.4945-3.5387 ms | 4.1147-4.2513 ms | 666.33-678.87 µs |
| `delete_all_rows`  | 3.4530-3.5107 ms | 6.9075-7.1015 ms | 2.2363-2.2817 ms |
| `delete_one_by_pk` | 3.4644-3.5255 ms | 4.1876-4.3089 ms | 682.68-699.75 µs |

Initial read:

```text
SQLite untracked-state CRUD is close to raw SQLite for reads, updates, and
deletes, but inserts are ~3.6x raw SQLite at 1k rows. RocksDB is faster than
raw SQLite on this fixture for point reads, all-row scans, point updates, and
point deletes, but is slower for inserts, update-all, and delete-all.

select_keys_only is not faster than select_all_rows for either Lix backend,
which suggests untracked scans still decode/materialize full rows even when
the requested projection is identity-only.
```

## Smoke I/O Baseline: 2026-05-12

Command:

```sh
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
```

Rows:

```text
workload: smoke/1k
metric: logical backend KV I/O after fixture setup
```

| backend     | operation          | read ops | read rows | read bytes | write batches | puts | deletes | write bytes |
| ----------- | ------------------ | -------: | --------: | ---------: | ------------: | ---: | ------: | ----------: |
| Lix SQLite  | `insert_all_rows`  |        0 |         0 |          0 |             1 | 1000 |       0 |   1,114,072 |
| Lix SQLite  | `select_all_rows`  |        1 |      1000 |  1,020,868 |             0 |    0 |       0 |           0 |
| Lix SQLite  | `select_keys_only` |        1 |      1000 |  1,020,868 |             0 |    0 |       0 |           0 |
| Lix SQLite  | `select_one_by_pk` |        1 |         1 |        478 |             0 |    0 |       0 |           0 |
| Lix SQLite  | `select_all_by_pk` |     1000 |      1000 |  1,114,072 |             0 |    0 |       0 |           0 |
| Lix SQLite  | `update_all_rows`  |        0 |         0 |          0 |             1 | 1000 |       0 |     474,316 |
| Lix SQLite  | `update_one_by_pk` |        0 |         0 |          0 |             1 |    1 |       0 |         271 |
| Lix SQLite  | `delete_all_rows`  |        0 |         0 |          0 |             1 |    0 |    1000 |      93,204 |
| Lix SQLite  | `delete_one_by_pk` |        0 |         0 |          0 |             1 |    0 |       1 |          43 |
| Lix RocksDB | `insert_all_rows`  |        0 |         0 |          0 |             1 | 1000 |       0 |   1,114,072 |
| Lix RocksDB | `select_all_rows`  |        1 |      1000 |  1,020,868 |             0 |    0 |       0 |           0 |
| Lix RocksDB | `select_keys_only` |        1 |      1000 |  1,020,868 |             0 |    0 |       0 |           0 |
| Lix RocksDB | `select_one_by_pk` |        1 |         1 |        478 |             0 |    0 |       0 |           0 |
| Lix RocksDB | `select_all_by_pk` |     1000 |      1000 |  1,114,072 |             0 |    0 |       0 |           0 |
| Lix RocksDB | `update_all_rows`  |        0 |         0 |          0 |             1 | 1000 |       0 |     474,316 |
| Lix RocksDB | `update_one_by_pk` |        0 |         0 |          0 |             1 |    1 |       0 |         271 |
| Lix RocksDB | `delete_all_rows`  |        0 |         0 |          0 |             1 |    0 |    1000 |      93,204 |
| Lix RocksDB | `delete_one_by_pk` |        0 |         0 |          0 |             1 |    0 |       1 |          43 |

I/O read:

```text
select_keys_only reads exactly the same 1,020,868 logical bytes as
select_all_rows. That is the clearest first optimization target: projection
should avoid full value hydration for key/header-only scans.

select_all_by_pk performs 1,000 get calls and reads 1,114,072 logical bytes.
A batched point-read API path should preserve the same bytes but collapse the
call count.

The write paths are already one logical batch per operation. Write-byte
optimization is mostly row encoding/layout work, not call-count work.
```

## Optimization Standard

The benchmark is a scorecard for the untracked-state module, not a general
storage benchmark. Keep changes in `packages/engine/src/untracked_state` unless
the measured bottleneck is a storage/backend primitive whose current shape
forces bad untracked behavior.

Preferred improvements:

```text
replace full namespace scans with prefix/range scans for version/schema/file filters
avoid full row decode for key/header projections
batch point reads instead of per-row load loops
stage delete/update batches without extra verification scans in caller hot paths
keep row encoding low-copy and projection-aware
```

Do not use tracked-state machinery to make untracked benchmarks look faster.
Untracked state owns this path.

## Optimization 1: Projection-Aware Covering Index

Date: 2026-05-12

Axis:

```text
I/O for identity-only projected scans
```

Change:

```text
Untracked-state writes now maintain a small identity index keyed by the same
row identity key. The index value stores scalar fields needed by projected
materialization: created_at, updated_at, and global.

Scans whose requested projection is fully covered by identity columns now use
backend scan_entries over this covering index instead of scanning and decoding
full row values.

When filters include version_ids, the reader issues one bounded key-prefix scan
per version. When filters include version_ids and schema_keys, it issues one
bounded prefix scan per version+schema pair. Broader filters still scan index
entries and filter decoded identities in memory.
```

Reference principle:

```text
This follows the same first-principles shape as projection pushdown / covering
index scans in query engines: do not read payload columns when the requested
projection is fully covered by the physical key. DataFusion optimizer tests and
rules model this with TableScan projection pushdown, and storage engines use
covering keys/indexes to avoid base-row reads for key-covered projections.
```

Verification:

```sh
cargo test -p lix_engine untracked_state::storage --features storage-benches
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/select_keys_only/1k'
```

Smoke I/O scoreboard:

| backend     | operation          | before read bytes | after read bytes | read-byte delta |
| ----------- | ------------------ | ----------------: | ---------------: | --------------: |
| Lix SQLite  | `select_keys_only` |         1,020,868 |          150,204 |          -85.3% |
| Lix RocksDB | `select_keys_only` |         1,020,868 |          150,204 |          -85.3% |

Smoke write I/O tradeoff:

| backend     | operation          | before puts/deletes | after puts/deletes | before write bytes | after write bytes |
| ----------- | ------------------ | ------------------: | -----------------: | -----------------: | ----------------: |
| Lix SQLite  | `insert_all_rows`  |          1000 / 0   |         2000 / 0   |          1,114,072 |         1,264,276 |
| Lix SQLite  | `update_all_rows`  |          1000 / 0   |         2000 / 0   |            474,316 |           624,520 |
| Lix SQLite  | `delete_all_rows`  |             0 / 1000 |            0 / 2000 |             93,204 |           186,408 |
| Lix RocksDB | `insert_all_rows`  |          1000 / 0   |         2000 / 0   |          1,114,072 |         1,264,276 |
| Lix RocksDB | `update_all_rows`  |          1000 / 0   |         2000 / 0   |            474,316 |           624,520 |
| Lix RocksDB | `delete_all_rows`  |             0 / 1000 |            0 / 2000 |             93,204 |           186,408 |

Smoke timing scoreboard:

| backend     | operation          | before timing      | after timing       | timing read |
| ----------- | ------------------ | -----------------: | ----------------: | ----------- |
| Lix SQLite  | `select_keys_only` | 5.1347-5.2920 ms   | 5.8884-6.0097 ms  | slower after scalar-preserving index |
| Lix RocksDB | `select_keys_only` | 1.6006-1.6339 ms   | 1.4754-1.5353 ms  | modest improvement |

Notes:

```text
The I/O win is structural and large, but scalar-preserving covering indexes
add write amplification and make SQLite select_keys_only slower in the smoke
timing row. This is still useful evidence for the I/O axis: avoiding full
payload hydration is correct, but the physical index/value shape needs the next
iteration before this target helps both backends on wall-clock time.
```

## Optimization 2: Batched Point Reads

Date: 2026-05-12

Axis:

```text
I/O call count for repeated exact primary-key reads
```

Change:

```text
Untracked-state storage now uses a batch-first load_rows reader API. Exact row
identities are encoded into chunked backend get_values requests, preserving
request order, duplicate identities, misses, and None for non-exact file_id
filters. The chunk size is 512 keys, so the smoke workload uses 2 backend gets
for 1,000 exact identities and the real workload uses 20 gets for 10,000 exact
identities.

The old untracked-state load_row reader API was removed and upstream untracked
callers were refactored through load_rows. Single-request batches keep a private
one-row fast path inside storage so point reads do not pay multi-row batch
bookkeeping.

The SQLite bench backend now executes a chunked get_values group as one
`SELECT key, value ... WHERE key IN (...)` statement and reconstructs the
requested order/misses in memory, instead of issuing one SELECT per key.
```

Verification:

```sh
cargo test -p lix_engine untracked_state::storage --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud --no-run
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/select_(one|all)_by_pk/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/real_workload/select_all_by_pk/10k'
```

Smoke I/O scoreboard:

| backend     | operation          | before get calls | after get calls | get keys | read rows | read bytes |
| ----------- | ------------------ | ---------------: | --------------: | -------: | --------: | ---------: |
| Lix SQLite  | `select_all_by_pk` |             1000 |               2 |     1000 |      1000 |  1,114,072 |
| Lix RocksDB | `select_all_by_pk` |             1000 |               2 |     1000 |      1000 |  1,114,072 |

Real-workload I/O scoreboard:

| backend     | operation          | before get calls | after get calls | get keys | read rows | read bytes |
| ----------- | ------------------ | ---------------: | --------------: | -------: | --------: | ---------: |
| Lix SQLite  | `select_all_by_pk` |            10000 |              20 |    10000 |     10000 |  5,460,528 |
| Lix RocksDB | `select_all_by_pk` |            10000 |              20 |    10000 |     10000 |  5,460,528 |

Smoke timing scoreboard:

| backend     | operation          | before timing    | after timing     | timing delta |
| ----------- | ------------------ | ---------------: | --------------: | -----------: |
| Lix SQLite  | `select_all_by_pk` | 9.8336-11.246 ms | 7.7282-7.8977 ms |        ~-27% |
| Lix RocksDB | `select_all_by_pk` | 3.1604-3.2220 ms | 2.7012-2.7677 ms |        ~-15% |

Real-workload timing snapshot:

| backend     | operation          | after timing       |
| ----------- | ------------------ | -----------------: |
| Lix SQLite  | `select_all_by_pk` | 33.817-34.455 ms   |
| Lix RocksDB | `select_all_by_pk` | 21.755-21.992 ms   |

Notes:

```text
This optimization does not reduce payload bytes; it removes per-key backend
request overhead for workloads that already know exact identities. After review,
the scorecard reports get calls and get keys separately because a logical
backend call can still contain many physical keys.

The implementation follows the same batch-oriented execution principle used by
DataFusion RecordBatches and DuckDB vectors: pass a bounded set of rows/keys
through the storage boundary, not one row per call. The single-row fast path is
private implementation detail under the batch-first API, not a retained public
reader API.
```
