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
after fixture setup is reset out of the counters. It reports headline `io ops`
and `io bytes` columns first, followed by read calls, returned/read rows,
request key bytes for point reads, returned key/value bytes for scans, write
batches, puts, deletes, and logical write bytes. It does not measure OS-level
disk pages, filesystem cache behavior, WAL flushes, RocksDB compaction bytes,
or keys examined but not returned by a backend scan.

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

## Optimization 3: Remove Identity Side Index

Date: 2026-05-12

Axis:

```text
write amplification from the identity covering index
```

Change:

```text
The separate untracked_state.identity_index namespace was removed. Untracked
state now stores one canonical row entry per identity again.

Scans no longer use the identity side index. They hydrate canonical row values,
which preserves stored created_at, updated_at, and global semantics for every
projection. This intentionally trades back the previous key-only read fast path
to remove write amplification from inserts, updates, and deletes.
```

Reference principle:

```text
This follows a write-path storage-layout principle: avoid maintaining a
secondary physical structure whose read benefit does not justify its write
amplification for the target workload. The artifact references model the same
tradeoff explicitly: Dolt chooses covering index iterators only when an index
covers the requested projections (`artifact/dolt/go/libraries/doltcore/sqle/index/index_reader.go`),
and Turso surfaces covering-index plan choices separately from base table
access (`artifact/turso/core/translate/display.rs`). In this layout, the side
index was the only scalar-preserving covering structure; removing it is a
deliberate write/read tradeoff, not a claim that timestamps are covered by the
primary key.
```

Verification:

```sh
cargo test -p lix_engine untracked_state::storage --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud --no-run
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|update_all_rows|delete_all_rows|select_keys_only)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/real_workload/(insert_all_rows|update_all_rows|delete_all_rows|select_keys_only)/10k'
```

Smoke I/O scoreboard:

| backend     | operation          | before puts/deletes | after puts/deletes | before bytes | after bytes |
| ----------- | ------------------ | ------------------: | -----------------: | -----------: | ----------: |
| Lix SQLite  | `insert_all_rows`  |            2000 / 0 |           1000 / 0 |    1,264,276 |   1,114,072 |
| Lix SQLite  | `update_all_rows`  |            2000 / 0 |           1000 / 0 |      624,520 |     474,316 |
| Lix SQLite  | `delete_all_rows`  |            0 / 2000 |           0 / 1000 |      186,408 |      93,204 |
| Lix RocksDB | `insert_all_rows`  |            2000 / 0 |           1000 / 0 |    1,264,276 |   1,114,072 |
| Lix RocksDB | `update_all_rows`  |            2000 / 0 |           1000 / 0 |      624,520 |     474,316 |
| Lix RocksDB | `delete_all_rows`  |            0 / 2000 |           0 / 1000 |      186,408 |      93,204 |

Smoke read I/O scoreboard:

| backend     | operation          | before read bytes | after read bytes | read-byte delta |
| ----------- | ------------------ | ----------------: | ---------------: | --------------: |
| Lix SQLite  | `select_keys_only` |           150,204 |        1,020,868 |        +579.7% |
| Lix RocksDB | `select_keys_only` |           150,204 |        1,020,868 |        +579.7% |

Smoke timing scoreboard:

| backend     | operation          | before timing     | after timing      | timing delta |
| ----------- | ------------------ | ----------------: | ---------------: | -----------: |
| Lix SQLite  | `insert_all_rows`  | 11.112-11.333 ms  | 8.4015-8.6852 ms |        ~-24% |
| Lix SQLite  | `select_keys_only` | 5.4289-5.5678 ms  | 5.2545-5.3894 ms |         ~-3% |
| Lix SQLite  | `update_all_rows`  | 9.4389-9.5586 ms  | 7.8316-7.9436 ms |        ~-17% |
| Lix SQLite  | `delete_all_rows`  | 8.8763-9.1404 ms  | 7.0143-7.0802 ms |        ~-22% |
| Lix RocksDB | `insert_all_rows`  | 4.9880-5.2201 ms  | 4.0144-4.1236 ms |        ~-20% |
| Lix RocksDB | `select_keys_only` | 1.3815-1.4017 ms  | 1.5747-1.5964 ms |        ~+14% |
| Lix RocksDB | `update_all_rows`  | 4.4034-4.5340 ms  | 3.2577-3.2920 ms |        ~-26% |
| Lix RocksDB | `delete_all_rows`  | 3.0574-3.0912 ms  | 2.2155-2.2492 ms |        ~-27% |

Real-workload I/O snapshot:

| backend     | operation          | read bytes | puts/deletes | write bytes |
| ----------- | ------------------ | ---------: | ------------: | ----------: |
| Lix SQLite  | `insert_all_rows`  |          0 |     10000 / 0 |   5,460,528 |
| Lix SQLite  | `select_all_rows`  |  4,516,832 |         0 / 0 |           0 |
| Lix SQLite  | `select_keys_only` |  4,516,832 |         0 / 0 |           0 |
| Lix SQLite  | `update_all_rows`  |          0 |     10000 / 0 |   4,789,908 |
| Lix SQLite  | `delete_all_rows`  |          0 |     0 / 10000 |     943,696 |
| Lix RocksDB | `insert_all_rows`  |          0 |     10000 / 0 |   5,460,528 |
| Lix RocksDB | `select_all_rows`  |  4,516,832 |         0 / 0 |           0 |
| Lix RocksDB | `select_keys_only` |  4,516,832 |         0 / 0 |           0 |
| Lix RocksDB | `update_all_rows`  |          0 |     10000 / 0 |   4,789,908 |
| Lix RocksDB | `delete_all_rows`  |          0 |     0 / 10000 |     943,696 |

Real-workload timing snapshot:

| backend     | operation          | after timing       |
| ----------- | ------------------ | -----------------: |
| Lix SQLite  | `insert_all_rows`  | 60.827-63.465 ms   |
| Lix SQLite  | `select_keys_only` | 15.528-15.929 ms   |
| Lix SQLite  | `update_all_rows`  | 53.970-55.711 ms   |
| Lix SQLite  | `delete_all_rows`  | 39.760-40.793 ms   |
| Lix RocksDB | `insert_all_rows`  | 39.827-40.662 ms   |
| Lix RocksDB | `select_keys_only` | 10.885-11.159 ms   |
| Lix RocksDB | `update_all_rows`  | 29.890-32.065 ms   |
| Lix RocksDB | `delete_all_rows`  | 18.187-18.624 ms   |

Notes:

```text
This recovers the write amplification introduced by Optimization 1 while
preserving hydrated row semantics. The tradeoff is that select_keys_only no
longer gets the side-index payload-free scan path; it reads canonical row values
again. The side index was introduced after the baseline and Lix has not shipped,
so this is an unshipped clean-cut removal with no migration for orphaned local
developer data.
```

## Optimization 4: Compact Row Values

Date: 2026-05-12

Axis:

```text
row-value payload bytes for reads and writes
```

Change:

```text
Untracked-state values no longer duplicate identity fields that are already in
the physical key. The row value stores only snapshot_content, metadata,
created_at, updated_at, and global. Point reads decode from the requested
identity plus the compact value. Scans now use entry scans so they can decode
identity from each key and row state from each value.

The value file identifier changed from LXUS to LXUV. Lix has not shipped yet,
so this is a clean-cut storage layout replacement rather than a compatibility
migration.
```

Reference principle:

```text
This follows a storage-normalization principle from the artifact databases:
avoid storing the same logical columns in both index key and row payload when
the read path naturally has both. Dolt's covering/non-covering index readers
separate projected columns available from key/value tuples from columns that
require primary-row access (`artifact/dolt/go/libraries/doltcore/sqle/index/prolly_index_iter.go`
and `artifact/dolt/go/libraries/doltcore/sqle/index/index_reader.go`).
```

Verification:

```sh
cargo test -p lix_engine untracked_state::storage --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud --no-run
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|select_all_rows|select_keys_only|select_one_by_pk|select_all_by_pk|update_all_rows|update_one_by_pk)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/real_workload/(insert_all_rows|select_all_rows|select_all_by_pk|update_all_rows)/10k'
```

Smoke I/O scoreboard:

| backend     | operation          | before bytes | after bytes | byte delta |
| ----------- | ------------------ | -----------: | ----------: | ---------: |
| Lix SQLite  | `insert_all_rows`  |    1,114,072 |     988,380 |     -11.3% |
| Lix SQLite  | `select_all_rows`  |    1,020,868 |     988,380 |      -3.2% |
| Lix SQLite  | `select_one_by_pk` |          478 |         350 |     -26.8% |
| Lix SQLite  | `select_all_by_pk` |    1,114,072 |     988,380 |     -11.3% |
| Lix SQLite  | `update_all_rows`  |      474,316 |     348,624 |     -26.5% |
| Lix SQLite  | `update_one_by_pk` |          271 |         195 |     -28.0% |
| Lix RocksDB | `insert_all_rows`  |    1,114,072 |     988,380 |     -11.3% |
| Lix RocksDB | `select_all_rows`  |    1,020,868 |     988,380 |      -3.2% |
| Lix RocksDB | `select_one_by_pk` |          478 |         350 |     -26.8% |
| Lix RocksDB | `select_all_by_pk` |    1,114,072 |     988,380 |     -11.3% |
| Lix RocksDB | `update_all_rows`  |      474,316 |     348,624 |     -26.5% |
| Lix RocksDB | `update_one_by_pk` |          271 |         195 |     -28.0% |

Real-workload I/O scoreboard:

| backend     | operation          | before bytes | after bytes | byte delta |
| ----------- | ------------------ | -----------: | ----------: | ---------: |
| Lix SQLite  | `insert_all_rows`  |    5,460,528 |   4,191,840 |     -23.2% |
| Lix SQLite  | `select_all_rows`  |    4,516,832 |   4,191,840 |      -7.2% |
| Lix SQLite  | `select_one_by_pk` |          359 |         247 |     -31.2% |
| Lix SQLite  | `select_all_by_pk` |    5,460,528 |   4,191,840 |     -23.2% |
| Lix SQLite  | `update_all_rows`  |    4,789,908 |   3,521,220 |     -26.5% |
| Lix SQLite  | `update_one_by_pk` |          271 |         195 |     -28.0% |
| Lix RocksDB | `insert_all_rows`  |    5,460,528 |   4,191,840 |     -23.2% |
| Lix RocksDB | `select_all_rows`  |    4,516,832 |   4,191,840 |      -7.2% |
| Lix RocksDB | `select_one_by_pk` |          359 |         247 |     -31.2% |
| Lix RocksDB | `select_all_by_pk` |    5,460,528 |   4,191,840 |     -23.2% |
| Lix RocksDB | `update_all_rows`  |    4,789,908 |   3,521,220 |     -26.5% |
| Lix RocksDB | `update_one_by_pk` |          271 |         195 |     -28.0% |

Smoke timing scoreboard:

| backend     | operation          | before timing    | after timing     | timing delta |
| ----------- | ------------------ | ---------------: | --------------: | -----------: |
| Lix SQLite  | `insert_all_rows`  | 8.4015-8.6852 ms | 7.3462-7.4688 ms |        ~-14% |
| Lix SQLite  | `select_all_rows`  | 5.3115-5.3508 ms | 4.9022-5.0208 ms |         ~-7% |
| Lix SQLite  | `select_keys_only` | 5.4276-5.6383 ms | 4.9786-5.0484 ms |         ~-9% |
| Lix SQLite  | `select_one_by_pk` | 4.2838-4.3564 ms | 3.8850-4.0993 ms |         ~-7% |
| Lix SQLite  | `select_all_by_pk` | 7.3265-7.7430 ms | 6.6975-6.7973 ms |        ~-10% |
| Lix SQLite  | `update_all_rows`  | 7.8754-8.0260 ms | 6.7130-6.9206 ms |        ~-14% |
| Lix SQLite  | `update_one_by_pk` | 4.3991-4.5165 ms | 3.8040-4.8891 ms | noisy improvement |
| Lix RocksDB | `insert_all_rows`  | 4.0083-4.0900 ms | 3.6245-3.7255 ms |         ~-9% |
| Lix RocksDB | `select_all_rows`  | 1.6062-1.6542 ms | 1.5225-1.5741 ms |         ~-6% |
| Lix RocksDB | `select_keys_only` | 1.5795-1.6126 ms | 1.5114-1.5333 ms |         ~-4% |
| Lix RocksDB | `select_all_by_pk` | 2.5475-2.5627 ms | 2.4169-2.4360 ms |         ~-5% |
| Lix RocksDB | `update_all_rows`  | 3.2083-3.2669 ms | 2.8141-2.9118 ms |        ~-13% |
| Lix RocksDB | `update_one_by_pk` | 644.32-656.44 µs | 616.13-642.18 µs |         ~-4% |

Real-workload timing snapshot:

| backend     | operation          | after timing      |
| ----------- | ------------------ | ----------------: |
| Lix SQLite  | `insert_all_rows`  | 49.065-49.767 ms  |
| Lix SQLite  | `select_all_rows`  | 14.786-15.298 ms  |
| Lix SQLite  | `select_all_by_pk` | 31.818-32.202 ms  |
| Lix SQLite  | `update_all_rows`  | 43.215-43.781 ms  |
| Lix RocksDB | `insert_all_rows`  | 22.254-22.571 ms  |
| Lix RocksDB | `select_all_rows`  | 9.7033-9.8601 ms  |
| Lix RocksDB | `select_all_by_pk` | 19.369-19.619 ms  |
| Lix RocksDB | `update_all_rows`  | 24.054-24.354 ms  |

Notes:

```text
This optimization is useful because it helps the main write axis while also
improving read bytes for point reads and batched point reads. Value-only scans
became entry scans, so scan I/O now includes keys; despite that, full-scan
logical bytes still fall modestly because the value no longer repeats identity
fields.
```

## Optimization 5: Pre-Canonicalized Write Fixtures

Date: 2026-05-12

Axis:

```text
benchmark isolation for insert/update/delete write timings
```

Change:

```text
Untracked write/delete benchmark fixtures now store pre-canonicalized
UntrackedStateRow values instead of MaterializedUntrackedStateRow values. The
measured write operation stages canonical rows directly, so insert/update/delete
timings no longer include benchmark fixture conversion, row cloning into the
canonical shape, or metadata serialization.

This does not change logical storage I/O. It makes the Lix benchmark match the
raw SQLite reference more closely: raw SQLite fixtures already hold prepared
row strings and the measured loop only executes the database write.
```

Reference principle:

```text
This follows the benchmark hygiene used by database benchmark suites in
artifact references: setup/prepare work is separate from measured execution.
Dolt's sysbench runner models prepare/run/cleanup as separate phases
(`artifact/dolt/go/performance/sysbench/testdef.go`), and Dolt microbenchmarks
reset the benchmark timer after setup (`artifact/dolt/go/performance/dolt_log_bench/dolt_log_test.go`).
Turso's memory benchmark also records setup as its own phase
(`artifact/turso/perf/memory/src/main.rs`).
```

Verification:

```sh
cargo test -p lix_engine untracked_state --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud --no-run
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|update_all_rows|delete_all_rows|update_one_by_pk|delete_one_by_pk)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/real_workload/(insert_all_rows|update_all_rows|delete_all_rows)/10k'
```

Smoke timing scoreboard:

| backend     | operation          | before timing     | after timing      | timing delta |
| ----------- | ------------------ | ----------------: | ---------------: | -----------: |
| Lix SQLite  | `insert_all_rows`  | 7.3462-7.4688 ms  | 7.0196-7.2650 ms |         ~-6% |
| Lix SQLite  | `update_all_rows`  | 6.7130-6.9206 ms  | 6.4660-6.7457 ms |         ~-4% |
| Lix SQLite  | `delete_all_rows`  | 6.9334-7.0701 ms  | 6.1155-6.2499 ms |        ~-12% |
| Lix SQLite  | `update_one_by_pk` | 3.8040-4.8891 ms  | 3.9106-4.0195 ms | noisy/no change |
| Lix SQLite  | `delete_one_by_pk` | 4.4915-4.6263 ms  | 4.2442-4.3429 ms |         ~-6% |
| Lix RocksDB | `insert_all_rows`  | 3.6245-3.7255 ms  | 3.2501-3.2761 ms |        ~-10% |
| Lix RocksDB | `update_all_rows`  | 2.8141-2.9118 ms  | 2.4815-2.5499 ms |        ~-12% |
| Lix RocksDB | `delete_all_rows`  | 2.1826-2.2233 ms  | 1.8543-1.8843 ms |        ~-15% |
| Lix RocksDB | `update_one_by_pk` | 616.13-642.18 µs  | 646.29-677.92 µs | regressed/noisy |
| Lix RocksDB | `delete_one_by_pk` | 682.09-721.56 µs  | 679.40-741.49 µs | no improvement |

Real-workload timing scoreboard:

| backend     | operation         | before timing    | after timing     | timing delta |
| ----------- | ----------------- | ---------------: | --------------: | -----------: |
| Lix SQLite  | `insert_all_rows` | 49.065-49.767 ms | 42.830-44.068 ms |        ~-12% |
| Lix SQLite  | `update_all_rows` | 43.215-43.781 ms | 38.958-39.573 ms |        ~-10% |
| Lix SQLite  | `delete_all_rows` | 39.760-40.793 ms | 29.272-30.118 ms |        ~-26% |
| Lix RocksDB | `insert_all_rows` | 22.254-22.571 ms | 17.956-18.269 ms |        ~-19% |
| Lix RocksDB | `update_all_rows` | 24.054-24.354 ms | 23.390-24.545 ms | no change |
| Lix RocksDB | `delete_all_rows` | 18.187-18.624 ms | 15.920-16.269 ms |        ~-12% |

Notes:

```text
This is a benchmark correction, not a storage-format change. It prevents the
CRUD suite from charging Lix write operations for materialized-to-canonical
fixture preparation that raw SQLite does not pay in its measured operation.
The actual untracked write path still stages canonical UntrackedStateRowRef
values into the same compact row-value format from Optimization 4.
```

## Measurement Update: I/O Score Columns

Date: 2026-05-12

Change:

```text
The untracked_state CRUD I/O report now includes `io ops` and `io bytes` as
the first numeric columns. `io ops` is read calls plus write batches. `io bytes`
is read bytes plus write bytes. This gives each operation a single logical
payload score to optimize alongside wall-clock timing.
```

Verification:

```sh
cargo fmt -p lix_engine
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud --no-run
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
```

Smoke I/O score:

| backend     | operation          | io ops | io bytes |
| ----------- | ------------------ | -----: | -------: |
| Lix SQLite  | `insert_all_rows`  |      1 |  988,380 |
| Lix SQLite  | `select_all_rows`  |      1 |  988,380 |
| Lix SQLite  | `select_keys_only` |      1 |  988,380 |
| Lix SQLite  | `select_one_by_pk` |      1 |      350 |
| Lix SQLite  | `select_all_by_pk` |      2 |  988,380 |
| Lix SQLite  | `update_all_rows`  |      1 |  348,624 |
| Lix SQLite  | `update_one_by_pk` |      1 |      195 |
| Lix SQLite  | `delete_all_rows`  |      1 |   93,204 |
| Lix SQLite  | `delete_one_by_pk` |      1 |       43 |
| Lix RocksDB | `insert_all_rows`  |      1 |  988,380 |
| Lix RocksDB | `select_all_rows`  |      1 |  988,380 |
| Lix RocksDB | `select_keys_only` |      1 |  988,380 |
| Lix RocksDB | `select_one_by_pk` |      1 |      350 |
| Lix RocksDB | `select_all_by_pk` |      2 |  988,380 |
| Lix RocksDB | `update_all_rows`  |      1 |  348,624 |
| Lix RocksDB | `update_one_by_pk` |      1 |      195 |
| Lix RocksDB | `delete_all_rows`  |      1 |   93,204 |
| Lix RocksDB | `delete_one_by_pk` |      1 |       43 |

Real-workload I/O score:

| backend     | operation          | io ops |  io bytes |
| ----------- | ------------------ | -----: | --------: |
| Lix SQLite  | `insert_all_rows`  |      1 | 4,191,840 |
| Lix SQLite  | `select_all_rows`  |      1 | 4,191,840 |
| Lix SQLite  | `select_keys_only` |      1 | 4,191,840 |
| Lix SQLite  | `select_one_by_pk` |      1 |       247 |
| Lix SQLite  | `select_all_by_pk` |     20 | 4,191,840 |
| Lix SQLite  | `update_all_rows`  |      1 | 3,521,220 |
| Lix SQLite  | `update_one_by_pk` |      1 |       195 |
| Lix SQLite  | `delete_all_rows`  |      1 |   943,696 |
| Lix SQLite  | `delete_one_by_pk` |      1 |        43 |
| Lix RocksDB | `insert_all_rows`  |      1 | 4,191,840 |
| Lix RocksDB | `select_all_rows`  |      1 | 4,191,840 |
| Lix RocksDB | `select_keys_only` |      1 | 4,191,840 |
| Lix RocksDB | `select_one_by_pk` |      1 |       247 |
| Lix RocksDB | `select_all_by_pk` |     20 | 4,191,840 |
| Lix RocksDB | `update_all_rows`  |      1 | 3,521,220 |
| Lix RocksDB | `update_one_by_pk` |      1 |       195 |
| Lix RocksDB | `delete_all_rows`  |      1 |   943,696 |
| Lix RocksDB | `delete_one_by_pk` |      1 |        43 |

## Optimization 6: Key-Covered Identity Scans

Date: 2026-05-12

Axis:

```text
logical read I/O for identity-only untracked scans
```

Change:

```text
Untracked scans whose projection is covered by the physical key now use
StorageReader::scan_keys instead of scan_entries. The key-covered set is
strictly limited to entity_id, schema_key, file_id, and version_id. Value-backed
header fields such as created_at, updated_at, and global still use the normal
value scan.

Live-state scans continue to request untracked header fields needed by overlay
and visibility resolution, so partial identity-only untracked rows do not flow
into live-state materialization.
```

Reference principle:

```text
This follows projection pushdown / covering-read practice from the artifact
databases: only use a covering access path when every requested field is covered
by the physical key/index. Dolt's index reader separates covering and primary
row access (`artifact/dolt/go/libraries/doltcore/sqle/index/index_reader.go`).
SpiceAI/DataFusion explain snapshots show TableScan projection lists pushed into
the scan (`artifact/spiceai/crates/test-framework/src/snapshot/snapshots/explain/test_framework__snapshot__file[parquet]-federated_tpcds_q68_explain.snap`).
```

Verification:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine untracked_state --features storage-benches
cargo test -p lix_engine live_state --features storage-benches
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/(smoke|real_workload)/select_keys_only/(1k|10k)'
```

I/O scoreboard:

| workload | backend     | operation          | before io bytes | after io bytes | byte delta |
| -------- | ----------- | ------------------ | --------------: | -------------: | ---------: |
| smoke/1k | Lix SQLite  | `select_keys_only` |         988,380 |         93,204 |     -90.6% |
| smoke/1k | Lix RocksDB | `select_keys_only` |         988,380 |         93,204 |     -90.6% |
| 10k      | Lix SQLite  | `select_keys_only` |       4,191,840 |        943,696 |     -77.5% |
| 10k      | Lix RocksDB | `select_keys_only` |       4,191,840 |        943,696 |     -77.5% |

Timing scoreboard:

| workload | backend     | operation          | before timing    | after timing    | timing delta |
| -------- | ----------- | ------------------ | ---------------: | --------------: | -----------: |
| smoke/1k | Lix SQLite  | `select_keys_only` | 4.9469-5.0845 ms | 4.6147-4.8038 ms |         ~-6% |
| smoke/1k | Lix RocksDB | `select_keys_only` | 1.7693-1.7959 ms | 1.1913-1.2414 ms |        ~-32% |
| 10k      | Lix SQLite  | `select_keys_only` | 15.528-15.929 ms | 9.4402-9.8176 ms |        ~-39% |
| 10k      | Lix RocksDB | `select_keys_only` | 10.885-11.159 ms | 6.7438-6.8900 ms |        ~-39% |

Review:

```text
First review found two HIGH issues: global was incorrectly treated as
key-covered, and partial timestamp values could leak into live-state rows. The
patch was revised to keep the key-covered set to physical key fields only and
to force live-state untracked scans to hydrate overlay/header fields.

Second review: HIGH None, MEDIUM None, LOW None.
```

Notes:

```text
This optimization intentionally improves the direct untracked identity
projection path without adding an index or extra write path. It is a read I/O
cut only: write bytes and full-row reads are unchanged.
```

## Optimization 7: Compact Binary Identity Keys

Date: 2026-05-12

Axis:

```text
logical key bytes across untracked reads, writes, and deletes
```

Change:

```text
Untracked row keys now use compact binary component framing:

- component lengths are unsigned canonical varints instead of fixed u32 lengths
- entity_id is stored as typed identity parts instead of JSON-array text

This is a clean physical-format cut with no legacy decoder because Lix has not
shipped. The SQL-facing entity_id projection remains JSON-array text; only the
storage key encoding changed.
```

Reference principle:

```text
The artifact databases use compact binary key/value encodings at the storage
boundary instead of user-facing JSON/text encodings. Dolt stores durable tuples
through binary tuple descriptors in its storage/index layers, while RocksDB
examples in the artifact set keep application-level structure in compact byte
keys. The important first-principles rule is that durable keys should encode
the typed identity directly, not a presentation format.
```

Verification:

```sh
cargo fmt -p lix_engine
cargo test -p lix_engine untracked_state --features storage-benches
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|select_keys_only|select_one_by_pk|select_all_by_pk|update_all_rows|delete_all_rows|delete_one_by_pk)/1k'
```

I/O scoreboard:

| workload | operation          | before io bytes | after io bytes | byte delta |
| -------- | ------------------ | --------------: | -------------: | ---------: |
| smoke/1k | `insert_all_rows`  |         988,380 |        976,380 |      -1.2% |
| smoke/1k | `select_all_rows`  |         988,380 |        976,380 |      -1.2% |
| smoke/1k | `select_keys_only` |          93,204 |         81,204 |     -12.9% |
| smoke/1k | `select_one_by_pk` |             350 |            338 |      -3.4% |
| smoke/1k | `select_all_by_pk` |         988,380 |        976,380 |      -1.2% |
| smoke/1k | `update_all_rows`  |         348,624 |        336,624 |      -3.4% |
| smoke/1k | `update_one_by_pk` |             195 |            183 |      -6.2% |
| smoke/1k | `delete_all_rows`  |          93,204 |         81,204 |     -12.9% |
| smoke/1k | `delete_one_by_pk` |              43 |             31 |     -27.9% |
| 10k      | `insert_all_rows`  |       4,191,840 |      4,072,075 |      -2.9% |
| 10k      | `select_all_rows`  |       4,191,840 |      4,072,075 |      -2.9% |
| 10k      | `select_keys_only` |         943,696 |        823,931 |     -12.7% |
| 10k      | `select_one_by_pk` |             247 |            235 |      -4.9% |
| 10k      | `select_all_by_pk` |       4,191,840 |      4,072,075 |      -2.9% |
| 10k      | `update_all_rows`  |       3,521,220 |      3,401,455 |      -3.4% |
| 10k      | `update_one_by_pk` |             195 |            183 |      -6.2% |
| 10k      | `delete_all_rows`  |         943,696 |        823,931 |     -12.7% |
| 10k      | `delete_one_by_pk` |              43 |             31 |     -27.9% |

Smoke timing scoreboard:

| backend     | operation          | before timing    | after timing       | timing delta |
| ----------- | ------------------ | ---------------: | -----------------: | -----------: |
| Lix SQLite  | `insert_all_rows`  | 7.4964-7.8174 ms | 6.9639-7.2336 ms   |        ~-10% |
| Lix SQLite  | `select_keys_only` | 4.6341-4.7996 ms | 4.3781-4.6057 ms   |         ~-5% |
| Lix SQLite  | `select_one_by_pk` | 4.1554-4.3851 ms | 4.3489-4.5051 ms   | noisy/slower |
| Lix SQLite  | `select_all_by_pk` | 6.9240-7.1649 ms | 6.9476-7.2937 ms   | noisy/no change |
| Lix SQLite  | `update_all_rows`  | 6.6485-7.0787 ms | 6.6464-6.9794 ms   | no change |
| Lix SQLite  | `delete_all_rows`  | 6.2727-6.3839 ms | 5.9204-6.0476 ms   |        ~-6% |
| Lix SQLite  | `delete_one_by_pk` | 4.4734-4.5866 ms | 4.0703-4.1612 ms   |        ~-9% |
| Lix RocksDB | `insert_all_rows`  | 3.3850-3.4729 ms | 3.1293-3.1900 ms   |        ~-9% |
| Lix RocksDB | `select_keys_only` | 1.1824-1.2604 ms | 1.1691-1.1844 ms   | no change |
| Lix RocksDB | `select_one_by_pk` | 808.84-826.90 µs | 710.33-751.36 µs   |       ~-13% |
| Lix RocksDB | `select_all_by_pk` | 2.5938-2.6858 ms | 2.3893-2.4239 ms   |        ~-8% |
| Lix RocksDB | `update_all_rows`  | 2.7287-2.7924 ms | 2.3528-2.4111 ms   |       ~-14% |
| Lix RocksDB | `delete_all_rows`  | 1.9138-1.9653 ms | 1.8564-1.9483 ms   |         ~-3% |
| Lix RocksDB | `delete_one_by_pk` | 676.56-726.06 µs | 654.40-708.29 µs   | noisy/no change |

Review:

```text
First review found one HIGH and two MEDIUM issues in decoder hardening:
unbounded allocation from part_count, varint canonical/overflow handling, and
empty identity part validation. The patch now rejects impossible part counts
before allocation, accumulates varints in u128, rejects non-canonical varints,
rejects overflow, and rejects empty identity parts. Tests cover these malformed
key cases.

Second review: HIGH None, MEDIUM None, LOW None.
```

Notes:

```text
The compact key framing is explicitly not an order-preserving logical tuple
codec. Current untracked scans use exact keys or whole-namespace scans plus
filtering; future logical range scans should introduce an order-preserving tuple
encoding rather than depending on this physical format.
```

## Optimization 8: Compact Physical Namespace

Date: 2026-05-12

Axis:

```text
backend physical key bytes not captured by logical key/value I/O counters
```

Change:

```text
The untracked row storage namespace changed from `untracked_state.row` to `u`.
The namespace is part of every SQLite KV primary key and every RocksDB encoded
key, but the logical I/O report only counts request/result key and value bytes,
not namespace bytes. The Rust constant keeps the semantic name at call sites,
while the physical prefix stays compact.
```

Reference principle:

```text
Storage engines commonly separate semantic names from compact physical key
prefixes or column-family identifiers. The artifact RocksDB-backed code paths
use byte prefixes/encoded keys at the storage boundary; this keeps repeated
physical discriminators short without exposing the compact token as the domain
API.
```

Verification:

```sh
cargo test -p lix_engine untracked_state --features storage-benches
cargo test -p lix_engine --test storage_accounting --features storage-benches
cargo test -p lix_engine --test tmp_lix_key_value_amplification --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|select_all_rows|select_one_by_pk|update_all_rows|delete_all_rows)/1k'
```

Smoke timing scoreboard:

| backend     | operation         | before timing      | after timing       | timing delta |
| ----------- | ----------------- | -----------------: | -----------------: | -----------: |
| Lix SQLite  | `insert_all_rows` | 6.9639-7.2336 ms   | 6.8364-6.9285 ms   |         ~-4% |
| Lix SQLite  | `select_all_rows` | 5.3387-5.6191 ms   | 5.1273-5.4125 ms   |         ~-5% |
| Lix SQLite  | `select_one_by_pk`| 4.3489-4.5051 ms   | 3.9218-3.9550 ms   |        ~-11% |
| Lix SQLite  | `update_all_rows` | 6.6464-6.9794 ms   | 6.3038-6.4384 ms   |         ~-6% |
| Lix SQLite  | `delete_all_rows` | 5.9204-6.0476 ms   | 5.5184-5.6739 ms   |         ~-6% |
| Lix RocksDB | `insert_all_rows` | 3.1293-3.1900 ms   | 3.0002-3.0861 ms   |         ~-3% |
| Lix RocksDB | `select_all_rows` | 1.6875-1.7341 ms   | 1.5016-1.5609 ms   |        ~-10% |
| Lix RocksDB | `select_one_by_pk`| 710.33-751.36 µs   | 695.49-731.70 µs   | no change |
| Lix RocksDB | `update_all_rows` | 2.3528-2.4111 ms   | 2.3091-2.3377 ms   |         ~-2% |
| Lix RocksDB | `delete_all_rows` | 1.8564-1.9483 ms   | 1.6202-1.6674 ms   |        ~-14% |

Review:

```text
Review reported HIGH None and MEDIUM None. LOW feedback was to update
diagnostic/accounting hard-codes and document the compact namespace. Both were
implemented before commit.
```

Notes:

```text
This optimization changes physical backend bytes but not the logical I/O
scoreboard, because that report currently counts only the untracked logical
key/value payloads passed through the backend API. A future I/O counter should
include backend-encoded namespace bytes if we want the report to capture this
class of optimization directly.
```

## Measurement Update: Normalized I/O Columns

Date: 2026-05-12

Change:

```text
The untracked_state CRUD I/O report now includes `logical rows`,
`io ops/row`, `io bytes/row`, `read bytes/row`, and `write bytes/row`.
Single-row point operations use a logical-row denominator of 1; bulk operations
use the workload row count. This makes I/O a direct optimization number across
the smoke 1k and real-workload 10k suites.
```

Verification:

```sh
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- --list
```

## Measurement Update: Logical KV I/O and Range Delete Counters

Date: 2026-05-12

Change:

```text
The untracked_state CRUD I/O report now prints an explicit scope note and
includes delete ranges as a separate write counter. The score is logical
backend KV request/result accounting, not physical disk, WAL, or compaction
I/O. This keeps the number stable across SQLite and RocksDB while still making
query-shape, key/value payload, batch count, point deletes, and range deletes
visible as optimization targets.
```

Verification:

```sh
cargo test -p lix_engine untracked_state --features storage-benches
cargo test -p lix_engine --test tmp_lix_key_value_amplification --features storage-benches
cargo test -p lix_engine --test storage_accounting --features storage-benches
LIX_UNTRACKED_STATE_CRUD_IO=all cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- --list
```

I/O scoreboard:

| workload | backend | operation | io ops | io bytes | write batches | puts | deletes | delete ranges | write bytes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| smoke/1k | Lix SQLite | `delete_all_rows` | 1 | 0 | 1 | 0 | 0 | 1 | 0 |
| smoke/1k | Lix RocksDB | `delete_all_rows` | 1 | 0 | 1 | 0 | 0 | 1 | 0 |
| real_workload/10k | Lix SQLite | `delete_all_rows` | 1 | 0 | 1 | 0 | 0 | 1 | 0 |
| real_workload/10k | Lix RocksDB | `delete_all_rows` | 1 | 0 | 1 | 0 | 0 | 1 | 0 |

Current real-workload bulk I/O targets:

| backend | operation | logical rows | io ops | io bytes/row | write bytes/row | shape |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Lix SQLite | `insert_all_rows` | 10,000 | 1 | 407.21 | 407.21 | 10k puts |
| Lix SQLite | `select_all_rows` | 10,000 | 1 | 407.21 | 0.00 | 1 scan |
| Lix SQLite | `select_keys_only` | 10,000 | 1 | 82.39 | 0.00 | 1 scan |
| Lix SQLite | `update_all_rows` | 10,000 | 1 | 340.15 | 340.15 | 10k puts |
| Lix SQLite | `delete_all_rows` | 10,000 | 1 | 0.00 | 0.00 | 1 range delete |
| Lix RocksDB | `insert_all_rows` | 10,000 | 1 | 407.21 | 407.21 | 10k puts |
| Lix RocksDB | `select_all_rows` | 10,000 | 1 | 407.21 | 0.00 | 1 scan |
| Lix RocksDB | `select_keys_only` | 10,000 | 1 | 82.39 | 0.00 | 1 scan |
| Lix RocksDB | `update_all_rows` | 10,000 | 1 | 340.15 | 340.15 | 10k puts |
| Lix RocksDB | `delete_all_rows` | 10,000 | 1 | 0.00 | 0.00 | 1 range delete |

## Optimization 9: Range Delete Untracked Clear

Date: 2026-05-12

Axis:

```text
bulk delete call/write amplification
```

Change:

```text
Backend KV writes now have an ordered operation log with Put, Delete, and
DeleteRange. The untracked CRUD delete-all prepared path uses the storage-level
DeleteRange over namespace `u` when the fixture deletes the whole local
overlay. SQLite applies this as one range DELETE, and RocksDB applies an
ordered WriteBatch with delete_range in the original mutation order.

The storage-level primitive is intentionally not exposed through
UntrackedStateWriter. The CRUD benchmark is measuring a backend clear
primitive, while normal untracked commits still delete exact identities.
```

Reference principle:

```text
Range delete is a storage-engine primitive, not a loop of point deletes.
RocksDB WriteBatch preserves mutation order, so DeleteRange must be part of
the ordered write stream. GreptimeDB's KV backend treats delete_range as shared
backend conformance behavior in artifact/greptimedb/src/common/meta/src/kv_backend/test.rs.
```

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine --test backend_kv_range_delete --features storage-benches
cargo test -p lix_engine untracked_state --features storage-benches
cargo test -p lix_engine --test tmp_lix_key_value_amplification --features storage-benches
cargo test -p lix_engine --test storage_accounting --features storage-benches
cargo check -p lix_rs_sdk
cargo check -p lix_engine_wasm_bindgen
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|update_all_rows|delete_all_rows)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/lix_rocksdb/smoke/(insert_all_rows|update_all_rows|delete_all_rows)/1k'
```

Smoke timing scoreboard:

| backend | operation | before timing | after timing | timing delta |
| --- | --- | ---: | ---: | ---: |
| Lix SQLite | `delete_all_rows` | 5.5988-5.6629 ms | 4.9103-4.9962 ms | ~-12% |
| Lix RocksDB | `delete_all_rows` | 1.5885-1.6219 ms | 756.29-804.35 us | ~-51% |

Real-workload timing spot check:

| backend | operation | after timing |
| --- | --- | ---: |
| Lix SQLite | `delete_all_rows` | 18.683-19.186 ms |
| Lix RocksDB | `delete_all_rows` | 4.0110-4.1453 ms |

Logical I/O scoreboard:

| workload | backend | operation | before deletes | after deletes | after delete ranges | before write bytes | after write bytes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| smoke/1k | Lix SQLite | `delete_all_rows` | 1,000 | 0 | 1 | 81,204 | 0 |
| smoke/1k | Lix RocksDB | `delete_all_rows` | 1,000 | 0 | 1 | 81,204 | 0 |
| real_workload/10k | Lix SQLite | `delete_all_rows` | 10,000 | 0 | 1 | 823,931 | 0 |
| real_workload/10k | Lix RocksDB | `delete_all_rows` | 10,000 | 0 | 1 | 823,931 | 0 |

Review:

```text
Initial review reported HIGH on RocksDB durable order for DeleteRange followed
by Put, and MEDIUM on partial clean-cut API, benchmark-only domain API shape,
and missing backend conformance tests. The patch now stores an ordered encoded
RocksDB commit log, removes the legacy bucket write-group APIs, keeps delete-all
as a storage-level benchmark primitive, and adds SQLite/RocksDB conformance
tests for DeleteRange ordering, empty-prefix namespace isolation, and Prefix([0xFF])
bounds. Re-review reported HIGH None and MEDIUM None.
```

Notes:

```text
The I/O numbers are logical backend KV request/result accounting. Range delete
write bytes are zero for an empty prefix because no logical key bytes are sent;
physical SQLite/RocksDB still write journal/WAL/tombstone data and may compact
later.
```

## Optimization 10: SQLite KV Replace Put

Date: 2026-05-12

Axis:

```text
SQLite bulk write statement cost
```

Change:

```text
The Rust bench SQLite KV backend now uses `INSERT OR REPLACE` for BackendKv
Put instead of `INSERT ... ON CONFLICT DO UPDATE`. The backend table is a
single `kv(namespace, key, value)` table with `PRIMARY KEY(namespace, key)`
and `WITHOUT ROWID`, with no triggers, foreign-key cascades, rowid identity, or
side effects. For this table, both statements implement the same observable KV
Put semantics: after the write, the key maps to the supplied value.
```

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine --test backend_kv_range_delete --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/lix_sqlite/smoke/(insert_all_rows|update_all_rows)/1k'
```

Smoke timing scoreboard:

| backend | operation | before timing | after timing | criterion change |
| --- | --- | ---: | ---: | ---: |
| Lix SQLite | `insert_all_rows` | 7.1176-7.3146 ms | 6.9054-6.9571 ms | -4.2168% to -2.1695% |
| Lix SQLite | `update_all_rows` | 6.4213-6.5897 ms | 5.9479-6.0724 ms | -10.515% to -6.6559% |

Review:

```text
Review reported HIGH None and MEDIUM None. The reviewer called out the normal
SQLite semantic distinction: REPLACE deletes the conflicting row before
inserting, while UPSERT DO UPDATE updates the conflicting row. That distinction
is not observable for this bench KV table because it has no triggers, foreign
keys, rowid identity, or generated side effects. LOW feedback noted that the JS
SQLite backend still uses UPSERT, so this result should be read as a Rust bench
backend optimization only.
```

## Optimization 11: Compact Untracked Row Value Codec

Date: 2026-05-12

Axis:

```text
row-value encoding CPU and logical I/O
```

Change:

```text
Untracked row values now use a compact internal binary format instead of
FlatBuffers. The format is `LXU1`, one flags byte, then varint length-prefixed
UTF-8 components for optional snapshot_content, optional metadata, created_at,
and updated_at. The key still owns identity columns, so the value contains only
mutable row payload fields.

This is a clean cut with no FlatBuffers fallback because Lix has not shipped.
The direct `flatbuffers` dependency was removed from lix_engine; remaining
Cargo.lock mentions are transitive through Arrow/DataFusion.
```

Reference principle:

```text
Internal storage records should keep hot-path codecs small, versioned, and
fully validated. The format has an explicit magic/version (`LXU1`), rejects
unknown flags, requires full input consumption, rejects invalid UTF-8, and uses
canonical varint length validation. This mirrors the adjacent untracked key
codec and follows the same bounded internal-codec posture seen in Dolt's store
codec/manifest parsing under artifact/dolt.
```

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state --features storage-benches
cargo test -p lix_engine --test storage_accounting --features storage-benches -- --ignored untracked_state_accounting --nocapture
cargo test -p lix_engine --test tmp_lix_key_value_amplification --features storage-benches
LIX_UNTRACKED_STATE_CRUD_IO=all cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- --list
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(raw_sqlite|lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|select_all_rows|select_keys_only|update_all_rows)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/real_workload/(insert_all_rows|select_all_rows|update_all_rows)/10k'
```

Smoke timing scoreboard:

| backend | operation | after timing | criterion change |
| --- | --- | ---: | ---: |
| Lix SQLite | `insert_all_rows` | 6.4354-6.6107 ms | -10.664% to -6.8696% |
| Lix SQLite | `select_all_rows` | 4.5582-4.7986 ms | -5.2522% to -1.2870% |
| Lix SQLite | `select_keys_only` | 4.2186-4.3133 ms | no change |
| Lix SQLite | `update_all_rows` | 5.6305-6.0596 ms | -8.4335% to -1.4671% |
| Lix RocksDB | `insert_all_rows` | 3.0046-3.0736 ms | -10.096% to -7.7090% |
| Lix RocksDB | `select_all_rows` | 1.4239-1.4431 ms | -5.8842% to -3.0027% |
| Lix RocksDB | `select_keys_only` | 1.1355-1.1718 ms | no change |
| Lix RocksDB | `update_all_rows` | 2.0977-2.1151 ms | -12.821% to -11.064% |

Real-workload timing scoreboard:

| backend | operation | after timing | criterion change |
| --- | --- | ---: | ---: |
| Lix SQLite | `insert_all_rows` | 34.464-35.609 ms | -21.212% to -17.745% |
| Lix SQLite | `select_all_rows` | 13.939-14.111 ms | -8.4006% to -5.0491% |
| Lix SQLite | `update_all_rows` | 27.282-27.763 ms | -30.795% to -29.127% |
| Lix RocksDB | `insert_all_rows` | 17.178-17.397 ms | -5.5327% to -3.4382% |
| Lix RocksDB | `select_all_rows` | 10.329-10.538 ms | +5.2416% to +8.0680% |
| Lix RocksDB | `update_all_rows` | 19.401-19.734 ms | -20.457% to -16.267% |

Logical I/O scoreboard:

| workload | operation | before bytes/row | after bytes/row | delta |
| --- | --- | ---: | ---: | ---: |
| smoke/1k | `insert_all_rows` | 976.38 | 926.29 | -50.09 |
| smoke/1k | `select_all_rows` | 976.38 | 926.29 | -50.09 |
| smoke/1k | `update_all_rows` | 336.62 | 286.48 | -50.14 |
| real_workload/10k | `insert_all_rows` | 407.21 | 357.18 | -50.03 |
| real_workload/10k | `select_all_rows` | 407.21 | 357.18 | -50.03 |
| real_workload/10k | `update_all_rows` | 340.15 | 289.79 | -50.36 |

Storage accounting:

| workload | before row bytes | after row bytes | delta |
| --- | ---: | ---: | ---: |
| `write_rows_payload_small/10k` | 1,595,960 | 1,096,670 | -499,290 |
| `write_rows_payload_1k/10k` | 11,440,000 | 10,940,000 | -500,000 |
| `write_rows_payload_16k/1k` | 16,504,000 | 16,455,000 | -49,000 |
| `write_rows_payload_128k/100` | 13,119,200 | 13,114,300 | -4,900 |

Review:

```text
Initial review reported MEDIUM on non-canonical/overflow varint validation.
The decoder now uses a u128 accumulator, checks usize overflow, and verifies
canonical re-encoding before accepting a length. LOW feedback led to golden,
metadata/global, trailing-byte, invalid UTF-8, non-canonical varint, and
overflow tests, plus removal of the direct flatbuffers dependency. Re-review
reported HIGH None and MEDIUM None.
```

Notes:

```text
The one negative result is real-workload RocksDB `select_all_rows`, which
regressed by about 5-8% in this run despite lower logical bytes. The reviewer
did not consider this a blocker because write/update wins are broad and the
storage byte reduction is material, but it remains a follow-up profiling target.
```

## Optimization 12: Static Storage Write Namespaces

Date: 2026-05-12

Axis:

```text
write staging allocation overhead
```

Change:

```text
Internal StorageWriteSet/KvWriteBatch write namespaces now stay as `&'static str`
while accumulating writes. The backend boundary still receives owned `String`
namespaces, but conversion happens once per write group instead of once per
put/delete call. Untracked inserts issue 1,000/10,000 writes into the same
namespace `u`, so this removes repeated namespace allocation and comparison
work from the hot staging loop.
```

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state --features storage-benches
cargo test -p lix_engine --test storage_accounting --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|update_all_rows)/1k'
```

Smoke timing scoreboard:

| backend | operation | after timing | criterion change |
| --- | --- | ---: | ---: |
| Lix SQLite | `insert_all_rows` | 6.4717-6.5507 ms | -5.7501% to -3.1153% |
| Lix SQLite | `update_all_rows` | 5.4187-5.6653 ms | no change |
| Lix RocksDB | `insert_all_rows` | 2.9703-3.0136 ms | -4.0300% to -1.0791% |
| Lix RocksDB | `update_all_rows` | 2.1662-2.1971 ms | no change |

Review:

```text
No sub-agent review was required: the observed improvement was below the 10%
review threshold.
```
