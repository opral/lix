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

| backend     | operation         | before puts/deletes | after puts/deletes | before write bytes | after write bytes |
| ----------- | ----------------- | ------------------: | -----------------: | -----------------: | ----------------: |
| Lix SQLite  | `insert_all_rows` |            1000 / 0 |           2000 / 0 |          1,114,072 |         1,264,276 |
| Lix SQLite  | `update_all_rows` |            1000 / 0 |           2000 / 0 |            474,316 |           624,520 |
| Lix SQLite  | `delete_all_rows` |            0 / 1000 |           0 / 2000 |             93,204 |           186,408 |
| Lix RocksDB | `insert_all_rows` |            1000 / 0 |           2000 / 0 |          1,114,072 |         1,264,276 |
| Lix RocksDB | `update_all_rows` |            1000 / 0 |           2000 / 0 |            474,316 |           624,520 |
| Lix RocksDB | `delete_all_rows` |            0 / 1000 |           0 / 2000 |             93,204 |           186,408 |

Smoke timing scoreboard:

| backend     | operation          |    before timing |     after timing | timing read                          |
| ----------- | ------------------ | ---------------: | ---------------: | ------------------------------------ |
| Lix SQLite  | `select_keys_only` | 5.1347-5.2920 ms | 5.8884-6.0097 ms | slower after scalar-preserving index |
| Lix RocksDB | `select_keys_only` | 1.6006-1.6339 ms | 1.4754-1.5353 ms | modest improvement                   |

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

| backend     | operation          |    before timing |     after timing | timing delta |
| ----------- | ------------------ | ---------------: | ---------------: | -----------: |
| Lix SQLite  | `select_all_by_pk` | 9.8336-11.246 ms | 7.7282-7.8977 ms |        ~-27% |
| Lix RocksDB | `select_all_by_pk` | 3.1604-3.2220 ms | 2.7012-2.7677 ms |        ~-15% |

Real-workload timing snapshot:

| backend     | operation          |     after timing |
| ----------- | ------------------ | ---------------: |
| Lix SQLite  | `select_all_by_pk` | 33.817-34.455 ms |
| Lix RocksDB | `select_all_by_pk` | 21.755-21.992 ms |

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

| backend     | operation         | before puts/deletes | after puts/deletes | before bytes | after bytes |
| ----------- | ----------------- | ------------------: | -----------------: | -----------: | ----------: |
| Lix SQLite  | `insert_all_rows` |            2000 / 0 |           1000 / 0 |    1,264,276 |   1,114,072 |
| Lix SQLite  | `update_all_rows` |            2000 / 0 |           1000 / 0 |      624,520 |     474,316 |
| Lix SQLite  | `delete_all_rows` |            0 / 2000 |           0 / 1000 |      186,408 |      93,204 |
| Lix RocksDB | `insert_all_rows` |            2000 / 0 |           1000 / 0 |    1,264,276 |   1,114,072 |
| Lix RocksDB | `update_all_rows` |            2000 / 0 |           1000 / 0 |      624,520 |     474,316 |
| Lix RocksDB | `delete_all_rows` |            0 / 2000 |           0 / 1000 |      186,408 |      93,204 |

Smoke read I/O scoreboard:

| backend     | operation          | before read bytes | after read bytes | read-byte delta |
| ----------- | ------------------ | ----------------: | ---------------: | --------------: |
| Lix SQLite  | `select_keys_only` |           150,204 |        1,020,868 |         +579.7% |
| Lix RocksDB | `select_keys_only` |           150,204 |        1,020,868 |         +579.7% |

Smoke timing scoreboard:

| backend     | operation          |    before timing |     after timing | timing delta |
| ----------- | ------------------ | ---------------: | ---------------: | -----------: |
| Lix SQLite  | `insert_all_rows`  | 11.112-11.333 ms | 8.4015-8.6852 ms |        ~-24% |
| Lix SQLite  | `select_keys_only` | 5.4289-5.5678 ms | 5.2545-5.3894 ms |         ~-3% |
| Lix SQLite  | `update_all_rows`  | 9.4389-9.5586 ms | 7.8316-7.9436 ms |        ~-17% |
| Lix SQLite  | `delete_all_rows`  | 8.8763-9.1404 ms | 7.0143-7.0802 ms |        ~-22% |
| Lix RocksDB | `insert_all_rows`  | 4.9880-5.2201 ms | 4.0144-4.1236 ms |        ~-20% |
| Lix RocksDB | `select_keys_only` | 1.3815-1.4017 ms | 1.5747-1.5964 ms |        ~+14% |
| Lix RocksDB | `update_all_rows`  | 4.4034-4.5340 ms | 3.2577-3.2920 ms |        ~-26% |
| Lix RocksDB | `delete_all_rows`  | 3.0574-3.0912 ms | 2.2155-2.2492 ms |        ~-27% |

Real-workload I/O snapshot:

| backend     | operation          | read bytes | puts/deletes | write bytes |
| ----------- | ------------------ | ---------: | -----------: | ----------: |
| Lix SQLite  | `insert_all_rows`  |          0 |    10000 / 0 |   5,460,528 |
| Lix SQLite  | `select_all_rows`  |  4,516,832 |        0 / 0 |           0 |
| Lix SQLite  | `select_keys_only` |  4,516,832 |        0 / 0 |           0 |
| Lix SQLite  | `update_all_rows`  |          0 |    10000 / 0 |   4,789,908 |
| Lix SQLite  | `delete_all_rows`  |          0 |    0 / 10000 |     943,696 |
| Lix RocksDB | `insert_all_rows`  |          0 |    10000 / 0 |   5,460,528 |
| Lix RocksDB | `select_all_rows`  |  4,516,832 |        0 / 0 |           0 |
| Lix RocksDB | `select_keys_only` |  4,516,832 |        0 / 0 |           0 |
| Lix RocksDB | `update_all_rows`  |          0 |    10000 / 0 |   4,789,908 |
| Lix RocksDB | `delete_all_rows`  |          0 |    0 / 10000 |     943,696 |

Real-workload timing snapshot:

| backend     | operation          |     after timing |
| ----------- | ------------------ | ---------------: |
| Lix SQLite  | `insert_all_rows`  | 60.827-63.465 ms |
| Lix SQLite  | `select_keys_only` | 15.528-15.929 ms |
| Lix SQLite  | `update_all_rows`  | 53.970-55.711 ms |
| Lix SQLite  | `delete_all_rows`  | 39.760-40.793 ms |
| Lix RocksDB | `insert_all_rows`  | 39.827-40.662 ms |
| Lix RocksDB | `select_keys_only` | 10.885-11.159 ms |
| Lix RocksDB | `update_all_rows`  | 29.890-32.065 ms |
| Lix RocksDB | `delete_all_rows`  | 18.187-18.624 ms |

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

| backend     | operation          |    before timing |     after timing |      timing delta |
| ----------- | ------------------ | ---------------: | ---------------: | ----------------: |
| Lix SQLite  | `insert_all_rows`  | 8.4015-8.6852 ms | 7.3462-7.4688 ms |             ~-14% |
| Lix SQLite  | `select_all_rows`  | 5.3115-5.3508 ms | 4.9022-5.0208 ms |              ~-7% |
| Lix SQLite  | `select_keys_only` | 5.4276-5.6383 ms | 4.9786-5.0484 ms |              ~-9% |
| Lix SQLite  | `select_one_by_pk` | 4.2838-4.3564 ms | 3.8850-4.0993 ms |              ~-7% |
| Lix SQLite  | `select_all_by_pk` | 7.3265-7.7430 ms | 6.6975-6.7973 ms |             ~-10% |
| Lix SQLite  | `update_all_rows`  | 7.8754-8.0260 ms | 6.7130-6.9206 ms |             ~-14% |
| Lix SQLite  | `update_one_by_pk` | 4.3991-4.5165 ms | 3.8040-4.8891 ms | noisy improvement |
| Lix RocksDB | `insert_all_rows`  | 4.0083-4.0900 ms | 3.6245-3.7255 ms |              ~-9% |
| Lix RocksDB | `select_all_rows`  | 1.6062-1.6542 ms | 1.5225-1.5741 ms |              ~-6% |
| Lix RocksDB | `select_keys_only` | 1.5795-1.6126 ms | 1.5114-1.5333 ms |              ~-4% |
| Lix RocksDB | `select_all_by_pk` | 2.5475-2.5627 ms | 2.4169-2.4360 ms |              ~-5% |
| Lix RocksDB | `update_all_rows`  | 3.2083-3.2669 ms | 2.8141-2.9118 ms |             ~-13% |
| Lix RocksDB | `update_one_by_pk` | 644.32-656.44 µs | 616.13-642.18 µs |              ~-4% |

Real-workload timing snapshot:

| backend     | operation          |     after timing |
| ----------- | ------------------ | ---------------: |
| Lix SQLite  | `insert_all_rows`  | 49.065-49.767 ms |
| Lix SQLite  | `select_all_rows`  | 14.786-15.298 ms |
| Lix SQLite  | `select_all_by_pk` | 31.818-32.202 ms |
| Lix SQLite  | `update_all_rows`  | 43.215-43.781 ms |
| Lix RocksDB | `insert_all_rows`  | 22.254-22.571 ms |
| Lix RocksDB | `select_all_rows`  | 9.7033-9.8601 ms |
| Lix RocksDB | `select_all_by_pk` | 19.369-19.619 ms |
| Lix RocksDB | `update_all_rows`  | 24.054-24.354 ms |

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

| backend     | operation          |    before timing |     after timing |    timing delta |
| ----------- | ------------------ | ---------------: | ---------------: | --------------: |
| Lix SQLite  | `insert_all_rows`  | 7.3462-7.4688 ms | 7.0196-7.2650 ms |            ~-6% |
| Lix SQLite  | `update_all_rows`  | 6.7130-6.9206 ms | 6.4660-6.7457 ms |            ~-4% |
| Lix SQLite  | `delete_all_rows`  | 6.9334-7.0701 ms | 6.1155-6.2499 ms |           ~-12% |
| Lix SQLite  | `update_one_by_pk` | 3.8040-4.8891 ms | 3.9106-4.0195 ms | noisy/no change |
| Lix SQLite  | `delete_one_by_pk` | 4.4915-4.6263 ms | 4.2442-4.3429 ms |            ~-6% |
| Lix RocksDB | `insert_all_rows`  | 3.6245-3.7255 ms | 3.2501-3.2761 ms |           ~-10% |
| Lix RocksDB | `update_all_rows`  | 2.8141-2.9118 ms | 2.4815-2.5499 ms |           ~-12% |
| Lix RocksDB | `delete_all_rows`  | 2.1826-2.2233 ms | 1.8543-1.8843 ms |           ~-15% |
| Lix RocksDB | `update_one_by_pk` | 616.13-642.18 µs | 646.29-677.92 µs | regressed/noisy |
| Lix RocksDB | `delete_one_by_pk` | 682.09-721.56 µs | 679.40-741.49 µs |  no improvement |

Real-workload timing scoreboard:

| backend     | operation         |    before timing |     after timing | timing delta |
| ----------- | ----------------- | ---------------: | ---------------: | -----------: |
| Lix SQLite  | `insert_all_rows` | 49.065-49.767 ms | 42.830-44.068 ms |        ~-12% |
| Lix SQLite  | `update_all_rows` | 43.215-43.781 ms | 38.958-39.573 ms |        ~-10% |
| Lix SQLite  | `delete_all_rows` | 39.760-40.793 ms | 29.272-30.118 ms |        ~-26% |
| Lix RocksDB | `insert_all_rows` | 22.254-22.571 ms | 17.956-18.269 ms |        ~-19% |
| Lix RocksDB | `update_all_rows` | 24.054-24.354 ms | 23.390-24.545 ms |    no change |
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

| workload | backend     | operation          |    before timing |     after timing | timing delta |
| -------- | ----------- | ------------------ | ---------------: | ---------------: | -----------: |
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

| backend     | operation          |    before timing |     after timing |    timing delta |
| ----------- | ------------------ | ---------------: | ---------------: | --------------: |
| Lix SQLite  | `insert_all_rows`  | 7.4964-7.8174 ms | 6.9639-7.2336 ms |           ~-10% |
| Lix SQLite  | `select_keys_only` | 4.6341-4.7996 ms | 4.3781-4.6057 ms |            ~-5% |
| Lix SQLite  | `select_one_by_pk` | 4.1554-4.3851 ms | 4.3489-4.5051 ms |    noisy/slower |
| Lix SQLite  | `select_all_by_pk` | 6.9240-7.1649 ms | 6.9476-7.2937 ms | noisy/no change |
| Lix SQLite  | `update_all_rows`  | 6.6485-7.0787 ms | 6.6464-6.9794 ms |       no change |
| Lix SQLite  | `delete_all_rows`  | 6.2727-6.3839 ms | 5.9204-6.0476 ms |            ~-6% |
| Lix SQLite  | `delete_one_by_pk` | 4.4734-4.5866 ms | 4.0703-4.1612 ms |            ~-9% |
| Lix RocksDB | `insert_all_rows`  | 3.3850-3.4729 ms | 3.1293-3.1900 ms |            ~-9% |
| Lix RocksDB | `select_keys_only` | 1.1824-1.2604 ms | 1.1691-1.1844 ms |       no change |
| Lix RocksDB | `select_one_by_pk` | 808.84-826.90 µs | 710.33-751.36 µs |           ~-13% |
| Lix RocksDB | `select_all_by_pk` | 2.5938-2.6858 ms | 2.3893-2.4239 ms |            ~-8% |
| Lix RocksDB | `update_all_rows`  | 2.7287-2.7924 ms | 2.3528-2.4111 ms |           ~-14% |
| Lix RocksDB | `delete_all_rows`  | 1.9138-1.9653 ms | 1.8564-1.9483 ms |            ~-3% |
| Lix RocksDB | `delete_one_by_pk` | 676.56-726.06 µs | 654.40-708.29 µs | noisy/no change |

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

| backend     | operation          |    before timing |     after timing | timing delta |
| ----------- | ------------------ | ---------------: | ---------------: | -----------: |
| Lix SQLite  | `insert_all_rows`  | 6.9639-7.2336 ms | 6.8364-6.9285 ms |         ~-4% |
| Lix SQLite  | `select_all_rows`  | 5.3387-5.6191 ms | 5.1273-5.4125 ms |         ~-5% |
| Lix SQLite  | `select_one_by_pk` | 4.3489-4.5051 ms | 3.9218-3.9550 ms |        ~-11% |
| Lix SQLite  | `update_all_rows`  | 6.6464-6.9794 ms | 6.3038-6.4384 ms |         ~-6% |
| Lix SQLite  | `delete_all_rows`  | 5.9204-6.0476 ms | 5.5184-5.6739 ms |         ~-6% |
| Lix RocksDB | `insert_all_rows`  | 3.1293-3.1900 ms | 3.0002-3.0861 ms |         ~-3% |
| Lix RocksDB | `select_all_rows`  | 1.6875-1.7341 ms | 1.5016-1.5609 ms |        ~-10% |
| Lix RocksDB | `select_one_by_pk` | 710.33-751.36 µs | 695.49-731.70 µs |    no change |
| Lix RocksDB | `update_all_rows`  | 2.3528-2.4111 ms | 2.3091-2.3377 ms |         ~-2% |
| Lix RocksDB | `delete_all_rows`  | 1.8564-1.9483 ms | 1.6202-1.6674 ms |        ~-14% |

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

| workload          | backend     | operation         | io ops | io bytes | write batches | puts | deletes | delete ranges | write bytes |
| ----------------- | ----------- | ----------------- | -----: | -------: | ------------: | ---: | ------: | ------------: | ----------: |
| smoke/1k          | Lix SQLite  | `delete_all_rows` |      1 |        0 |             1 |    0 |       0 |             1 |           0 |
| smoke/1k          | Lix RocksDB | `delete_all_rows` |      1 |        0 |             1 |    0 |       0 |             1 |           0 |
| real_workload/10k | Lix SQLite  | `delete_all_rows` |      1 |        0 |             1 |    0 |       0 |             1 |           0 |
| real_workload/10k | Lix RocksDB | `delete_all_rows` |      1 |        0 |             1 |    0 |       0 |             1 |           0 |

Current real-workload bulk I/O targets:

| backend     | operation          | logical rows | io ops | io bytes/row | write bytes/row | shape          |
| ----------- | ------------------ | -----------: | -----: | -----------: | --------------: | -------------- |
| Lix SQLite  | `insert_all_rows`  |       10,000 |      1 |       407.21 |          407.21 | 10k puts       |
| Lix SQLite  | `select_all_rows`  |       10,000 |      1 |       407.21 |            0.00 | 1 scan         |
| Lix SQLite  | `select_keys_only` |       10,000 |      1 |        82.39 |            0.00 | 1 scan         |
| Lix SQLite  | `update_all_rows`  |       10,000 |      1 |       340.15 |          340.15 | 10k puts       |
| Lix SQLite  | `delete_all_rows`  |       10,000 |      1 |         0.00 |            0.00 | 1 range delete |
| Lix RocksDB | `insert_all_rows`  |       10,000 |      1 |       407.21 |          407.21 | 10k puts       |
| Lix RocksDB | `select_all_rows`  |       10,000 |      1 |       407.21 |            0.00 | 1 scan         |
| Lix RocksDB | `select_keys_only` |       10,000 |      1 |        82.39 |            0.00 | 1 scan         |
| Lix RocksDB | `update_all_rows`  |       10,000 |      1 |       340.15 |          340.15 | 10k puts       |
| Lix RocksDB | `delete_all_rows`  |       10,000 |      1 |         0.00 |            0.00 | 1 range delete |

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

| backend     | operation         |    before timing |     after timing | timing delta |
| ----------- | ----------------- | ---------------: | ---------------: | -----------: |
| Lix SQLite  | `delete_all_rows` | 5.5988-5.6629 ms | 4.9103-4.9962 ms |        ~-12% |
| Lix RocksDB | `delete_all_rows` | 1.5885-1.6219 ms | 756.29-804.35 us |        ~-51% |

Real-workload timing spot check:

| backend     | operation         |     after timing |
| ----------- | ----------------- | ---------------: |
| Lix SQLite  | `delete_all_rows` | 18.683-19.186 ms |
| Lix RocksDB | `delete_all_rows` | 4.0110-4.1453 ms |

Logical I/O scoreboard:

| workload          | backend     | operation         | before deletes | after deletes | after delete ranges | before write bytes | after write bytes |
| ----------------- | ----------- | ----------------- | -------------: | ------------: | ------------------: | -----------------: | ----------------: |
| smoke/1k          | Lix SQLite  | `delete_all_rows` |          1,000 |             0 |                   1 |             81,204 |                 0 |
| smoke/1k          | Lix RocksDB | `delete_all_rows` |          1,000 |             0 |                   1 |             81,204 |                 0 |
| real_workload/10k | Lix SQLite  | `delete_all_rows` |         10,000 |             0 |                   1 |            823,931 |                 0 |
| real_workload/10k | Lix RocksDB | `delete_all_rows` |         10,000 |             0 |                   1 |            823,931 |                 0 |

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

| backend    | operation         |    before timing |     after timing |     criterion change |
| ---------- | ----------------- | ---------------: | ---------------: | -------------------: |
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

| backend     | operation          |     after timing |     criterion change |
| ----------- | ------------------ | ---------------: | -------------------: |
| Lix SQLite  | `insert_all_rows`  | 6.4354-6.6107 ms | -10.664% to -6.8696% |
| Lix SQLite  | `select_all_rows`  | 4.5582-4.7986 ms | -5.2522% to -1.2870% |
| Lix SQLite  | `select_keys_only` | 4.2186-4.3133 ms |            no change |
| Lix SQLite  | `update_all_rows`  | 5.6305-6.0596 ms | -8.4335% to -1.4671% |
| Lix RocksDB | `insert_all_rows`  | 3.0046-3.0736 ms | -10.096% to -7.7090% |
| Lix RocksDB | `select_all_rows`  | 1.4239-1.4431 ms | -5.8842% to -3.0027% |
| Lix RocksDB | `select_keys_only` | 1.1355-1.1718 ms |            no change |
| Lix RocksDB | `update_all_rows`  | 2.0977-2.1151 ms | -12.821% to -11.064% |

Real-workload timing scoreboard:

| backend     | operation         |     after timing |     criterion change |
| ----------- | ----------------- | ---------------: | -------------------: |
| Lix SQLite  | `insert_all_rows` | 34.464-35.609 ms | -21.212% to -17.745% |
| Lix SQLite  | `select_all_rows` | 13.939-14.111 ms | -8.4006% to -5.0491% |
| Lix SQLite  | `update_all_rows` | 27.282-27.763 ms | -30.795% to -29.127% |
| Lix RocksDB | `insert_all_rows` | 17.178-17.397 ms | -5.5327% to -3.4382% |
| Lix RocksDB | `select_all_rows` | 10.329-10.538 ms | +5.2416% to +8.0680% |
| Lix RocksDB | `update_all_rows` | 19.401-19.734 ms | -20.457% to -16.267% |

Logical I/O scoreboard:

| workload          | operation         | before bytes/row | after bytes/row |  delta |
| ----------------- | ----------------- | ---------------: | --------------: | -----: |
| smoke/1k          | `insert_all_rows` |           976.38 |          926.29 | -50.09 |
| smoke/1k          | `select_all_rows` |           976.38 |          926.29 | -50.09 |
| smoke/1k          | `update_all_rows` |           336.62 |          286.48 | -50.14 |
| real_workload/10k | `insert_all_rows` |           407.21 |          357.18 | -50.03 |
| real_workload/10k | `select_all_rows` |           407.21 |          357.18 | -50.03 |
| real_workload/10k | `update_all_rows` |           340.15 |          289.79 | -50.36 |

Storage accounting:

| workload                       | before row bytes | after row bytes |    delta |
| ------------------------------ | ---------------: | --------------: | -------: |
| `write_rows_payload_small/10k` |        1,595,960 |       1,096,670 | -499,290 |
| `write_rows_payload_1k/10k`    |       11,440,000 |      10,940,000 | -500,000 |
| `write_rows_payload_16k/1k`    |       16,504,000 |      16,455,000 |  -49,000 |
| `write_rows_payload_128k/100`  |       13,119,200 |      13,114,300 |   -4,900 |

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

| backend     | operation         |     after timing |     criterion change |
| ----------- | ----------------- | ---------------: | -------------------: |
| Lix SQLite  | `insert_all_rows` | 6.4717-6.5507 ms | -5.7501% to -3.1153% |
| Lix SQLite  | `update_all_rows` | 5.4187-5.6653 ms |            no change |
| Lix RocksDB | `insert_all_rows` | 2.9703-3.0136 ms | -4.0300% to -1.0791% |
| Lix RocksDB | `update_all_rows` | 2.1662-2.1971 ms |            no change |

Review:

```text
No sub-agent review was required: the observed improvement was below the 10%
review threshold.
```

## Optimization 13: Pre-size Untracked Row Keys

Date: 2026-05-12

Axis:

```text
key encoding allocation overhead
```

Change:

```text
Untracked row key encoding now computes the exact encoded key length before
allocation and uses `Vec::with_capacity`. The key format itself is unchanged:
varint length-prefixed version/schema/entity components, a file marker byte,
and an optional file component.
```

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state --features storage-benches
cargo test -p lix_engine untracked_state::storage::tests::row_key_capacity_matches_encoded_length --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|update_all_rows)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/real_workload/(insert_all_rows|update_all_rows)/10k'
```

Smoke timing scoreboard:

| backend     | operation         |     after timing |     criterion change |
| ----------- | ----------------- | ---------------: | -------------------: |
| Lix SQLite  | `insert_all_rows` | 6.2908-6.5005 ms |            no change |
| Lix SQLite  | `update_all_rows` | 5.3675-5.7329 ms |            no change |
| Lix RocksDB | `insert_all_rows` | 2.8327-2.8934 ms | -5.3882% to -3.2482% |
| Lix RocksDB | `update_all_rows` | 1.9386-1.9769 ms | -10.766% to -8.6780% |

Real-workload timing scoreboard:

| backend     | operation         |     after timing |     criterion change |
| ----------- | ----------------- | ---------------: | -------------------: |
| Lix SQLite  | `insert_all_rows` | 35.126-49.489 ms |            no change |
| Lix SQLite  | `update_all_rows` | 26.065-26.967 ms | -5.5578% to -1.9782% |
| Lix RocksDB | `insert_all_rows` | 15.753-17.072 ms | -9.2094% to -1.4494% |
| Lix RocksDB | `update_all_rows` | 17.239-17.664 ms | -12.243% to -9.6426% |

Review:

```text
Sub-agent review reported HIGH None and MEDIUM None. LOW feedback noted that
the sizing helper duplicates key framing rules, so a focused unit test now
asserts encoded key capacity exactly matches encoded length for null-file,
file-id, tuple identity, and varint-boundary component shapes.
```

## Optimization 14: Fused Untracked Scan Materialization

Date: 2026-05-12

Axis:

```text
read scan CPU/allocation and selective-filter hydration I/O
```

Change:

```text
Untracked scans now decode, filter, limit, and materialize in one pass instead
of first collecting every canonical row or identity into an intermediate Vec.
Unfiltered scans push LIMIT into the backend scan request. Selective full-row
filters with version/entity predicates or a LIMIT use key-first filtering and
hydrate only matching values; broad schema/file filters keep the single entry
scan because the CRUD all-match workload measured 29-45% slower with forced
key-first hydration.
```

This follows the same pushdown principle used by the artifact databases:
DataFusion/SpiceAI plans push projection/filter/limit work as close to
`TableScan` as possible, while scan implementations avoid materializing
intermediate row shapes unless they reduce downstream work.

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/(smoke|real_workload)/select_all_rows/(1k|10k)'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/select_keys_only/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/real_workload/select_keys_only/10k'
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
```

Smoke timing scoreboard:

| backend     | operation          |     after timing |         criterion change |
| ----------- | ------------------ | ---------------: | -----------------------: |
| Lix SQLite  | `select_all_rows`  | 4.5361-4.7843 ms | no change in final rerun |
| Lix SQLite  | `select_keys_only` | 3.9784-4.0757 ms |     -8.4525% to -4.1087% |
| Lix RocksDB | `select_all_rows`  | 1.4993-1.5245 ms | no change in final rerun |
| Lix RocksDB | `select_keys_only` | 1.0759-1.0957 ms |     -6.9517% to -3.6168% |

Real-workload timing scoreboard:

| backend     | operation          |     after timing |                                             criterion change |
| ----------- | ------------------ | ---------------: | -----------------------------------------------------------: |
| Lix SQLite  | `select_all_rows`  | 11.519-11.637 ms | noisy final rerun; earlier fused run showed 10.836-11.049 ms |
| Lix SQLite  | `select_keys_only` | 7.0505-7.1594 ms |                                         -17.109% to -15.229% |
| Lix RocksDB | `select_all_rows`  | 10.371-10.578 ms |                                         -5.1995% to -2.0906% |
| Lix RocksDB | `select_keys_only` | 7.2173-7.4856 ms |                                         -8.1440% to -4.0246% |

Logical I/O scoreboard:

| workload | backend     | operation          | io ops | io bytes/row | note                              |
| -------- | ----------- | ------------------ | -----: | -----------: | --------------------------------- |
| smoke/1k | Lix SQLite  | `select_all_rows`  |      1 |       926.29 | broad filter keeps one entry scan |
| smoke/1k | Lix SQLite  | `select_keys_only` |      1 |        81.20 | key-only scan                     |
| smoke/1k | Lix RocksDB | `select_all_rows`  |      1 |       926.29 | broad filter keeps one entry scan |
| smoke/1k | Lix RocksDB | `select_keys_only` |      1 |        81.20 | key-only scan                     |

Review:

```text
Initial sub-agent review reported HIGH on `limit = Some(0)` returning one row
after the fused loop, and MEDIUM on filtered full-row scans still hydrating
values before identity filtering. The patch now returns early for zero limits,
adds focused zero-limit coverage, and adds a key-first hydration path for
selective full-row filters while preserving the faster single entry scan for
broad schema/file filters. Re-review reported HIGH None and MEDIUM None. LOW
feedback led to a namespace invariant check, a selectivity-gate comment, and a
filtered full-scan limit/order test.
```

## Optimization 15: Reserve Untracked Write Ops

Date: 2026-05-12

Axis:

```text
write staging allocation overhead
```

Change:

```text
StorageWriteSet/KvWriteBatch can now reserve operation capacity for a namespace.
Untracked `stage_rows` and `stage_delete_rows` reserve from the iterator lower
bound before pushing thousands of Put/Delete ops. This keeps the public storage
shape unchanged while avoiding repeated Vec growth in the write staging loop.
Zero-size reserves are no-ops so empty staging does not create empty write
groups.
```

The artifact precedent is conservative preallocation: Turso caps parsed log-op
preallocation when counts come from data, but reserves exact write-buffer sizes
after computing trusted sizes. Here the hint comes from internal bounded
iterators over already-built rows, so reserving the lower bound is conservative.

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|update_all_rows|delete_one_by_pk)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/real_workload/(insert_all_rows|update_all_rows)/10k'
```

Smoke timing scoreboard:

| backend     | operation          |     after timing |     criterion change |
| ----------- | ------------------ | ---------------: | -------------------: |
| Lix SQLite  | `insert_all_rows`  | 6.1619-6.2883 ms | -10.373% to -6.5691% |
| Lix SQLite  | `update_all_rows`  | 5.5052-5.5752 ms |            no change |
| Lix SQLite  | `delete_one_by_pk` | 3.9391-4.0600 ms |            no change |
| Lix RocksDB | `insert_all_rows`  | 2.8385-2.9012 ms |            no change |
| Lix RocksDB | `update_all_rows`  | 1.9541-1.9905 ms |            no change |
| Lix RocksDB | `delete_one_by_pk` | 620.94-648.62 us |            no change |

Real-workload timing scoreboard:

| backend     | operation         |     after timing |           criterion change |
| ----------- | ----------------- | ---------------: | -------------------------: |
| Lix SQLite  | `insert_all_rows` | 33.097-34.801 ms | noisy, no confirmed change |
| Lix SQLite  | `update_all_rows` | 26.578-27.010 ms |                  no change |
| Lix RocksDB | `insert_all_rows` | 15.744-15.995 ms |                  no change |
| Lix RocksDB | `update_all_rows` | 17.554-18.019 ms |                  no change |

Review:

```text
Sub-agent review reported HIGH None and MEDIUM None. LOW feedback noted that
zero-size reserves should not create empty groups, which is now fixed, and
called out that reserving from size_hint is appropriate for current internal
bounded iterators but should stay conservative if exposed to untrusted iterator
sources.
```

## Optimization 16: Allocation-Free Varint Canonicality

Date: 2026-05-12

Axis:

```text
decode CPU for key/value component framing
```

Change:

```text
Untracked key and value varint helpers now fast-path one-byte lengths. Decode
canonicality checks no longer allocate and re-encode a temporary varint; they
compare the consumed byte count with the minimal encoded length for the decoded
value. The byte format is unchanged.
```

This follows the artifact database pattern of keeping physical-key codecs
allocation-free on hot paths. Turso computes varint encoded lengths directly in
its on-disk SQLite helpers, and compact key/value stores use minimal varint
length as the canonicality rule instead of allocating a second encoding.

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|select_all_rows|select_keys_only|select_all_by_pk|update_all_rows)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/real_workload/(select_all_rows|select_keys_only|select_all_by_pk|update_all_rows)/10k'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
```

Smoke timing scoreboard:

| backend     | operation          |     after timing |              criterion change |
| ----------- | ------------------ | ---------------: | ----------------------------: |
| Lix SQLite  | `insert_all_rows`  | 6.3281-6.5346 ms |                     no change |
| Lix SQLite  | `select_all_rows`  | 4.6744-4.7888 ms |           no confirmed change |
| Lix SQLite  | `select_keys_only` | 3.8831-3.9651 ms | -7.1689% to -3.3830% on rerun |
| Lix SQLite  | `select_all_by_pk` | 5.9516-6.1213 ms |          -11.543% to -5.8961% |
| Lix SQLite  | `update_all_rows`  | 5.1776-5.2511 ms |          -6.8677% to -1.0810% |
| Lix RocksDB | `insert_all_rows`  | 2.9005-2.9490 ms |                     no change |
| Lix RocksDB | `select_all_rows`  | 1.3425-1.3580 ms |          -8.1836% to -5.7179% |
| Lix RocksDB | `select_keys_only` | 1.0283-1.0462 ms |          -6.4457% to -3.3464% |
| Lix RocksDB | `select_all_by_pk` | 2.0737-2.1070 ms |          -10.168% to -8.5019% |
| Lix RocksDB | `update_all_rows`  | 1.9983-2.0416 ms |                     no change |

Real-workload timing scoreboard:

| backend     | operation          |     after timing |     criterion change |
| ----------- | ------------------ | ---------------: | -------------------: |
| Lix SQLite  | `select_all_rows`  | 9.4119-9.5364 ms | -21.556% to -19.390% |
| Lix SQLite  | `select_keys_only` | 7.6532-7.7342 ms |   no change on rerun |
| Lix SQLite  | `select_all_by_pk` | 27.832-28.123 ms | -13.295% to -11.918% |
| Lix SQLite  | `update_all_rows`  | 26.682-27.161 ms |            no change |
| Lix RocksDB | `select_all_rows`  | 9.3239-9.5333 ms | -12.948% to -10.668% |
| Lix RocksDB | `select_keys_only` | 6.6582-6.7487 ms | -10.549% to -6.9986% |
| Lix RocksDB | `select_all_by_pk` | 16.889-17.208 ms | -13.476% to -11.552% |
| Lix RocksDB | `update_all_rows`  | 17.635-17.872 ms |            no change |

Logical I/O scoreboard:

| workload          | backend     | operation          | io ops | io bytes/row | note                  |
| ----------------- | ----------- | ------------------ | -----: | -----------: | --------------------- |
| real_workload/10k | Lix SQLite  | `select_all_rows`  |      1 |       357.18 | unchanged byte format |
| real_workload/10k | Lix SQLite  | `select_keys_only` |      1 |        82.39 | unchanged byte format |
| real_workload/10k | Lix SQLite  | `select_all_by_pk` |     20 |       357.18 | unchanged byte format |
| real_workload/10k | Lix RocksDB | `select_all_rows`  |      1 |       357.18 | unchanged byte format |
| real_workload/10k | Lix RocksDB | `select_keys_only` |      1 |        82.39 | unchanged byte format |
| real_workload/10k | Lix RocksDB | `select_all_by_pk` |     20 |       357.18 | unchanged byte format |

Review:

```text
Sub-agent review reported HIGH None, MEDIUM None, and LOW None. The reviewer
confirmed that minimal encoded length uniquely determines the canonical
unsigned LEB128-style representation, so replacing re-encode-and-compare with
the consumed-length check preserves malformed-varint rejection while removing
per-varint allocation.
```

## Optimization 17: Build Untracked Write Group Once

Date: 2026-05-12

Axis:

```text
write staging CPU for same-namespace untracked batches
```

Change:

```text
Untracked `stage_rows` and `stage_delete_rows` now build one `KvWriteGroup`
for the untracked namespace and push it into the write set after staging. This
avoids a per-row namespace lookup in `StorageWriteSet::put/delete`.

`StorageWriteSet::push_group` merges with an existing same-namespace group to
preserve same-namespace operation order if callers compose multiple staging
helpers. It pushes a new group directly when no matching namespace exists, so
the reserved operation allocation from staging is preserved.

The previous `reserve_namespace_ops` compatibility surface was removed because
the group-once path replaced its only use.
```

The artifact precedent is a single coherent transaction write set: database
write paths accumulate ordered mutations and flush them as a batch, rather than
re-discovering the target collection for each row. The patch keeps one
same-namespace operation stream, which also follows the delete-range guidance
from Greptime-style KV backends where range deletes are first-class operations
with conformance coverage.

Verification:

```sh
cargo check -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine push_group_merges_with_existing_namespace_preserving_same_namespace_order
cargo test -p lix_engine untracked_state --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench storage -- 'storage/untracked_state/write_rows/10k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/(smoke|real_workload)/(insert_all_rows|update_all_rows|delete_one_by_pk)/((1k)|(10k))'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
```

Storage API timing scoreboard:

| operation                                |     after timing |                                                                                                criterion change |
| ---------------------------------------- | ---------------: | --------------------------------------------------------------------------------------------------------------: |
| `storage/untracked_state/write_rows/10k` | 13.750-13.877 ms | final rerun no change vs fixed patch; initial candidate showed -11.893% to -10.671% vs prior committed baseline |

CRUD timing scoreboard:

| backend     | workload          | operation          |     after timing |                criterion change |
| ----------- | ----------------- | ------------------ | ---------------: | ------------------------------: |
| Lix SQLite  | smoke/1k          | `insert_all_rows`  | 6.4785-6.5531 ms | noisy regression on final rerun |
| Lix SQLite  | smoke/1k          | `update_all_rows`  | 5.2559-5.8523 ms |                       no change |
| Lix SQLite  | smoke/1k          | `delete_one_by_pk` | 3.6881-3.7679 ms |                       no change |
| Lix RocksDB | smoke/1k          | `insert_all_rows`  | 2.8482-2.8600 ms |            -3.1032% to -1.0621% |
| Lix RocksDB | smoke/1k          | `update_all_rows`  | 1.9511-1.9779 ms |                       no change |
| Lix RocksDB | smoke/1k          | `delete_one_by_pk` | 610.51-640.97 us |                       no change |
| Lix SQLite  | real_workload/10k | `insert_all_rows`  | 32.996-33.908 ms |                       no change |
| Lix SQLite  | real_workload/10k | `update_all_rows`  | 26.521-26.668 ms |                       no change |
| Lix SQLite  | real_workload/10k | `delete_one_by_pk` | 3.3077-3.4608 ms |                       no change |
| Lix RocksDB | real_workload/10k | `insert_all_rows`  | 15.877-16.045 ms |                       no change |
| Lix RocksDB | real_workload/10k | `update_all_rows`  | 17.837-18.264 ms |                noisy regression |
| Lix RocksDB | real_workload/10k | `delete_one_by_pk` | 2.3824-2.5173 ms |                       no change |

Logical I/O scoreboard:

| workload          | backend     | operation          | io ops | io bytes/row | note                                    |
| ----------------- | ----------- | ------------------ | -----: | -----------: | --------------------------------------- |
| real_workload/10k | Lix SQLite  | `insert_all_rows`  |      1 |       357.18 | unchanged logical batch and byte format |
| real_workload/10k | Lix SQLite  | `update_all_rows`  |      1 |       289.79 | unchanged logical batch and byte format |
| real_workload/10k | Lix SQLite  | `delete_one_by_pk` |      1 |        31.00 | unchanged logical batch and byte format |
| real_workload/10k | Lix RocksDB | `insert_all_rows`  |      1 |       357.18 | unchanged logical batch and byte format |
| real_workload/10k | Lix RocksDB | `update_all_rows`  |      1 |       289.79 | unchanged logical batch and byte format |
| real_workload/10k | Lix RocksDB | `delete_one_by_pk` |      1 |        31.00 | unchanged logical batch and byte format |

Review:

```text
Initial sub-agent review reported HIGH on default builds failing because
`KvWriteGroup` was only re-exported under `storage-benches`, and MEDIUM on
duplicate same-namespace groups weakening operation ordering when later writes
used the legacy mutators. The patch now re-exports `KvWriteGroup`
unconditionally, merges pushed groups into existing same-namespace groups,
removes the dead reserve API, and adds a unit test covering pushed groups
followed by a later same-namespace range delete.

Re-review reported one MEDIUM performance concern: the merge implementation
reallocated even when no same-namespace group existed. The final patch pushes
the prepared group directly on first namespace use and only merges on duplicate
namespaces. Final re-review reported HIGH None, MEDIUM None, and LOW None.
```

## Optimization 18: Larger Point-Read Batches

Date: 2026-05-12

Axis:

```text
logical backend I/O call count for batched exact primary-key loads
```

Change:

```text
Increase the untracked row point-load chunk size from 512 to 2048 rows. This
keeps the exact same storage format and request semantics, but reduces 10k-row
`select_all_by_pk` from 20 backend `get_values` calls to 5 calls. The same
bounded chunk size also applies to key-backed filtered scans that hydrate rows
after first scanning matching keys.
```

This follows the bounded batching pattern used in the artifact set. Turso's FTS
design commits indexed documents in fixed batches and keeps chunk/cache sizes
bounded (`artifact/turso/docs/fts.md`), while GreptimeDB's export/import design
uses independently retryable chunks with explicit target chunk sizes
(`artifact/greptimedb/docs/rfcs/2025-12-30-export-import-v2.md`). Here the
batch is a fixed 2048 keys, which is 2049 SQLite bind parameters in the bench
backend because the namespace is bound separately.

Verification:

```sh
cargo test -p lix_engine untracked_state --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/(smoke|real_workload)/select_all_by_pk/((1k)|(10k))'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
```

Timing scoreboard:

| backend     | workload          | operation          |     after timing |           criterion change |
| ----------- | ----------------- | ------------------ | ---------------: | -------------------------: |
| Lix SQLite  | smoke/1k          | `select_all_by_pk` | 5.8907-6.1321 ms | no confirmed timing change |
| Lix RocksDB | smoke/1k          | `select_all_by_pk` | 2.0348-2.0778 ms | no confirmed timing change |
| Lix SQLite  | real_workload/10k | `select_all_by_pk` | 27.731-28.055 ms | no confirmed timing change |
| Lix RocksDB | real_workload/10k | `select_all_by_pk` | 16.656-16.945 ms | no confirmed timing change |

Logical I/O scoreboard:

| workload          | backend     | operation          | before get calls | after get calls | read rows | io bytes/row |
| ----------------- | ----------- | ------------------ | ---------------: | --------------: | --------: | -----------: |
| real_workload/10k | Lix SQLite  | `select_all_by_pk` |               20 |               5 |     10000 |       357.18 |
| real_workload/10k | Lix RocksDB | `select_all_by_pk` |               20 |               5 |     10000 |       357.18 |

Review:

```text
Sub-agent review reported HIGH None and MEDIUM None. LOW feedback asked for
more precise SQLite bind-parameter wording, acknowledging that key-backed
filtered scans share this chunk size, and concrete artifact references. The
comment and log now call out 2048 keys / 2049 SQLite parameters, the filtered
scan scope, and bounded batching references from Turso and GreptimeDB artifacts.
```

## Optimization 19: Sort Untracked Point Writes By Key

Date: 2026-05-12

Axis:

```text
SQLite/RocksDB write locality for untracked insert/update batches
```

Change:

```text
Untracked row staging now stable-sorts the staged point write group by encoded
storage key before pushing it into the write set. Same-key operations keep their
original order, preserving last-writer-wins behavior, while independent keys
are written in physical key order.
```

This is a clean internal ordering change: it does not change the key format,
value format, logical I/O, or backend API. The database-storage precedent is
ordered bulk mutation: Dolt's prolly tree patcher asserts patches are sorted by
key for tree construction and patch traversal
(`artifact/dolt/go/store/prolly/tree/tree_patcher.go`), and its mutable-map
tests include bulk insert and mixed mutation cases
(`artifact/dolt/go/store/prolly/mutable_map_write_test.go`). Here the stable
sort gives SQLite's `WITHOUT ROWID` B-tree and RocksDB's write path a locality
friendly mutation order while preserving observable same-key ordering.

This is not a universal write-staging CPU win: the in-memory storage benchmark
regresses slightly because it pays sort CPU without a backend locality benefit.
The CRUD win comes from applying the already-staged batch to ordered backends in
physical key order.

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine sort_point_ops_by_key_preserves_same_key_order
cargo test -p lix_engine untracked_state --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(raw_sqlite|lix_sqlite|lix_rocksdb)/(smoke|real_workload)/(insert_all_rows|update_all_rows|delete_all_rows)/((1k)|(10k))'
cargo bench -p lix_engine --features storage-benches --bench storage -- 'storage/untracked_state/write_rows/10k'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
```

CRUD timing scoreboard:

| backend     | workload          | operation         |     after timing |                                  criterion change |
| ----------- | ----------------- | ----------------- | ---------------: | ------------------------------------------------: |
| Raw SQLite  | real_workload/10k | `insert_all_rows` | 15.895-16.133 ms |                                    baseline rerun |
| Raw SQLite  | real_workload/10k | `update_all_rows` | 33.304-33.840 ms |                                    baseline rerun |
| Raw SQLite  | real_workload/10k | `delete_all_rows` | 11.137-11.663 ms |                                    baseline rerun |
| Lix SQLite  | smoke/1k          | `insert_all_rows` | 5.9955-6.0372 ms |                              -5.1291% to -3.1555% |
| Lix SQLite  | smoke/1k          | `update_all_rows` | 5.2671-5.3644 ms |                                         no change |
| Lix SQLite  | smoke/1k          | `delete_all_rows` | 4.7058-4.7534 ms |                                         no change |
| Lix RocksDB | smoke/1k          | `insert_all_rows` | 2.7324-2.7764 ms |                              -7.9693% to -4.8101% |
| Lix RocksDB | smoke/1k          | `update_all_rows` | 1.9376-1.9641 ms |                                         no change |
| Lix RocksDB | smoke/1k          | `delete_all_rows` | 755.54-780.90 us |                                         no change |
| Lix SQLite  | real_workload/10k | `insert_all_rows` | 27.334-28.621 ms |             improved vs pre-sort 33.796-34.597 ms |
| Lix SQLite  | real_workload/10k | `update_all_rows` | 23.670-24.306 ms |                              -28.117% to -25.169% |
| Lix SQLite  | real_workload/10k | `delete_all_rows` | 16.893-18.402 ms |  not affected by sorted staging; noisy regression |
| Lix RocksDB | real_workload/10k | `insert_all_rows` | 14.000-14.418 ms |                              -10.123% to -7.1869% |
| Lix RocksDB | real_workload/10k | `update_all_rows` | 17.039-17.382 ms |                                         no change |
| Lix RocksDB | real_workload/10k | `delete_all_rows` | 3.2595-3.6471 ms | not affected by sorted staging; noisy improvement |

Storage API timing scoreboard:

| operation                                |     after timing |                                                                       criterion change |
| ---------------------------------------- | ---------------: | -------------------------------------------------------------------------------------: |
| `storage/untracked_state/write_rows/10k` | 13.803-13.963 ms | +2.2178% to +4.1553%; in-memory staging pays sort CPU without backend locality benefit |

Logical I/O scoreboard:

| workload          | backend     | operation         | io ops | io bytes/row | note                                    |
| ----------------- | ----------- | ----------------- | -----: | -----------: | --------------------------------------- |
| real_workload/10k | Lix SQLite  | `insert_all_rows` |      1 |       357.18 | unchanged logical batch and byte format |
| real_workload/10k | Lix SQLite  | `update_all_rows` |      1 |       289.79 | unchanged logical batch and byte format |
| real_workload/10k | Lix RocksDB | `insert_all_rows` |      1 |       357.18 | unchanged logical batch and byte format |
| real_workload/10k | Lix RocksDB | `update_all_rows` |      1 |       289.79 | unchanged logical batch and byte format |

Review:

```text
Sub-agent review reported HIGH None and MEDIUM None. LOW feedback asked for a
sharper helper contract, more explicit benchmark interpretation, and more
precise artifact wording. The helper now panics if a range delete is passed to
the point-op sorter, and this log now calls the Dolt reference an ordered bulk
mutation precedent while explicitly noting the in-memory staging tradeoff.
```

## Optimization 20: Raw Bind SQLite KV Writes

Date: 2026-05-12

Axis:

```text
SQLite backend write-loop binding overhead
```

Change:

```text
The SQLite bench backend write loop now binds put/delete statement parameters
with rusqlite's low-level `raw_bind_parameter` API and executes with
`raw_execute`. Every parameter is rebound on each iteration; statement text,
transaction semantics, and logical storage I/O are unchanged.
```

This keeps the same prepared-statement reuse pattern already used throughout
the SQLite backend, but avoids rebuilding the `params!` parameter container in
the hot per-row KV write loop. The artifact precedent is direct prepared
statement binding: Turso's sync engine binds statement parameters explicitly in
hot database sync operations
(`artifact/turso/sync/engine/src/database_sync_operations.rs`), and Turso's
SQLite compatibility tests exercise low-level bind APIs for integer, text, and
blob parameters (`artifact/turso/sqlite3/tests/compat/mod.rs`).

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo bench -p lix_engine --features storage-benches --bench storage -- 'storage/api/sqlite_tempfile/write_kv_batch_put/10k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/lix_sqlite/real_workload/(insert_all_rows|update_all_rows|delete_one_by_pk)/10k'
```

Timing scoreboard:

| benchmark                                         | operation                |     after timing |     criterion change |
| ------------------------------------------------- | ------------------------ | ---------------: | -------------------: |
| storage/api/sqlite_tempfile                       | `write_kv_batch_put/10k` | 12.653-12.755 ms | -4.0026% to -2.8143% |
| untracked_state_crud/lix_sqlite/real_workload/10k | `insert_all_rows`        | 26.158-26.514 ms | -5.0459% to -3.0589% |
| untracked_state_crud/lix_sqlite/real_workload/10k | `update_all_rows`        | 23.062-23.609 ms | -4.5133% to -1.0401% |
| untracked_state_crud/lix_sqlite/real_workload/10k | `delete_one_by_pk`       | 3.1464-3.2165 ms |            no change |

Logical I/O:

```text
Unchanged. This only removes per-row rusqlite parameter-container overhead in
the bench SQLite backend write loop.
```

Review:

```text
No sub-agent review: improvement is below the >=10% review threshold.
```

## Optimization 21: SQLite Exclusive Namespace Clear

Date: 2026-05-12

Axis:

```text
SQLite delete-all physical execution for an empty-prefix namespace range delete
```

Change:

```text
The SQLite bench backend now detects an empty-prefix DeleteRange whose target
namespace is the only namespace present in the KV table. In that case it uses
SQLite's table-wide `DELETE FROM kv` path instead of a namespace/key predicate.
If any other namespace is present, it falls back to the bounded range delete.
```

The exclusivity proof uses ordered first/last namespace probes on the
`PRIMARY KEY(namespace, key)` table rather than a negative namespace scan. This
keeps the fast path limited to the same physical scope as raw SQLite's
`DELETE FROM untracked_state`, while preserving multi-namespace correctness.
GreptimeDB's SQL KV backends keep range deletes bounded by key predicates, and
use direct full-table deletes only when the logical KV table itself is the
target scope (`artifact/greptimedb/src/common/meta/src/kv_backend/rds/mysql.rs`,
`artifact/greptimedb/src/common/meta/src/kv_backend/rds/postgres.rs`).

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine --test backend_kv_range_delete --features storage-benches
cargo test -p lix_engine untracked_state --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(raw_sqlite|lix_sqlite|lix_rocksdb)/(smoke|real_workload)/delete_all_rows/((1k)|(10k))'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
```

CRUD timing scoreboard:

| backend     | workload          | operation         |     after timing |                                                               criterion change |
| ----------- | ----------------- | ----------------- | ---------------: | -----------------------------------------------------------------------------: |
| Raw SQLite  | smoke/1k          | `delete_all_rows` | 3.3902-3.4523 ms |                                                                 baseline rerun |
| Lix SQLite  | smoke/1k          | `delete_all_rows` | 4.0924-4.2547 ms |                                     still improved vs pre-opt 4.7058-4.7534 ms |
| Lix RocksDB | smoke/1k          | `delete_all_rows` | 730.68-753.71 us |                                                                      no change |
| Raw SQLite  | real_workload/10k | `delete_all_rows` | 11.506-11.651 ms |                                                                 baseline rerun |
| Lix SQLite  | real_workload/10k | `delete_all_rows` | 5.9017-6.2668 ms | -17.216% to -10.547% vs first fast-path run; ~-63% vs pre-opt 16.475-16.727 ms |
| Lix RocksDB | real_workload/10k | `delete_all_rows` | 3.3158-3.8089 ms |                                                                      no change |

Logical I/O scoreboard:

| workload          | backend     | operation         | io ops | io bytes/row | write batches | puts | deletes | delete ranges | write bytes/row |
| ----------------- | ----------- | ----------------- | -----: | -----------: | ------------: | ---: | ------: | ------------: | --------------: |
| real_workload/10k | Lix SQLite  | `delete_all_rows` |      1 |         0.00 |             1 |    0 |       0 |             1 |            0.00 |
| real_workload/10k | Lix RocksDB | `delete_all_rows` |      1 |         0.00 |             1 |    0 |       0 |             1 |            0.00 |

Review:

```text
Sub-agent review initially reported MEDIUM findings for missing direct
fast-path coverage and an O(n) negative namespace-exclusivity probe. The test
now uses a fresh SQLite backend for the exclusive-namespace branch, and the
proof now uses first/last ordered namespace probes. Re-review reported HIGH
None and MEDIUM None.
```

## Optimization 22: SQLite Dedicated Untracked Table

Date: 2026-05-12

Axis:

```text
SQLite physical namespace overhead for untracked-state rows
```

Change:

```text
The SQLite bench backend now stores namespace `u` in a dedicated `kv_u`
WITHOUT ROWID table keyed by `key` instead of storing untracked rows in the
shared `kv(namespace, key, value)` table. Non-untracked namespaces still use
the shared table. The backend routing covers get, exists, scans, point writes,
point deletes, and range deletes for namespace `u`.
```

This removes the physical namespace column and namespace predicates from the
hot untracked row table while keeping the generic logical KV API unchanged.
The artifact precedent is physical separation by storage scope: GreptimeDB's
SQL KV backends use one concrete SQL table as the scope for KV operations and
keep range predicates inside that table
(`artifact/greptimedb/src/common/meta/src/kv_backend/rds/mysql.rs`,
`artifact/greptimedb/src/common/meta/src/kv_backend/rds/postgres.rs`). The
untracked namespace is the same kind of isolated local-row scope in this bench
backend.

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state --features storage-benches
cargo test -p lix_engine --test backend_kv_range_delete --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/lix_sqlite/real_workload/(insert_all_rows|select_all_rows|select_keys_only|select_one_by_pk|select_all_by_pk|update_all_rows|update_one_by_pk|delete_all_rows|delete_one_by_pk)/10k'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
```

CRUD timing scoreboard:

| backend    | workload          | operation          |     after timing |                                                                criterion change |
| ---------- | ----------------- | ------------------ | ---------------: | ------------------------------------------------------------------------------: |
| Lix SQLite | real_workload/10k | `insert_all_rows`  | 26.134-26.540 ms | no change in final run; earlier isolated insert run showed -6.8890% to -3.2348% |
| Lix SQLite | real_workload/10k | `select_all_rows`  | 9.8629-9.9627 ms |                                                                       no change |
| Lix SQLite | real_workload/10k | `select_keys_only` | 7.0095-7.4929 ms |                                                                       no change |
| Lix SQLite | real_workload/10k | `select_one_by_pk` | 2.5131-2.5977 ms |                                                            +1.8925% to +5.6445% |
| Lix SQLite | real_workload/10k | `select_all_by_pk` | 26.878-27.743 ms |                                                            -5.1055% to -1.4740% |
| Lix SQLite | real_workload/10k | `update_all_rows`  | 22.229-22.437 ms |                                                            -7.8911% to -2.7472% |
| Lix SQLite | real_workload/10k | `update_one_by_pk` | 3.0898-3.2108 ms |                                                                       no change |
| Lix SQLite | real_workload/10k | `delete_all_rows`  | 5.8115-5.9552 ms |                                                                       no change |
| Lix SQLite | real_workload/10k | `delete_one_by_pk` | 3.2543-3.3737 ms |                                                            +1.8307% to +6.5731% |

Logical I/O:

```text
Unchanged. This is a physical SQLite layout optimization; the logical KV
request/result counters still report the same keys, values, batches, point
operations, and range deletes.
```

Review:

```text
No sub-agent review: measured improvement is below the >=10% review threshold.
The single-row point-operation regressions are small and remain faster than raw
SQLite point operations in the real workload.
```

## Optimization 23: Untracked Format Gate

Date: 2026-05-12

Axis:

```text
future migration safety for durable untracked-state rows
```

Change:

```text
Untracked writes now stage a format marker in namespace `lix.storage_format`
at key `untracked_state` with value `1`. Untracked reads accept empty unmarked
stores, accept marker value `1`, and reject unknown marker values or existing
untracked rows without a marker.
```

This is a migration-hardening change, not a backward-compatibility decoder.
The current row formats remain clean-cut internal formats, but future
migrations now have one explicit gate instead of discovering incompatibility
through row-level decode failures. The read gate also checks the legacy `u`
namespace so pre-gate rows cannot become silently invisible.

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state::storage --features storage-benches
cargo test -p lix_engine storage --features storage-benches
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/no_such_benchmark'
```

I/O scoreboard:

| workload          | operation          | read shape after gate    | write shape after gate |
| ----------------- | ------------------ | ------------------------ | ---------------------- |
| smoke/1k          | `select_all_rows`  | 1 marker get + 1 scan    | unchanged              |
| smoke/1k          | `select_one_by_pk` | 1 marker get + 1 row get | unchanged              |
| smoke/1k          | `insert_all_rows`  | unchanged                | 1001 puts              |
| real_workload/10k | `insert_all_rows`  | unchanged                | 10001 puts             |

Review:

```text
No sub-agent review. This is intentional hardening and adds a small fixed
metadata read/write cost; it is not a performance optimization.
```

## Optimization 24: Versioned Untracked Keyspace

Date: 2026-05-12

Axis:

```text
durable keyspace migration boundary
```

Change:

```text
The active untracked row namespace changed from `u` to `u1`. The old `u`
namespace is retained only as a legacy-detection namespace in the format gate.
The SQLite bench backend routes `u1` through the same dedicated untracked table
path that previously recognized `u`.
```

The namespace version makes a future key codec migration first-principles:
new readers can dual-scan `u1` and `u2`, migrate, or clear explicitly instead
of guessing from unversioned key bytes. This keeps the row key compact while
making the storage-format boundary visible at the namespace level.

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state::storage --features storage-benches
cargo test -p lix_engine --test backend_kv_range_delete --features storage-benches
```

Compatibility tests:

```text
Added tests that writes install the format marker, current unmarked rows are
rejected, legacy `u` rows are rejected, and key golden bytes remain intentional.
```

Review:

```text
No sub-agent review. Backward compatibility is explicitly out of scope; future
migration safety is the goal.
```

## Optimization 25: Composite Key Prefix Scans

Date: 2026-05-12

Axis:

```text
filter pushdown for ordered KV scans
```

Change:

```text
Untracked scan planning now builds component-aligned key prefixes for
`version_id` and `version_id + schema_key` filters. Those scans use bounded
KV prefix ranges instead of always scanning the whole untracked namespace.
```

The key codec is still intentionally not a general order-preserving tuple
codec, but its leading length-framed components can safely form exact component
prefixes. This pushes the filters that match the physical key prefix down to
the backend for both SQLite and RocksDB while preserving Rust-side filtering for
entity and nullable file predicates.

Verification:

```sh
cargo test -p lix_engine untracked_state::storage --features storage-benches
```

Tests:

```text
Added component-prefix golden tests and retained filtered scan ordering tests.
```

Review:

```text
No sub-agent review. This is a cross-backend scan-shape hardening with existing
behavior tests; targeted benchmark movement is mixed with the format-gate cost.
```

## Optimization 26: Projection Semantics Before Key-Only Speed

Date: 2026-05-12

Axis:

```text
avoid fabricated row state for projected untracked scans
```

Change:

```text
`scan_rows` no longer returns identity-only rows with fake empty timestamps and
`global = false`. Until a typed projected-row API exists, `scan_rows` hydrates
real row values so non-optional fields in MaterializedUntrackedStateRow remain
real row state.
```

This deliberately trades back the previous key-only scan shortcut. The current
API returns a materialized row with non-optional scalar fields, so returning
synthetic defaults violates the type contract. A future projected-row API can
restore key-covered identity scans without pretending absent fields are real.

Smoke timing scoreboard:

| backend     | operation          |     after timing |     criterion change |
| ----------- | ------------------ | ---------------: | -------------------: |
| Lix SQLite  | `select_keys_only` | 4.5585-4.6645 ms | +3.5932% to +12.702% |
| Lix RocksDB | `select_keys_only` | 1.3733-1.4033 ms | +33.075% to +36.028% |

I/O scoreboard:

| workload          | backend     | operation          | read calls | read rows | read bytes/row |
| ----------------- | ----------- | ------------------ | ---------: | --------: | -------------: |
| smoke/1k          | Lix SQLite  | `select_keys_only` |          2 |      1001 |         926.31 |
| smoke/1k          | Lix RocksDB | `select_keys_only` |          2 |      1001 |         926.31 |
| real_workload/10k | Lix SQLite  | `select_keys_only` |          2 |     10001 |         357.18 |
| real_workload/10k | Lix RocksDB | `select_keys_only` |          2 |     10001 |         357.18 |

Review:

```text
No sub-agent review. The regression is expected from semantic hardening:
MaterializedUntrackedStateRow now carries real scalar state. The follow-up
optimization is a typed projected-row return path.
```

## Optimization 27: Storage-Level Chunked Point Reads

Date: 2026-05-12

Axis:

```text
backend request sizing outside untracked_state
```

Change:

```text
The fixed point-read chunk size moved out of untracked_state into a generic
storage helper, `get_values_single_namespace_chunked`. Untracked batch loads
and key-first filtered hydration now use that helper.
```

The chunk limit still defaults to 2048 keys, but the SQLite bind-parameter
reason no longer lives in untracked-state business logic. This keeps the next
step straightforward: make the helper consult backend capabilities without
rewriting untracked readers again.

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine storage --features storage-benches
```

Review:

```text
No sub-agent review. This is a storage abstraction move with unchanged backend
API and unchanged logical point-read semantics.
```

## Optimization 28: Storage-Level Sorted Point Writes

Date: 2026-05-12

Axis:

```text
ordered backend write locality as a storage concern
```

Change:

```text
Exact untracked deletes now use the existing storage-level
`KvWriteGroup::sort_point_ops_by_key` helper, matching untracked row puts.
The helper remains generic and preserves same-key operation order.
```

This keeps write-locality behavior in the storage write group instead of
encoding it only in untracked inserts. `DeleteRange` remains excluded from the
point-sort helper because range deletes have observable ordering semantics in
the ordered write stream.

Verification:

```sh
cargo test -p lix_engine storage --features storage-benches
cargo test -p lix_engine --test backend_kv_range_delete --features storage-benches
```

Smoke timing scoreboard:

| backend     | operation         |     after timing | criterion change |
| ----------- | ----------------- | ---------------: | ---------------: |
| Lix SQLite  | `delete_all_rows` | 3.8975-3.9759 ms |        no change |
| Lix RocksDB | `delete_all_rows` | 769.70-798.33 us |     within noise |

Review:

```text
No sub-agent review. This is a small consistency hardening on top of the
already-reviewed ordered write operation model.
```

## Optimization 29: Batch-First Untracked Read API

Date: 2026-05-12

Axis:

```text
logical read API before backend refactor
```

Change:

```text
Untracked readers now expose `read_many` with ordered `Get` and `Scan`
requests plus semantic projections (`Identity`, `Header`, `Payload`, `Full`).
The storage backend API is unchanged; untracked_state lowers the logical batch
to the existing point-read and scan helpers.
```

This removes the clean API dependency on separate `scan_rows`, `load_rows`,
and `existing_identities` reader methods. Mixed requests preserve outer request
order, point gets preserve identity order and missing-row `None`s, and scan
projection no longer depends on free-form column strings.

Verification:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
```

Review:

```text
No sub-agent review. The change intentionally keeps backend/storage APIs
unchanged so SQLite/RocksDB-specific optimization can happen in a later pass.
```

## Optimization 30: Batch Read API Benchmark Scoreboard

Date: 2026-05-12

Axis:

```text
post read_many benchmark scoreboard
```

Change:

```text
Benchmark-only log entry for the batch-first untracked read API. No code
change was made for this entry.
```

Commands:

```sh
LIX_UNTRACKED_STATE_CRUD_IO=all cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(raw_sqlite|lix_sqlite|lix_rocksdb)/(smoke|real_workload)/(insert_all_rows|select_all_rows|select_keys_only|select_one_by_pk|select_all_by_pk|update_all_rows|update_one_by_pk|delete_all_rows|delete_one_by_pk)/((1k)|(10k))'
```

Timing scoreboard:

| workload      | backend     | operation          |          time 95% CI | mean change 95% CI |
| ------------- | ----------- | ------------------ | -------------------: | -----------------: |
| smoke         | raw_sqlite  | `insert_all_rows`  |   2.364 ms..2.463 ms |     -0.52%..+2.39% |
| smoke         | raw_sqlite  | `select_all_rows`  |   3.588 ms..3.699 ms |     -1.78%..+1.38% |
| smoke         | raw_sqlite  | `select_one_by_pk` |   3.351 ms..3.454 ms |     -4.19%..-1.43% |
| smoke         | raw_sqlite  | `update_all_rows`  |   5.570 ms..5.642 ms |     +0.58%..+3.52% |
| smoke         | raw_sqlite  | `update_one_by_pk` |   3.388 ms..3.490 ms |     +1.38%..+5.47% |
| smoke         | raw_sqlite  | `delete_all_rows`  |   3.407 ms..3.528 ms |     -1.75%..+1.84% |
| smoke         | raw_sqlite  | `delete_one_by_pk` |   3.515 ms..3.625 ms |     +3.06%..+5.61% |
| real_workload | raw_sqlite  | `insert_all_rows`  | 17.064 ms..17.574 ms |     -1.85%..+4.18% |
| real_workload | raw_sqlite  | `select_all_rows`  | 11.967 ms..12.248 ms |     -7.32%..-1.40% |
| real_workload | raw_sqlite  | `select_one_by_pk` | 10.473 ms..10.774 ms |     -1.90%..+1.31% |
| real_workload | raw_sqlite  | `update_all_rows`  | 34.029 ms..34.464 ms |     -3.56%..-1.73% |
| real_workload | raw_sqlite  | `update_one_by_pk` | 10.690 ms..10.970 ms |     +2.03%..+7.55% |
| real_workload | raw_sqlite  | `delete_all_rows`  | 10.962 ms..11.204 ms |     -6.10%..-2.90% |
| real_workload | raw_sqlite  | `delete_one_by_pk` | 10.046 ms..10.186 ms |     -1.32%..+2.81% |
| smoke         | lix_sqlite  | `insert_all_rows`  |   6.092 ms..6.206 ms |     -0.57%..+5.33% |
| smoke         | lix_sqlite  | `select_all_rows`  |   4.484 ms..4.556 ms |    -22.03%..+4.16% |
| smoke         | lix_sqlite  | `select_keys_only` |   4.410 ms..4.793 ms |     -2.60%..+2.17% |
| smoke         | lix_sqlite  | `select_one_by_pk` |   3.636 ms..3.735 ms |     -6.67%..-0.92% |
| smoke         | lix_sqlite  | `select_all_by_pk` |   6.384 ms..6.536 ms |     +2.70%..+6.15% |
| smoke         | lix_sqlite  | `update_all_rows`  |   5.021 ms..5.213 ms |     -6.87%..-2.88% |
| smoke         | lix_sqlite  | `update_one_by_pk` |   3.753 ms..3.810 ms |     -4.00%..-0.35% |
| smoke         | lix_sqlite  | `delete_all_rows`  |   3.921 ms..4.029 ms |     -4.76%..+1.00% |
| smoke         | lix_sqlite  | `delete_one_by_pk` |   3.903 ms..3.983 ms |     +4.40%..+7.53% |
| real_workload | lix_sqlite  | `insert_all_rows`  | 26.007 ms..26.531 ms |     -0.86%..+1.53% |
| real_workload | lix_sqlite  | `select_all_rows`  | 12.820 ms..13.108 ms |   -33.64%..-31.85% |
| real_workload | lix_sqlite  | `select_keys_only` | 12.830 ms..13.068 ms |   +72.57%..+84.98% |
| real_workload | lix_sqlite  | `select_one_by_pk` |   2.497 ms..2.557 ms |     -3.06%..+1.15% |
| real_workload | lix_sqlite  | `select_all_by_pk` | 31.928 ms..33.148 ms |    -10.81%..-6.64% |
| real_workload | lix_sqlite  | `update_all_rows`  | 21.815 ms..22.253 ms |     -1.41%..+0.88% |
| real_workload | lix_sqlite  | `update_one_by_pk` |   3.118 ms..3.385 ms |     -1.54%..+7.53% |
| real_workload | lix_sqlite  | `delete_all_rows`  |   5.606 ms..5.871 ms |   -60.31%..-58.22% |
| real_workload | lix_sqlite  | `delete_one_by_pk` |   3.172 ms..3.297 ms |     -2.85%..+2.98% |
| smoke         | lix_rocksdb | `insert_all_rows`  |   2.797 ms..2.833 ms |     -3.21%..-0.84% |
| smoke         | lix_rocksdb | `select_all_rows`  |   1.368 ms..1.414 ms |     -0.32%..+3.37% |
| smoke         | lix_rocksdb | `select_keys_only` |   1.358 ms..1.398 ms |     -2.21%..+0.69% |
| smoke         | lix_rocksdb | `select_one_by_pk` | 713.38 us..753.56 us |     +0.50%..+6.25% |
| smoke         | lix_rocksdb | `select_all_by_pk` |   2.428 ms..2.528 ms |    +6.10%..+10.26% |
| smoke         | lix_rocksdb | `update_all_rows`  |   2.022 ms..2.103 ms |     -3.10%..+1.71% |
| smoke         | lix_rocksdb | `update_one_by_pk` | 595.66 us..613.18 us |     -4.80%..-1.06% |
| smoke         | lix_rocksdb | `delete_all_rows`  | 777.79 us..796.75 us |     -0.35%..+4.04% |
| smoke         | lix_rocksdb | `delete_one_by_pk` | 611.87 us..626.67 us |   -45.67%..-10.78% |
| real_workload | lix_rocksdb | `insert_all_rows`  | 14.002 ms..14.886 ms |     -2.01%..+4.31% |
| real_workload | lix_rocksdb | `select_all_rows`  |   9.543 ms..9.755 ms |    +8.04%..+10.74% |
| real_workload | lix_rocksdb | `select_keys_only` |   9.077 ms..9.361 ms |   +41.37%..+46.11% |
| real_workload | lix_rocksdb | `select_one_by_pk` |   3.206 ms..3.258 ms |     +4.11%..+7.50% |
| real_workload | lix_rocksdb | `select_all_by_pk` | 20.064 ms..20.737 ms |   +14.13%..+20.43% |
| real_workload | lix_rocksdb | `update_all_rows`  | 18.661 ms..19.753 ms |    +5.63%..+13.66% |
| real_workload | lix_rocksdb | `update_one_by_pk` |   2.693 ms..2.740 ms |   +17.36%..+19.86% |
| real_workload | lix_rocksdb | `delete_all_rows`  |   3.370 ms..3.724 ms |    -11.12%..-0.66% |
| real_workload | lix_rocksdb | `delete_one_by_pk` |   2.048 ms..2.440 ms |    -12.91%..+6.06% |

I/O scoreboard:

| workload          | backend     | operation          | logical rows | io ops | io ops/row | io bytes | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes | read bytes/row | write batches |  puts | deletes | delete ranges | write bytes | write bytes/row |
| ----------------- | ----------- | ------------------ | -----------: | -----: | ---------: | -------: | -----------: | ---------: | --------: | -------: | ---------: | --------: | ---------: | -------------: | ------------: | ----: | ------: | ------------: | ----------: | --------------: |
| smoke/1k          | lix_sqlite  | `insert_all_rows`  |         1000 |      1 |       0.00 |   926311 |       926.31 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |  1001 |       0 |             0 |      926311 |          926.31 |
| smoke/1k          | lix_sqlite  | `select_all_rows`  |         1000 |      2 |       0.00 |   926311 |       926.31 |          2 |         1 |        1 |          1 |      1001 |     926311 |         926.31 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| smoke/1k          | lix_sqlite  | `select_keys_only` |         1000 |      2 |       0.00 |   926311 |       926.31 |          2 |         1 |        1 |          1 |      1001 |     926311 |         926.31 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| smoke/1k          | lix_sqlite  | `select_one_by_pk` |            1 |      2 |       2.00 |      306 |       306.00 |          2 |         2 |        2 |          0 |         2 |        306 |         306.00 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| smoke/1k          | lix_sqlite  | `select_all_by_pk` |         1000 |      2 |       0.00 |   926311 |       926.31 |          2 |         2 |     1001 |          0 |      1001 |     926311 |         926.31 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| smoke/1k          | lix_sqlite  | `update_all_rows`  |         1000 |      1 |       0.00 |   286494 |       286.49 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |  1001 |       0 |             0 |      286494 |          286.49 |
| smoke/1k          | lix_sqlite  | `update_one_by_pk` |            1 |      1 |       1.00 |      149 |       149.00 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     2 |       0 |             0 |         149 |          149.00 |
| smoke/1k          | lix_sqlite  | `delete_all_rows`  |         1000 |      1 |       0.00 |       16 |         0.02 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     1 |       0 |             1 |          16 |            0.02 |
| smoke/1k          | lix_sqlite  | `delete_one_by_pk` |            1 |      1 |       1.00 |       47 |        47.00 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     1 |       1 |             0 |          47 |           47.00 |
| smoke/1k          | lix_rocksdb | `insert_all_rows`  |         1000 |      1 |       0.00 |   926311 |       926.31 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |  1001 |       0 |             0 |      926311 |          926.31 |
| smoke/1k          | lix_rocksdb | `select_all_rows`  |         1000 |      2 |       0.00 |   926311 |       926.31 |          2 |         1 |        1 |          1 |      1001 |     926311 |         926.31 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| smoke/1k          | lix_rocksdb | `select_keys_only` |         1000 |      2 |       0.00 |   926311 |       926.31 |          2 |         1 |        1 |          1 |      1001 |     926311 |         926.31 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| smoke/1k          | lix_rocksdb | `select_one_by_pk` |            1 |      2 |       2.00 |      306 |       306.00 |          2 |         2 |        2 |          0 |         2 |        306 |         306.00 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| smoke/1k          | lix_rocksdb | `select_all_by_pk` |         1000 |      2 |       0.00 |   926311 |       926.31 |          2 |         2 |     1001 |          0 |      1001 |     926311 |         926.31 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| smoke/1k          | lix_rocksdb | `update_all_rows`  |         1000 |      1 |       0.00 |   286494 |       286.49 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |  1001 |       0 |             0 |      286494 |          286.49 |
| smoke/1k          | lix_rocksdb | `update_one_by_pk` |            1 |      1 |       1.00 |      149 |       149.00 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     2 |       0 |             0 |         149 |          149.00 |
| smoke/1k          | lix_rocksdb | `delete_all_rows`  |         1000 |      1 |       0.00 |       16 |         0.02 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     1 |       0 |             1 |          16 |            0.02 |
| smoke/1k          | lix_rocksdb | `delete_one_by_pk` |            1 |      1 |       1.00 |       47 |        47.00 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     1 |       1 |             0 |          47 |           47.00 |
| real_workload/10k | lix_sqlite  | `insert_all_rows`  |        10000 |      1 |       0.00 |  3571791 |       357.18 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 | 10001 |       0 |             0 |     3571791 |          357.18 |
| real_workload/10k | lix_sqlite  | `select_all_rows`  |        10000 |      2 |       0.00 |  3571791 |       357.18 |          2 |         1 |        1 |          1 |     10001 |    3571791 |         357.18 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| real_workload/10k | lix_sqlite  | `select_keys_only` |        10000 |      2 |       0.00 |  3571791 |       357.18 |          2 |         1 |        1 |          1 |     10001 |    3571791 |         357.18 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| real_workload/10k | lix_sqlite  | `select_one_by_pk` |            1 |      2 |       2.00 |      201 |       201.00 |          2 |         2 |        2 |          0 |         2 |        201 |         201.00 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| real_workload/10k | lix_sqlite  | `select_all_by_pk` |        10000 |      6 |       0.00 |  3571791 |       357.18 |          6 |         6 |    10001 |          0 |     10001 |    3571791 |         357.18 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| real_workload/10k | lix_sqlite  | `update_all_rows`  |        10000 |      1 |       0.00 |  2897916 |       289.79 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 | 10001 |       0 |             0 |     2897916 |          289.79 |
| real_workload/10k | lix_sqlite  | `update_one_by_pk` |            1 |      1 |       1.00 |      149 |       149.00 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     2 |       0 |             0 |         149 |          149.00 |
| real_workload/10k | lix_sqlite  | `delete_all_rows`  |        10000 |      1 |       0.00 |       16 |         0.00 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     1 |       0 |             1 |          16 |            0.00 |
| real_workload/10k | lix_sqlite  | `delete_one_by_pk` |            1 |      1 |       1.00 |       47 |        47.00 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     1 |       1 |             0 |          47 |           47.00 |
| real_workload/10k | lix_rocksdb | `insert_all_rows`  |        10000 |      1 |       0.00 |  3571791 |       357.18 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 | 10001 |       0 |             0 |     3571791 |          357.18 |
| real_workload/10k | lix_rocksdb | `select_all_rows`  |        10000 |      2 |       0.00 |  3571791 |       357.18 |          2 |         1 |        1 |          1 |     10001 |    3571791 |         357.18 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| real_workload/10k | lix_rocksdb | `select_keys_only` |        10000 |      2 |       0.00 |  3571791 |       357.18 |          2 |         1 |        1 |          1 |     10001 |    3571791 |         357.18 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| real_workload/10k | lix_rocksdb | `select_one_by_pk` |            1 |      2 |       2.00 |      201 |       201.00 |          2 |         2 |        2 |          0 |         2 |        201 |         201.00 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| real_workload/10k | lix_rocksdb | `select_all_by_pk` |        10000 |      6 |       0.00 |  3571791 |       357.18 |          6 |         6 |    10001 |          0 |     10001 |    3571791 |         357.18 |             0 |     0 |       0 |             0 |           0 |            0.00 |
| real_workload/10k | lix_rocksdb | `update_all_rows`  |        10000 |      1 |       0.00 |  2897916 |       289.79 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 | 10001 |       0 |             0 |     2897916 |          289.79 |
| real_workload/10k | lix_rocksdb | `update_one_by_pk` |            1 |      1 |       1.00 |      149 |       149.00 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     2 |       0 |             0 |         149 |          149.00 |
| real_workload/10k | lix_rocksdb | `delete_all_rows`  |        10000 |      1 |       0.00 |       16 |         0.00 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     1 |       0 |             1 |          16 |            0.00 |
| real_workload/10k | lix_rocksdb | `delete_one_by_pk` |            1 |      1 |       1.00 |       47 |        47.00 |          0 |         0 |        0 |          0 |         0 |          0 |           0.00 |             1 |     1 |       1 |             0 |          47 |           47.00 |

Notes:

```text
`select_keys_only` currently hydrates full row values through scan_rows, so its
I/O bytes match `select_all_rows`. The next storage/API optimization target is
a typed projected-row path that keeps identity/header scans from reading full
payloads while preserving real row semantics.

Real-workload delete_all remains the clearest structural win from range
deletes: both backends report one write batch, one put tombstone marker, and
one delete range for clearing 10k rows.
```

## Optimization 31: Projected Identity Scan API

Date: 2026-05-12

Axis: projection-shaped reads for key-only scan speed.

Change:

```text
Untracked readers now expose projected `get_many` and `scan` APIs.
Identity scans consume `scan_keys` and return `UntrackedStateProjectedRow`
identities without materializing `MaterializedUntrackedStateRow` or reading row
values. Existing full-row `read_many` remains as a compatibility collector.

Transaction identity-existence checks now use `get_many(Identity)`, so presence
checks do not hydrate full rows.
```

Verification commands:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state::storage --features storage-benches
LIX_UNTRACKED_STATE_CRUD_IO=all cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/(smoke|real_workload)/select_keys_only/((1k)|(10k))'
```

I/O scoreboard:

| workload          | backend     | operation          | before read bytes/row | after read bytes/row | after read shape       |
| ----------------- | ----------- | ------------------ | --------------------: | -------------------: | ---------------------- |
| smoke/1k          | lix_sqlite  | `select_keys_only` |                926.31 |                81.22 | format get + scan_keys |
| smoke/1k          | lix_rocksdb | `select_keys_only` |                926.31 |                81.22 | format get + scan_keys |
| real_workload/10k | lix_sqlite  | `select_keys_only` |                357.18 |                82.39 | format get + scan_keys |
| real_workload/10k | lix_rocksdb | `select_keys_only` |                357.18 |                82.39 | format get + scan_keys |

Timing scoreboard:

| workload      | backend     | operation          |          time 95% CI | mean change 95% CI |
| ------------- | ----------- | ------------------ | -------------------: | -----------------: |
| smoke         | lix_sqlite  | `select_keys_only` | 3.9915 ms..4.0920 ms | -13.603%..-9.4365% |
| smoke         | lix_rocksdb | `select_keys_only` | 1.0640 ms..1.0939 ms | -22.893%..-20.316% |
| real_workload | lix_sqlite  | `select_keys_only` | 7.1105 ms..7.3013 ms | -45.274%..-43.433% |
| real_workload | lix_rocksdb | `select_keys_only` | 6.7865 ms..6.8910 ms | -27.074%..-24.552% |

Notes:

```text
Full scans are intentionally unchanged. Code that wants key-only speed must use
the projected `scan` or `get_many` API; the compatibility `read_many`
collector still returns materialized rows and therefore preserves full-row
semantics.

Header and payload projections still read row values in this version. Future
work can reduce that further with partial decode or a physical layout that
stores hot header fields separately from large payload fields.
```

## Optimization 32: Split Header/Payload Physical Layout

Date: 2026-05-13

Axis: projection-shaped physical layout for untracked rows.

Change:

```text
Untracked storage format `2` hard-cuts from one physical row value to two
projection-shaped namespaces:

  uh2: identity key -> header value
  up2: identity key -> payload value

Identity and header reads use the header namespace. Payload reads use the
payload namespace. Full reads join header and payload by identical identity
keys. No compatibility read path is kept for old `u1` rows.
```

Verification commands:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state:: --features storage-benches
LIX_UNTRACKED_STATE_CRUD_IO=all cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/(smoke|real_workload)/(insert_all_rows|select_all_rows|select_keys_only|select_headers_only)/((1k)|(10k))'
```

I/O scoreboard:

| workload          | backend     | operation             | before read bytes/row | after read bytes/row | after read shape            |
| ----------------- | ----------- | --------------------- | --------------------: | -------------------: | --------------------------- |
| smoke/1k          | lix_sqlite  | `select_keys_only`    |                 81.22 |                81.22 | format get + header keys    |
| smoke/1k          | lix_sqlite  | `select_headers_only` |                926.31 |               136.22 | format get + header entries |
| smoke/1k          | lix_sqlite  | `select_all_rows`     |                926.31 |              1012.51 | header scan + payload scan  |
| real_workload/10k | lix_sqlite  | `select_keys_only`    |                 82.39 |                82.39 | format get + header keys    |
| real_workload/10k | lix_sqlite  | `select_headers_only` |                357.18 |               137.39 | format get + header entries |
| real_workload/10k | lix_sqlite  | `select_all_rows`     |                357.18 |               444.57 | header scan + payload scan  |
| real_workload/10k | lix_rocksdb | `select_headers_only` |                357.18 |               137.39 | format get + header entries |
| real_workload/10k | lix_rocksdb | `select_all_rows`     |                357.18 |               444.57 | header scan + payload scan  |

Write-shape tradeoff:

| workload          | backend     | operation         | before write bytes/row | after write bytes/row | before puts | after puts |
| ----------------- | ----------- | ----------------- | ---------------------: | --------------------: | ----------: | ---------: |
| smoke/1k          | lix_sqlite  | `insert_all_rows` |                 926.31 |               1012.51 |        1001 |       2001 |
| real_workload/10k | lix_sqlite  | `insert_all_rows` |                 357.18 |                444.57 |       10001 |      20001 |
| real_workload/10k | lix_rocksdb | `insert_all_rows` |                 357.18 |                444.57 |       10001 |      20001 |

Timing scoreboard:

| workload      | backend     | operation             |          time 95% CI | note                    |
| ------------- | ----------- | --------------------- | -------------------: | ----------------------- |
| smoke         | lix_sqlite  | `select_keys_only`    | 4.4368 ms..4.5699 ms | unchanged key-only path |
| smoke         | lix_sqlite  | `select_headers_only` | 4.6419 ms..4.7906 ms | new header-only row     |
| smoke         | lix_sqlite  | `select_all_rows`     | 4.8480 ms..4.9678 ms | lockstep full scan      |
| smoke         | lix_rocksdb | `select_headers_only` | 1.3244 ms..1.3491 ms | new header-only row     |
| smoke         | lix_rocksdb | `select_all_rows`     | 1.5734 ms..1.6007 ms | lockstep full scan      |
| real_workload | lix_sqlite  | `select_keys_only`    | 7.3973 ms..7.5315 ms | unchanged key-only path |
| real_workload | lix_sqlite  | `select_headers_only` | 9.4884 ms..9.5998 ms | header avoids payload   |
| real_workload | lix_sqlite  | `select_all_rows`     | 13.444 ms..14.735 ms | full still reads both   |
| real_workload | lix_rocksdb | `select_keys_only`    | 7.0426 ms..7.3303 ms | unchanged key-only path |
| real_workload | lix_rocksdb | `select_headers_only` | 8.9787 ms..9.2293 ms | header avoids payload   |
| real_workload | lix_rocksdb | `select_all_rows`     | 10.725 ms..11.062 ms | full still reads both   |
| real_workload | lix_rocksdb | `insert_all_rows`     | 28.385 ms..30.075 ms | double physical puts    |

Notes:

```text
The read-side goal landed: header reads are now O(rows * (key + header)) instead
of O(rows * full row). Full reads remain O(rows * full row) and use lockstep
header/payload scans to avoid chunked point-get overhead.

The write-side tradeoff is real: each live row now writes two physical records,
so put counts roughly double and write bytes increase by the duplicated identity
key. This is acceptable for the projection experiment, but future write
optimization should consider shorter payload keys, row ids, or a storage
multi-column/column-family abstraction if write throughput becomes the primary
bottleneck.
```

## Optimization 33: clean-cut untracked read surface

Date: 2026-05-13

Axis: remove the legacy mixed `read_many` abstraction after introducing
projection-shaped `get_many` and `scan`.

Change:

```text
UntrackedStateStoreReader now exposes only:

  get_many(UntrackedStateGetManyRequest)
  scan(UntrackedStateScanRequest)

The old read_many request/response/result types and private materialized read
path were removed. Existing materialized callers now request Full projection
through get_many/scan and explicitly convert the projected rows at the call
boundary.
```

Verification commands:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state:: --features storage-benches
rg -n "read_many|UntrackedStateReadManyRequest|UntrackedStateReadManyResponse|UntrackedStateReadRequest|UntrackedStateReadResult" packages/engine/src packages/engine/tests packages/engine/benches
```

Notes:

```text
This is API hardening, not a direct benchmark optimization. The performance
contract is clearer: point reads use get_many, ordered/ranged reads use scan,
and projection controls bytes read. The removed read_many wrapper can no longer
hide scan-vs-get costs or require fake materialized rows for identity-only
results.
```

## Optimization 34: projection-aware storage scan API

Date: 2026-05-13

Axis: move untracked scan projection lowering into storage/backend.

Change:

```text
Added additive scan2 APIs to StorageReader and BackendReadTransaction:

  primary_namespace: key order, cursor, range, returned keys
  joined_namespaces: same-key namespaces allowed for joined values
  projection: KeysOnly or Values([namespace...])

The default fallback lowers to existing scan/get primitives:

  KeysOnly -> scan_keys
  primary Values -> scan_entries
  joined Values -> primary scan plus grouped get_values for joined namespaces

Untracked scan now issues one logical scan2 request per page:

  Identity -> primary uh2, KeysOnly
  Header   -> primary uh2, Values([uh2])
  Payload  -> primary up2, Values([up2])
  Full     -> primary uh2, joined up2, Values([uh2, up2])
```

Verification commands:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state:: --features storage-benches
cargo test -p lix_engine storage:: --lib
```

Notes:

```text
This keeps the old storage APIs in place while giving untracked_state a single
first-principles scan request shape. Missing joined payload values are preserved
as None by storage scan2 and rejected by untracked full projection; header and
identity projections remain payload-free.
```

Smoke scoreboard:

```sh
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|select_all_rows|select_keys_only|select_headers_only)/1k'
```

Smoke I/O scoreboard:

| workload | backend     | operation             | logical rows | io ops | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes/row | puts | deletes | delete ranges | write bytes/row |
| -------- | ----------- | --------------------- | -----------: | -----: | -----------: | ---------: | --------: | -------: | ---------: | --------: | -------------: | ---: | ------: | ------------: | --------------: |
| smoke/1k | lix_sqlite  | `insert_all_rows`     |         1000 |      1 |      1012.51 |          0 |         0 |        0 |          0 |         0 |           0.00 | 2001 |       0 |             0 |         1012.51 |
| smoke/1k | lix_sqlite  | `select_all_rows`     |         1000 |      3 |      1012.51 |          3 |         2 |     1001 |          1 |      2001 |        1012.51 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `insert_all_rows`     |         1000 |      1 |      1012.51 |          0 |         0 |        0 |          0 |         0 |           0.00 | 2001 |       0 |             0 |         1012.51 |
| smoke/1k | lix_rocksdb | `select_all_rows`     |         1000 |      3 |      1012.51 |          3 |         2 |     1001 |          1 |      2001 |        1012.51 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |

Smoke timing scoreboard:

| backend     | operation                |          time 95% CI | Criterion change                 |
| ----------- | ------------------------ | -------------------: | -------------------------------- |
| lix_sqlite  | `insert_all_rows/1k`     | 7.1249 ms..7.4199 ms | +0.6466%..+4.7433%, within noise |
| lix_sqlite  | `select_all_rows/1k`     | 6.1300 ms..6.2851 ms | +23.890%..+27.821%, regressed    |
| lix_sqlite  | `select_keys_only/1k`    | 4.4649 ms..4.5179 ms | -2.1406%..+1.4118%, no change    |
| lix_sqlite  | `select_headers_only/1k` | 4.7178 ms..4.9949 ms | -2.4591%..+4.5266%, no change    |
| lix_rocksdb | `insert_all_rows/1k`     | 3.4858 ms..3.5621 ms | -1.4041%..+1.8484%, no change    |
| lix_rocksdb | `select_all_rows/1k`     | 2.0157 ms..2.0460 ms | +26.753%..+29.613%, regressed    |
| lix_rocksdb | `select_keys_only/1k`    | 1.0817 ms..1.1319 ms | -2.8529%..+1.5468%, no change    |
| lix_rocksdb | `select_headers_only/1k` | 1.3171 ms..1.4736 ms | -2.5478%..+4.6533%, no change    |

Smoke notes:

```text
The default scan2 fallback preserves key-only and header-only I/O. Full scans
now use one primary header scan plus grouped payload get_values in the fallback,
so read bytes remain unchanged but select_all_rows regresses versus the prior
lockstep dual-scan implementation until backend-specific scan2 overrides lower
the joined projection more directly.
```

Native scan2 backend override follow-up:

```text
Implemented native scan2 overrides in the benchmark SQLite and RocksDB
backends used by the untracked_state CRUD scoreboard.

SQLite lowers Values([primary, joined...]) to one ordered primary scan with
LEFT JOINs on requested joined namespaces by key. RocksDB lowers committed
multi-namespace scan2 to one ordered primary iterator plus co-ordered joined
iterators advanced to each primary key. Both preserve primary_namespace as the
only source of key order, cursor, range, and returned keys.

The benchmark counting wrapper now forwards scan2 to the inner backend instead
of accidentally invoking the default fallback at the wrapper layer.
```

Verification commands:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine storage:: --lib
cargo test -p lix_engine untracked_state:: --features storage-benches
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|select_all_rows|select_keys_only|select_headers_only)/1k'
```

Native scan2 smoke I/O scoreboard:

| workload | backend     | operation             | logical rows | io ops | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes/row | puts | deletes | delete ranges | write bytes/row |
| -------- | ----------- | --------------------- | -----------: | -----: | -----------: | ---------: | --------: | -------: | ---------: | --------: | -------------: | ---: | ------: | ------------: | --------------: |
| smoke/1k | lix_sqlite  | `insert_all_rows`     |         1000 |      1 |      1012.51 |          0 |         0 |        0 |          0 |         0 |           0.00 | 2001 |       0 |             0 |         1012.51 |
| smoke/1k | lix_sqlite  | `select_all_rows`     |         1000 |      2 |       931.31 |          2 |         1 |        1 |          1 |      2001 |         931.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `insert_all_rows`     |         1000 |      1 |      1012.51 |          0 |         0 |        0 |          0 |         0 |           0.00 | 2001 |       0 |             0 |         1012.51 |
| smoke/1k | lix_rocksdb | `select_all_rows`     |         1000 |      2 |       931.31 |          2 |         1 |        1 |          1 |      2001 |         931.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |

Native scan2 smoke timing scoreboard:

| backend     | operation                |          time 95% CI | Criterion change              |
| ----------- | ------------------------ | -------------------: | ----------------------------- |
| lix_sqlite  | `insert_all_rows/1k`     | 7.2089 ms..7.5665 ms | -0.5906%..+6.4073%, no change |
| lix_sqlite  | `select_all_rows/1k`     | 5.3959 ms..5.8378 ms | -11.361%..-6.8866%, improved  |
| lix_sqlite  | `select_keys_only/1k`    | 4.4697 ms..4.6229 ms | -0.9992%..+2.8956%, no change |
| lix_sqlite  | `select_headers_only/1k` | 4.5916 ms..4.7424 ms | -5.4266%..+0.4231%, no change |
| lix_rocksdb | `insert_all_rows/1k`     | 3.4360 ms..3.5090 ms | -1.9164%..+1.2597%, no change |
| lix_rocksdb | `select_all_rows/1k`     | 1.5716 ms..1.6313 ms | -22.970%..-20.656%, improved  |
| lix_rocksdb | `select_keys_only/1k`    | 1.0892 ms..1.2081 ms | -0.9241%..+7.5818%, no change |
| lix_rocksdb | `select_headers_only/1k` | 1.2370 ms..1.3243 ms | -10.975%..-2.6151%, improved  |

Native scan2 notes:

```text
The hard cut recovered the scan2 abstraction cost in smoke. Full scans now use
one logical scan2 backend call after the format marker read, avoid duplicated
payload key bytes in the accounting report, and improve select_all_rows on both
bench backends versus the default fallback.
```

Storage plan smoke benchmark follow-up:

```text
Added a 1k smoke storage-plan benchmark group to untracked_state_crud:

  untracked_state_crud/storage_plans/lix_sqlite/smoke/*
  untracked_state_crud/storage_plans/lix_rocksdb/smoke/*
  untracked_state_crud/storage_plans/raw_sqlite_projected/smoke/*

The backend API plans use the same seeded untracked uh2/up2 rows:

  scan_keys_header
  scan_entries_header
  dual_scan_full
  scan2_join_full
  primary_scan_get_payload_full

The raw SQLite projected-table ceiling uses a single WITHOUT ROWID table with:

  key BLOB PRIMARY KEY
  header BLOB NOT NULL
  payload BLOB NOT NULL
```

Verification commands:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/storage_plans/.*/smoke/.*/1k'
```

Storage plan smoke timing scoreboard:

| backend/profile      | plan                               |          time 95% CI |
| -------------------- | ---------------------------------- | -------------------: |
| lix_sqlite           | `scan_keys_header/1k`              | 4.1816 ms..4.3506 ms |
| lix_sqlite           | `scan_entries_header/1k`           | 4.1440 ms..4.3025 ms |
| lix_sqlite           | `dual_scan_full/1k`                | 4.6349 ms..6.5113 ms |
| lix_sqlite           | `scan2_join_full/1k`               | 4.7852 ms..4.9004 ms |
| lix_sqlite           | `primary_scan_get_payload_full/1k` | 5.3915 ms..5.5910 ms |
| lix_rocksdb          | `scan_keys_header/1k`              | 786.11 us..873.01 us |
| lix_rocksdb          | `scan_entries_header/1k`           | 849.93 us..863.63 us |
| lix_rocksdb          | `dual_scan_full/1k`                | 1.0261 ms..1.0476 ms |
| lix_rocksdb          | `scan2_join_full/1k`               | 1.0155 ms..1.0280 ms |
| lix_rocksdb          | `primary_scan_get_payload_full/1k` | 1.4692 ms..1.5426 ms |
| raw_sqlite_projected | `scan_keys/1k`                     | 3.8663 ms..3.9491 ms |
| raw_sqlite_projected | `scan_header/1k`                   | 3.8932 ms..3.9778 ms |
| raw_sqlite_projected | `scan_full/1k`                     | 3.8851 ms..4.0478 ms |

Storage plan notes:

```text
The benchmark isolates the backend/storage plan cost from untracked decoding.
For RocksDB, scan2 co-iteration and dual scan are effectively tied, while
primary scan + payload get is materially slower. For SQLite, raw projected
single-table scans are the ceiling and are faster than the generic KV plans;
scan2 join is more stable than the dual-scan run here but still above the raw
projected table ceiling.
```

## Optimization 35: packed KV untracked rows + value-projection scan2

Hard-cut untracked storage from split `uh2`/`up2` namespaces to one packed `u3`
KV namespace. `scan2` is now a single-namespace physical scan API with
`KeysOnly`, `FullValue`, and projected value parts instead of same-key namespace
joins. Untracked writes one framed value per live row, and header/payload scans
project the framed value part they need.

Implementation notes:

```text
- New untracked format marker: 3.
- Old split marker 2 is rejected instead of compatibility-read.
- Packed row value: LXU2 + flags + fixed-width decimal header/payload lengths
  + existing header bytes + existing payload bytes.
- SQLite bench backend lowers framed header/payload projections to substr(...)
  over the single untracked KV table.
- RocksDB bench backend uses one ordered iterator and projects value bytes after
  reading each entry.
```

Verification commands:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine storage:: --lib
cargo test -p lix_engine untracked_state:: --features storage-benches
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|select_all_rows|select_keys_only|select_headers_only)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/storage_plans/.*/smoke/.*/1k'
```

Smoke I/O scoreboard:

| workload | backend     | operation             | logical rows | io ops | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes/row | puts | deletes | delete ranges | write bytes/row |
| -------- | ----------- | --------------------- | -----------: | -----: | -----------: | ---------: | --------: | -------: | ---------: | --------: | -------------: | ---: | ------: | ------------: | --------------: |
| smoke/1k | lix_sqlite  | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_sqlite  | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_rocksdb | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |

Smoke timing scoreboard:

| backend     | operation                |          time 95% CI | Criterion change              |
| ----------- | ------------------------ | -------------------: | ----------------------------- |
| lix_sqlite  | `insert_all_rows/1k`     | 6.3080 ms..6.8760 ms | -12.396%..+15.196%, no change |
| lix_sqlite  | `select_all_rows/1k`     | 4.5881 ms..4.8284 ms | -18.716%..-13.567%, improved  |
| lix_sqlite  | `select_keys_only/1k`    | 4.3094 ms..4.3693 ms | -5.8866%..-2.0172%, improved  |
| lix_sqlite  | `select_headers_only/1k` | 4.5250 ms..4.6154 ms | -3.1345%..-0.0591%, no change |
| lix_rocksdb | `insert_all_rows/1k`     | 2.9480 ms..3.0842 ms | -16.730%..-12.347%, improved  |
| lix_rocksdb | `select_all_rows/1k`     | 1.5211 ms..1.5601 ms | -2.9810%..+7.3395%, no change |
| lix_rocksdb | `select_keys_only/1k`    | 1.0761 ms..1.1772 ms | -8.3188%..+0.7880%, no change |
| lix_rocksdb | `select_headers_only/1k` | 1.2944 ms..1.3642 ms | +1.3066%..+8.0670%, regressed |

Storage plan smoke timing scoreboard:

| backend/profile      | plan                           |          time 95% CI |
| -------------------- | ------------------------------ | -------------------: |
| lix_sqlite           | `scan_keys_row/1k`             | 3.8841 ms..3.9590 ms |
| lix_sqlite           | `scan2_header_valuepart/1k`    | 4.0210 ms..4.1437 ms |
| lix_sqlite           | `dual_scan2_header_payload/1k` | 4.3417 ms..4.4672 ms |
| lix_sqlite           | `scan2_full_value/1k`          | 3.8748 ms..4.0528 ms |
| lix_sqlite           | `scan_keys_get_full_value/1k`  | 5.0485 ms..5.2669 ms |
| lix_rocksdb          | `scan_keys_row/1k`             | 814.86 us..837.35 us |
| lix_rocksdb          | `scan2_header_valuepart/1k`    | 923.10 us..959.31 us |
| lix_rocksdb          | `dual_scan2_header_payload/1k` | 1.1901 ms..1.2265 ms |
| lix_rocksdb          | `scan2_full_value/1k`          | 885.74 us..911.21 us |
| lix_rocksdb          | `scan_keys_get_full_value/1k`  | 1.4346 ms..1.4937 ms |
| raw_sqlite_projected | `scan_keys/1k`                 | 3.8810 ms..3.9701 ms |
| raw_sqlite_projected | `scan_header/1k`               | 3.8971 ms..4.0348 ms |
| raw_sqlite_projected | `scan_full/1k`                 | 3.9954 ms..4.1977 ms |

Optimization 35 notes:

```text
The packed KV shape removes the second live-row put in smoke: insert_all_rows
drops from 2001 puts to 1001 puts. Full read I/O is one packed value per key;
the fixed frame adds about 25 bytes/row versus the native scan2 split-read
scoreboard, but write bytes still drop versus the split format because the
payload namespace key is gone.

The storage-plan ceiling moved as intended. SQLite scan2_full_value is now at
the raw projected SQLite scan ceiling for 1k smoke, and RocksDB scan2_full_value
is faster than the previous joined scan2 storage-plan result. Header-only
RocksDB is slightly slower in this run because RocksDB still reads the full
packed value before slicing; this is the expected tradeoff of keeping a simple
KV value shape.
```

## Optimization 36: additive scan_plan API + untracked migration

Added the first-principles backend-lowerable `scan_plan` API alongside legacy
`scan2`. The legacy APIs stay source-compatible. Untracked scans now compile
filters into ordered key spans and issue one logical `scan_plan` request per
page; the default backend/storage fallback lowers spans through existing
`scan_keys` / `scan_entries`.

Implementation notes:

```text
- scan2 remains available for legacy callers and benchmark adapters.
- scan_plan owns namespace, spans, after, page_size, and projection.
- Untracked pushes version/schema/entity/file filters into key spans where the
  row-key shape supports it, then keeps residual Rust filters for correctness.
- No native SQLite/RocksDB scan_plan override was added in this pass, so
  projected value-part scans use the fallback and read full packed values.
```

Verification and scoreboard commands:

```sh
cargo check -p lix_engine --features storage-benches --benches --tests
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|select_all_rows|select_keys_only|select_headers_only)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/storage_plans/.*/smoke/.*/1k'
```

Smoke I/O scoreboard:

| workload | backend     | operation             | logical rows | io ops | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes/row | puts | deletes | delete ranges | write bytes/row |
| -------- | ----------- | --------------------- | -----------: | -----: | -----------: | ---------: | --------: | -------: | ---------: | --------: | -------------: | ---: | ------: | ------------: | --------------: |
| smoke/1k | lix_sqlite  | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_sqlite  | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_headers_only` |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_rocksdb | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_headers_only` |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |

Smoke timing scoreboard:

| backend     | operation                |          time 95% CI | Criterion change              |
| ----------- | ------------------------ | -------------------: | ----------------------------- |
| lix_sqlite  | `insert_all_rows/1k`     | 6.0872 ms..6.2532 ms | -29.367%..-4.3810%, improved  |
| lix_sqlite  | `select_all_rows/1k`     | 4.6176 ms..4.8058 ms | -2.2873%..+3.1899%, no change |
| lix_sqlite  | `select_keys_only/1k`    | 4.0556 ms..4.1575 ms | -5.8344%..-1.8699%, improved  |
| lix_sqlite  | `select_headers_only/1k` | 4.4472 ms..4.5585 ms | -2.0573%..+1.9168%, no change |
| lix_rocksdb | `insert_all_rows/1k`     | 2.9690 ms..3.0612 ms | -2.3209%..+3.2582%, no change |
| lix_rocksdb | `select_all_rows/1k`     | 1.5107 ms..1.5502 ms | -8.8005%..+2.1618%, no change |
| lix_rocksdb | `select_keys_only/1k`    | 1.0937 ms..1.1454 ms | -1.7199%..+5.2797%, no change |
| lix_rocksdb | `select_headers_only/1k` | 1.3231 ms..1.3614 ms | +0.0782%..+4.4395%, no change |

Storage plan smoke timing scoreboard:

| backend/profile      | plan                           |          time 95% CI |
| -------------------- | ------------------------------ | -------------------: |
| lix_sqlite           | `scan_keys_row/1k`             | 3.7184 ms..3.8961 ms |
| lix_sqlite           | `scan2_header_valuepart/1k`    | 4.0606 ms..4.1722 ms |
| lix_sqlite           | `dual_scan2_header_payload/1k` | 4.4911 ms..4.9946 ms |
| lix_sqlite           | `scan2_full_value/1k`          | 3.8834 ms..4.0531 ms |
| lix_sqlite           | `scan_keys_get_full_value/1k`  | 5.1495 ms..5.2216 ms |
| lix_rocksdb          | `scan_keys_row/1k`             | 816.41 us..848.09 us |
| lix_rocksdb          | `scan2_header_valuepart/1k`    | 917.51 us..937.14 us |
| lix_rocksdb          | `dual_scan2_header_payload/1k` | 1.1864 ms..1.2589 ms |
| lix_rocksdb          | `scan2_full_value/1k`          | 872.04 us..967.55 us |
| lix_rocksdb          | `scan_keys_get_full_value/1k`  | 1.3636 ms..1.4090 ms |
| raw_sqlite_projected | `scan_keys/1k`                 | 3.7906 ms..3.9605 ms |
| raw_sqlite_projected | `scan_header/1k`               | 3.7746 ms..3.8746 ms |
| raw_sqlite_projected | `scan_full/1k`                 | 3.7996 ms..3.8960 ms |

Optimization 36 notes:

```text
This pass is an API/ownership cut, not a backend-native lowering pass. The key
result is that untracked now expresses one physical scan plan with spans, cursor,
limit, and projection while scan2 remains available.

The I/O scoreboard exposes the next backend task clearly: select_headers_only
now reads 956.31 bytes/row through fallback scan_entries instead of the 136.22
bytes/row native scan2 value-part path from Optimization 35. Timings were still
within noise for 1k smoke, but the I/O regression confirms that SQLite/RocksDB
native scan_plan overrides are the next cut.

The storage plan scoreboard above is still the legacy scan2 storage-plan group;
it remains useful as the current native-projection ceiling until scan_plan-native
storage-plan probes are added.
```

## Optimization 37: native scan_plan lowering for bench backends

Implemented native `scan_plan` overrides for the storage benchmark backends and
added direct scan-plan probes to the storage-plan smoke group. The additive
`scan2` API remains intact; `scan_plan` is now the backend-lowerable path used
by untracked scans and measurable directly in the benchmark harness.

Implementation notes:

```text
- SQLite lowers scan_plan spans into ordered SQL key predicates.
- SQLite projects Header, Payload, FullValue, and multi-part projections in one
  query using the packed-row substr(...) expressions.
- RocksDB iterates spans directly; KeysOnly stays on key scans, while value-part
  projections read the packed value once and slice in backend code.
- The counting benchmark wrapper now forwards scan_plan to the inner backend so
  I/O probes measure native backend behavior instead of the default fallback.
- The storage-plan smoke group now includes scan_plan_header_valuepart,
  scan_plan_header_payload_parts, and scan_plan_full_value.
```

Verification and scoreboard commands:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine storage:: --lib
cargo test -p lix_engine untracked_state:: --lib
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(insert_all_rows|select_all_rows|select_keys_only|select_headers_only)/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/storage_plans/.*/smoke/.*/1k'
```

Smoke I/O scoreboard:

| workload | backend     | operation             | logical rows | io ops | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes/row | puts | deletes | delete ranges | write bytes/row |
| -------- | ----------- | --------------------- | -----------: | -----: | -----------: | ---------: | --------: | -------: | ---------: | --------: | -------------: | ---: | ------: | ------------: | --------------: |
| smoke/1k | lix_sqlite  | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_sqlite  | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_rocksdb | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |

Smoke timing scoreboard:

| backend     | operation                |          time 95% CI | Criterion change              |
| ----------- | ------------------------ | -------------------: | ----------------------------- |
| lix_sqlite  | `insert_all_rows/1k`     | 6.2740 ms..6.4024 ms | no change                     |
| lix_sqlite  | `select_all_rows/1k`     | 4.6824 ms..6.4854 ms | no change                     |
| lix_sqlite  | `select_keys_only/1k`    | 4.2193 ms..4.5494 ms | +1.0785%..+7.2012%, regressed |
| lix_sqlite  | `select_headers_only/1k` | 4.4338 ms..4.6643 ms | no change                     |
| lix_rocksdb | `insert_all_rows/1k`     | 2.9315 ms..3.0184 ms | no change                     |
| lix_rocksdb | `select_all_rows/1k`     | 1.6008 ms..1.6429 ms | change within noise threshold |
| lix_rocksdb | `select_keys_only/1k`    | 1.0587 ms..1.0765 ms | -6.6473%..-2.4297%, improved  |
| lix_rocksdb | `select_headers_only/1k` | 1.3091 ms..1.3535 ms | no change                     |

Storage plan smoke timing scoreboard:

| backend/profile      | plan                                |          time 95% CI |
| -------------------- | ----------------------------------- | -------------------: |
| lix_sqlite           | `scan_keys_row/1k`                  | 3.9207 ms..4.1203 ms |
| lix_sqlite           | `scan2_header_valuepart/1k`         | 4.0325 ms..4.1304 ms |
| lix_sqlite           | `dual_scan2_header_payload/1k`      | 4.6262 ms..4.7536 ms |
| lix_sqlite           | `scan2_full_value/1k`               | 3.9868 ms..4.1495 ms |
| lix_sqlite           | `scan_plan_header_valuepart/1k`     | 4.0912 ms..4.1735 ms |
| lix_sqlite           | `scan_plan_header_payload_parts/1k` | 4.3856 ms..4.4931 ms |
| lix_sqlite           | `scan_plan_full_value/1k`           | 3.9665 ms..4.1127 ms |
| lix_sqlite           | `scan_keys_get_full_value/1k`       | 5.2596 ms..5.4632 ms |
| lix_rocksdb          | `scan_keys_row/1k`                  | 800.32 us..828.66 us |
| lix_rocksdb          | `scan2_header_valuepart/1k`         | 945.05 us..1.0279 ms |
| lix_rocksdb          | `dual_scan2_header_payload/1k`      | 1.2541 ms..1.3306 ms |
| lix_rocksdb          | `scan2_full_value/1k`               | 930.68 us..947.22 us |
| lix_rocksdb          | `scan_plan_header_valuepart/1k`     | 988.21 us..1.0164 ms |
| lix_rocksdb          | `scan_plan_header_payload_parts/1k` | 1.0254 ms..1.0646 ms |
| lix_rocksdb          | `scan_plan_full_value/1k`           | 940.60 us..979.61 us |
| lix_rocksdb          | `scan_keys_get_full_value/1k`       | 1.3951 ms..1.4365 ms |
| raw_sqlite_projected | `scan_keys/1k`                      | 3.9161 ms..4.0282 ms |
| raw_sqlite_projected | `scan_header/1k`                    | 3.9471 ms..4.1030 ms |
| raw_sqlite_projected | `scan_full/1k`                      | 4.1792 ms..4.3182 ms |

Optimization 37 notes:

```text
The native scan_plan path restores header-only I/O from the Optimization 36
fallback regression: select_headers_only is back to 136.22 bytes/row instead of
956.31 bytes/row for both SQLite and RocksDB in the smoke I/O probe.

The direct storage-plan scoreboard shows scan_plan_full_value at parity with
scan2_full_value, and one-call scan_plan_header_payload_parts beating the
legacy dual scan2 header+payload plan in this smoke run. SQLite improves from
4.6262..4.7536 ms to 4.3856..4.4931 ms; RocksDB improves from
1.2541..1.3306 ms to 1.0254..1.0646 ms.

The remaining ceiling is physical layout, not API ownership: RocksDB still has
to read packed values for Header/Payload projections, and SQLite projected
header scans are near the raw projected SQLite ceiling but still pay packed-row
substr decoding.
```

## Optimization 38: v3 read-plan API for projected point reads and dense get planning

Added an additive backend/storage `read3` API and migrated untracked reads to
use it. The older `get_values`, `exists_many`, `scan2`, and `scan_plan` APIs
remain available.

Implementation notes:

```text
- New read3 source shapes: Keys, Spans, and KeysOrSpans.
- New read3 projection shape: KeysOnly or ValueParts(Header/Payload/FullValue).
- read3 pages carry keys, presence bits, projected value pages, optional
  request indexes, and scan cursors.
- Default fallback lowers point reads to get_values/exists_many and span reads
  to scan_plan.
- Untracked get_many now uses read3 for identity/header/payload/full point
  reads, preserving request order, duplicates, and misses.
- Untracked scan now uses read3 span reads instead of direct scan_plan calls.
- SQLite bench backend lowers projected point reads to one SQL query with the
  same substr(...) projection expressions used for scan_plan.
- SQLite KeysOrSpans uses a dense scan path at 512+ requested keys.
- RocksDB bench backend lowers read3 to MultiGet for sparse keys, bounded
  scan_plan for spans, and dense scan substitution at 4096+ requested keys.
```

Verification commands:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine storage::context::tests::read3 --lib
cargo test -p lix_engine untracked_state:: --lib
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/(select_all_by_pk|select_one_by_pk)/1k'
```

Smoke I/O notes:

```text
The v3 path preserves previous scan projection byte counts:
- select_keys_only: 81.22 bytes/row for both SQLite and RocksDB.
- select_headers_only: 136.22 bytes/row for both SQLite and RocksDB.
- select_all_by_pk: 956.31 bytes/row for both SQLite and RocksDB.

The I/O probe now records dense KeysOrSpans reads as scan-like logical reads
when the backend takes the span-capable path.
```

Focused smoke timing scoreboard:

| backend     | operation             |          time 95% CI | Criterion change             |
| ----------- | --------------------- | -------------------: | ---------------------------- |
| lix_sqlite  | `select_one_by_pk/1k` | 3.7533 ms..3.9476 ms | no change                    |
| lix_sqlite  | `select_all_by_pk/1k` | 5.4244 ms..5.6657 ms | no change                    |
| lix_rocksdb | `select_one_by_pk/1k` | 731.18 us..749.49 us | no change                    |
| lix_rocksdb | `select_all_by_pk/1k` | 2.3044 ms..2.3442 ms | -13.434%..-9.9288%, improved |

Optimization 38 notes:

```text
The API cut is now in place: untracked expresses projected point reads and
span-capable dense reads through one storage request, and backends own the
physical lowering. SQLite can project Header/Payload point reads without
materializing full packed values. RocksDB keeps sparse reads on MultiGet and
only switches to scan substitution above the smoke-regression threshold.

The next benchmark work is a density matrix for select_all_by_pk at 1%, 10%,
50%, and 100%, plus payload-size variants for projected point reads. That will
let the backend thresholds become measured policy instead of hardcoded smoke
heuristics.
```

Full smoke scoreboard rerun:

```sh
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/.*/1k'
```

Full smoke I/O scoreboard:

| workload | backend     | operation             | logical rows | io ops | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes/row | puts | deletes | delete ranges | write bytes/row |
| -------- | ----------- | --------------------- | -----------: | -----: | -----------: | ---------: | --------: | -------: | ---------: | --------: | -------------: | ---: | ------: | ------------: | --------------: |
| smoke/1k | lix_sqlite  | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_sqlite  | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_one_by_pk`    |            1 |      2 |       336.00 |          2 |         1 |        1 |          1 |         2 |         336.00 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_all_by_pk`    |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `update_all_rows`     |         1000 |      1 |       316.49 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          316.49 |
| smoke/1k | lix_sqlite  | `update_one_by_pk`    |            1 |      1 |       179.00 |          0 |         0 |        0 |          0 |         0 |           0.00 |    2 |       0 |             0 |          179.00 |
| smoke/1k | lix_sqlite  | `delete_all_rows`     |         1000 |      1 |         0.02 |          0 |         0 |        0 |          0 |         0 |           0.00 |    1 |       0 |             3 |            0.02 |
| smoke/1k | lix_sqlite  | `delete_one_by_pk`    |            1 |      1 |        47.00 |          0 |         0 |        0 |          0 |         0 |           0.00 |    1 |       1 |             0 |           47.00 |
| smoke/1k | lix_rocksdb | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_rocksdb | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_one_by_pk`    |            1 |      2 |       336.00 |          2 |         1 |        1 |          1 |         2 |         336.00 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_all_by_pk`    |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `update_all_rows`     |         1000 |      1 |       316.49 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          316.49 |
| smoke/1k | lix_rocksdb | `update_one_by_pk`    |            1 |      1 |       179.00 |          0 |         0 |        0 |          0 |         0 |           0.00 |    2 |       0 |             0 |          179.00 |
| smoke/1k | lix_rocksdb | `delete_all_rows`     |         1000 |      1 |         0.02 |          0 |         0 |        0 |          0 |         0 |           0.00 |    1 |       0 |             3 |            0.02 |
| smoke/1k | lix_rocksdb | `delete_one_by_pk`    |            1 |      1 |        47.00 |          0 |         0 |        0 |          0 |         0 |           0.00 |    1 |       1 |             0 |           47.00 |

Full smoke timing scoreboard:

| backend     | operation                |          time 95% CI | Criterion change              |
| ----------- | ------------------------ | -------------------: | ----------------------------- |
| lix_sqlite  | `insert_all_rows/1k`     | 6.2531 ms..6.4806 ms | no change                     |
| lix_sqlite  | `select_all_rows/1k`     | 4.8661 ms..5.0333 ms | +3.1894%..+6.8280%, regressed |
| lix_sqlite  | `select_keys_only/1k`    | 4.4372 ms..4.7988 ms | +5.8552%..+11.063%, regressed |
| lix_sqlite  | `select_headers_only/1k` | 4.7482 ms..4.8692 ms | change within noise threshold |
| lix_sqlite  | `select_one_by_pk/1k`    | 4.1262 ms..4.4447 ms | +6.2468%..+14.613%, regressed |
| lix_sqlite  | `select_all_by_pk/1k`    | 5.7155 ms..5.8201 ms | +1.3034%..+6.3218%, regressed |
| lix_sqlite  | `update_all_rows/1k`     | 5.6017 ms..5.6758 ms | +7.9624%..+11.557%, regressed |
| lix_sqlite  | `update_one_by_pk/1k`    | 3.8087 ms..3.9978 ms | +3.4119%..+8.1866%, regressed |
| lix_sqlite  | `delete_all_rows/1k`     | 4.1421 ms..4.3714 ms | +4.5092%..+10.994%, regressed |
| lix_sqlite  | `delete_one_by_pk/1k`    | 4.0305 ms..4.3760 ms | +1.1131%..+12.458%, regressed |
| lix_rocksdb | `insert_all_rows/1k`     | 2.9472 ms..3.0212 ms | no change                     |
| lix_rocksdb | `select_all_rows/1k`     | 1.5464 ms..1.6078 ms | no change                     |
| lix_rocksdb | `select_keys_only/1k`    | 1.0740 ms..1.1004 ms | no change                     |
| lix_rocksdb | `select_headers_only/1k` | 1.3613 ms..1.3823 ms | no change                     |
| lix_rocksdb | `select_one_by_pk/1k`    | 729.42 us..753.85 us | no change                     |
| lix_rocksdb | `select_all_by_pk/1k`    | 2.4002 ms..2.4466 ms | +4.1741%..+6.6691%, regressed |
| lix_rocksdb | `update_all_rows/1k`     | 2.2743 ms..2.3183 ms | +10.645%..+14.566%, regressed |
| lix_rocksdb | `update_one_by_pk/1k`    | 645.84 us..660.21 us | +5.2407%..+9.3978%, regressed |
| lix_rocksdb | `delete_all_rows/1k`     | 808.73 us..831.73 us | +1.7133%..+5.2046%, regressed |
| lix_rocksdb | `delete_one_by_pk/1k`    | 651.97 us..666.67 us | +3.5656%..+9.0407%, regressed |

Full smoke scoreboard note:

```text
The full smoke timing run is noisier than the focused read-only rerun and flags
write/delete regressions even though Optimization 38 only changes read paths.
The stable I/O table is the stronger correctness signal here. Before using the
timing table to tune thresholds, rerun the density matrix and compare forced
point vs forced scan strategies in the same Criterion baseline window.
```

## Optimization 39: explicit read3 strategies and compact scan presence

Implementation:

- Added `BackendKvRead3Strategy::{Auto, Points, Scan}` and mirrored storage-level strategy.
- Kept untracked point reads on `Auto`, while scan reads declare `Scan`.
- Added `BackendKvRead3Presence::{All, Bitmap}` and storage mirror so span scans no longer allocate one `true` bit per returned row.
- Updated fallback, SQLite bench backend, RocksDB bench backend, untracked consumers, and I/O accounting to use compact presence helpers.
- Added bench-only `LIX_READ3_STRATEGY=points|scan` override for backend `Auto` decisions so point-vs-scan density experiments can run without code edits.

Verification:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine storage::context::tests::read3 --lib
cargo test -p lix_engine untracked_state:: --lib
LIX_UNTRACKED_STATE_CRUD_IO=smoke LIX_READ3_STRATEGY=scan cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb)/smoke/.*/1k'
```

Default smoke I/O scoreboard:

| workload | backend     | operation             | logical rows | io ops | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes/row | puts | deletes | delete ranges | write bytes/row |
| -------- | ----------- | --------------------- | -----------: | -----: | -----------: | ---------: | --------: | -------: | ---------: | --------: | -------------: | ---: | ------: | ------------: | --------------: |
| smoke/1k | lix_sqlite  | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_sqlite  | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_one_by_pk`    |            1 |      2 |       336.00 |          2 |         1 |        1 |          1 |         2 |         336.00 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_all_by_pk`    |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `update_all_rows`     |         1000 |      1 |       316.49 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          316.49 |
| smoke/1k | lix_sqlite  | `update_one_by_pk`    |            1 |      1 |       179.00 |          0 |         0 |        0 |          0 |         0 |           0.00 |    2 |       0 |             0 |          179.00 |
| smoke/1k | lix_sqlite  | `delete_all_rows`     |         1000 |      1 |         0.02 |          0 |         0 |        0 |          0 |         0 |           0.00 |    1 |       0 |             3 |            0.02 |
| smoke/1k | lix_sqlite  | `delete_one_by_pk`    |            1 |      1 |        47.00 |          0 |         0 |        0 |          0 |         0 |           0.00 |    1 |       1 |             0 |           47.00 |
| smoke/1k | lix_rocksdb | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_rocksdb | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_one_by_pk`    |            1 |      2 |       336.00 |          2 |         1 |        1 |          1 |         2 |         336.00 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_all_by_pk`    |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `update_all_rows`     |         1000 |      1 |       316.49 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          316.49 |
| smoke/1k | lix_rocksdb | `update_one_by_pk`    |            1 |      1 |       179.00 |          0 |         0 |        0 |          0 |         0 |           0.00 |    2 |       0 |             0 |          179.00 |
| smoke/1k | lix_rocksdb | `delete_all_rows`     |         1000 |      1 |         0.02 |          0 |         0 |        0 |          0 |         0 |           0.00 |    1 |       0 |             3 |            0.02 |
| smoke/1k | lix_rocksdb | `delete_one_by_pk`    |            1 |      1 |        47.00 |          0 |         0 |        0 |          0 |         0 |           0.00 |    1 |       1 |             0 |           47.00 |

Default smoke timing scoreboard:

| backend     | operation                |          time 95% CI | Criterion change              |
| ----------- | ------------------------ | -------------------: | ----------------------------- |
| lix_sqlite  | `insert_all_rows/1k`     | 6.1586 ms..6.3194 ms | no change                     |
| lix_sqlite  | `select_all_rows/1k`     | 4.7206 ms..4.8481 ms | -5.6665%..-1.7116%, improved  |
| lix_sqlite  | `select_keys_only/1k`    | 4.3573 ms..4.4722 ms | change within noise threshold |
| lix_sqlite  | `select_headers_only/1k` | 4.4960 ms..4.5854 ms | -5.5679%..-2.0745%, improved  |
| lix_sqlite  | `select_one_by_pk/1k`    | 3.8902 ms..3.9732 ms | -9.3443%..-3.7231%, improved  |
| lix_sqlite  | `select_all_by_pk/1k`    | 5.3756 ms..5.6342 ms | -8.4561%..-4.1783%, improved  |
| lix_sqlite  | `update_all_rows/1k`     | 5.5015 ms..5.5965 ms | change within noise threshold |
| lix_sqlite  | `update_one_by_pk/1k`    | 3.5835 ms..3.7482 ms | -10.145%..-6.1479%, improved  |
| lix_sqlite  | `delete_all_rows/1k`     | 4.1619 ms..4.2600 ms | no change                     |
| lix_sqlite  | `delete_one_by_pk/1k`    | 3.7839 ms..3.9009 ms | -13.033%..-3.4056%, improved  |
| lix_rocksdb | `insert_all_rows/1k`     | 2.9601 ms..3.0870 ms | change within noise threshold |
| lix_rocksdb | `select_all_rows/1k`     | 1.5837 ms..1.6157 ms | no change                     |
| lix_rocksdb | `select_keys_only/1k`    | 1.0909 ms..1.1107 ms | no change                     |
| lix_rocksdb | `select_headers_only/1k` | 1.3553 ms..1.3919 ms | no change                     |
| lix_rocksdb | `select_one_by_pk/1k`    | 727.88 us..766.40 us | no change                     |
| lix_rocksdb | `select_all_by_pk/1k`    | 2.3705 ms..2.5896 ms | no change                     |
| lix_rocksdb | `update_all_rows/1k`     | 2.1938 ms..2.2540 ms | -4.7430%..-2.2151%, improved  |
| lix_rocksdb | `update_one_by_pk/1k`    | 626.44 us..633.12 us | -4.3659%..-1.0469%, improved  |
| lix_rocksdb | `delete_all_rows/1k`     | 805.21 us..844.93 us | no change                     |
| lix_rocksdb | `delete_one_by_pk/1k`    | 654.47 us..672.61 us | no change                     |

Result:

```text
All checks/tests passed. The default smoke I/O scoreboard stayed stable.
The compact scan presence change recovered the previous SQLite read-path
regressions: select_all_rows, select_headers_only, select_one_by_pk, and
select_all_by_pk all improved in this Criterion window. RocksDB read paths are
mostly neutral at smoke/1k.

The important new benchmark knob remains:

  LIX_READ3_STRATEGY=points|scan

Use it with the density matrix to compare Auto, forced points, and forced scan
for select_all_by_pk and projected point-read variants.
```

## Optimization 40: clean redb embedded backend baseline after read_v3 hard cut

Implementation context:

- Kept redb as the third embedded backend in the untracked-state smoke matrix.
- Reran the baseline after the hard Rust type cut that removed public `scan_plan_v3`/`ScanPlanV3` APIs.
- The storage-plan smoke surface now uses `read_v3_scan_*` labels and span-based `read_v3` requests.
- The shared storage bench backends no longer carry native v3 overrides; the isolated untracked CRUD backend copies remain local to the untracked benchmark harness.

Verification:

```sh
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb|lix_redb)/smoke/.*/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/storage_plans/.*/smoke/.*/1k'
```

Smoke I/O scoreboard:

| workload | backend     | operation             | logical rows | io ops | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes/row | puts | deletes | delete ranges | write bytes/row |
| -------- | ----------- | --------------------- | -----------: | -----: | -----------: | ---------: | --------: | -------: | ---------: | --------: | -------------: | ---: | ------: | ------------: | --------------: |
| smoke/1k | lix_sqlite  | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_sqlite  | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_one_by_pk`    |            1 |      2 |       336.00 |          2 |         1 |        1 |          1 |         2 |         336.00 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_all_by_pk`    |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_rocksdb | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_one_by_pk`    |            1 |      2 |       336.00 |          2 |         1 |        1 |          1 |         2 |         336.00 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_all_by_pk`    |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_redb    | `insert_all_rows`     |         1000 |      1 |       956.31 |          0 |         0 |        0 |          0 |         0 |           0.00 | 1001 |       0 |             0 |          956.31 |
| smoke/1k | lix_redb    | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_redb    | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_redb    | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_redb    | `select_one_by_pk`    |            1 |      2 |       336.00 |          2 |         1 |        1 |          1 |         2 |         336.00 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_redb    | `select_all_by_pk`    |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |

Smoke timing scoreboard:

| backend     | operation                |          time 95% CI | Criterion change |
| ----------- | ------------------------ | -------------------: | ---------------- |
| lix_sqlite  | `insert_all_rows/1k`     | 6.1705 ms..6.5242 ms | no change        |
| lix_sqlite  | `select_all_rows/1k`     | 4.8608 ms..4.9797 ms | no change        |
| lix_sqlite  | `select_keys_only/1k`    | 4.3879 ms..4.4533 ms | regressed        |
| lix_sqlite  | `select_headers_only/1k` | 4.5437 ms..4.6608 ms | no change        |
| lix_sqlite  | `select_one_by_pk/1k`    | 3.9575 ms..4.0997 ms | no change        |
| lix_sqlite  | `select_all_by_pk/1k`    | 6.5438 ms..6.7136 ms | regressed        |
| lix_sqlite  | `update_all_rows/1k`     | 5.4140 ms..5.6726 ms | regressed        |
| lix_sqlite  | `update_one_by_pk/1k`    | 3.6751 ms..3.9019 ms | no change        |
| lix_sqlite  | `delete_all_rows/1k`     | 3.9683 ms..4.1307 ms | noise threshold  |
| lix_sqlite  | `delete_one_by_pk/1k`    | 3.8248 ms..3.8816 ms | no change        |
| lix_rocksdb | `insert_all_rows/1k`     | 2.9503 ms..3.0185 ms | noise threshold  |
| lix_rocksdb | `select_all_rows/1k`     | 1.5737 ms..1.6118 ms | no change        |
| lix_rocksdb | `select_keys_only/1k`    | 1.0849 ms..1.0980 ms | noise threshold  |
| lix_rocksdb | `select_headers_only/1k` | 1.3278 ms..1.3584 ms | no change        |
| lix_rocksdb | `select_one_by_pk/1k`    | 706.16 us..722.46 us | improved         |
| lix_rocksdb | `select_all_by_pk/1k`    | 2.4093 ms..2.4856 ms | regressed        |
| lix_rocksdb | `update_all_rows/1k`     | 2.2358 ms..2.3192 ms | no change        |
| lix_rocksdb | `update_one_by_pk/1k`    | 622.57 us..641.32 us | noise threshold  |
| lix_rocksdb | `delete_all_rows/1k`     | 800.54 us..824.54 us | regressed        |
| lix_rocksdb | `delete_one_by_pk/1k`    | 629.34 us..643.77 us | no change        |
| lix_redb    | `insert_all_rows/1k`     | 4.5348 ms..4.6237 ms | noise threshold  |
| lix_redb    | `select_all_rows/1k`     | 2.8495 ms..3.0134 ms | no change        |
| lix_redb    | `select_keys_only/1k`    | 2.3631 ms..2.5655 ms | no change        |
| lix_redb    | `select_headers_only/1k` | 2.7244 ms..2.9029 ms | noise threshold  |
| lix_redb    | `select_one_by_pk/1k`    | 1.8965 ms..1.9896 ms | no change        |
| lix_redb    | `select_all_by_pk/1k`    | 3.5581 ms..3.6421 ms | noise threshold  |
| lix_redb    | `update_all_rows/1k`     | 4.0360 ms..4.1914 ms | no change        |
| lix_redb    | `update_one_by_pk/1k`    | 2.0634 ms..2.1023 ms | no change        |
| lix_redb    | `delete_all_rows/1k`     | 3.1496 ms..3.3372 ms | no change        |
| lix_redb    | `delete_one_by_pk/1k`    | 2.0703 ms..2.1887 ms | no change        |

Storage-plan timing scoreboard:

| backend              | operation                        |          time 95% CI |
| -------------------- | -------------------------------- | -------------------: |
| lix_sqlite           | `read_v3_scan_header/1k`         | 3.9804 ms..4.0945 ms |
| lix_sqlite           | `read_v3_scan_header_payload/1k` | 4.1507 ms..4.3383 ms |
| lix_sqlite           | `read_v3_scan_full/1k`           | 4.0813 ms..4.2249 ms |
| lix_rocksdb          | `read_v3_scan_header/1k`         | 965.27 us..1.0048 ms |
| lix_rocksdb          | `read_v3_scan_header_payload/1k` | 1.1031 ms..1.1130 ms |
| lix_rocksdb          | `read_v3_scan_full/1k`           | 952.04 us..1.0140 ms |
| lix_redb             | `read_v3_scan_header/1k`         | 2.4152 ms..2.5452 ms |
| lix_redb             | `read_v3_scan_header_payload/1k` | 2.3546 ms..2.4451 ms |
| lix_redb             | `read_v3_scan_full/1k`           | 2.3781 ms..2.4564 ms |
| raw_sqlite_projected | `scan_keys/1k`                   | 3.9019 ms..3.9637 ms |
| raw_sqlite_projected | `scan_header/1k`                 | 4.0650 ms..4.3097 ms |
| raw_sqlite_projected | `scan_full/1k`                   | 4.0692 ms..4.1740 ms |

Result:

```text
The hard type cut did not change logical I/O. The direct storage-plan baseline
is now read_v3-only, with read_v3_scan_* labels. redb remains between SQLite
and RocksDB on most read paths. Shared storage bench backends use fallback v3
lowering, while the isolated untracked CRUD harness owns the backend matrix.
```

## Optimization 41: clean-slate read4 table read API

Implementation:

- Kept `read_v3` unchanged as the legacy/stable API.
- Added `read4` as a separate table-aware backend/storage API with table, keyspace, access segments, projection, output order, limit, and optional session.
- Removed all `read4 -> read_v3` lowering. Default backend/storage `read4` now returns unsupported unless a backend implements it.
- Migrated untracked-state `get_many` and `scan` to `read4`.
- Added direct `read4` support for `UnitTestBackend`.
- Added isolated untracked CRUD backend `read4` implementations for SQLite, RocksDB, and redb through a bench-local primitive lowering that uses only `get_values`/`exists_many`/`scan_*` primitives, not v3.
- Added `read4_density` smoke benchmarks for access shape, request order, density, projection, and backend.

Verification:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state:: --lib
cargo test -p lix_engine storage:: --lib
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb|lix_redb)/smoke/.*/1k'
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(storage_plans|read4_density)/.*/smoke/.*/1k'
```

Smoke I/O scoreboard:

| workload | backend     | operation             | logical rows | io ops | io bytes/row | read calls | get calls | get keys | scan calls | read rows | read bytes/row | puts | deletes | delete ranges | write bytes/row |
| -------- | ----------- | --------------------- | -----------: | -----: | -----------: | ---------: | --------: | -------: | ---------: | --------: | -------------: | ---: | ------: | ------------: | --------------: |
| smoke/1k | lix_sqlite  | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_one_by_pk`    |            1 |      2 |       336.00 |          2 |         2 |        2 |          0 |         2 |         336.00 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_sqlite  | `select_all_by_pk`    |         1000 |      2 |       956.31 |          2 |         2 |     1001 |          0 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_one_by_pk`    |            1 |      2 |       336.00 |          2 |         2 |        2 |          0 |         2 |         336.00 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_rocksdb | `select_all_by_pk`    |         1000 |      2 |       956.31 |          2 |         2 |     1001 |          0 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_redb    | `select_all_rows`     |         1000 |      2 |       956.31 |          2 |         1 |        1 |          1 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_redb    | `select_keys_only`    |         1000 |      2 |        81.22 |          2 |         1 |        1 |          1 |      1001 |          81.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_redb    | `select_headers_only` |         1000 |      2 |       136.22 |          2 |         1 |        1 |          1 |      1001 |         136.22 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_redb    | `select_one_by_pk`    |            1 |      2 |       336.00 |          2 |         2 |        2 |          0 |         2 |         336.00 |    0 |       0 |             0 |            0.00 |
| smoke/1k | lix_redb    | `select_all_by_pk`    |         1000 |      2 |       956.31 |          2 |         2 |     1001 |          0 |      1001 |         956.31 |    0 |       0 |             0 |            0.00 |

Smoke timing scoreboard:

| backend     | operation                |          time 95% CI | Criterion change              |
| ----------- | ------------------------ | -------------------: | ----------------------------- |
| lix_sqlite  | `insert_all_rows/1k`     | 6.2419 ms..6.4714 ms | no change                     |
| lix_sqlite  | `select_all_rows/1k`     | 4.7866 ms..4.8497 ms | change within noise threshold |
| lix_sqlite  | `select_keys_only/1k`    | 4.2114 ms..4.3957 ms | no change                     |
| lix_sqlite  | `select_headers_only/1k` | 4.4702 ms..4.7379 ms | no change                     |
| lix_sqlite  | `select_one_by_pk/1k`    | 3.6653 ms..3.8698 ms | -9.6279%..-5.4624%, improved  |
| lix_sqlite  | `select_all_by_pk/1k`    | 6.4267 ms..6.7797 ms | no change                     |
| lix_sqlite  | `update_all_rows/1k`     | 5.4256 ms..5.5791 ms | no change                     |
| lix_sqlite  | `update_one_by_pk/1k`    | 3.6891 ms..3.9044 ms | no change                     |
| lix_sqlite  | `delete_all_rows/1k`     | 3.9997 ms..4.1288 ms | no change                     |
| lix_sqlite  | `delete_one_by_pk/1k`    | 4.0195 ms..4.1404 ms | +5.6412%..+8.8642%, regressed |
| lix_rocksdb | `insert_all_rows/1k`     | 3.0216 ms..3.0834 ms | change within noise threshold |
| lix_rocksdb | `select_all_rows/1k`     | 1.5802 ms..1.6212 ms | no change                     |
| lix_rocksdb | `select_keys_only/1k`    | 1.0697 ms..1.0929 ms | no change                     |
| lix_rocksdb | `select_headers_only/1k` | 1.3291 ms..1.3467 ms | no change                     |
| lix_rocksdb | `select_one_by_pk/1k`    | 720.14 us..741.85 us | change within noise threshold |
| lix_rocksdb | `select_all_by_pk/1k`    | 2.4891 ms..2.5159 ms | no change                     |
| lix_rocksdb | `update_all_rows/1k`     | 2.2690 ms..2.3220 ms | no change                     |
| lix_rocksdb | `update_one_by_pk/1k`    | 621.93 us..635.23 us | no change                     |
| lix_rocksdb | `delete_all_rows/1k`     | 787.28 us..818.72 us | no change                     |
| lix_rocksdb | `delete_one_by_pk/1k`    | 635.63 us..657.40 us | no change                     |
| lix_redb    | `insert_all_rows/1k`     | 4.4612 ms..4.5865 ms | no change                     |
| lix_redb    | `select_all_rows/1k`     | 2.9481 ms..3.0123 ms | no change                     |
| lix_redb    | `select_keys_only/1k`    | 2.4153 ms..2.5026 ms | no change                     |
| lix_redb    | `select_headers_only/1k` | 2.5958 ms..2.7106 ms | no change                     |
| lix_redb    | `select_one_by_pk/1k`    | 1.9637 ms..2.0740 ms | no change                     |
| lix_redb    | `select_all_by_pk/1k`    | 3.4413 ms..3.6038 ms | -6.1158%..-1.0365%, improved  |
| lix_redb    | `update_all_rows/1k`     | 3.9753 ms..4.0628 ms | -5.5038%..-1.9926%, improved  |
| lix_redb    | `update_one_by_pk/1k`    | 1.9353 ms..2.0011 ms | -6.7472%..-3.0746%, improved  |
| lix_redb    | `delete_all_rows/1k`     | 3.0395 ms..3.1133 ms | -6.1895%..-1.6092%, improved  |
| lix_redb    | `delete_one_by_pk/1k`    | 2.0532 ms..2.1614 ms | no change                     |

Storage-plan read4 scoreboard:

| backend              | operation                         |          time 95% CI |
| -------------------- | --------------------------------- | -------------------: |
| lix_sqlite           | `read4_span_header/1k`            | 3.9859 ms..4.1296 ms |
| lix_sqlite           | `read4_span_header_payload/1k`    | 4.1161 ms..4.2445 ms |
| lix_sqlite           | `read4_span_full/1k`              | 4.0951 ms..4.3016 ms |
| lix_rocksdb          | `read4_span_header/1k`            | 900.81 us..934.57 us |
| lix_rocksdb          | `read4_span_header_payload/1k`    | 1.0239 ms..1.0518 ms |
| lix_rocksdb          | `read4_span_full/1k`              | 921.62 us..936.47 us |
| lix_redb             | `read4_span_header/1k`            | 2.1153 ms..2.2535 ms |
| lix_redb             | `read4_span_header_payload/1k`    | 2.2150 ms..2.3908 ms |
| lix_redb             | `read4_span_full/1k`              | 2.1921 ms..2.3669 ms |
| raw_sqlite_projected | `scan_keys/1k`                    | 4.0738 ms..4.1925 ms |
| raw_sqlite_projected | `scan_header/1k`                  | 3.8343 ms..3.9384 ms |
| raw_sqlite_projected | `scan_full/1k`                    | 4.0588 ms..4.1244 ms |

Representative read4 density scoreboard, 100% sorted/key-order:

| backend     | shape    | projection |          time 95% CI |
| ----------- | -------- | ---------- | -------------------: |
| lix_sqlite  | points   | keys       | 4.2507 ms..4.4441 ms |
| lix_sqlite  | points   | header     | 5.0973 ms..5.1559 ms |
| lix_sqlite  | points   | full       | 5.0189 ms..5.0739 ms |
| lix_sqlite  | run      | keys       | 4.2715 ms..4.3971 ms |
| lix_sqlite  | run      | header     | 5.1906 ms..5.3609 ms |
| lix_sqlite  | run      | full       | 5.1739 ms..5.2597 ms |
| lix_sqlite  | span     | keys       | 4.0149 ms..4.0932 ms |
| lix_sqlite  | span     | header     | 3.9989 ms..4.1038 ms |
| lix_sqlite  | span     | full       | 4.2653 ms..4.6410 ms |
| lix_rocksdb | points   | keys       | 837.92 us..880.87 us |
| lix_rocksdb | points   | header     | 1.3181 ms..1.3686 ms |
| lix_rocksdb | points   | full       | 1.3256 ms..1.3598 ms |
| lix_rocksdb | run      | keys       | 873.02 us..895.84 us |
| lix_rocksdb | run      | header     | 1.3573 ms..1.3705 ms |
| lix_rocksdb | run      | full       | 1.3757 ms..1.3925 ms |
| lix_rocksdb | span     | keys       | 750.29 us..780.11 us |
| lix_rocksdb | span     | header     | 892.75 us..968.85 us |
| lix_rocksdb | span     | full       | 904.00 us..913.62 us |
| lix_redb    | points   | keys       | 2.2210 ms..2.3418 ms |
| lix_redb    | points   | header     | 2.3790 ms..2.4861 ms |
| lix_redb    | points   | full       | 2.3025 ms..2.3791 ms |
| lix_redb    | run      | keys       | 2.1401 ms..2.2761 ms |
| lix_redb    | run      | header     | 2.3888 ms..2.4507 ms |
| lix_redb    | run      | full       | 2.3222 ms..2.4302 ms |
| lix_redb    | span     | keys       | 2.0343 ms..2.1936 ms |
| lix_redb    | span     | header     | 2.1726 ms..2.2599 ms |
| lix_redb    | span     | full       | 2.1484 ms..2.2201 ms |

Result:

```text
read4 is now an independent API surface. The implementation explicitly does not
call read_v3, and default read4 support is unsupported unless a backend opts in.

The smoke scoreboard confirms untracked now reaches read4: point-read I/O is
accounted as point/get-style access instead of scan-style v3 fallback. The
storage-plan and density scoreboards establish the clean baseline for native
read4 lowering. Today, run and point shapes remain close because the isolated
primitive bench implementation lowers runs through keyed point reads. Full-span
100% reads are already better than 100% point reads on RocksDB and redb, and
often better on SQLite, which points directly at the next cut: native run/span
lowering instead of primitive keyed lowering.
```

## Optimization 42: use read4 Run as a native dense access shape

Implementation:

- Changed untracked point-read planning so `read4_untracked_keys` partitions
  requested keys into the actual covering `Run` spans instead of copying every
  key into every run segment.
- Changed the isolated SQLite untracked CRUD backend to lower
  `BackendKvAccessSegment::Run` through bounded ordered range reads.
- SQLite run reads now scan the run range once, apply SQL projection, and then
  reorder/miss-fill results back to the requested order.
- RocksDB and redb already had run lowering through backend range collection,
  so this pass leaves those paths intact.

Verification:

```sh
cargo fmt -p lix_engine
cargo check -p lix_engine --features storage-benches --benches --tests
cargo test -p lix_engine untracked_state:: --lib
cargo test -p lix_engine storage:: --lib
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'untracked_state_crud/(lix_sqlite|lix_rocksdb|lix_redb)/smoke/select_(one_by_pk|all_by_pk)/1k'
LIX_UNTRACKED_STATE_CRUD_IO=smoke cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud __io_probe_no_timing__
```

Targeted smoke timing scoreboard:

| backend     | operation              |          time 95% CI | Criterion change              |
| ----------- | ---------------------- | -------------------: | ----------------------------- |
| lix_sqlite  | `select_one_by_pk/1k`  | 3.7340 ms..3.8815 ms | no change                     |
| lix_sqlite  | `select_all_by_pk/1k`  | 5.5339 ms..5.7322 ms | -16.599%..-13.257%, improved  |
| lix_rocksdb | `select_one_by_pk/1k`  | 738.94 us..758.48 us | no change                     |
| lix_rocksdb | `select_all_by_pk/1k`  | 2.3339 ms..2.3857 ms | no change                     |
| lix_redb    | `select_one_by_pk/1k`  | 1.8551 ms..1.9052 ms | -6.8747%..-3.0904%, improved  |
| lix_redb    | `select_all_by_pk/1k`  | 3.5972 ms..3.7912 ms | no change                     |

Smoke I/O result:

```text
The logical I/O shape is unchanged: public untracked reads still show two read
calls because the format marker and row read are both expressed as read4. The
`get calls`/`scan calls` columns are logical accounting buckets inside
record_read4, not legacy get_values/scan_* API usage.
```

Result:

```text
The first measurable benefit from read4's extra vocabulary is SQLite dense
by-primary-key reads. Treating Run as a real bounded range access shape improves
select_all_by_pk/1k by roughly 13-17% versus the previous v4 point-like
lowering. This is still not a 2x result because the physical row layout remains
packed and the operation still materializes the same row values; the API is now
able to expose the dense access shape to backends.
```
