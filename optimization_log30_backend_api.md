# Optimization Log 30: Backend API Write-Batch Baseline

Date: 2026-05-19

Goal: simplify the generic backend write API for maximum write performance
without making backends understand Lix domain stores.

This log is the pre-baseline for replacing arbitrary backend `put_many` /
`delete_many` mutation sinks with a sealed canonical write-batch contract.
The target is shared write-path performance for untracked state, tracked state,
and other domain stores that stage through `StorageWriteSet`.

## Hypothesis

The current backend API is generic, but too general for the hot write path:

```text
BackendWrite::put_many(PutBatch)
BackendWrite::delete_many(&[Key])
BackendWrite::delete_range(KeyRange)
```

`PutBatch` does not tell the backend whether keys are sorted, unique,
insert-only, overwrite-capable, or disjoint from deletes. Storage already wants
canonical final mutations, so the backend should receive a sealed physical
batch with those facts established once above the backend boundary.

Expected backend-level wins:

```text
SQLite:
  use plain INSERT for insert-only batches instead of ON CONFLICT upsert
  reduce B-tree lookup/rebalance and WAL work for sorted unique physical keys

RocksDB:
  reduce memtable comparator/skiplist work for ordered batches
  preserve one WriteBatch and one DB write

redb:
  preserve one write transaction while improving tree insertion locality

storage_v2:
  avoid repeated validation/lowering/key-copy work on hot paths
  make duplicate/ordering semantics a storage seal concern, not backend policy
```

## Current Contract Notes

The specs already point toward this shape:

```text
backend_spec.md:
  backend = ordered byte keys, opaque byte values, batched writes, atomic commit

storage_spec.md:
  StorageWriteSet is canonical final mutations, not an ordered write script
  domain stores must stage at most one mutation per (space, key)
  storage lowers grouped mutations to backend batches
```

Important implementation mismatch to track:

```text
storage_spec.md says release duplicate validation should be O(1) no-op.
Current StorageWriteSet::commit always calls validate(), and validate() builds
an O(K) duplicate-detection hash map before every lower/commit.
```

## Proposed API Direction

Keep unsorted staging above the backend. Remove unsorted arbitrary puts from the
backend hot-path contract.

Target layering:

```text
StorageWriteSetBuilder:
  convenient domain-store staging order
  may be unsorted

SealedStorageWriteSet:
  physical keys encoded
  sorted by physical key
  duplicate-free
  operation conflicts resolved or rejected
  batch hints computed once

BackendWriteBatch:
  borrowed or moved view of sealed physical mutations
  one backend write call before commit
```

Sketch:

```rust
pub struct BackendWriteBatch<'a> {
    pub groups: &'a [BackendWriteGroup<'a>],
}

pub struct BackendWriteGroup<'a> {
    pub range_deletes: &'a [KeyRange],
    pub point_deletes: &'a [KeyRef<'a>],
    pub puts: &'a [PutRef<'a>],
    pub hints: WriteGroupHints,
}

pub struct WriteGroupHints {
    pub keys_sorted: bool,
    pub keys_unique: bool,
    pub insert_only: bool,
    pub disjoint_from_deletes: bool,
}
```

## Scorecard Commands

Run these before and after the API change.

### Direct Untracked 10k

```sh
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- insert_all_rows/10k
LIX_UNTRACKED_STATE_CRUD_IO=real_workload cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- insert_all_rows/10k
```

Profile direct SQLite and RocksDB storage paths:

```sh
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- insert_all_rows/10k

perf record -F 997 -g -o /tmp/lix-untracked-sqlite-pre.perf.data -- \
  target/release/deps/untracked_state_crud-<hash> \
  lix_sqlite/real_workload/insert_all_rows/10k --bench --profile-time 10

perf record -F 997 -g -o /tmp/lix-untracked-rocksdb-pre.perf.data -- \
  target/release/deps/untracked_state_crud-<hash> \
  lix_rocksdb/real_workload/insert_all_rows/10k --bench --profile-time 10
```

### Generic Storage Write Path

```sh
cargo bench -p lix_engine --features storage-benches --bench storage_v2 -- 'storage_v2/write_set_(lowering|construction|build_and_commit)'
```

Useful direct backend profile mode:

```sh
STORAGE_V2_BENCH_DIRECT_PROFILE_ONLY=1 \
STORAGE_V2_BENCH_DIRECT_PROFILE_BACKEND=sqlite_temp \
cargo bench -p lix_engine --features storage-benches --bench storage_v2

STORAGE_V2_BENCH_DIRECT_PROFILE_ONLY=1 \
STORAGE_V2_BENCH_DIRECT_PROFILE_BACKEND=rocksdb_temp \
cargo bench -p lix_engine --features storage-benches --bench storage_v2
```

### Domain Store Coverage

```sh
cargo bench -p lix_engine --features storage-benches --bench storage -- 'storage/(tracked_state|untracked_state)'
cargo bench -p lix_engine --features storage-benches --bench storage -- 'storage/tracked_state_fast'
```

These are the domain-level scorecard rows that should move if backend write
batching improves without relying on SQL2 changes.

### Full SQL Guardrail

```sh
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'session_execute_untracked/.*/insert_all_rows/10k'
```

This is a guardrail only. The full SQL path is dominated by SQL parsing,
literal JSON construction, allocation, schema/json normalization, and
transaction preparation. It should not be the success criterion for the backend
API change.

## Baseline Scorecard: 2026-05-19

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- insert_all_rows/10k
```

Rows:

```text
workload: real_workload/10k
fixture: packages/engine/benches/untracked_state_crud/pnpm-lock.fixture.json
shape: flattened JSON-pointer rows
```

| Workload                                                                                     |                   Backend / Path | Pre-baseline |
| -------------------------------------------------------------------------------------------- | -------------------------------: | -----------: |
| `untracked_state_crud/raw_sqlite/real_workload/insert_all_rows/10k`                          | raw SQLite `WITHOUT ROWID` table |      15.6 ms |
| `untracked_state_crud/lix_sqlite/real_workload/insert_all_rows/10k`                          |           generic KV over SQLite |     28-32 ms |
| `untracked_state_crud/lix_rocksdb/real_workload/insert_all_rows/10k`                         |          generic KV over RocksDB |     11-14 ms |
| `untracked_state_crud/lix_redb/real_workload/insert_all_rows/10k`                            |             generic KV over redb | 17.5-18.2 ms |
| `untracked_state_crud/session_execute_untracked/in_memory/real_workload/insert_all_rows/10k` |            full SQL/session path |   199-203 ms |

Logical storage I/O for direct Lix insert:

```text
operation: insert_all_rows/10k
backend calls: 1 write batch
puts: 10,000
deletes: 0
logical write bytes: 2,764,468
logical write bytes / row: 276.45
```

This confirms the direct untracked storage path is already one backend batch.
The remaining direct-storage delta is inside write-set sealing/lowering and the
backend's handling of the 10,000 keyed entries, not backend round trips.

## Perf Baseline: Direct SQLite

Profile command:

```sh
perf record -F 997 -g -o /tmp/lix-untracked-sqlite-profile.perf.data -- \
  target/release/deps/untracked_state_crud-bed4d7da9e6f51af \
  lix_sqlite/real_workload/insert_all_rows/10k --bench --profile-time 5
```

Top children from `perf report --children`:

| Area                             |                      Approx children | Notes                                  |
| -------------------------------- | -----------------------------------: | -------------------------------------- |
| `sqlite3_step`                   |                                63.9% | per-row SQLite insert/upsert execution |
| `sqlite3BtreeInsert`             |                                25.7% | B-tree insertion path                  |
| `balance` / `balance_nonroot`    |                        22.8% / 19.2% | page balancing/rebalance work          |
| `sqlite3VdbeHalt` / commit phase |                                19.9% | transaction finish and WAL frames      |
| `pagerWalFrames` / `pwrite`      |                        17.5% / 10.7% | WAL write path                         |
| `sqlite3BtreeIndexMoveto`        |        3.6% self in no-children view | index lookup/probe                     |
| `bytes::Bytes` clone/drop        | 3.1% / 1.6% self in no-children view | storage value/key ownership overhead   |

Interpretation:

```text
SQLite direct KV insert is backend dominated. A sealed insert-only/sorted batch
could plausibly help here by avoiding ON CONFLICT update machinery and by
improving B-tree insertion locality.
```

## Perf Baseline: Direct RocksDB

Profile command:

```sh
perf record -F 997 -g -o /tmp/lix-untracked-rocksdb-profile.perf.data -- \
  target/release/deps/untracked_state_crud-bed4d7da9e6f51af \
  lix_rocksdb/real_workload/insert_all_rows/10k --bench --profile-time 4
```

Top children from `perf report --children`:

| Area                                     | Approx children | Notes                            |
| ---------------------------------------- | --------------: | -------------------------------- |
| `RocksDbWrite::commit` / `rocksdb_write` |           42.9% | one DB write of the staged batch |
| `WriteToWAL`                             |           21.1% | WAL append/checksum/write        |
| `WriteBatchInternal::InsertInto`         |           20.6% | memtable insertion               |
| `MemTable::Add`                          |           16.7% | memtable add path                |
| `InlineSkipList::Insert`                 |           14.3% | skiplist insertion               |
| `RecomputeSpliceLevels`                  |           11.5% | skiplist comparator/search work  |
| `MemTable::KeyComparator` / `memcmp`     |     5.7% / 2.5% | key comparison cost              |

Interpretation:

```text
RocksDB direct KV insert is already faster than raw SQLite on this fixture, but
there is visible sorted-key/write-batch opportunity in memtable insertion and
comparator work.
```

## Perf Baseline: Full Session Untracked

Profile command:

```sh
perf record -F 997 -g -o /tmp/lix-untracked-session-profile.perf.data -- \
  target/release/deps/untracked_state_crud-bed4d7da9e6f51af \
  session_execute_untracked/in_memory/real_workload/insert_all_rows/10k --bench --profile-time 5
```

Findings:

```text
untracked_state::codec::encode_row_ref: ~0.5%
FlatBuffer builder symbols: below 0.2% each
dominant symbols: malloc/free/realloc, memmove/memcmp, sqlparser tokenizer and
parser, serde_json parse/serialize, BTreeMap search/insert, string clone/format
```

Interpretation:

```text
Do not use the 199-203 ms full session path as the success metric for backend
API work. That path needs a separate runtime/SQL fast-write campaign.
```

## Storage v2 Baseline: 2026-05-19

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench storage_v2 -- 'storage_v2/write_set_(lowering|construction|build_and_commit)'
```

Representative rows:

| Workload                                              | Pre-baseline |
| ----------------------------------------------------- | -----------: |
| `write_set_lowering/puts_k1024_g1_v32`                |    119.46 us |
| `write_set_lowering/puts_k1024_g16_v32`               |    104.13 us |
| `write_set_lowering/puts_k8192_g16_v32`               |    990.85 us |
| `write_set_lowering/mixed80_20_k1024_g16_v32`         |     99.36 us |
| `write_set_construction/checked/puts_k1024_g16_v32`   |     58.53 us |
| `write_set_construction/canonical/puts_k1024_g16_v32` |     55.09 us |
| `write_set_construction/checked/puts_k8192_g16_v32`   |    448.95 us |
| `write_set_construction/canonical/puts_k8192_g16_v32` |    426.67 us |

Representative build-and-commit rows:

| Backend      | Workload                          | Pre-baseline |
| ------------ | --------------------------------- | -----------: |
| in-memory    | `canonical/puts_k1024_g16_v32`    |    262.31 us |
| SQLite temp  | `canonical/puts_k1024_g16_v32`    |    2.4406 ms |
| redb temp    | `canonical/puts_k1024_g16_v32`    |    2.0065 ms |
| RocksDB temp | `canonical/puts_k1024_g16_v32`    |    587.27 us |
| SQLite temp  | `canonical/puts_k1024_g16_v65536` |    310.91 ms |
| redb temp    | `canonical/puts_k1024_g16_v65536` |    253.60 ms |
| RocksDB temp | `canonical/puts_k1024_g16_v65536` |    221.50 ms |

Interpretation:

```text
For 1,024 small puts, storage_v2 build-and-commit is backend dominated for
SQLite/redb/RocksDB. Construction and lowering are measurable but small next to
real backend commit costs.

Canonical construction is consistently a little faster than checked
construction in the measured rows, but commit currently still pays validation.
```

## Direct Write-Order Baseline: 2026-05-19

Added benchmark:

```text
storage_v2/direct_write_order/{in_memory,sqlite_temp,redb_temp,rocksdb_temp}/{sorted,reverse_sorted,shuffled}
```

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench storage_v2 -- 'storage_v2/direct_write_order'
```

Shape:

```text
10,000 puts
one storage space
32-byte values
one backend write transaction
one backend put_many call
fresh empty backend per iteration
```

Results:

| Backend      |    Sorted | Reverse sorted |  Shuffled |
| ------------ | --------: | -------------: | --------: |
| in-memory    | 1.5822 ms |      692.20 us | 1.7927 ms |
| SQLite temp  | 8.1363 ms |      11.596 ms | 10.343 ms |
| redb temp    | 8.9056 ms |      8.9796 ms | 9.4208 ms |
| RocksDB temp | 2.4787 ms |      3.1867 ms | 5.1316 ms |

Interpretation:

```text
The sorted-key signal is real for SQLite and RocksDB:
  SQLite sorted is ~30% faster than reverse sorted and ~21% faster than shuffled.
  RocksDB sorted is ~22% faster than reverse sorted and ~52% faster than shuffled.

redb is less sensitive but still prefers sorted over shuffled.

This supports making sorted canonical physical batches the backend-facing
contract, provided seal/sort overhead is kept below the backend win for the
target workloads.
```

## Seal/Sort Cost Baseline: 2026-05-19

Added benchmark:

```text
storage_v2/write_batch_seal_sort/{sorted,reverse_sorted,shuffled}
```

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench storage_v2 -- 'storage_v2/write_batch_seal_sort'
```

Shape:

```text
10,000 physical PutEntry values
one storage space
32-byte values
sort by physical key
validate adjacent duplicate keys after sort
no backend I/O
```

Results:

| Input order    | Seal/sort cost |
| -------------- | -------------: |
| sorted         |      186.12 us |
| reverse sorted |      197.90 us |
| shuffled       |      821.47 us |

Interpretation:

```text
Seal/sort cost is smaller than the observed backend ordering win:
  SQLite sorted vs shuffled saves ~2.21 ms, while shuffled seal/sort costs ~0.82 ms.
  RocksDB sorted vs shuffled saves ~2.65 ms, while shuffled seal/sort costs ~0.82 ms.

This makes an internal sealed-batch prototype worth trying. The prototype still
needs to include physical key encoding and real StorageWriteSet integration, so
this microbench is an optimistic lower bound for the full seal path.
```

## Prototype Delta: Sealed Put Mode

Implemented a first sealed point-write API surface:

```rust
struct SealedWriteSet {
    puts: Vec<SealedPut>,      // globally sorted physical keys
    deletes: Vec<Key>,         // sorted physical point deletes
}

struct SealedPut {
    key: Key,
    value: StoredValue,
    mode: PutMode,
}

enum PutMode {
    InsertNew, // atomic absence requirement
    Upsert,    // overwrite allowed
}
```

Backend behavior:

```text
BackendWrite::apply_sealed_write_set is now the backend-facing sealed entry
point. The default implementation preserves current semantics by accepting
only Upsert puts and point deletes; InsertNew returns Unsupported(Preconditions)
unless the backend overrides it.

SQLite support backend overrides the method:
  InsertNew -> plain INSERT INTO entries(key, value) VALUES (?1, ?2)
  Upsert    -> existing INSERT ... ON CONFLICT DO UPDATE path

In-memory backend overrides the method and enforces InsertNew against its
snapshot/overlay under its existing single-writer contract.
```

Added smoke benchmark:

```text
storage_v2/sealed_put_mode/sqlite_temp/{insert_new,upsert}/{sorted,reverse_sorted,shuffled}
```

Smoke baseline before this change:

```sh
STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches --bench storage_v2 -- 'storage_v2/direct_write_order/(sqlite_temp|rocksdb_temp)/(sorted|shuffled)'
```

| Workload                                   | Smoke pre |
| ------------------------------------------ | --------: |
| `direct_write_order/sqlite_temp/sorted`    | 8.0116 ms |
| `direct_write_order/sqlite_temp/shuffled`  | 10.212 ms |
| `direct_write_order/rocksdb_temp/sorted`   | 2.4879 ms |
| `direct_write_order/rocksdb_temp/shuffled` | 5.1764 ms |

Smoke after this change:

```sh
STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches --bench storage_v2 -- 'storage_v2/sealed_put_mode/sqlite_temp/(insert_new|upsert)/(sorted|shuffled)|storage_v2/direct_write_order/sqlite_temp/sorted'
```

| Workload                                          | Smoke post |
| ------------------------------------------------- | ---------: |
| `direct_write_order/sqlite_temp/sorted`           |  7.8615 ms |
| `sealed_put_mode/sqlite_temp/insert_new/sorted`   |  7.8595 ms |
| `sealed_put_mode/sqlite_temp/insert_new/shuffled` |  8.1254 ms |
| `sealed_put_mode/sqlite_temp/upsert/sorted`       |  7.8865 ms |
| `sealed_put_mode/sqlite_temp/upsert/shuffled`     |  8.4485 ms |

Interpretation:

```text
The structural sorted-stream API works, but the isolated SQLite plain-INSERT
path did not show a meaningful win over the current sorted upsert path in the
smoke run. The best comparable rows were effectively tied:

  direct sorted upsert:        7.8615 ms
  sealed InsertNew sorted:     7.8595 ms
  sealed Upsert sorted:        7.8865 ms

The shuffled sealed rows are still faster than the old direct shuffled baseline
because the sealed builder sorts before applying. That confirms the sorted-key
win again, not an InsertNew-specific win.

Because the smoke did not move the needle for plain INSERT, the full scorecard
was not run yet. Next measurement should isolate SQLite statement cost with a
raw/direct microbench that compares plain INSERT and ON CONFLICT with identical
already-sorted inputs and no extra sealing path.
```

## Prototype Delta: StorageWriteSet Seal and Apply

Implemented real `StorageWriteSet` lowering through the sealed API:

```text
StorageWriteSet::lower_validated_into now:
  encodes logical `(StorageSpace, Key)` into physical byte keys
  emits PutMode::Upsert for every storage put
  globally sorts physical puts by key
  globally sorts physical point deletes by key
  calls BackendWrite::apply_sealed_write_set once for the whole write set
```

This changes write-set lowering from the old shape:

```text
one put_many per touched put space
one delete_many per touched delete space
backend_calls = put_batches + delete_batches
```

to:

```text
one sealed physical apply for the whole point write set
backend_calls = 1 for any non-empty write set
put_batches/delete_batches still report logical touched operation groups
```

Added end-to-end benchmark row:

```text
storage_v2/write_set_build_and_commit/{backend}/seal_and_apply/puts_k1024_g16_v32
```

Focused smoke command:

```sh
STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches --bench storage_v2 -- 'storage_v2/write_set_build_and_commit/(sqlite_temp|rocksdb_temp|redb_temp)/(canonical|seal_and_apply)/puts_k1024_g16_v32'
```

Smoke results:

| Workload                                                                    | Smoke post |
| --------------------------------------------------------------------------- | ---------: |
| `write_set_build_and_commit/sqlite_temp/canonical/puts_k1024_g16_v32`       |  2.2774 ms |
| `write_set_build_and_commit/sqlite_temp/seal_and_apply/puts_k1024_g16_v32`  |  2.6371 ms |
| `write_set_build_and_commit/redb_temp/canonical/puts_k1024_g16_v32`         |  1.9531 ms |
| `write_set_build_and_commit/redb_temp/seal_and_apply/puts_k1024_g16_v32`    |  1.8936 ms |
| `write_set_build_and_commit/rocksdb_temp/canonical/puts_k1024_g16_v32`      |  574.85 us |
| `write_set_build_and_commit/rocksdb_temp/seal_and_apply/puts_k1024_g16_v32` |  599.63 us |

Interpretation:

```text
The production StorageWriteSet path now uses sealed sorted physical writes.
The `canonical` and `seal_and_apply` rows currently exercise the same lowering
path; the separate name was added as a scorecard row for this prototype. Treat
differences between those two rows as smoke noise/order effects until we add a
true legacy-space-batch comparison row.

Comparing the current canonical smoke to the earlier baseline scorecard:
  SQLite moved from 2.4406 ms to 2.2774 ms on this smoke run.
  redb moved from 2.0065 ms to 1.9531 ms.
  RocksDB moved from 587.27 us to 574.85 us.

This is directionally positive but not enough to call without a non-smoke run.
The likely real effect is sorted physical order plus a single sealed apply,
not InsertNew.
```

## Legacy Space-Batch Comparison Row

Added benchmark-only legacy lowering:

```text
storage_v2/write_set_build_and_commit/{backend}/legacy_space_batches/puts_k1024_g16_v32
```

This row reconstructs the old write shape without changing production code:

```text
group physical puts by storage space
preserve per-space staging order
call put_many once per touched put space
call delete_many once per touched delete space
commit once
```

Focused smoke command:

```sh
STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches --bench storage_v2 -- 'storage_v2/write_set_build_and_commit/(sqlite_temp|rocksdb_temp|redb_temp)/(legacy_space_batches|canonical)/puts_k1024_g16_v32'
```

Focused smoke results:

| Backend | Legacy space batches | Current sealed canonical |                Delta |
| ------- | -------------------: | -----------------------: | -------------------: |
| SQLite  |            2.3239 ms |                2.3390 ms |  sealed ~0.6% slower |
| redb    |            1.8861 ms |                1.9706 ms |  sealed ~4.5% slower |
| RocksDB |            509.65 us |                581.46 us | sealed ~14.1% slower |

Interpretation:

```text
This benchmark shape is only 1,024 puts across 16 spaces, so each legacy batch
contains 64 keys. For this shape, the cost of building one global sorted
physical batch outweighs the backend-locality benefit. RocksDB is notably worse,
likely because each per-space legacy batch is already sorted and small, while
the sealed path pays a global sort and extra SealedPut construction without
improving key order enough.

This means we should not run the full non-smoke scorecard expecting a win from
unconditional global seal/sort. Before non-smoke, add/select a large one-space
write-set benchmark that matches the untracked 10k path; the existing 1,024 x
16-space storage_v2 case is a poor proxy for that workload.
```

## Hard-Cut Trial: Sorted Space Batches

Changed production `StorageWriteSet` lowering away from global sealed apply and
to the simple hard cut:

```text
preserve per-space put_many/delete_many call shape
sort physical puts within each touched space before put_many
sort physical point deletes within each touched space before delete_many
```

Added benchmark-only comparison row:

```text
storage_v2/write_set_build_and_commit/{backend}/sorted_space_batches/puts_k1024_g16_v32
```

Focused smoke command:

```sh
STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches --bench storage_v2 -- 'storage_v2/write_set_build_and_commit/(sqlite_temp|rocksdb_temp|redb_temp)/(legacy_space_batches|sorted_space_batches|canonical)/puts_k1024_g16_v32'
```

Focused smoke results:

| Backend | Legacy unsorted space batches | Sorted space batches | Production canonical |
| ------- | ----------------------------: | -------------------: | -------------------: |
| SQLite  |                     2.2303 ms |            2.6777 ms |            2.4573 ms |
| redb    |                     1.8412 ms |            1.9444 ms |            2.2304 ms |
| RocksDB |                     505.27 us |            507.38 us |            588.76 us |

Interpretation:

```text
For the existing 1,024 writes / 16 spaces case, sorting inside each 64-row
space batch is not a win. RocksDB is roughly neutral, SQLite/redb are slower.
This is expected because the benchmark's generated keys are already staged in
sorted order within each space; sorted_space_batches mostly adds sort overhead.

The production canonical row should match sorted_space_batches conceptually, but
smoke variance and extra StorageWriteSet construction/validation make the exact
numbers noisy. The important conclusion is that this 16-space benchmark does
not validate the hard cut for the untracked 10k path.

Next benchmark needed before a non-smoke scorecard:
  storage_v2/write_set_build_and_commit/{backend}/legacy_space_batches/puts_k10000_g1_v32
  storage_v2/write_set_build_and_commit/{backend}/sorted_space_batches/puts_k10000_g1_v32
  storage_v2/write_set_build_and_commit/{backend}/canonical/puts_k10000_g1_v32

That shape matches untracked_state's one-space 10k insert much better.
```

## One-Space 10k Write-Set Smoke

Added write case:

```text
storage_v2/write_set_build_and_commit/{backend}/{row}/puts_k10000_g1_v32
```

Focused smoke command:

```sh
STORAGE_V2_BENCH_SMOKE=1 cargo bench -p lix_engine --features storage-benches --bench storage_v2 -- 'storage_v2/write_set_build_and_commit/(sqlite_temp|rocksdb_temp|redb_temp)/(legacy_space_batches|sorted_space_batches|canonical)/puts_k10000_g1_v32'
```

Focused smoke results:

| Backend | Legacy unsorted space batch | Sorted space batch | Production canonical |
| ------- | --------------------------: | -----------------: | -------------------: |
| SQLite  |                   8.6757 ms |          9.0789 ms |            9.3700 ms |
| redb    |                   9.4625 ms |          9.3550 ms |            10.116 ms |
| RocksDB |                   3.7227 ms |          3.6866 ms |            4.4318 ms |

Delta from legacy to sorted-space:

| Backend |               Delta |
| ------- | ------------------: |
| SQLite  | sorted ~4.6% slower |
| redb    | sorted ~1.1% faster |
| RocksDB | sorted ~1.0% faster |

Interpretation:

```text
For one-space 10k, sorted-space is a small win for redb/RocksDB but loses on
SQLite in this smoke run. However, the generated one-space keys are already in
sorted order, so sorted-space mostly measures sort overhead. It does not test
the case that motivated the change: unsorted logical staging becoming sorted
physical backend order.

Production canonical includes StorageWriteSet construction/validation and is
slower than the benchmark-only direct legacy/sorted rows, so use it as the
real-path guardrail rather than as the clean algorithm comparison.

Next: add a shuffled one-space 10k write case so legacy receives unsorted keys
and sorted-space can demonstrate whether physical sort pays for itself on the
actual bad input shape.
```

## Rollback Decision

Decision:

```text
Do not keep the sealed-write / InsertNew / sorted-space production changes.
Target threshold is >=6% improvement, and the smoke data did not support that.
```

Code state after rollback:

```text
Production backend API remains put_many/delete_many/delete_range.
StorageWriteSet lowering remains the original per-space, per-operation lowering.
The experimental sealed backend API and SQLite InsertNew path were removed.
The benchmark-only failed comparison rows were removed.
The useful direct write-order and seal/sort benchmarks remain.
```

Reason:

```text
Plain SQLite INSERT did not materially beat no-conflict upsert.
Global sealed sorting regressed the 1,024 x 16-space case.
Sorted per-space batches did not clear the >=6% threshold on one-space 10k smoke.
The next credible perf target should attack a measured bottleneck with a direct
>=6% path, likely allocation/key encoding or the full session/staging path,
rather than changing backend write semantics.
```

## Raw Untracked Physical Layout Experiment

Experiment:

```text
Move untracked row identity fields out of the stored value.

Old production value shape:
  key   = implicit components: version_id + schema_key + entity_id + file_id
  value = FlatBuffer full row, including version_id/schema_key/entity_id/file_id

New value shape:
  storage space = untracked_state.row.v1 / 0x0001_0002
  key   = LXUK + v1 + checked length-prefixed version_id/schema_key/entity_id + file_id tag/value
  value = LXUP + v1 + checked optional tags/length-prefixed payload fields
```

Implementation:

```text
packages/engine/src/untracked_state/codec.rs
  added explicit payload v1 encoder/decoder
  no old-value compatibility in production decode path

packages/engine/src/untracked_state/storage.rs
  added explicit key v1 encoder/decoder
  writes versioned payload-only values
  reconstructs full rows from key + payload on point reads and scans

packages/engine/src/storage_bench.rs
packages/engine/benches/untracked_state_crud/main.rs
  added paired physical_layout bench rows:
    full_row_value
    payload_only_value
```

Important bench correction:

```text
The existing untracked_state_crud/lix_* direct insert rows were not an exact
production untracked physical-layout benchmark. They used a synthetic value
containing only snapshot JSON. They are still useful generic storage rows, but
not sufficient to evaluate identity duplication in the untracked value.
```

Paired benchmark command:

```sh
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'physical_layout/.*/insert_all_rows/.*/10k'
```

10k raw storage insert results:

| Backend | Full-row value | Payload-only value |        Delta |
| ------- | -------------: | -----------------: | -----------: |
| SQLite  |      50.633 ms |          37.068 ms | 26.8% faster |
| RocksDB |      27.654 ms |          15.132 ms | 45.3% faster |
| redb    |      31.719 ms |          23.691 ms | 25.3% faster |

Post-review hardening:

```text
Three reviewers agreed the direction is sound but asked for explicit durable
format handling before keeping the production change.

Applied:
  key format is now LXUK + v1 + checked components
  payload format is now LXUP + v1 + checked fields
  storage space was bumped from untracked_state.row / 0x0001_0001 to
    untracked_state.row.v1 / 0x0001_0002 because backward compatibility is
    intentionally not required
  unsupported versions fail explicitly
  malformed/truncated/trailing bytes fail explicitly
  old full-row values are not accepted by production payload decode
  delete staging now propagates key-encoding errors instead of panicking
  key/payload roundtrip and malformed tests were added
```

Benchmark scope:

```text
The paired physical_layout benchmark uses the same LXUK v1 key format for both
arms and isolates value layout: full-row FlatBuffer value vs LXUP v1
payload-only value. It is an insert/write-byte benchmark, not a read/decode,
update, full CRUD, SQL/session, or backend API benchmark.
```

Verdict:

```text
This clears the >=6% raw storage insert threshold. The real raw-layout knob is
value bytes/duplication, not backend write API shape.
```

## Raw Untracked Binary Entity Key Experiment

Experiment:

```text
Replace entity_id JSON-array text inside LXUK keys with a binary tuple:

Old entity component:
  entity_id_len:u32be | entity_id_json_array:utf8

New entity component:
  entity_part_count:u32be | {entity_part_len:u32be | entity_part:utf8}*
```

Motivation:

```text
Avoid JSON array punctuation/escaping in keys and reduce key bytes, especially
for string identities containing quotes, slashes, or multiple tuple parts.
```

Validation:

```sh
cargo test -p lix_engine storage:: --lib
cargo check -p lix_engine --benches
cargo bench -p lix_engine --features storage-benches --bench untracked_state_crud -- 'physical_layout/.*/insert_all_rows/.*/10k'
```

10k raw storage insert results after binary entity keys:

| Backend | Full-row value | Payload-only value |   Payload-only delta vs prior |
| ------- | -------------: | -----------------: | ----------------------------: |
| SQLite  |      50.195 ms |          37.057 ms | effectively flat vs 37.068 ms |
| RocksDB |      26.368 ms |          15.295 ms |     ~1.1% slower vs 15.132 ms |
| redb    |      32.983 ms |          24.275 ms |     ~2.5% slower vs 23.691 ms |

Verdict:

```text
Binary entity tuple keys did not produce a >=6% raw insert win on this fixture.
The key-byte reduction is too small relative to backend insert/write overhead
after the payload-only value cut. Treat this as a compactness/format choice,
not a measured performance optimization.
```

## Domain Store Baseline Blocker

Attempted command:

```sh
cargo bench -p lix_engine --features storage-benches --bench storage -- 'storage/(tracked_state|untracked_state)/(write_root|write_rows|update_existing|overwrite_existing|insert_new_keys)'
```

Result:

```text
error: no bench target named `storage` in `lix_engine` package
```

The files exist under `packages/engine/benches/storage/`, but
`packages/engine/Cargo.toml` currently registers only:

```text
storage_v2
untracked_state_crud
```

Tracked-state and storage-domain baseline rows are therefore not available from
Cargo yet. To complete the domain-store portion of this scorecard, either:

```text
1. register benches/storage/main.rs as a Cargo bench target with storage-benches,
   or
2. migrate the relevant tracked_state/untracked_state write rows into
   storage_v2 as registered benchmark groups.
```

## Post-Delta Report Template

After the API change, fill this table with the same commands and machine:

| Workload                           |                              Backend / Path |                           Pre | Post |     Delta |
| ---------------------------------- | ------------------------------------------: | ----------------------------: | ---: | --------: |
| untracked direct insert 10k        |                                   SQLite KV |                      28-32 ms |  TBD |       TBD |
| untracked direct insert 10k        |                                  RocksDB KV |                      11-14 ms |  TBD |       TBD |
| untracked direct insert 10k        |                                     redb KV |                  17.5-18.2 ms |  TBD |       TBD |
| storage_v2 write_set_lowering      |                        `puts_k1024_g16_v32` |                     104.13 us |  TBD |       TBD |
| storage_v2 build_and_commit        |  SQLite temp `canonical/puts_k1024_g16_v32` |                     2.4406 ms |  TBD |       TBD |
| storage_v2 build_and_commit        | RocksDB temp `canonical/puts_k1024_g16_v32` |                     587.27 us |  TBD |       TBD |
| storage_v2 direct_write_order      |                           SQLite sorted 10k |                     8.1363 ms |  TBD |       TBD |
| storage_v2 direct_write_order      |                          RocksDB sorted 10k |                     2.4787 ms |  TBD |       TBD |
| storage_v2 write_batch_seal_sort   |                                shuffled 10k |                     821.47 us |  TBD |       TBD |
| storage/tracked_state write root   |                         configured backends | blocked: bench target missing |  TBD |       TBD |
| storage/untracked_state write rows |                         configured backends | blocked: bench target missing |  TBD |       TBD |
| full session untracked insert 10k  |                                   in-memory |                    199-203 ms |  TBD | guardrail |

Use `perf diff` for symbol-level changes:

```sh
perf diff /tmp/lix-untracked-sqlite-pre.perf.data /tmp/lix-untracked-sqlite-post.perf.data
perf diff /tmp/lix-untracked-rocksdb-pre.perf.data /tmp/lix-untracked-rocksdb-post.perf.data
```

Success criteria:

```text
direct storage insert improves for SQLite without regressing RocksDB/redb
storage_v2 write-set seal/lower overhead does not increase materially
tracked_state and untracked_state domain write benches do not regress
full SQL/session benchmark is reported separately and not used to reject a
backend-level win
```
