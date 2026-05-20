# Tracked CRUD Optimization Log

## Baseline: 2026-05-19 corrected fixture setup

Command used for the regular scorecard:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
```

Command used for the accounting scorecard:

```sh
LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
```

The regular scorecard is intentionally the 1k smoke workload. The full 10k
matrix is too slow for iteration while the SQL path is unoptimized; use targeted
10k filters for headline checks such as `insert_all_rows/10k`.

This baseline supersedes the first post-rebase scorecard. That run timed
database/session creation and `insert_all(&rows)` inside read, update, and
delete benchmarks, which made the non-insert numbers mostly setup cost. The
current harness creates either an empty or seeded fixture in Criterion's setup
closure, then borrows that fixture with `iter_batched_ref` so fixture teardown is
excluded from the timed window too.

Workload:

- Source fixture: `packages/engine/benches/fixtures/pnpm-lock.fixture.json`
- Shape: flattened JSON-pointer rows
- Smoke size: 1,000 rows
- Criterion: 10 samples, 250 ms warmup, 1 s measurement for smoke groups

Notes:

- `transaction` builds `TransactionWriteRow`s directly and stages them through
  the transaction layer. It bypasses SQL/DataFusion but still exercises
  normalization, validation, changelog segments/indexes, commit visibility,
  version refs, and tracked-state projection roots.
- `sql_session` runs on `InMemoryStorageBackend`; the copied SQLite/RocksDB/redb
  backend support modules do not satisfy the SQL session read bounds.
- SQL update benches are gated behind `LIX_TRACKED_STATE_CRUD_SQL_UPDATE=1`.
  The supported per-row `UPDATE ... WHERE path = ...` shape is functionally
  valid but too slow for the default scorecard; Criterion estimated about
  25 minutes for `sql_session/update_all_rows/1k`.

## 1k Smoke Scorecard

Times below use Criterion point estimates from the corrected fixture rerun.

### Direct KV Layout

| Backend | Insert all | Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    4.21 ms |   656 us |         510 us |        1.63 ms |    3.80 ms |     946 us |    1.96 ms |     663 us |
| RocksDB |     537 us |   176 us |        7.27 us |         532 us |     586 us |    18.0 us |    31.8 us |    10.5 us |
| redb    |    7.25 ms |   141 us |        11.3 us |         337 us |    8.17 ms |    4.16 ms |    4.13 ms |    3.97 ms |

### Transaction Layer

Direct transaction API, bypassing SQL.

| Backend | Insert all | Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   34.07 ms | 18.92 ms |        8.40 ms |         8.79 s |  102.24 ms |   65.75 ms |  104.40 ms |   66.72 ms |
| RocksDB |   29.09 ms | 18.42 ms |        8.43 ms |         8.08 s |   93.32 ms |   64.16 ms |   87.40 ms |   63.03 ms |
| redb    |   42.07 ms | 17.36 ms |        8.02 ms |         7.94 s |  103.47 ms |   70.04 ms |   96.50 ms |   70.26 ms |

### SQL Session

| Backend   | Insert all | Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| in-memory |   73.88 ms | 22.69 ms |        6.15 ms |       29.78 ms |   excluded |   excluded |  104.21 ms |   83.30 ms |

## 1k Smoke Accounting

The accounting report is opt-in and runs outside Criterion's timed closures. It
uses the same smoke fixture to expose physical amplification and post-insert
layout footprint.

### Write Amplification

These counts are backend-independent for the current logical layout; the same
numbers were observed for SQLite, RocksDB, and redb.

| Layer       | Operation        | Logical rows |  Puts | Point deletes | Range deletes | Touched spaces | Backend calls | Put batches | Delete batches | Written bytes | Put amp |
| ----------- | ---------------- | -----------: | ----: | ------------: | ------------: | -------------: | ------------: | ----------: | -------------: | ------------: | ------: |
| kv_layout   | insert_all       |        1,000 | 1,000 |             0 |             0 |              1 |             1 |           1 |              0 |       396,363 |   1.00x |
| kv_layout   | update_all       |        1,000 | 1,000 |             0 |             0 |              1 |             1 |           1 |              0 |       482,607 |   1.00x |
| kv_layout   | update_one_by_pk |            1 |     1 |             0 |             0 |              1 |             1 |           1 |              0 |         6,693 |   1.00x |
| kv_layout   | delete_all       |        1,000 |     0 |             0 |             1 |              0 |             1 |           0 |              1 |             0 |   0.00x |
| kv_layout   | delete_one_by_pk |            1 |     0 |             1 |             0 |              1 |             1 |           0 |              1 |             0 |   0.00x |
| transaction | insert_all       |        1,000 | 3,037 |             0 |             0 |              9 |             9 |           9 |              0 |     2,031,993 |   3.04x |
| transaction | update_all       |        1,000 | 3,037 |             0 |             0 |              9 |             9 |           9 |              0 |     2,118,264 |   3.04x |
| transaction | update_one_by_pk |            1 |    11 |             0 |             0 |              9 |             9 |           9 |              0 |        16,237 |  11.00x |
| transaction | delete_all       |        1,000 | 3,037 |             0 |             0 |              9 |             9 |           9 |              0 |     1,487,657 |   3.04x |
| transaction | delete_one_by_pk |            1 |    11 |             0 |             0 |              9 |             9 |           9 |              0 |        15,872 |  11.00x |

### Layout Footprint After Insert

These counts are also backend-independent for the current fixture content. The
transaction table inventories every native storage space.

| Layer       |     Space id | Space                                  |  Rows | Key bytes | Value bytes |
| ----------- | -----------: | -------------------------------------- | ----: | --------: | ----------: |
| kv_layout   | `0x00020001` | `tracked_state.crud.row.v1`            | 1,000 |    87,244 |     396,363 |
| transaction | `0x00010002` | `untracked_state.row.v1`               |     2 |       120 |         273 |
| transaction | `0x00020001` | `json_store.json`                      |     0 |         0 |           0 |
| transaction | `0x00040001` | `tracked_state.tree.chunk`             |    33 |     1,188 |     413,693 |
| transaction | `0x00040003` | `tracked_state.tree.root.by_file`      |     0 |         0 |           0 |
| transaction | `0x00040004` | `tracked_state.projection`             |     2 |        71 |         288 |
| transaction | `0x00050001` | `binary_cas.manifest`                  |     0 |         0 |           0 |
| transaction | `0x00050002` | `binary_cas.manifest_chunk`            |     0 |         0 |           0 |
| transaction | `0x00050003` | `binary_cas.chunk`                     |     0 |         0 |           0 |
| transaction | `0x00060001` | `changelog.segment`                    |     2 |       124 |   1,156,775 |
| transaction | `0x00060002` | `changelog.commit_visibility`          |     2 |        71 |         509 |
| transaction | `0x00060003` | `changelog.index.by_commit`            |     2 |        71 |         428 |
| transaction | `0x00060004` | `changelog.index.by_change`            | 1,016 |    40,559 |     214,639 |
| transaction | `0x00060005` | `changelog.index.by_change_membership` | 1,016 |    79,023 |           0 |
| transaction | `0x00060006` | `changelog.index.visible_change`       | 1,016 |    40,559 |     283,664 |

## 10k Reference Checks

The full 10k matrix was started once after the rebase to understand scale, but
it is not the regular scorecard. Completed reference numbers:

- `kv_layout/lix_sqlite/insert_all_rows/10k`: 22.78 ms
- `kv_layout/lix_rocksdb/insert_all_rows/10k`: 12.60 ms
- `kv_layout/lix_redb/insert_all_rows/10k`: 62.50 ms
- `sql_session/in_memory/insert_all_rows/10k`: 5.24 s
- `sql_session/in_memory/read_all_rows/10k`: 5.41 s
- `sql_session/in_memory/read_one_by_pk/10k`: 5.20 s
- `sql_session/in_memory/read_all_by_pk/10k`: 5.69 s

10k SQL update with bulk `CASE` failed because that expression shape is not
supported by the SQL layer. Per-row SQL update works but is too slow to include
in routine scorecards.
