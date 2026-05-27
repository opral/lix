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
  branch refs, and tracked-state commit roots.
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

## Optimization Run: visible_change -> commit_id

Date: 2026-05-20

Change:

- `changelog.index.visible_change` now stores `change_id -> commit_id`.
- It no longer stores a full `CommitVisibility` locator/checksum payload per
  visible change.
- Readers still verify through current `commit_visibility(commit_id)` and
  segment membership before treating a change as visible.
- Routine smoke replaced `read_all_by_pk/1k` with `read_many_by_pk/10`; the old
  serial 1,000-key benchmark took about 80-90 seconds per backend group and was
  not a useful CRUD smoke signal.

Smoke scorecard after this change. `read_many_by_pk` reads 10 primary keys:

### Direct KV Layout

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    2.32 ms |   473 us |         295 us |          352 us |    2.62 ms |     670 us |    1.13 ms |     501 us |
| RocksDB |     448 us |   158 us |        3.12 us |         9.14 us |     483 us |    9.34 us |    5.82 us |    12.2 us |
| redb    |    7.43 ms |   199 us |        12.6 us |         22.2 us |    8.15 ms |    4.14 ms |    4.58 ms |    4.64 ms |

### Transaction Layer

Direct transaction API, bypassing SQL.

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   34.77 ms | 18.09 ms |        8.86 ms |        85.40 ms |  100.97 ms |   68.25 ms |   91.43 ms |   68.17 ms |
| RocksDB |   30.27 ms | 17.68 ms |        8.21 ms |        83.35 ms |   90.37 ms |   66.39 ms |   88.94 ms |   64.31 ms |
| redb    |   43.34 ms | 16.84 ms |        8.22 ms |        80.19 ms |  103.48 ms |   69.60 ms |   96.40 ms |   68.96 ms |

### SQL Session

| Backend   | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| in-memory |   70.25 ms | 20.34 ms |        5.81 ms |         9.27 ms |   excluded |   excluded |  101.20 ms |   82.01 ms |

Accounting delta for 1k transaction insert/update/delete:

| Metric                         |    Before |     After |    Delta |
| ------------------------------ | --------: | --------: | -------: |
| `visible_change` rows          |     1,016 |     1,016 |        0 |
| `visible_change` key bytes     |    40,559 |    40,559 |        0 |
| `visible_change` value bytes   |   283,664 |    36,432 | -247,232 |
| transaction `insert_all` bytes | 2,031,993 | 1,787,993 | -244,000 |
| transaction `update_all` bytes | 2,118,264 | 1,874,264 | -244,000 |
| transaction `delete_all` bytes | 1,487,657 | 1,243,657 | -244,000 |

Net: about 12% less transaction write bytes on 1k `insert_all`, 11.5% less on
`update_all`, and 16.4% less on `delete_all`. Put amplification is unchanged
because the index still has one row per visible change.

Layout footprint after 1k transaction insert:

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
| transaction | `0x00060006` | `changelog.index.visible_change`       | 1,016 |    40,559 |      36,432 |

## Optimization Run: by_change_membership only indexes adopted changes

Date: 2026-05-20

Change:

- `changelog.index.by_change_membership` now stores adopted/merge memberships
  only.
- Authored changes already have a direct `changelog.index.by_change` locator,
  so authored membership rows were duplicate physical index entries.
- This is a hard cut with no backward compatibility path.

Smoke scorecard after this change. `read_many_by_pk` reads 10 primary keys:

### Direct KV Layout

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    2.24 ms |   500 us |         286 us |          351 us |    2.53 ms |     618 us |    1.22 ms |     552 us |
| RocksDB |     438 us |   161 us |        5.94 us |         19.5 us |     549 us |    14.4 us |    6.96 us |    6.63 us |
| redb    |    6.99 ms |   150 us |        10.8 us |         20.3 us |    8.00 ms |    3.96 ms |    4.17 ms |    3.84 ms |

### Transaction Layer

Direct transaction API, bypassing SQL.

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   33.07 ms | 17.95 ms |        8.81 ms |        86.18 ms |   97.71 ms |   65.77 ms |   95.75 ms |   68.97 ms |
| RocksDB |   30.49 ms | 17.64 ms |        8.59 ms |        83.26 ms |   92.16 ms |   64.95 ms |   88.70 ms |   64.81 ms |
| redb    |   38.69 ms | 16.56 ms |        8.06 ms |        82.17 ms |  100.84 ms |   71.61 ms |   98.09 ms |   69.78 ms |

### SQL Session

| Backend   | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| in-memory |   70.48 ms | 20.16 ms |        6.25 ms |         6.90 ms |   excluded |   excluded |  110.40 ms |   86.32 ms |

Accounting delta for 1k transaction insert/update/delete, relative to the
previous `visible_change -> commit_id` run:

| Metric                              |    Before |     After |   Delta |
| ----------------------------------- | --------: | --------: | ------: |
| `by_change_membership` rows         |     1,016 |         0 |  -1,016 |
| `by_change_membership` key bytes    |    79,023 |         0 | -79,023 |
| `by_change_membership` value bytes  |         0 |         0 |       0 |
| transaction `insert_all` puts       |     3,037 |     2,037 |  -1,000 |
| transaction `update_all` puts       |     3,037 |     2,037 |  -1,000 |
| transaction `delete_all` puts       |     3,037 |     2,037 |  -1,000 |
| transaction `update_one_by_pk` puts |        11 |        10 |      -1 |
| transaction `delete_one_by_pk` puts |        11 |        10 |      -1 |
| transaction touched spaces          |         9 |         8 |      -1 |
| transaction `insert_all` bytes      | 1,787,993 | 1,787,993 |       0 |
| transaction `update_all` bytes      | 1,874,264 | 1,874,264 |       0 |
| transaction `delete_all` bytes      | 1,243,657 | 1,243,657 |       0 |

Net: one whole index space disappears for authored-only workloads. Put
amplification drops from 3.04x to 2.04x for 1k all-row transaction operations,
and single-row transaction mutations drop from 11 puts to 10 puts. The
accounted write bytes are unchanged because this index had empty values; the
layout footprint still shrinks by 1,016 rows and 79,023 key bytes.

Layout footprint after 1k transaction insert:

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
| transaction | `0x00060005` | `changelog.index.by_change_membership` |     0 |         0 |           0 |
| transaction | `0x00060006` | `changelog.index.visible_change`       | 1,016 |    40,559 |      36,432 |

## Pre Physical Layout Change Smoke: commit/change direct-layout planning

Date: 2026-05-20

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
```

Purpose:

- Capture the current 1k smoke scorecard before changing the physical layout
  away from segment-centered changelog storage.
- This run is still on the existing implemented layout:
  `changelog.segment`, `commit_visibility`, `by_commit`, `by_change`,
  authored-only `by_change_membership`, `visible_change`, and
  `tracked_state.projection`.
- `read_many_by_pk` reads 10 primary keys.

### Direct KV Layout

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    2.47 ms |   561 us |         342 us |          391 us |    2.81 ms |     703 us |    1.25 ms |     555 us |
| RocksDB |     489 us |   173 us |        6.26 us |         14.5 us |     536 us |    12.7 us |    7.69 us |    7.37 us |
| redb    |    7.22 ms |   179 us |        28.6 us |         28.6 us |    8.18 ms |    3.94 ms |    4.28 ms |    4.01 ms |

### Transaction Layer

Direct transaction API, bypassing SQL.

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   35.54 ms | 18.93 ms |        9.69 ms |        89.16 ms |  100.36 ms |   72.15 ms |   96.67 ms |   67.77 ms |
| RocksDB |   30.25 ms | 17.97 ms |        8.42 ms |        86.40 ms |   96.97 ms |   66.33 ms |   89.20 ms |   65.77 ms |
| redb    |   40.77 ms | 18.22 ms |        8.79 ms |        83.34 ms |  105.39 ms |   81.10 ms |  105.89 ms |   74.69 ms |

### SQL Session

| Backend   | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| in-memory |   76.84 ms | 21.72 ms |        6.68 ms |         7.75 ms |   excluded |   excluded |  111.17 ms |   87.64 ms |

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

## Hard-cut direct changelog smoke: 2026-05-21

Commands:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
```

Notes:

- This run is after the physical-layout hard cut to direct
  `changelog.commit`, direct `changelog.change`,
  `changelog.commit_change_ref_chunk`, and `tracked_state.commit_root`.
- The scorecard also required finishing the backend support `open(path)` API so
  the bench backends derive their durable write lock internally.
- Criterion: 10 samples, 250 ms warmup, 1 s measurement for smoke groups.
- Values below are Criterion point estimates.

### 1k Smoke Scorecard

#### Direct KV Layout

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    2.29 ms |   501 us |         329 us |          333 us |    2.75 ms |     614 us |    1.17 ms |     520 us |
| RocksDB |     438 us |   158 us |        2.84 us |         9.55 us |     569 us |    13.6 us |    5.72 us |    6.90 us |
| redb    |    8.68 ms |   195 us |        15.0 us |         29.0 us |    8.55 ms |    3.92 ms |    4.64 ms |    4.39 ms |

#### Transaction Layer

Direct transaction API, bypassing SQL.

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   12.74 ms |  3.20 ms |         181 us |          668 us |   15.13 ms |    2.27 ms |   12.87 ms |    2.24 ms |
| RocksDB |   10.39 ms |  2.87 ms |        86.2 us |          420 us |   12.70 ms |    1.93 ms |   11.41 ms |    1.73 ms |
| redb    |   21.77 ms |  3.00 ms |        90.8 us |          432 us |   20.97 ms |    6.41 ms |   20.22 ms |    6.48 ms |

#### SQL Session

| Backend   | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| in-memory |   17.61 ms |  6.22 ms |        1.30 ms |         1.35 ms |   excluded |   excluded |   14.48 ms |    6.52 ms |

### 1k Smoke Accounting

The logical write counts were identical across SQLite, RocksDB, and redb.

| Layer       | Operation        | Logical rows |  Puts | Point deletes | Range deletes | Touched spaces | Backend calls | Written bytes | Put amp | Delete amp |
| ----------- | ---------------- | -----------: | ----: | ------------: | ------------: | -------------: | ------------: | ------------: | ------: | ---------: |
| kv_layout   | insert_all       |        1,000 | 1,000 |             0 |             0 |              1 |             1 |       396,363 |   1.00x |      0.00x |
| kv_layout   | update_all       |        1,000 | 1,000 |             0 |             0 |              1 |             1 |       482,607 |   1.00x |      0.00x |
| kv_layout   | update_one_by_pk |            1 |     1 |             0 |             0 |              1 |             1 |         6,693 |   1.00x |      0.00x |
| kv_layout   | delete_all       |        1,000 |     0 |             0 |             1 |              0 |             1 |             0 |   0.00x |      0.00x |
| kv_layout   | delete_one_by_pk |            1 |     0 |             1 |             0 |              1 |             1 |             0 |   0.00x |      1.00x |
| transaction | insert_all       |        1,000 | 2,037 |             0 |             0 |              7 |             7 |       827,460 |   2.04x |      0.00x |
| transaction | update_all       |        1,000 | 2,037 |             0 |             0 |              7 |             7 |       941,913 |   2.04x |      0.00x |
| transaction | update_one_by_pk |            1 |    12 |             0 |             0 |              7 |             7 |        28,608 |  12.00x |      0.00x |
| transaction | delete_all       |        1,000 | 1,037 |             0 |             0 |              7 |             7 |       508,404 |   1.04x |      0.00x |
| transaction | delete_one_by_pk |            1 |    11 |             0 |             0 |              7 |             7 |        28,307 |  11.00x |      0.00x |

### Layout Footprint After Insert

The transaction layout footprint was identical across SQLite, RocksDB, and
redb for this fixture.

| Layer       | Space id     | Space                               |  Rows | Key bytes | Value bytes |
| ----------- | ------------ | ----------------------------------- | ----: | --------: | ----------: |
| kv_layout   | `0x00020001` | `tracked_state.crud.row.v1`         | 1,000 |    87,244 |     396,363 |
| transaction | `0x00010002` | `untracked_state.row.v1`            |     2 |       120 |         273 |
| transaction | `0x00020001` | `json_store.json`                   | 1,018 |    36,648 |     299,846 |
| transaction | `0x00040001` | `tracked_state.tree_chunk`          |    33 |     1,188 |     243,324 |
| transaction | `0x00040004` | `tracked_state.commit_root`         |     2 |        71 |         288 |
| transaction | `0x00050001` | `binary_cas.manifest`               |     0 |         0 |           0 |
| transaction | `0x00050002` | `binary_cas.manifest_chunk`         |     0 |         0 |           0 |
| transaction | `0x00050003` | `binary_cas.chunk`                  |     0 |         0 |           0 |
| transaction | `0x00060001` | `changelog.commit`                  |     2 |        71 |         270 |
| transaction | `0x00060002` | `changelog.change`                  | 1,016 |    40,559 |     189,738 |
| transaction | `0x00060003` | `changelog.commit_change_ref_chunk` |     2 |        81 |     117,699 |

## Commit Change Ref Chunk Codec Cut: 2026-05-21

Commands:

```sh
cargo test -p lix_engine changelog --no-fail-fast
LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
```

Notes:

- Hard-cut the `changelog.commit_change_ref_chunk` value codec in place.
- The chunk value no longer stores `commit_id`; the reader reconstructs it from
  the chunk key/read context.
- The chunk value now uses chunk-local dictionaries for repeated `schema_key`
  and `file_id` values, stores those dictionary references as `u16`, and uses a
  compact one-part `EntityPk` encoding for the common CRUD case.
- This is a byte/footprint optimization. Criterion timings were noisy: some
  unrelated `kv_layout` benches reported regressions even though this patch does
  not touch that path. Treat the accounting deltas below as the reliable signal.

### 1k Smoke Scorecard

Transaction layer, direct transaction API, Criterion point estimates:

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   12.83 ms |  3.06 ms |         188 us |          657 us |   14.79 ms |    2.26 ms |   14.09 ms |    2.24 ms |
| RocksDB |   11.44 ms |  2.96 ms |        67.1 us |          436 us |   12.43 ms |    2.00 ms |   11.49 ms |    1.62 ms |
| redb    |   21.07 ms |  2.91 ms |        82.5 us |          388 us |   21.40 ms |    6.21 ms |   19.02 ms |    6.21 ms |

SQL session:

| Backend   | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| in-memory |   16.89 ms |  5.68 ms |        1.31 ms |         1.48 ms |   excluded |   excluded |   14.19 ms |    6.25 ms |

### 1k Smoke Accounting

The logical write counts were unchanged. Written bytes dropped because the
`changelog.commit_change_ref_chunk` values are smaller.

| Layer       | Operation        | Logical rows |  Puts | Point deletes | Range deletes | Touched spaces | Backend calls | Written bytes | Put amp | Delete amp |
| ----------- | ---------------- | -----------: | ----: | ------------: | ------------: | -------------: | ------------: | ------------: | ------: | ---------: |
| kv_layout   | insert_all       |        1,000 | 1,000 |             0 |             0 |              1 |             1 |       396,363 |   1.00x |      0.00x |
| kv_layout   | update_all       |        1,000 | 1,000 |             0 |             0 |              1 |             1 |       482,607 |   1.00x |      0.00x |
| kv_layout   | update_one_by_pk |            1 |     1 |             0 |             0 |              1 |             1 |         6,693 |   1.00x |      0.00x |
| kv_layout   | delete_all       |        1,000 |     0 |             0 |             1 |              0 |             1 |             0 |   0.00x |      0.00x |
| kv_layout   | delete_one_by_pk |            1 |     0 |             1 |             0 |              1 |             1 |             0 |   0.00x |      1.00x |
| transaction | insert_all       |        1,000 | 2,037 |             0 |             0 |              7 |             7 |       811,445 |   2.04x |      0.00x |
| transaction | update_all       |        1,000 | 2,037 |             0 |             0 |              7 |             7 |       925,898 |   2.04x |      0.00x |
| transaction | update_one_by_pk |            1 |    12 |             0 |             0 |              7 |             7 |        28,577 |  12.00x |      0.00x |
| transaction | delete_all       |        1,000 | 1,037 |             0 |             0 |              7 |             7 |       492,389 |   1.04x |      0.00x |
| transaction | delete_one_by_pk |            1 |    11 |             0 |             0 |              7 |             7 |        28,276 |  11.00x |      0.00x |

### Layout Footprint After Insert

The transaction layout footprint was identical across SQLite, RocksDB, and
redb for this fixture.

| Layer       | Space id     | Space                               |  Rows | Key bytes | Value bytes |
| ----------- | ------------ | ----------------------------------- | ----: | --------: | ----------: |
| kv_layout   | `0x00020001` | `tracked_state.crud.row.v1`         | 1,000 |    87,244 |     396,363 |
| transaction | `0x00010002` | `untracked_state.row.v1`            |     2 |       120 |         273 |
| transaction | `0x00020001` | `json_store.json`                   | 1,018 |    36,648 |     299,846 |
| transaction | `0x00040001` | `tracked_state.tree_chunk`          |    33 |     1,188 |     243,324 |
| transaction | `0x00040004` | `tracked_state.commit_root`         |     2 |        71 |         288 |
| transaction | `0x00050001` | `binary_cas.manifest`               |     0 |         0 |           0 |
| transaction | `0x00050002` | `binary_cas.manifest_chunk`         |     0 |         0 |           0 |
| transaction | `0x00050003` | `binary_cas.chunk`                  |     0 |         0 |           0 |
| transaction | `0x00060001` | `changelog.commit`                  |     2 |        71 |         270 |
| transaction | `0x00060002` | `changelog.change`                  | 1,016 |    40,559 |     189,738 |
| transaction | `0x00060003` | `changelog.commit_change_ref_chunk` |     2 |        81 |     101,287 |

### Delta From Previous Entry

| Metric                                 |  Before |   After |  Delta |
| -------------------------------------- | ------: | ------: | -----: |
| `commit_change_ref_chunk` value bytes  | 117,699 | 101,287 | -13.9% |
| transaction `insert_all` written bytes | 827,460 | 811,445 |  -1.9% |
| transaction `update_all` written bytes | 941,913 | 925,898 |  -1.7% |
| transaction `delete_all` written bytes | 508,404 | 492,389 |  -3.2% |

## Bounded Commit Change Ref Chunks: 2026-05-20

Commands:

```sh
cargo test -p lix_engine changelog --no-fail-fast
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
```

Notes:

- Added real bounded chunking for `changelog.commit_change_ref_chunk`.
- The target chunk size is 64 KiB, the hard max is 128 KiB, and the entry cap
  is 2048 entries.
- The first implementation measured each candidate chunk by cloning and
  re-encoding the whole growing chunk. That kept bytes stable, but made the
  write path effectively quadratic and pushed 1k transaction writes to roughly
  90-105 ms.
- The fixed implementation uses an incremental size estimator that mirrors the
  chunk codec layout, then validates final chunks in debug builds. This keeps
  the bounded-chunk contract without reintroducing a giant synchronous CPU
  cost.

### Regression Fix Scorecard

Transaction layer, direct transaction API, Criterion point estimates after the
incremental estimator fix:

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   13.12 ms |  3.17 ms |         191 us |          747 us |   15.17 ms |    2.30 ms |   13.94 ms |    2.35 ms |
| RocksDB |   10.20 ms |  2.90 ms |        61.9 us |          402 us |   11.86 ms |    1.57 ms |   11.42 ms |    1.64 ms |
| redb    |   20.96 ms |  2.76 ms |        78.9 us |          378 us |   20.76 ms |    6.04 ms |   19.16 ms |    6.17 ms |

SQL session:

| Backend   | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| in-memory |   17.43 ms |  5.58 ms |        1.26 ms |         1.37 ms |   excluded |   excluded |   14.56 ms |    6.42 ms |

### Delta From Regressed Chunker

The previous bounded-chunk implementation was not kept, but the scorecard made
the regression obvious:

| Workload                      | Regressed chunker | Fixed chunker | Result |
| ----------------------------- | ----------------: | ------------: | -----: |
| SQLite transaction insert 1k  |           ~100 ms |      13.12 ms |  fixed |
| RocksDB transaction insert 1k |            ~89 ms |      10.20 ms |  fixed |
| redb transaction insert 1k    |           ~105 ms |      20.96 ms |  fixed |
| SQL session insert 1k         |           ~100 ms |      17.43 ms |  fixed |

### 1k Smoke Accounting

Accounting was unchanged by the estimator fix. Bounded chunking increases the
1k fixture's `changelog.commit_change_ref_chunk` rows from 2 to 3 because the
large commit ref set now splits, while value bytes stay essentially flat versus
the dictionary codec cut.

| Metric                                | Codec cut | Bounded chunks | Delta |
| ------------------------------------- | --------: | -------------: | ----: |
| `commit_change_ref_chunk` rows        |         2 |              3 |    +1 |
| `commit_change_ref_chunk` key bytes   |        81 |            126 |   +45 |
| `commit_change_ref_chunk` value bytes |   101,287 |        101,325 |   +38 |
| transaction `insert_all` puts         |     2,037 |          2,038 |    +1 |
| transaction `insert_all` bytes        |   811,445 |        811,483 |   +38 |

## Lazy SQL Schema Planning For Direct Transactions: 2026-05-20

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'transaction/lix_sqlite/smoke/insert_all_rows/1k'
```

Notes:

- `Transaction::open` no longer prepares the SQL/DataFusion-visible schema
  catalog for every transaction.
- Direct transaction writes still load the compact schema facts needed for row
  normalization and validation.
- SQL read/write execution paths explicitly prepare the SQL-visible schema
  cache before planning.

### Focused Result

Criterion reported a statistically significant improvement for SQLite direct
transaction inserts:

```text
tracked_state_crud/transaction/lix_sqlite/smoke/insert_all_rows/1k
  time:   [12.560 ms 12.906 ms 13.322 ms]
  change: [-22.883% -16.827% -10.637%] (p = 0.00 < 0.05)
  Performance has improved.
```

This confirms the first-principles cut: direct transaction writes should use
the changelog/tracked-state row path and avoid SQL planning surfaces unless the
caller is actually executing SQL.

## Lazy SQL Schema Planning Full Smoke: 2026-05-20

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
```

Notes:

- Full smoke confirms the lazy SQL schema planning cut is mostly neutral across
  the broader scorecard.
- The target direct transaction insert path stays near the fixed bounded-chunk
  scorecard, with SQLite at 12.99 ms, RocksDB at 10.40 ms, and redb at
  21.62 ms.
- Storage accounting is unchanged by this cut; it only changes when the
  SQL-visible schema catalog is prepared.

### Transaction Layer

Criterion point estimates:

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   12.99 ms |  3.18 ms |         190 us |          658 us |   16.72 ms |    1.95 ms |   14.69 ms |    2.03 ms |
| RocksDB |   10.40 ms |  2.90 ms |        62.7 us |          413 us |   11.22 ms |    1.47 ms |   13.39 ms |    1.64 ms |
| redb    |   21.62 ms |  2.88 ms |        76.7 us |          415 us |   20.89 ms |    6.13 ms |   20.27 ms |    6.03 ms |

### SQL Session

| Backend   | Insert all | Read all | Read one by PK | Read many by PK | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: |
| in-memory |   17.92 ms |  5.75 ms |        1.22 ms |         1.32 ms |   14.38 ms |    6.33 ms |

### Delta From Previous Full Smoke

The previous full smoke was the bounded-chunk regression fix entry.

| Workload                             | Previous |  Current |  Delta |
| ------------------------------------ | -------: | -------: | -----: |
| SQLite transaction insert 1k         | 13.12 ms | 12.99 ms |  -1.0% |
| RocksDB transaction insert 1k        | 10.20 ms | 10.40 ms |  +2.0% |
| redb transaction insert 1k           | 20.96 ms | 21.62 ms |  +3.1% |
| SQL session insert 1k                | 17.43 ms | 17.92 ms |  +2.8% |
| SQLite transaction update_one_by_pk  |  2.30 ms |  1.95 ms | -15.2% |
| SQLite transaction delete_one_by_pk  |  2.35 ms |  2.03 ms | -13.7% |
| RocksDB transaction update_one_by_pk |  1.57 ms |  1.47 ms |  -6.6% |
| RocksDB transaction delete_all 1k    | 11.42 ms | 13.39 ms | +17.2% |

Criterion flagged SQLite update-all and RocksDB delete-all as regressions, but
the changes do not line up with this patch's direct insert target and are likely
smoke-run variance or unrelated backend noise. The direct transaction insert
path remains effectively stable in the full scorecard after the focused
SQLite-only run showed a significant local improvement against Criterion's
cached baseline.

## Transaction Bench Harness Excludes Fixture Teardown: 2026-05-20

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
```

Notes:

- Refactored transaction CRUD benchmarks to use `iter_custom` so measured time
  wraps only the transaction operation.
- Fixture setup and teardown are now outside the returned Criterion duration.
  This keeps SQLite connection close/drop out of the transaction insert timing.
- Added a write-connection pool to the SQLite benchmark/test backend, matching
  the existing read-pool shape, so committed write handles can be reused.
- Storage accounting is unchanged. This is a measurement-harness cleanup, not a
  physical-layout change.

### KV Layout

Criterion point estimates:

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    1.64 ms |   329 us |         150 us |          202 us |    1.55 ms |    89.9 us |     513 us |    48.1 us |
| RocksDB |     449 us |   164 us |        4.20 us |         10.7 us |     487 us |    16.4 us |    6.63 us |    4.69 us |
| redb    |    7.37 ms |   174 us |        12.4 us |         27.8 us |    9.18 ms |    4.15 ms |    4.37 ms |    3.91 ms |

### Transaction Layer

Criterion point estimates:

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   13.66 ms |  3.28 ms |         246 us |          695 us |   15.39 ms |    1.96 ms |   13.71 ms |    2.23 ms |
| RocksDB |   10.22 ms |  2.90 ms |        63.7 us |          406 us |   11.58 ms |    1.46 ms |   11.18 ms |    1.55 ms |
| redb    |   20.17 ms |  2.78 ms |        80.8 us |          399 us |   21.79 ms |    6.34 ms |   20.79 ms |    6.35 ms |

### SQL Session

| Backend   | Insert all | Read all | Read one by PK | Read many by PK | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: |
| in-memory |   17.27 ms |  5.73 ms |        1.35 ms |         1.45 ms |   14.74 ms |    6.54 ms |

### Delta From Previous Full Smoke

The previous full smoke was the lazy SQL schema planning entry.

| Workload                          | Previous |  Current |  Delta |
| --------------------------------- | -------: | -------: | -----: |
| SQLite transaction insert 1k      | 12.99 ms | 13.66 ms |  +5.2% |
| RocksDB transaction insert 1k     | 10.40 ms | 10.22 ms |  -1.7% |
| redb transaction insert 1k        | 21.62 ms | 20.17 ms |  -6.7% |
| SQL session insert 1k             | 17.92 ms | 17.27 ms |  -3.6% |
| SQLite transaction update_all 1k  | 16.72 ms | 15.39 ms |  -8.0% |
| SQLite transaction delete_all 1k  | 14.69 ms | 13.71 ms |  -6.7% |
| RocksDB transaction delete_all 1k | 13.39 ms | 11.18 ms | -16.5% |
| SQLite kv_layout insert 1k        |  2.40 ms |  1.64 ms | -31.7% |
| SQLite kv_layout read_one_by_pk   |   291 us |   150 us | -48.4% |

The biggest visible scorecard shifts are in the SQLite `kv_layout` baselines,
which now also avoid timing fixture teardown. Transaction-path movement is more
mixed but mostly neutral-to-positive outside SQLite insert variance. The focused
SQLite transaction insert run immediately before this full smoke measured
13.18 ms and Criterion reported an 8.8% improvement; the full smoke's SQLite
transaction insert sample landed at 13.66 ms and Criterion reported no
significant change.
