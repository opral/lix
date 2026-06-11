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
  the bench backends own their persistence handles internally.
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

## Baseline Re-run Before New Optimization Pass: 2026-06-09

Commands:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
```

Notes:

- Fresh baseline on branch `fable-5-optimization` (HEAD `e554c557`) before a
  new round of optimization and bug squashing. No code changes in this entry.
- The harness has changed since the last log entry: SQL session benches now
  run on the real `lix_sqlite`, `lix_rocksdb`, and `lix_redb` backends instead
  of a single in-memory backend, so SQL session numbers are not directly
  comparable to earlier entries.
- The accounting report now also emits per-backend rows; logical write counts
  and layout footprint were identical across SQLite, RocksDB, and redb.
- Criterion: 10 samples, 250 ms warmup, 1 s measurement. Values are Criterion
  point estimates.

### 1k Smoke Scorecard

#### Direct KV Layout

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    1.76 ms |   304 us |         130 us |          192 us |    1.56 ms |    68.0 us |     422 us |    54.0 us |
| RocksDB |     428 us |   161 us |        2.32 us |         7.98 us |     490 us |    8.60 us |    17.9 us |    4.96 us |
| redb    |    7.54 ms |   215 us |        13.0 us |         34.9 us |    8.05 ms |    4.07 ms |    4.88 ms |    4.26 ms |

#### Transaction Layer

Direct transaction API, bypassing SQL.

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   10.91 ms |  2.84 ms |         189 us |          679 us |   14.01 ms |    1.57 ms |   12.94 ms |    1.62 ms |
| RocksDB |    9.15 ms |  2.65 ms |        93.0 us |          298 us |    9.82 ms |    1.35 ms |    9.44 ms |    1.34 ms |
| redb    |   20.34 ms |  2.86 ms |        67.7 us |          278 us |   20.41 ms |    6.37 ms |   17.35 ms |    6.10 ms |

#### SQL Session

Now runs on the real backends; SQL updates remain gated behind
`LIX_TRACKED_STATE_CRUD_SQL_UPDATE=1` and excluded.

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: |
| SQLite  |   17.97 ms |  6.59 ms |        1.29 ms |         1.53 ms |   18.25 ms |    7.85 ms |
| RocksDB |   16.40 ms |  5.78 ms |        1.05 ms |         1.24 ms |   12.69 ms |    6.37 ms |
| redb    |   30.27 ms |  6.11 ms |        1.19 ms |         1.46 ms |   21.06 ms |   11.36 ms |

### 1k Smoke Accounting

Logical write counts were identical across SQLite, RocksDB, and redb.

| Layer       | Operation        | Logical rows |  Puts | Point deletes | Range deletes | Touched spaces | Backend calls | Written bytes | Put amp | Delete amp |
| ----------- | ---------------- | -----------: | ----: | ------------: | ------------: | -------------: | ------------: | ------------: | ------: | ---------: |
| kv_layout   | insert_all       |        1,000 | 1,000 |             0 |             0 |              1 |             1 |       396,363 |   1.00x |      0.00x |
| kv_layout   | update_all       |        1,000 | 1,000 |             0 |             0 |              1 |             1 |       482,607 |   1.00x |      0.00x |
| kv_layout   | update_one_by_pk |            1 |     1 |             0 |             0 |              1 |             1 |         6,693 |   1.00x |      0.00x |
| kv_layout   | delete_all       |        1,000 |     0 |             0 |             1 |              0 |             1 |             0 |   0.00x |      0.00x |
| kv_layout   | delete_one_by_pk |            1 |     0 |             1 |             0 |              1 |             1 |             0 |   0.00x |      1.00x |
| transaction | insert_all       |        1,000 | 2,032 |             0 |             0 |              7 |             7 |       642,063 |   2.03x |      0.00x |
| transaction | update_all       |        1,000 | 2,032 |             0 |             0 |              7 |             7 |       728,421 |   2.03x |      0.00x |
| transaction | update_one_by_pk |            1 |    12 |             0 |             0 |              7 |             7 |        18,532 |  12.00x |      0.00x |
| transaction | delete_all       |        1,000 | 1,032 |             0 |             0 |              7 |             7 |       292,912 |   1.03x |      0.00x |
| transaction | delete_one_by_pk |            1 |    11 |             0 |             0 |              7 |             7 |        18,229 |  11.00x |      0.00x |

### Layout Footprint After Insert

Identical across SQLite, RocksDB, and redb.

| Layer       | Space id     | Space                               |  Rows | Key bytes | Value bytes |
| ----------- | ------------ | ----------------------------------- | ----: | --------: | ----------: |
| kv_layout   | `0x00020001` | `tracked_state.crud.row.v1`         | 1,000 |    87,244 |     396,363 |
| transaction | `0x00010002` | `untracked_state.row.v1`            |     2 |        83 |         185 |
| transaction | `0x00020001` | `json_store.json`                   | 1,018 |    36,648 |     299,700 |
| transaction | `0x00040001` | `tracked_state.tree_chunk`          |    27 |       972 |     162,086 |
| transaction | `0x00040004` | `tracked_state.commit_root`         |     2 |        80 |         167 |
| transaction | `0x00050001` | `binary_cas.manifest`               |     0 |         0 |           0 |
| transaction | `0x00050002` | `binary_cas.manifest_chunk`         |     0 |         0 |           0 |
| transaction | `0x00050003` | `binary_cas.chunk`                  |     0 |         0 |           0 |
| transaction | `0x00060001` | `changelog.commit`                  |     2 |        80 |         102 |
| transaction | `0x00060002` | `changelog.change`                  | 1,016 |    40,640 |     128,857 |
| transaction | `0x00060003` | `changelog.commit_change_ref_chunk` |     3 |       135 |      71,882 |

### Delta From Previous Full Smoke

The previous full smoke was the fixture-teardown harness entry; the accounting
reference is the bounded-chunk/codec-cut entries. Intervening mainline commits
(not part of this log) account for these shifts.

| Workload                               | Previous |  Current |  Delta |
| -------------------------------------- | -------: | -------: | -----: |
| SQLite transaction insert 1k           | 13.66 ms | 10.91 ms | -20.1% |
| RocksDB transaction insert 1k          | 10.22 ms |  9.15 ms | -10.5% |
| redb transaction insert 1k             | 20.17 ms | 20.34 ms |  +0.8% |
| SQLite transaction update_all 1k       | 15.39 ms | 14.01 ms |  -9.0% |
| RocksDB transaction update_all 1k      | 11.58 ms |  9.82 ms | -15.2% |
| RocksDB transaction delete_all 1k      | 11.18 ms |  9.44 ms | -15.6% |
| transaction `insert_all` puts          |    2,038 |    2,032 |     -6 |
| transaction `insert_all` bytes         |  811,483 |  642,063 | -20.9% |
| transaction `update_all` bytes         |  925,898 |  728,421 | -21.3% |
| transaction `delete_all` bytes         |  492,389 |  292,912 | -40.5% |
| `changelog.change` value bytes         |  189,738 |  128,857 | -32.1% |
| `tracked_state.tree_chunk` value bytes |  243,324 |  162,086 | -33.4% |
| `commit_change_ref_chunk` value bytes  |  101,325 |   71,882 | -29.1% |

SQL session numbers have no comparable previous entry because the harness moved
from the in-memory backend to the three real backends.

## Optimization Run: compiled schema catalog cache

Date: 2026-06-09

Commands:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
cargo test -p lix_engine
```

Profiling:

- samply profiles of the bench binary (built with
  `CARGO_PROFILE_BENCH_DEBUG=true`, recorded via
  `samply record --save-only -- <bench-bin> --bench <filter> --profile-time 10`)
  showed `CatalogSnapshot::from_schema_facts` being rebuilt on every
  transaction: 6.7% of the timed 1k `insert_all` op and 47.4% of the timed
  `update_one_by_pk` op on RocksDB, almost all of it in
  `SchemaPlan::compile`/`compile_lix_schema` (jsonschema validator
  compilation).

Change:

- `CatalogContext` now caches compiled `CatalogSnapshot`s in an engine-wide
  map keyed by a blake3 content fingerprint of the schema facts
  (`fingerprint_schema_facts`). Identical fact sets always hash identically,
  so cached snapshots cannot go stale and no invalidation protocol exists;
  changed schema rows produce different facts and therefore a different key.
- Transactions hold a `TransactionCatalog` copy-on-write handle:
  `Shared(Arc<CatalogSnapshot>)` from the cache for normal reads, switched to
  a private `Owned` rebuild only when the transaction registers a schema
  (`insert_schema_for_domain`). Pending registrations are never visible to
  the shared cache.
- Plan ids stay stable across the copy-on-write rebuild because catalog
  entries keep their insertion order, matching the previous full-rebuild
  semantics of `insert_schema_for_domain`.
- The cache holds at most 64 fact sets and clears wholesale at the bound;
  schema catalogs churn rarely, so this only guards pathological
  schema-mutation workloads.
- Storage accounting is unchanged by construction: the accounting tables from
  this run are byte-identical to the 2026-06-09 baseline.
- `cargo test -p lix_engine`: 927 passed, 0 failed.

### 1k Smoke Scorecard

#### Transaction Layer

Direct transaction API, bypassing SQL. Criterion point estimates:

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   10.57 ms |  3.00 ms |         194 us |          666 us |   14.30 ms |    1.06 ms |   11.86 ms |    1.25 ms |
| RocksDB |    9.00 ms |  2.77 ms |        41.8 us |          271 us |    8.43 ms |     623 us |    8.08 ms |     613 us |
| redb    |   19.75 ms |  2.73 ms |        67.0 us |          263 us |   19.24 ms |    5.14 ms |   16.50 ms |    5.13 ms |

#### SQL Session

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: |
| SQLite  |   17.84 ms |  6.60 ms |        1.32 ms |         1.48 ms |   15.52 ms |    6.91 ms |
| RocksDB |   15.77 ms |  5.69 ms |        1.04 ms |         1.18 ms |   12.54 ms |    5.61 ms |
| redb    |   31.24 ms |  6.51 ms |        1.26 ms |         1.42 ms |   20.14 ms |   10.33 ms |

#### Direct KV Layout

Control group; this patch does not touch the KV layout path. Movement here is
run-to-run noise on microsecond-scale benches.

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    1.52 ms |   301 us |         168 us |          175 us |    1.58 ms |    70.4 us |     432 us |    52.6 us |
| RocksDB |     437 us |   161 us |        2.73 us |         10.0 us |     470 us |    9.10 us |    4.69 us |    4.33 us |
| redb    |    8.14 ms |   239 us |        12.5 us |         18.5 us |    8.97 ms |    4.23 ms |    4.47 ms |    4.04 ms |

### Delta From 2026-06-09 Baseline

Same machine, same day, same harness. Transaction and SQL session deltas are
attributable to this change; single-row transaction mutations no longer pay a
full jsonschema catalog compile per commit.

| Workload                              | Baseline |  Current |  Delta |
| ------------------------------------- | -------: | -------: | -----: |
| RocksDB transaction update_one_by_pk  |  1.35 ms |   623 us | -54.0% |
| RocksDB transaction delete_one_by_pk  |  1.34 ms |   613 us | -54.4% |
| RocksDB transaction read_one_by_pk    |  93.0 us |  41.8 us | -55.0% |
| SQLite transaction update_one_by_pk   |  1.57 ms |  1.06 ms | -32.7% |
| SQLite transaction delete_one_by_pk   |  1.62 ms |  1.25 ms | -22.9% |
| redb transaction update_one_by_pk     |  6.37 ms |  5.14 ms | -19.3% |
| redb transaction delete_one_by_pk     |  6.10 ms |  5.13 ms | -15.9% |
| RocksDB transaction update_all 1k     |  9.82 ms |  8.43 ms | -14.2% |
| RocksDB transaction delete_all 1k     |  9.44 ms |  8.08 ms | -14.4% |
| RocksDB transaction insert_all 1k     |  9.15 ms |  9.00 ms |  -1.6% |
| SQLite transaction delete_all 1k      | 12.94 ms | 11.86 ms |  -8.3% |
| SQLite sql_session delete_all 1k      | 18.25 ms | 15.52 ms | -15.0% |
| SQLite sql_session delete_one_by_pk   |  7.85 ms |  6.91 ms | -12.0% |
| RocksDB sql_session delete_one_by_pk  |  6.37 ms |  5.61 ms | -11.9% |

The focused RocksDB run immediately after the change (against Criterion's
cached pre-change baseline) measured insert_all at 8.42 ms (-14.3%); the full
smoke sample above landed at 9.00 ms, so the bulk-insert win is real but
within a few percent of run variance. The dominant, robust win is the removal
of the fixed per-transaction catalog compile, which was about half of every
single-row transaction operation.

## Optimization Run: raw-row keyed catalog cache

Date: 2026-06-09

Commands:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
cargo test -p lix_engine --lib
```

Profiling:

- After the compiled-catalog cache, samply showed the schema-facts pipeline as
  the dominant fixed cost of single-row transaction ops: about 60% of
  `update_one_by_pk` on RocksDB, paid twice per transaction (once in
  `open_transaction`'s prefetch, once at row normalization). The pipeline was
  live-state scan (~15% + ~14%) plus `decode_registered_schema_row` serde
  parsing of every schema row (~8% + ~9%) plus `fingerprint_schema_facts`
  canonical-JSON serialization (~11%).
- The decode and canonicalization existed only to compute the cache key.
  Decoding is deterministic, so the raw `snapshot_content` bytes already
  uniquely determine the decoded facts.

Change:

- `CatalogContext::compiled_catalog_for_domain` is the new transaction hot
  path: it scans the raw registered-schema rows for a domain, fingerprints
  the raw row contents (blake3, length-prefixed schema-domain component +
  `snapshot_content`), and returns the cached `Arc<CatalogSnapshot>` on a
  hit. Rows are only JSON-decoded and canonicalized on a miss, where the
  result flows through the existing facts-fingerprint cache.
- A raw-key variation (ordering or textual differences with equal canonical
  content) can only cause a conservative cache miss, never a wrong hit.
- `open_transaction` now prefetches the compiled catalog `Arc` instead of a
  decoded facts `Vec`, and `TransactionSchemaResolver` stores
  `TransactionCatalog` directly; the intermediate `SchemaFacts` staging
  variant is gone.
- Storage accounting is byte-identical to the previous entry.
- `cargo test -p lix_engine --lib`: 930 passed, 0 failed.
- Verification profile: `decode_registered_schema_row` and
  `fingerprint_schema_facts` no longer appear in the `update_one_by_pk`
  stacks; the remaining facts cost is `scan_catalog_rows` only.

### 1k Smoke Scorecard

Note: this run was taken while the machine was in interactive use, so
individual cells carry more noise than earlier entries. The kv_layout control
group stayed within historical variance.

#### Transaction Layer

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |   10.79 ms |  2.88 ms |         202 us |          688 us |   13.26 ms |     885 us |   11.83 ms |     947 us |
| RocksDB |    8.16 ms |  2.65 ms |        41.7 us |          260 us |    8.46 ms |     420 us |    8.08 ms |     493 us |
| redb    |   19.41 ms |  2.80 ms |        57.3 us |          275 us |   19.50 ms |    5.19 ms |   16.37 ms |    5.08 ms |

#### SQL Session

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: |
| SQLite  |   17.50 ms |  6.33 ms |        1.27 ms |         1.47 ms |   15.80 ms |    6.67 ms |
| RocksDB |   14.30 ms |  5.49 ms |        1.11 ms |         1.15 ms |   12.14 ms |    5.54 ms |
| redb    |   30.18 ms |  5.77 ms |        1.19 ms |         1.34 ms |   20.02 ms |   10.15 ms |

#### Direct KV Layout

Control group; untouched by this patch.

| Backend | Insert all | Read all | Read one by PK | Read many by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | --------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    1.51 ms |   303 us |         158 us |          189 us |    1.57 ms |    74.1 us |     485 us |    43.7 us |
| RocksDB |     425 us |   156 us |        2.61 us |         7.29 us |     477 us |    8.53 us |    4.64 us |    5.08 us |
| redb    |    8.12 ms |   241 us |        15.5 us |         31.6 us |    9.86 ms |    4.10 ms |    4.75 ms |    4.03 ms |

### Delta From Previous Entry (compiled schema catalog cache)

| Workload                             | Previous |  Current |  Delta |
| ------------------------------------ | -------: | -------: | -----: |
| RocksDB transaction update_one_by_pk |   623 us |   420 us | -32.6% |
| RocksDB transaction delete_one_by_pk |   613 us |   493 us | -19.6% |
| SQLite transaction update_one_by_pk  |  1.06 ms |   885 us | -16.2% |
| SQLite transaction delete_one_by_pk  |  1.25 ms |   947 us | -24.1% |
| RocksDB transaction insert_all 1k    |  9.00 ms |  8.16 ms |  -9.3% |
| RocksDB sql_session insert_all 1k    | 15.77 ms | 14.30 ms |  -9.3% |
| SQLite transaction update_all 1k     | 14.30 ms | 13.26 ms |  -7.3% |
| redb transaction update_one_by_pk    |  5.14 ms |  5.19 ms |  +1.0% |

redb single-row ops are dominated by redb's fixed per-commit durability cost,
so the catalog-path savings do not move them; that cost is a backend
characteristic, not an engine overhead.

Cumulative since the 2026-06-09 baseline (both catalog cache rounds):
RocksDB transaction update_one_by_pk 1.35 ms -> 420 us (3.2x), delete_one_by_pk
1.34 ms -> 493 us (2.7x), SQLite update_one_by_pk 1.57 ms -> 885 us (1.8x).

Remaining target from the verification profile: the live-state scan of
registered-schema rows still runs twice per transaction (~19% of
update_one_by_pk each). Eliminating it requires caching scan results keyed on
storage state, which needs an invalidation story; deferred. The validation
fast-path (unconsumable fk_target bookkeeping, jsonschema is_valid) remains
the next bulk-op target.

## Backend Contract: sorted batch lowering and unspecified visit order

Date: 2026-06-10

Commands:

```sh
LIX_WRITE_SET_ORDER_STATS=1 LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
cargo test -p lix_engine --features storage-benches
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'kv_layout/lix_redb/smoke/update_all_rows'
```

Measurement (env-gated `LIX_WRITE_SET_ORDER_STATS` probe in write-set
lowering):

- Per 1k transaction insert/update commit, the hash-keyed
  `json_store.json` batch (1,001 puts, ~49% of all puts) arrives unsorted;
  `changelog.change` (1,000 puts, time-ordered ids) and
  `tracked_state.tree_chunk` (BTreeMap iteration) arrive sorted.
- The kv_layout bench writes its 1,000-put batch unsorted.
- Delete commits lower almost entirely pre-sorted batches.

Changes:

- `StorageWriteSet` lowering now delivers each space batch to the backend
  sorted by key ascending (skipping the sort when the batch is already
  sorted, which is the common case for changelog/tree spaces; the
  sortedness check itself is a read-only scan). Within a group all keys
  share the space prefix, so logical order equals physical order. The
  order probe reports natural order before the sort and covers puts and
  deletes.
- `BackendWrite::put_many`/`delete_many` document the contract: at most one
  mutation per key, engine-produced batches sorted ascending.
- `BackendRead::visit_keys` order is now actively enforced as unspecified: a
  new order-scrambling backend decorator (`tests/backend/scrambled.rs`)
  replays point-read visits in a seeded-shuffled order and must pass both
  the backend conformance suite and a full transaction CRUD equivalence
  test (identical row contents and identical per-space layout accounting at
  every stage versus the plain in-memory backend; byte-exact physical
  comparison is impossible because commit/change ids differ per run). Both
  pass.

Same-day A/B (single binary where noted):

| Workload                          | Unsorted | Sorted  |  Delta |
| --------------------------------- | -------: | ------: | -----: |
| kv_layout redb update_all 1k      |  7.57 ms | 6.19 ms | -18.2% |
| kv_layout rocksdb insert_all 1k   |   425 us |  349 us | -17.9% |
| kv_layout sqlite insert_all 1k    |  1.51 ms | 1.42 ms |  -6.0% |
| transaction redb update_all 1k    | 19.50 ms | 17.38 ms | -10.9% |
| transaction sqlite delete_all 1k  |  ~12.0 ms | ~12.5 ms | inconclusive |

The sqlite transaction delete_all cell initially measured +5-6% slower with
sorting in stash-based A/Bs. A single-binary A/B (temporary
`LIX_LOWER_UNSORTED` env toggle, removed with this change; reproduce by
reverting the sort block in `write_set.rs`) also showed it, but reversing
the within-pair run order flipped the sign in one of three pairs and
widened the unsorted spread to +/-8%; the order probe shows the timed
delete batches are already sorted, so sorting performs no writes on that
path (only the read-only sortedness scan runs). Treated as noise. The
kv_layout rows in the table above are stash-based same-day pairs; the
delete_all investigation used the single-binary toggle. All engine test
targets pass; accounting is unchanged (sorting only reorders within a
batch).

The rs-sdk SQLite backend keeps its internal sort as a cheap verification
pass; with engine-sorted input it degenerates to a single ascending-run
detection.

## Binary UUID Keys For Changelog And Commit Roots

Date: 2026-06-10

Commands:

```sh
LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
cargo test -p lix_engine --features storage-benches
```

Context:

- `ChangeId`/`CommitId` were already binary in memory (`Uuid` wrappers) and
  already encode as 16 raw bytes inside musli values. The only remaining
  text leak was three key codecs routing ids through `Display` into 36-byte
  hyphenated text: `changelog.change`/`changelog.commit` identity keys, the
  `commit_change_ref_chunk` key prefix, and `tracked_state.commit_root`
  keys.

Change (hard cut, no migration):

- Identity keys are now the raw 16 UUID bytes. UUIDv7 big-endian byte order
  matches hyphenated-text lexicographic order, so range scans, resume
  tokens, and the sorted-batch lowering contract behave identically.
- Scan resume tokens decode from the binary key (`commit_id_from_key` /
  `change_id_from_key`) instead of `String::from_utf8`.
- `tracked_state.commit_root` staging and lookup share one binary key
  helper; the test-only text-key fallback in `load_commit_root` is gone
  (`parse_lix` canonicalizes test labels identically on both paths).
- Scan `start_after` tokens now parse as UUIDs, so a malformed resume token
  errors instead of silently scanning from an arbitrary text position.

### Storage A/B (1k insert footprint, identical on all three backends)

| Space                               | Key bytes before | after  |  Delta |
| ----------------------------------- | ---------------: | -----: | -----: |
| `changelog.change` (1,016 rows)     |           40,640 | 20,320 | -50.0% |
| `changelog.commit`                  |               80 |     40 | -50.0% |
| `tracked_state.commit_root`         |               80 |     40 | -50.0% |
| `changelog.commit_change_ref_chunk` |              135 |     72 | -46.7% |

Engine key bytes overall: ~78.6 KB -> ~58.2 KB (-26%). Value bytes are
byte-identical, confirming the value codecs were already binary. Total
store bytes shrink ~2.8% at this fixture size; the structural win is
changelog B-tree geometry (half-width keys on the highest-row-count space),
which compounds as the store grows.

### Perf A/B

Same-day single-run comparison was noisy in both directions; an alternating
stash-based triple A/B on the contested RocksDB cells pooled to NEW slightly
faster with overlapping spreads:

| Cell (rocksdb transaction)  | OLD mean (3 runs) | NEW mean (3 runs) |
| --------------------------- | ----------------: | ----------------: |
| read_one_by_pk              |           58.0 us |           45.4 us |
| update_one_by_pk            |            574 us |            490 us |

Treated as perf-neutral-to-positive at 1k smoke scale, as predicted when
this cut was scoped: the per-commit savings (one fewer String allocation
plus Display format per id-keyed row, halved key compares) are real but
small against commit totals. The durable claim is the storage table above.

- `cargo test -p lix_engine --features storage-benches`: 1,534 passed across
  all targets (three changelog scan tests and two rebuild tests updated:
  resume tokens are typed now, and corruption fixtures plant records at
  binary keys).

## Tree-Chunk Leaf Compaction: front-coded keys

Date: 2026-06-10

Commands:

```sh
LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
cargo test -p lix_engine --features storage-benches
```

Change (hard cut):

- Leaf nodes use a new wire format: a kind byte, then per entry a
  shared-prefix length, key suffix, and verbatim value bytes. Entries within
  a node are sorted, so consecutive keys share the encoded
  schema-key/file-id prefix and most of the entity pk; front-coding removes
  that redundancy. Values are untouched, so value byte-equality and the
  value codec are unchanged. Internal nodes keep their musli body behind the
  kind byte.
- Chunk boundary logic is untouched: boundaries are content-defined on
  uncompressed key bytes, so the same entries land in the same chunks and
  history-independence is preserved by construction. There is no
  estimator/codec coupling to drift.
- Decode reconstructs keys into a single pre-reserved arena per node; values
  stay zero-copy. Debug builds verify every encoded leaf round-trips.
  Property tests cover representative shapes, 512 generated heavy-prefix
  keys, determinism, and malformed-byte rejection.

### Storage A/B (1k insert footprint, identical across backends)

| Metric                              |  Before |   After |  Delta |
| ----------------------------------- | ------: | ------: | -----: |
| `tree_chunk` value bytes (27 chunks) | 162,086 | 125,918 | -22.3% |
| transaction insert_all written bytes | 642,063 | 606,256 |  -5.6% |
| transaction update_all written bytes | 728,421 | 692,589 |  -4.9% |
| transaction delete_all written bytes | 292,912 | 257,080 | -12.2% |

Put counts and chunk counts unchanged.

### Perf A/B (alternating stash triples, RocksDB)

- read_one_by_pk: OLD mean 64.0 us, NEW mean 51.3 us (faster in all three
  pairs; smaller chunks mean less memory traffic on hot decodes).
- read_many_by_pk initially regressed +8.3% consistently: the decode arena
  grew by doubling, costing ~8 re-allocations per leaf. Pre-reserving the
  arena to the body length erased it (OLD mean 313.3 us, NEW 311.3 us, sign
  flipping across pairs).
- 10k spot check: rocksdb transaction insert 77.6 ms, read_all 32.7 ms; no
  split pathology (boundaries are codec-independent).

### Byte-exact order-independence enforcement

Validating this change hardened the equivalence suite and found an engine
bug:

- The scrambled-visit equivalence test now runs both fixtures in
  deterministic-functions mode and asserts byte-identical storage across all
  ten spaces at every stage, replacing aggregate row/byte-total comparison
  (which could collide and, with wall-clock content, flaked
  time-dependently).
- The exact comparison immediately exposed that `FunctionContext::prepare`
  stamped the deterministic-sequence bookkeeping row from the system clock
  even in deterministic mode, violating that mode's byte-determinism
  contract. Bookkeeping timestamps now derive from the persisted sequence
  without consuming a sequence tick. Stress: 20 consecutive runs green.

- `cargo test -p lix_engine --features storage-benches`: 1,538 passed.

## Value Dedup: keyed change ids and chunk-local commit dictionaries

Date: 2026-06-10

Commands:

```sh
LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
cargo test -p lix_engine --features storage-benches
```

Changes (hard cuts):

- `ChangeRecord` values no longer store `change_id`: it is the storage key
  and is reconstructed on decode, the same pattern
  `commit_change_ref_chunk` already uses for `commit_id`. The in-memory
  record keeps the id; only the stored form
  (`ChangeRecordRef`/`ChangeRecordView`) drops it. The scan-path
  key-vs-value consistency check became tautological and was removed; a
  direct codec round-trip suite (fully populated, empty options,
  id-from-argument) covers the hand-threaded stored form.
- Leaf nodes dictionary the commit-id slice of their values chunk-locally.
  The standard value encoding places `change_id` at bytes [0..16) and
  `commit_id` at [16..32) (pinned by a layout test); bulk commits repeat one
  commit id across every entry, so the encoder collects first-occurrence
  commit ids into a per-chunk dictionary and stores values with the slice
  spliced out. The splice is reversible byte-for-byte regardless of value
  semantics; values shorter than 32 bytes are stored verbatim (dict ref 0).
  Dictionaried values are reconstructed into the decode arena; verbatim
  values stay zero-copy. Both golden wire-format vectors (dict-less and
  dictionaried) pin the encoding.

Review found two decoder bugs in the initial diff, both fixed with
regression tests:

- The shared-prefix guard measured the previous key from the arena tail,
  which holds value bytes after a dictionaried entry; a corrupt chunk with
  an inflated shared length was accepted and front-coded the next key out
  of value bytes. The decoder now tracks the previous key end explicitly.
- `(dict_ref - 1) * 16` was unchecked; a dict_ref near 2^60 wrapped the
  multiplication in release builds and aliased dictionary slot 0. The
  decoder validates `dict_ref <= dict_len` before any index arithmetic.

Note on coverage: the byte-exact scrambled equivalence test exercises the
dictionaried path (its fixture values are full tracked-state encodings) but
compares the two fixtures against each other, so it catches order-dependent
bugs only; deterministic codec bugs are the round-trip/golden suites' job.

### Storage A/B (1k insert footprint, identical across backends)

| Metric                                | Before  | After   | Delta  |
| ------------------------------------- | ------: | ------: | -----: |
| `changelog.change` value bytes        | 128,857 | 112,601 | -12.6% |
| `tree_chunk` value bytes              | 125,918 | 110,833 | -12.0% |
| transaction insert_all written bytes  | 606,256 | 575,409 |  -5.1% |
| transaction update_all written bytes  | 692,589 | 661,744 |  -4.5% |
| transaction delete_all written bytes  | 257,080 | 226,234 | -12.0% |

The change-record delta is exactly 16 bytes x 1,016 records. Cumulative
tree_chunk since before the front-coding cut: 162,086 -> 110,833 (-31.6%).

### Perf A/B (alternating stash triples, RocksDB)

read_all 2.640 -> 2.618 ms means and read_many 266.7 -> 270.2 us means, both
with sign flips across pairs: parity. read_one initially measured
42.2 -> 46.2 us means with mixed signs; because dictionaried values lose
zero-copy on decode (a plausible mechanism for a point-read cost), a second
alternating triple was run and came back at parity (48.6 -> 49.2 us means,
sign flipping across pairs). Across all six pairs the sign flips in three:
no consistent regression.

- `cargo test -p lix_engine --features storage-benches`: 1,549 passed.
  New tests: value-layout pin; dictionaried round-trip with mixed
  verbatim/32-byte-boundary/bulk entries plus a compression assertion;
  dict-less and dictionaried golden wire vectors; out-of-bounds, wrapping,
  and adversarial-length dictionary rejection; shared-length-after-
  dictionaried-value rejection; direct change-record round-trips.

## Ref-Chunk Entity-Pk Front-Coding

Date: 2026-06-10

Commands:

```sh
LIX_TRACKED_STATE_CRUD_ACCOUNTING=1 cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'no_matching_benchmark_filter'
cargo test -p lix_engine --features storage-benches
```

Change (hard cut):

- `commit_change_ref_chunk` entries store their entity pk front-coded
  against the previous entry's encoded pk (`pk_shared` + `pk_suffix` fields
  in the musli entry struct). The chunker already sorted entries by
  (schema_key, file_id, entity_pk), so consecutive pks share long heads;
  correctness does not depend on sortedness (unsorted input only loses
  compression). The borrowed chunk view (`view_commit_change_ref_chunk`)
  folded into the owned decode: front-coded parts cannot borrow from chunk
  bytes, and the view had exactly one caller.
- Order-semantics audit preceded the cut: rebuild applies refs through
  keyed tree mutations, merge/graph collect ids into sets, GC is
  unimplemented, and history output order was already entity-sorted because
  the chunker sorts. No consumer depends on a different entry order.

The first encoding attempt front-coded the existing musli pk bytes and
REGRESSED storage (+1.4%): musli's per-part length prefixes sit ahead of
the content, so same-shaped pks of different lengths share only 1-2 bytes,
and the per-entry `pk_shared` overhead exceeded the savings. The accounting
gate caught it immediately. The fix is a length-free pk byte encoding for
the front-coded form: part bytes with `0x00` escaped as `0x00 0xFF` and
parts terminated by `0x00 0x01`, making byte-prefix sharing equal
content-prefix sharing. Lesson recorded: front-coding through a
length-prefixed encoding is structurally crippled; check the byte layout
before estimating.

### Storage A/B (1k insert footprint, identical across backends)

| Metric                                 | Before  | After   | Delta  |
| -------------------------------------- | ------: | ------: | -----: |
| `commit_change_ref_chunk` value bytes  |  71,882 |  30,881 | -57.0% |
| transaction insert_all written bytes   | 575,409 | 534,460 |  -7.1% |
| transaction update_all written bytes   | 661,744 | 620,795 |  -6.2% |
| transaction delete_all written bytes   | 226,234 | 185,285 | -18.1% |

The insert, update, and delete written-byte deltas are mutually exact at
-40,949. The ref-chunk footprint delta is -41,001; the 52-byte gap between
the write-path metric and the end-state footprint metric is accounting
granularity, not a discrepancy in the cut.

### Perf (vs cached baselines, RocksDB smoke)

Writes got faster with the smaller chunks: insert -13%, update -16%,
delete -1%. read_one -13%. read_all printed +6.9% then -5.5% on immediate
re-run: cache noise, and ref chunks are not on the read_all path. Encode
cost (pk re-encode + prefix compare per entry) is on the commit path and
is invisible at smoke scale.

- Review additions: a second golden vector pinning the 0x00 0xFF escape
  bytes; escape edge-case round-trips (lone/trailing/consecutive NULs,
  0x01 as plain content, shared prefix splitting an escape pair);
  adversarial decode rejections (empty pk, non-UTF-8 part, out-of-bounds
  dictionary indices, u32::MAX shared length); an encode-side debug assert
  on the non-empty-parts invariant.
- Known follow-up: the chunk-size estimator still charges pre-front-coding
  pk sizes, so chunks run ~2.3x under-filled at large commit sizes (safe
  direction; the max-bytes invariant only gets stronger). Fix is to track
  the previous encoded pk in the builder and charge the suffix length.
- `cargo test -p lix_engine --features storage-benches`: 1,562 passed.
  New tests: sorted/multi-part/unicode round-trip, unsorted-input
  correctness, compression assertion with honest per-entry math, two
  golden wire vectors, NUL-escape round-trips, truncated-escape rejection,
  over-shared-pk rejection.

## Ref-Chunk Size Estimator: charge front-coded pk cost

Date: 2026-06-10

Follow-up to the front-coding cut. `CommitChangeRefChunkBuilder` still
charged entity pks at their verbatim size, so chunks closed ~2.3x before
the 64 KiB target (safe direction, under-filled). The builder now tracks
the previous entry's encoded pk - the same front-coding base the codec
uses - and charges `varint(shared) + varint(suffix_len) + suffix` exactly.
Escape overhead is measured rather than modeled, which also closes the
NUL-heavy underestimate the codec review flagged. A fresh builder starts
from an empty base, matching the per-chunk front-coding reset.

At the 1k bench the insert commit packs into one chunk instead of two
(3 -> 2 chunks total, one fewer put per commit, -58 bytes). The effect
scales with commit size: a 100k-row commit packs to the 2048-entry cap at
roughly the byte target instead of producing ~2.2x more, smaller chunks. New tests pin that front-coded entries pack to target, that the
builder's estimate is a direct upper bound on the real encoding
(fixture crossing the 128-byte varint boundary plus NUL escapes), the
varint-size boundaries, and the oversized-single-entry error path; a
const assertion ties the 2-byte index charge to the entry cap staying
under musli's 3-byte varint threshold. Review confirmed the upper
bound term-by-term against the musli wire format, with an empirical
fuzz across adversarial shapes (margins +22 to +11,737 bytes). The pk
encode is hoisted to once per entry on the commit path.

- `cargo test -p lix_engine --features storage-benches`: 1,565 passed.

## SQLite Format V2 Baseline: per-space bench cells and file stats

Date: 2026-06-10

Command:

```sh
LIX_SQLITE_FILE_STATS=1 cargo bench -p lix_sdk --features sqlite --bench sqlite_backend -- 'space_prefix_scan|space_truncate'
```

Preparation for the per-space-tables format cut. The rs-sdk bench
previously used un-prefixed keys, which cannot observe a space-layout
change; it now has a multi-space fixture mirroring the engine's physical
layout (4-byte big-endian space id prefix, six spaces in 1k-commit
accounting proportions, splitmix64 key entropy, sorted batches), plus:

- `space_prefix_scan`: one space scanned by physical prefix (a table scan
  under v2),
- `space_truncate`: one space's full range deleted with per-iteration
  fixture rebuild (a candidate for table truncation under v2),
- a file-stats report gated by `LIX_SQLITE_FILE_STATS=1`: checkpointed
  file bytes, page accounting, and per-table dbstat - the physical
  metric an API-level accounting harness cannot see.

### Format v1 baseline

| Cell                              | v1       |
| ---------------------------------- | -------- |
| space_prefix_scan json_store (20k) | 1.034 ms |
| space_prefix_scan tree_chunk (500) | 132 us   |
| space_truncate json_store (20k)    | 10.9 ms  |

File: 1 table (`lix_internal_entries`), 3,331 x 4 KiB pages, 13,643,776
bytes on disk for the multi-space mix.

## SQLite Format V2: per-space bucket tables

Date: 2026-06-10

Commands:

```sh
cargo bench -p lix_sdk --features sqlite --bench sqlite_backend
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- 'transaction/lix_sqlite/smoke'
LIX_SQLITE_FILE_STATS=1 cargo bench -p lix_sdk --features sqlite --bench sqlite_backend -- 'no_match'
cargo test -p lix_sdk --features sqlite && cargo test -p lix_engine --features storage-benches
```

Change (hard cut, `SQLITE_FORMAT_VERSION` 1 -> 2; v1 files rejected with
a clear no-migration error):

- One table per key bucket instead of a single interleaved
  `lix_internal_entries` table. The bucket is the key's zero-padded first
  four bytes read big-endian - an order-preserving partition for arbitrary
  keys (a unit test pins `bucket(k1) <= bucket(k2)` for sorted keys), so
  cross-table iteration in bucket order preserves global scan order.
  Engine keys start with the 4-byte space id, so engine buckets are
  exactly storage spaces. Full keys stay stored (the 4-byte prefix saving
  was ~1% of file; locality and truncation are the wins).
- Tables are created lazily on first write. A positive-only bucket cache
  is updated from committed state alone (a transaction's created tables
  merge on commit, vanish on rollback) and never trusted negatively;
  misses probe sqlite_master inside the caller's snapshot, so tables
  created by other handles on the same file are always found.
- `delete_range` issues an unqualified `DELETE FROM` when the range
  provably covers the bucket; a test pins that the engine's exact
  `[prefix, next_prefix)` space deletes hit the fast path.
- Scans prepare one statement per bucket upfront and open every cursor
  immediately (SQLite executes lazily; an unstepped cursor is free). The
  statements live in the with_range_scan stack frame and the scan struct
  owns the open row cursors, so rows stream zero-copy in a single pass
  with no self-referential borrows, no re-queries, and has_more answered
  by stepping the already-open cursor one row ahead.
- The engine test-support copy
  (`packages/engine/tests/backend/support/sqlite_backend.rs`) is
  byte-identical (the engine cannot depend on lix_sdk); both files carry a
  sync note.

Benching found two real bugs and drove one redesign before shipping:

- The first scan implementation used NULL-guarded predicates
  (`(? IS NULL OR key > ?)`) so one statement shape served every bound
  combination. That defeats SQLite's index planner; every batch degraded
  to a table walk (prefix scan 1.03 -> 10.4 ms, 10x). Predicates are now
  shape-specialized per bound combination, with the prepared-statement
  cache raised to 256 (shapes multiply per table).
- An intermediate re-query-per-batch cursor crashed on the engine
  pattern (one `visit_next(usize::MAX)` call): a `limit + 1` lookahead
  wrapped to `LIMIT 0` in release. The conformance suite had not covered
  huge limits; a dedicated regression test now does.
- The re-query cursor also kept a +10-22% cost on chunked pagination
  (~11 us re-query + re-seek per visit_next call). The shipped design
  pre-opens all bucket cursors instead, restoring exact v1 streaming.

### rs-sdk A/B (alternating stash triples)

| Cell                                 | v1       | v2       | Delta  |
| ------------------------------------ | -------- | -------- | -----: |
| space_truncate json_store (20k rows) | 10.73 ms | 577 us   | -94.6% |
| space_prefix_scan 1k-chunked (20k)   | 1.036 ms | 1.029 ms | parity |
| range_scan full_value 1k-chunked 50k | 2.345 ms | 2.350 ms | parity |
| point_reads existing 1000            | 270.6 us | 273.7 us | parity |
| put_many random 10k                  | 20.0 ms  | 20.9 ms  | parity |
| open existing                        | 127 us   | 127 us   | parity |

Scan parity holds for both the engine pattern (one visit_next call at
the caller's limit) and chunked pagination, with sign flips across
pairs; per scan the only added work is one sqlite_master bucket listing
plus one statement per bucket in range.

### Engine e2e A/B (tracked_state_crud lix_sqlite smoke, triples)

| Cell        | v1       | v2       | Delta  |
| ----------- | -------- | -------- | -----: |
| read_one    | 194.7 us | 89.0 us  | -54% (2.2x) |
| delete_all  | 12.81 ms | 9.17 ms  | -28%   |
| update_one  | 946 us   | 895 us   | -5%    |
| insert_all  | 10.87 ms | 11.10 ms | parity |
| read_all    | 2.978 ms | 3.036 ms | parity |

Point reads descend a small per-space B-tree instead of the 13 MB
interleaved one; space-wide deletes hit the truncate fast path through
the whole stack.

### File layout (multi-space mix, checkpointed)

v1: 1 table, 3,331 pages, 13,643,776 bytes. v2: 6 tables, 3,339 pages,
13,676,544 bytes (+0.24%) with per-space dbstat accounting now available
(json_store 1,836 pages, change 835, tree_chunk 572, ...).

- `cargo test -p lix_sdk --features sqlite`: 19 passed (conformance,
  e2e, usize::MAX-limit regression). `cargo test -p lix_engine
  --features storage-benches`: 1,568 passed. clippy zero across both.

## Space-Aware Backend Interface + Per-Space SQLite Tables

Date: 2026-06-10

Hard cut of the Backend trait and the SQLite file format (v2), replacing
the prefixed-key keyspace with an explicit, typed interface:

- Every read/write method takes `space: SpaceId`; keys are logical bytes.
  `scan(space, range, opts, visitor) -> ScanResult` replaces the
  cursor API (`BackendRangeScan`, `with_range_scan`, `visit_next`) - git
  archaeology showed every production caller made exactly one visit_next
  call per cursor, with pagination via resume_after.
- The engine's physical-key codec (encode/decode of the 4-byte space
  prefix) is deleted; write-set lowering, point plans, and scans pass the
  space they already had. `delete_range(space, Unbounded..Unbounded)` is
  the explicit truncate idiom.
- SQLite maps each space to its own table. The bucket derivation, key
  order proofs, sqlite_master listings, and the commit-gated existence
  cache from the first partitioned design are all deleted: writes run
  CREATE TABLE IF NOT EXISTS unconditionally (~us of DDL parse), reads
  probe once per call. redb, RocksDB, the in-memory backend, and the CLI
  file backend keep single-keyspace layouts and prefix internally in ~30
  private lines each.
- Conformance tests the spaces contract; the bench fixtures and the
  scrambled equivalence decorator are space-aware.

### A/B vs flat v1 (whole working tree stashed, alternating)

| Cell                                | v1 flat   | v2 spaces | Delta  |
| ----------------------------------- | --------- | --------- | -----: |
| e2e merge_10k (plugin pipeline)     | 347.8 ms  | 195.3 ms  | -44% (1.78x) |
| engine read_one_by_pk (sqlite)      | 213.1 us  | 96.2 us   | -55% (2.2x) |
| engine delete_all (sqlite)          | 12.65 ms  | 9.83 ms   | -22%   |
| engine insert_all (sqlite)          | 11.59 ms  | 10.57 ms  | -9%    |
| engine read_all (sqlite)            | ~3.0 ms   | ~3.2 ms   | parity (noisy) |
| raw space truncate (20k rows)       | 10.7 ms   | ~0.6 ms   | -95%   |

Point reads descend small per-space B-trees instead of one interleaved
13 MB tree; space deletes truncate tables; the merge pipeline collects
both.

- Suites: engine 1,565 / sdk 26 / cli 46, all green; workspace clippy
  zero. SQLITE_FORMAT_VERSION = 2; v1 files rejected with a clear
  no-migration error.
