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
