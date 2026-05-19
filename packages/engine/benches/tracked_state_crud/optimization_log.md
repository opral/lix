# Tracked CRUD Optimization Log

## Baseline: 2026-05-19 after rebase onto `origin/main`

Command used for the regular scorecard:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
```

The regular scorecard is intentionally the 1k smoke workload. The full 10k
matrix is too slow for iteration while the SQL path is unoptimized; use targeted
10k filters for headline checks such as `insert_all_rows/10k`.

Workload:

- Source fixture: `packages/engine/benches/fixtures/pnpm-lock.fixture.json`
- Shape: flattened JSON-pointer rows
- Smoke size: 1,000 rows
- Criterion: 10 samples, 250 ms warmup, 1 s measurement for smoke groups

Notes:

- `physical_api` currently delegates to the same direct KV implementation as
  `kv_layout`. Keep the layer name stable so it can be switched to real
  changelog/tracked-state APIs later.
- `sql_session` runs on `InMemoryStorageBackend`; the copied SQLite/RocksDB/redb
  backend support modules do not satisfy the SQL session read bounds.
- SQL update benches are gated behind `LIX_TRACKED_STATE_CRUD_SQL_UPDATE=1`.
  The supported per-row `UPDATE ... WHERE path = ...` shape is functionally
  valid but too slow for the default scorecard; Criterion estimated about
  25 minutes for `sql_session/update_all_rows/1k`.

## 1k Smoke Scorecard

Times below use Criterion point estimates from the rerun after the rebase.

### Direct KV Layout

| Backend | Insert all | Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    3.91 ms |  4.39 ms |        4.27 ms |        5.42 ms |    6.40 ms |    4.26 ms |    4.84 ms |    4.16 ms |
| RocksDB |    2.70 ms |  3.12 ms |        3.08 ms |        3.24 ms |    3.25 ms |    2.93 ms |    2.79 ms |    2.68 ms |
| redb    |   44.74 ms | 44.56 ms |       43.11 ms |       43.42 ms |   51.45 ms |   54.59 ms |   49.76 ms |   55.17 ms |

### Physical API Layer

Currently mirrors direct KV layout.

| Backend | Insert all | Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    3.78 ms |  4.61 ms |        4.24 ms |        5.20 ms |    6.17 ms |    4.35 ms |    4.88 ms |    4.10 ms |
| RocksDB |    2.98 ms |  3.05 ms |        2.71 ms |        3.34 ms |    4.31 ms |    3.47 ms |    3.06 ms |    2.80 ms |
| redb    |   49.06 ms | 45.98 ms |       44.58 ms |       47.09 ms |   52.07 ms |   48.43 ms |   49.78 ms |   50.40 ms |

### SQL Session

| Backend   | Insert all |  Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| --------- | ---------: | --------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| in-memory |   80.01 ms | 106.37 ms |       87.60 ms |      108.99 ms |   excluded |   excluded |  180.89 ms |  164.91 ms |

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
