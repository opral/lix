# Tracked CRUD Optimization Log

## Baseline: 2026-05-19 corrected fixture setup

Command used for the regular scorecard:

```sh
cargo bench -p lix_engine --features storage-benches --bench tracked_state_crud -- smoke
```

The regular scorecard is intentionally the 1k smoke workload. The full 10k
matrix is too slow for iteration while the SQL path is unoptimized; use targeted
10k filters for headline checks such as `insert_all_rows/10k`.

This baseline supersedes the first post-rebase scorecard. That run timed
database/session creation and `insert_all(&rows)` inside read, update, and
delete benchmarks, which made the non-insert numbers mostly setup cost. The
current harness creates either an empty or seeded fixture in Criterion's setup
closure, then times only the operation under test.

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

Times below use Criterion point estimates from the corrected fixture rerun.

### Direct KV Layout

| Backend | Insert all | Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    2.35 ms |   652 us |         507 us |        1.63 ms |    2.78 ms |     688 us |    1.22 ms |     619 us |
| RocksDB |     685 us |   375 us |         198 us |         762 us |     727 us |     200 us |     217 us |     228 us |
| redb    |   17.90 ms | 11.31 ms |       11.32 ms |       10.77 ms |   19.64 ms |   15.02 ms |   15.66 ms |   18.92 ms |

### Physical API Layer

Currently mirrors direct KV layout.

| Backend | Insert all | Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    2.37 ms |   641 us |         486 us |        1.53 ms |    2.63 ms |     685 us |    1.15 ms |     595 us |
| RocksDB |     654 us |   404 us |         202 us |         752 us |     795 us |     228 us |     219 us |     214 us |
| redb    |   18.05 ms | 11.54 ms |       11.99 ms |       12.70 ms |   19.43 ms |   14.76 ms |   14.88 ms |   18.78 ms |

### SQL Session

| Backend   | Insert all | Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| in-memory |   70.19 ms | 20.73 ms |        6.01 ms |       27.85 ms |   excluded |   excluded |   98.16 ms |   83.30 ms |

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
