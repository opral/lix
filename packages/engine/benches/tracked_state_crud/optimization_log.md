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
closure, then borrows that fixture with `iter_batched_ref` so fixture teardown is
excluded from the timed window too.

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
| SQLite  |    4.21 ms |   656 us |         510 us |        1.63 ms |    3.80 ms |     946 us |    1.96 ms |     663 us |
| RocksDB |     537 us |   176 us |        7.27 us |         532 us |     586 us |    18.0 us |    31.8 us |    10.5 us |
| redb    |    7.25 ms |   141 us |        11.3 us |         337 us |    8.17 ms |    4.16 ms |    4.13 ms |    3.97 ms |

### Physical API Layer

Currently mirrors direct KV layout.

| Backend | Insert all | Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| ------- | ---------: | -------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| SQLite  |    3.69 ms |   672 us |         450 us |        1.58 ms |    3.55 ms |     795 us |    1.92 ms |     692 us |
| RocksDB |     524 us |   180 us |        8.27 us |         543 us |     556 us |    20.9 us |    11.6 us |    8.75 us |
| redb    |    7.06 ms |   124 us |        13.3 us |         337 us |    8.04 ms |    4.04 ms |    4.13 ms |    3.99 ms |

### SQL Session

| Backend   | Insert all | Read all | Read one by PK | Read all by PK | Update all | Update one | Delete all | Delete one |
| --------- | ---------: | -------: | -------------: | -------------: | ---------: | ---------: | ---------: | ---------: |
| in-memory |   73.88 ms | 22.69 ms |        6.15 ms |       29.78 ms |   excluded |   excluded |  104.21 ms |   83.30 ms |

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
