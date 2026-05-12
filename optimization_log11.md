# Optimization Log 11

Commit store + tracked state physical layout.

## Files

```text
physical_layout.md
physical_layout_reference.md
packages/engine/tests/log11_physical_tracked.rs
packages/engine/benches/log11_physical/main.rs
```

## Commands

Physical catalog:

```sh
cargo test --manifest-path packages/engine/Cargo.toml --test log11_physical_tracked -- --ignored --nocapture
```

Runtime baseline:

```sh
cargo bench -p lix_engine --features storage-benches --bench log11_physical -- smoke
cargo bench -p lix_engine --features storage-benches --bench log11_physical -- scale
```

Storage guardrail:

```sh
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

## Benchmark Shape

```text
fixture: packages/engine/benches/fixtures/pnpm-lock.fixture.json
schema:  JSON pointer rows
smoke:   1,000 rows
scale:   10,000 rows
change:  10% rows for update/delete
backends: raw SQLite, Lix SQLite, Lix RocksDB
budget:  Lix <= 1.5x raw SQLite for equivalent rows
```

Rows:

```text
write_root_all_rows
write_delta_10pct_updates
write_tombstone_10pct_deletes
get_many_exact_keys
get_many_missing_keys
exists_many_exact_keys
scan_keys_only
scan_headers_only
scan_full_rows
prefix_scan_schema
prefix_scan_schema_file_null
changed_keys_update_10pct
changed_keys_delta_chain_10x1pct
materialize_delta_chain_10x1pct
```

## Baseline Dimensions

Optimize across these scorecards. A change should say which scorecard it moves.

```text
runtime
  write
  exact-read
  scan/projection
  diff/changed-key
  delta-chain materialization

physical catalog
  entries written per logical workload
  bytes written per logical workload
  namespace fanout
  json_store.pack vs json_store.json placement

storage size
  bytes on disk after inserted/version/merge workflows

layering
  commit_store should not depend on tracked_state
  tracked_state may derive from commit_store facts
```

## Physical Catalog Baseline

Run:

```sh
cargo test --manifest-path packages/engine/Cargo.toml --test log11_physical_tracked -- --ignored --nocapture
```

Observed tracked namespaces:

```text
commit_store.commit
tracked_state.delta_pack
json_store.pack
json_store.json
```

100 inserts in one statement:

```sql
INSERT INTO json_pointer (path, value) VALUES (... 100 rows ...)
```

| namespace                  | entries |  bytes |
| -------------------------- | ------: | -----: |
| `commit_store.commit`      |       1 |    205 |
| `tracked_state.delta_pack` |       1 | 13,287 |
| `json_store.pack`          |       1 | 34,513 |

100 updates by primary key in one statement:

```sql
UPDATE json_pointer
SET value = CASE path
  WHEN '<pk-1>' THEN lix_json(...)
  WHEN '<pk-2>' THEN lix_json(...)
  ...
END
WHERE path IN ('<pk-1>', '<pk-2>', ...)
```

| namespace                  | entries |   bytes |
| -------------------------- | ------: | ------: |
| `commit_store.commit`      |       1 |     205 |
| `tracked_state.delta_pack` |       1 |  13,318 |
| `json_store.pack`          |       1 |  20,896 |
| `json_store.json`          |       1 | 110,035 |

100 deletes by primary key in one statement:

```sql
DELETE FROM json_pointer WHERE path IN ('<pk-1>', '<pk-2>', ...)
```

| namespace                  | entries |  bytes |
| -------------------------- | ------: | -----: |
| `commit_store.commit`      |       1 |    205 |
| `tracked_state.delta_pack` |       1 | 13,187 |

Logical write amplification:

| workload            | logical shape             | commits | delta packs | JSON packs | direct JSON |
| ------------------- | ------------------------- | ------: | ----------: | ---------: | ----------: |
| 100 inserts         | one SQL statement         |       1 |           1 |          1 |           0 |
| 100 updates batched | one `CASE` + `IN` update  |       1 |           1 |          1 |           1 |
| 100 deletes batched | one `WHERE path IN (...)` |       1 |           1 |          0 |           0 |

Notes:

```text
commit_store.change_pack is not written by this tracked path.
commit_store currently falls back to tracked_state.delta_pack for authored
change-pack reads. That is a layer-boundary problem.

Per-row UPDATE/DELETE statements produce one commit and one delta pack per
statement. That is statement-level behavior, not the target physical layout
baseline.
```

## Smoke Baseline

Date: 2026-05-11

Command:

```sh
cargo bench -p lix_engine --features storage-benches --bench log11_physical -- smoke
```

1.5x budget rows:

| axis       | row                                | raw SQLite | Lix SQLite | SQLite ratio | SQLite status | Lix RocksDB | RocksDB ratio | RocksDB status |
| ---------- | ---------------------------------- | ---------: | ---------: | -----------: | ------------- | ----------: | ------------: | -------------- |
| write      | `write_root_all_rows/1k`           |  2.6236 ms |  4.4211 ms |        1.69x | fail          |   4.7627 ms |         1.82x | fail           |
| write      | `write_delta_10pct_updates/1k`     |  1.3592 ms |  1.8159 ms |        1.34x | pass          |   1.0988 ms |         0.81x | pass           |
| write      | `write_tombstone_10pct_deletes/1k` |  1.3766 ms |  1.7097 ms |        1.24x | pass          |   1.0513 ms |         0.76x | pass           |
| exact-read | `get_many_exact_keys/1k`           |  2.1494 ms |  2.8214 ms |        1.31x | pass          |   2.1793 ms |         1.01x | pass           |
| exact-read | `get_many_missing_keys/1k`         |  13.264 ms |  1.8212 ms |        0.14x | pass          |   1.1950 ms |         0.09x | pass           |
| exact-read | `exists_many_exact_keys/1k`        |  2.0692 ms |  2.8026 ms |        1.35x | pass          |   2.6106 ms |         1.26x | pass           |
| scan       | `scan_keys_only/1k`                |  1.2371 ms |  1.7849 ms |        1.44x | pass          |   1.1633 ms |         0.94x | pass           |
| scan       | `scan_headers_only/1k`             |  1.1984 ms |  1.9736 ms |        1.65x | fail          |   1.1132 ms |         0.93x | pass           |
| scan       | `scan_full_rows/1k`                |  1.3014 ms |  2.5752 ms |        1.98x | fail          |   1.8547 ms |         1.43x | pass           |
| scan       | `prefix_scan_schema/1k`            |  1.2984 ms |  2.3946 ms |        1.84x | fail          |   1.6607 ms |         1.28x | pass           |
| scan       | `prefix_scan_schema_file_null/1k`  |  1.3926 ms |  2.2678 ms |        1.63x | fail          |   1.6319 ms |         1.17x | pass           |

Rows without raw SQLite equivalent:

| row                                   | Lix SQLite | Lix RocksDB | target shape                           |
| ------------------------------------- | ---------: | ----------: | -------------------------------------- |
| `changed_keys_update_10pct/1k`        |  2.2360 ms |   1.4146 ms | scales with changed keys               |
| `changed_keys_delta_chain_10x1pct/1k` |  2.2136 ms |   1.3062 ms | scales with changed keys + chain depth |
| `materialize_delta_chain_10x1pct/1k`  |  3.7067 ms |   2.2268 ms | avoids unrelated delta-pack decoding   |

## Hotspots

Current smoke failures:

```text
1. write_root_all_rows fails on SQLite and RocksDB.
2. SQLite scan_headers_only fails.
3. SQLite scan_full_rows fails.
4. SQLite prefix_scan_schema fails.
5. SQLite prefix_scan_schema_file_null fails.
```

Current passes:

```text
1. Delta updates and tombstones are inside budget on both backends.
2. Exact reads are inside budget on both backends.
3. RocksDB scans are inside budget.
4. SQLite scan_keys_only is inside budget, but close.
```

Layering hotspot:

```text
commit_store must stop depending on tracked_state to load authored change-pack
facts. The lower layer should not reconstruct commit-store facts from a higher
tracked-state projection.
```

## Scale Baseline

Not recorded yet.

Run:

```sh
cargo bench -p lix_engine --features storage-benches --bench log11_physical -- scale
```

## Optimization Axes

```text
write
exact-read
scan/projection
diff/changed-key
delta-chain materialization
storage-size
backend API
layering/dependency direction
payload packing
```

## Entry Template

```text
## Optimization N: <name>

Axis:

Shape:

Change:

Commands:

Scorecards moved:
  runtime:
  physical catalog:
  storage size:
  layering:

Decision:
```
