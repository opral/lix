# Optimization Log 6: JSON Pointer CRUD

Goal: make typed-table JSON pointer CRUD fast enough that Lix behaves like a
normal embedded CRUD database for this workload.

Target workload:

```text
table: json_pointer
columns: path TEXT primary-key shape, value JSON
fixture: packages/engine/benches/fixtures/pnpm-lock.fixture.json
rows: 1000 smoke rows from all JSON nodes, including containers
query surface:
  INSERT INTO json_pointer (path, value)
  SELECT path, value FROM json_pointer
  SELECT path, value FROM json_pointer WHERE path = ?
  UPDATE json_pointer SET value = ...
  DELETE FROM json_pointer
```

No `lix_file` row is required for this scorecard. This is intentionally the
plain CRUD path through a registered typed schema.

## Success Criteria

Speed:

```text
Lix with SQLite backend: <= 2.0x raw SQLite median
Lix with RocksDB backend: <= 1.8x raw SQLite median
```

Storage:

```text
Lix with SQLite backend: <= 2.0x raw SQLite bytes on disk
Lix with RocksDB backend: <= 2.0x raw SQLite bytes on disk
```

The raw SQLite baseline uses the same fixture rows and an equivalent
`json_pointer(path TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID` table in a temp
file.

## Baseline

Commands:

```sh
cargo bench -p lix_engine --bench json_pointer_crud --features storage-benches
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

Current smoke speed baseline:

| operation               | raw SQLite median | Lix SQLite target | Lix SQLite current | status | Lix RocksDB target | Lix RocksDB current | status |
| ----------------------- | ----------------: | ----------------: | -----------------: | ------ | -----------------: | ------------------: | ------ |
| `insert_all_nodes`      |          3.753 ms |       <= 7.506 ms |          374.32 ms | fail   |        <= 6.755 ms |           319.73 ms | fail   |
| `select_all_path_value` |          1.197 ms |       <= 2.394 ms |           14.95 ms | fail   |        <= 2.155 ms |            11.51 ms | fail   |
| `select_by_pk_path`     |          1.517 ms |       <= 3.034 ms |             3.51 s | fail   |        <= 2.731 ms |              3.34 s | fail   |
| `update_all_values`     |          1.414 ms |       <= 2.828 ms |           35.59 ms | fail   |        <= 2.545 ms |            24.18 ms | fail   |
| `delete_all_nodes`      |          1.178 ms |       <= 2.356 ms |             2.64 s | fail   |        <= 2.120 ms |              1.83 s | fail   |

Current 1000-row storage baseline:

| backend     | bytes on disk |       target | status    |
| ----------- | ------------: | -----------: | --------- |
| raw SQLite  |     1,692,456 |    reference | reference |
| Lix SQLite  |     1,075,136 | <= 3,384,912 | pass      |
| Lix RocksDB |       993,888 | <= 3,384,912 | pass      |

Baseline interpretation:

```text
Storage already passes comfortably for both Lix backends.

CRUD speed does not pass any target yet. The loudest bottlenecks are repeated
primary-key lookups and bulk delete, both measured in seconds for 1000 rows.
Insert is also far outside the target, while full scan and bulk update are
closer but still roughly 10-25x over the raw SQLite reference.
```

## Optimization Order

Work the scorecard in this order:

1. `select_by_pk_path`
2. `delete_all_nodes`
3. `insert_all_nodes`
4. `update_all_values`
5. `select_all_path_value`

Rationale:

```text
Primary-key reads reveal per-query planning/provider overhead. Bulk delete
reveals write/delete transaction machinery. Insert is the main mutation hot
path. Update and full scan are still failing, but their current numbers are
closer to the target than PK reads and delete.
```

## Entry Template

Use one entry per kept optimization.

```text
## Optimization N: <short name>

Commit: <hash> or uncommitted on <hash>

Target operation:
  insert_all_nodes | select_all_path_value | select_by_pk_path |
  update_all_values | delete_all_nodes | storage

Change:
  What changed?
  Why should this reduce CRUD overhead?
  What invariant is preserved?

Results:
  Include raw SQLite, Lix SQLite, and Lix RocksDB rows for every impacted CRUD
  operation. Include 1000-row storage if the change can affect bytes on disk.

Verification:
  Exact commands run.
```
