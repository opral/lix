# Optimization Log 7: Physical Layout for CRUD + Branch/Merge

Goal: find the optimal physical storage layout for Lix's core tracked-state
workflow as quickly as possible.

This log uses JSON-pointer shaped data as the shared workload because it looks
like real `plugin-json-v2` output: many small entities keyed by JSON pointer,
including container nodes and leaves.

## Core Workflow

The layout must prove itself across the operations Lix users actually compose:

```text
CRUD:
  INSERT INTO json_pointer (path, value)
  SELECT path, value FROM json_pointer
  SELECT path, value FROM json_pointer WHERE path = ?
  UPDATE json_pointer SET value = ...
  DELETE FROM json_pointer

Branching:
  create_version over an existing tracked state

Merge / diff:
  merge_version after source-only edits
  merge_version after divergent target/source edits

Storage:
  bytes on disk after insert
  bytes on disk after create_version
  bytes on disk after fast-forward merge
  bytes on disk after divergent merge
```

The purpose is not to win a single CRUD microbenchmark. The purpose is to learn
which physical layout lets Lix cheaply answer the three core tracked-state
questions:

```text
What exists at this version?
What changed between these versions?
What is the current value for these exact entity identities?
```

## Fixture

```text
fixture: packages/engine/benches/fixtures/pnpm-lock.fixture.json
source: checked-in JSON conversion of the repo pnpm-lock.yaml
rows: all JSON nodes flattened to json_pointer rows
smoke: first 1000 rows
scale: first 10000 rows
table: json_pointer
identity: path
value: JSON node value
file_id: NULL
```

The fixture intentionally does not require a real `lix_file` row. The benchmark
registers the `plugin-json-v2` `json_pointer` schema and treats Lix as the
normal typed-table CRUD and versioned-state database.

## Scorecard

Speed is measured for both backends:

```text
Lix with SQLite backend
Lix with RocksDB backend
```

Raw SQLite remains a reference for simple CRUD machine limits, but it is not
the goal. Large gaps must be explained by Lix semantics or by an intentional
layout tradeoff. Gaps caused by accidental scans, repeated delta decoding,
unbatched point reads, or avoidable write amplification are optimization
targets.

Storage is measured on disk for the same 1000-row fixture and workflow stages.
The initial guardrail is that Lix should stay compact while adding branching
and merge metadata; storage growth should be structural and explainable.

## Current Benchmark Surface

Command:

```sh
cargo bench -p lix_engine --bench json_pointer_crud --features storage-benches
```

Benchmark groups:

```text
json_pointer_crud/raw_sqlite/baseline
json_pointer_crud/raw_sqlite/smoke
json_pointer_crud/raw_sqlite/scale
json_pointer_crud/raw_storage_sqlite/baseline
json_pointer_crud/raw_storage_sqlite/smoke
json_pointer_crud/raw_storage_sqlite/scale
json_pointer_crud/raw_storage_rocksdb/baseline
json_pointer_crud/raw_storage_rocksdb/smoke
json_pointer_crud/raw_storage_rocksdb/scale
json_pointer_crud/lix_sqlite/baseline
json_pointer_crud/lix_sqlite/smoke
json_pointer_crud/lix_sqlite/scale
json_pointer_crud/lix_rocksdb/baseline
json_pointer_crud/lix_rocksdb/smoke
json_pointer_crud/lix_rocksdb/scale
```

Raw Storage API timings:

```text
write_root_all_rows/{100,1k,10k}
get_many_exact_keys/{100,1k,10k}
get_many_missing_keys/{100,1k,10k}
exists_many_exact_keys/{100,1k,10k}
scan_keys_only/{100,1k,10k}
scan_headers_only/{100,1k,10k}
scan_full_rows/{100,1k,10k}
prefix_scan_schema/{100,1k,10k}
prefix_scan_schema_file_null/{100,1k,10k}
write_delta_10pct_updates/{100,1k,10k}
write_tombstone_10pct_deletes/{100,1k,10k}
changed_keys_update_10pct/{100,1k,10k}
changed_keys_delta_chain_10x1pct/{100,1k,10k}
materialize_delta_chain_10x1pct/{100,1k,10k}
```

E2E workflow timings:

```text
insert_all_rows/{100,1k,10k}
select_all_path_value/{100,1k,10k}
select_one_by_pk/{100,1k,10k}
update_all_values/{100,1k,10k}
update_one_by_pk/{100,1k,10k}
delete_all_rows/{100,1k,10k}
delete_one_by_pk/{100,1k,10k}
create_version/{100,1k,10k}
merge_version_fast_forward_10pct_updates/{100,1k,10k}
merge_version_divergent_10pct_updates/{100,1k,10k}
```

Storage command:

```sh
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

Storage rows:

```text
raw SQLite inserted
Lix SQLite / inserted
Lix SQLite / after create_version
Lix SQLite / after fast-forward merge
Lix SQLite / after divergent merge
Lix RocksDB / inserted
Lix RocksDB / after create_version
Lix RocksDB / after fast-forward merge
Lix RocksDB / after divergent merge
```

## First Optimization Axis

Optimize exact-key and changed-key access through live/tracked state.

Rationale:

```text
CRUD insert needs committed identity checks.
SELECT ... WHERE path = ? needs exact-key lookup.
UPDATE and DELETE need current-row lookup by identity.
create_version should stay bounded over large tracked states.
merge_version needs changed-key discovery, not full-state hydration.
```

The latest insert profile showed the hot path dominated by validation loading
committed identity rows through scan/delta materialization:

```text
validate_prepared_writes
  -> load_committed_constraint_row
  -> scan_committed_constraint_rows
  -> TrackedStateStoreReader::scan_rows_at_commit
  -> delta_commit_ids_since_projection_root
  -> load_delta_pack
  -> decode_delta_pack
```

That makes the first physical-layout question concrete:

```text
Can the storage layout and reader APIs answer batched exact-key lookups and
changed-key queries without broad scans or repeated delta-pack decoding?
```

## Baseline: 2026-05-10

Commands:

```sh
cargo bench -p lix_engine --bench json_pointer_crud --features storage-benches -- 'json_pointer_crud/raw_storage_sqlite/baseline|json_pointer_crud/raw_storage_rocksdb/baseline|json_pointer_crud/lix_sqlite/baseline|json_pointer_crud/lix_rocksdb/baseline'
cargo bench -p lix_engine --bench json_pointer_crud --features storage-benches -- 'json_pointer_crud/raw_sqlite/baseline|json_pointer_crud/raw_sqlite/smoke|json_pointer_crud/raw_storage_sqlite/smoke|json_pointer_crud/raw_storage_rocksdb/smoke|json_pointer_crud/lix_sqlite/smoke|json_pointer_crud/lix_rocksdb/smoke'
cargo test -p lix_engine --features storage-benches --test json_pointer_crud_storage -- --ignored --nocapture --test-threads=1
```

Raw Storage API scoreboard:

| operation                          | SQLite 100 | SQLite 1k | SQLite x | RocksDB 100 | RocksDB 1k | RocksDB x |
| ---------------------------------- | ---------: | --------: | -------: | ----------: | ---------: | --------: |
| `write_root_all_rows`              |  3.1846 ms | 8.1773 ms |    2.57x |   3.8257 ms |  6.1633 ms |     1.61x |
| `get_many_exact_keys`              |  1.5000 ms | 4.6368 ms |    3.09x |   1.1740 ms |  3.5542 ms |     3.03x |
| `get_many_missing_keys`            |  0.8035 ms | 2.5875 ms |    3.22x |   0.7961 ms |  1.5991 ms |     2.01x |
| `exists_many_exact_keys`           |  1.4055 ms | 6.0771 ms |    4.32x |   1.2785 ms |  4.0307 ms |     3.15x |
| `scan_keys_only`                   |  0.8837 ms | 3.9795 ms |    4.50x |   0.6067 ms |  2.0074 ms |     3.31x |
| `scan_headers_only`                |  0.9404 ms | 3.3325 ms |    3.54x |   0.6108 ms |  2.0698 ms |     3.39x |
| `scan_full_rows`                   |  1.4135 ms | 5.9223 ms |    4.19x |   1.1202 ms |  3.3596 ms |     3.00x |
| `prefix_scan_schema`               |  1.3817 ms | 4.9885 ms |    3.61x |   1.0670 ms |  3.3352 ms |     3.13x |
| `prefix_scan_schema_file_null`     |  1.4228 ms | 4.6936 ms |    3.30x |   1.0670 ms |  3.6555 ms |     3.43x |
| `write_delta_10pct_updates`        |  0.9485 ms | 3.1167 ms |    3.29x |   0.5760 ms |  1.5234 ms |     2.64x |
| `write_tombstone_10pct_deletes`    |  0.9206 ms | 2.8465 ms |    3.09x |   0.5681 ms |  1.4518 ms |     2.55x |
| `changed_keys_update_10pct`        |  2.6219 ms | 72.513 ms |   27.66x |   2.1347 ms |  68.299 ms |    32.00x |
| `changed_keys_delta_chain_10x1pct` |  1.5310 ms | 10.956 ms |    7.16x |   1.2778 ms |  8.9643 ms |     7.02x |
| `materialize_delta_chain_10x1pct`  |  1.1405 ms | 5.7006 ms |    5.00x |   0.9339 ms |  3.1625 ms |     3.39x |

`exists_many_exact_keys` currently uses the tracked-state row-loading path as
the semantic equivalent. It is a named scoreboard slot for a future lighter
exists-only primitive.

E2E workflow scoreboard:

| axis         | operation                                  | raw SQLite 100 | raw SQLite 1k | raw x | Lix SQLite 100 | Lix SQLite 1k | Lix SQLite x | Lix RocksDB 100 | Lix RocksDB 1k | Lix RocksDB x |
| ------------ | ------------------------------------------ | -------------: | ------------: | ----: | -------------: | ------------: | -----------: | --------------: | -------------: | ------------: |
| CRUD         | `insert_all_rows`                          |      1.4715 ms |     2.5578 ms | 1.74x |      21.690 ms |     382.34 ms |       17.63x |       19.807 ms |      317.34 ms |        16.02x |
| CRUD         | `select_all_path_value`                    |      0.8791 ms |     1.2311 ms | 1.40x |      5.8882 ms |     13.336 ms |        2.26x |       5.5689 ms |      11.019 ms |         1.98x |
| CRUD         | `select_one_by_pk`                         |      0.8001 ms |     1.1339 ms | 1.42x |      2.0720 ms |     6.1576 ms |        2.97x |       2.0085 ms |      3.8542 ms |         1.92x |
| CRUD         | `update_all_values`                        |      0.8417 ms |     1.4807 ms | 1.76x |      9.2526 ms |     30.266 ms |        3.27x |       8.2602 ms |      22.054 ms |         2.67x |
| CRUD         | `update_one_by_pk`                         |      0.8527 ms |     1.2591 ms | 1.48x |      4.4169 ms |     10.040 ms |        2.27x |       3.6020 ms |      7.3052 ms |         2.03x |
| CRUD         | `delete_all_rows`                          |      0.9204 ms |     1.2384 ms | 1.35x |      40.927 ms |      2.4630 s |       60.18x |       38.043 ms |       1.7949 s |        47.18x |
| CRUD         | `delete_one_by_pk`                         |      0.8174 ms |     1.2215 ms | 1.49x |      5.6983 ms |     12.400 ms |        2.18x |       4.3247 ms |      8.9218 ms |         2.06x |
| Branch       | `create_version`                           |            n/a |           n/a |   n/a |      4.0152 ms |     8.0948 ms |        2.02x |       3.8455 ms |      6.1184 ms |         1.59x |
| Merge / diff | `merge_version_fast_forward_10pct_updates` |            n/a |           n/a |   n/a |      45.680 ms |     995.44 ms |       21.79x |       44.270 ms |      900.68 ms |        20.35x |
| Merge / diff | `merge_version_divergent_10pct_updates`    |            n/a |           n/a |   n/a |      77.602 ms |      2.0777 s |       26.77x |       81.869 ms |       1.9656 s |        24.01x |

`raw SQLite reference` applies only to plain CRUD over the equivalent
`json_pointer(path TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID` table. Branch
and merge are Lix semantic operations, so they have no raw SQLite equivalent in
this table.

Storage scoreboard:

| backend / workflow                     | 100 bytes | 100 bytes/row |  1k bytes | 1k bytes/row | bytes x |
| -------------------------------------- | --------: | ------------: | --------: | -----------: | ------: |
| raw SQLite / inserted                  |   936,584 |       9,365.8 | 1,692,456 |      1,692.5 |   1.81x |
| Lix SQLite / inserted                  |   337,656 |       3,376.6 | 1,075,136 |      1,075.1 |   3.18x |
| Lix SQLite / after create_version      |   345,896 |       3,459.0 | 1,087,496 |      1,087.5 |   3.14x |
| Lix SQLite / after fast-forward merge  |   588,976 |       5,889.8 | 5,287,488 |      5,287.5 |   8.98x |
| Lix SQLite / after divergent merge     | 1,268,776 |      12,687.8 | 5,615,168 |      5,615.2 |   4.43x |
| Lix RocksDB / inserted                 |   280,077 |       2,800.8 |   993,888 |        993.9 |   3.55x |
| Lix RocksDB / after create_version     |   281,943 |       2,819.4 |   995,754 |        995.8 |   3.53x |
| Lix RocksDB / after fast-forward merge |   298,593 |       2,985.9 | 1,160,310 |      1,160.3 |   3.89x |
| Lix RocksDB / after divergent merge    |   337,030 |       3,370.3 | 1,528,244 |      1,528.2 |   4.53x |

Baseline interpretation:

```text
The Raw Storage API rows now separate layout capability from E2E machinery.
Direct tracked-state `get_many` and full scan are low single-digit
milliseconds, while changed-key discovery for 10% updates scales far worse than
the scan/read primitives.

The E2E CRUD rows show the current pressure from the typed-table surface:
inserts are hundreds of milliseconds at 1000 rows and bulk deletes are seconds,
with much steeper 100-to-1000 growth than raw SQLite. Single-row PK operations
are now measured as one row selected, updated, or deleted from a populated
table.

create_version is already bounded enough to use as a guardrail, but merge/diff
is also seconds for only 10% changed rows over a 1000-row JSON-pointer state.

Storage after plain insert is compact for both backends. create_version adds
very little storage, which matches the desired branch shape. SQLite-backed Lix
grows sharply after fast-forward/divergent merge, while RocksDB grows much more
gradually. That backend split is a useful signal for the physical-layout work:
merge/diff layout and checkpoint/packing policy need to be evaluated across
both backends, not just through CRUD timings.
```

## Entry Template

Use one entry per kept layout or access-path change.

```text
## Optimization N: <short name>

Commit: <hash> or uncommitted on <hash>

Hypothesis:
  What physical layout or access-path change is being tested?

Raw Storage API scoreboard:
  Include the impacted raw storage rows for SQLite and RocksDB.

E2E Workflow scoreboard:
  Include the impacted CRUD, create_version, and merge_version rows.
  Include raw SQLite reference where the operation has one.

Storage scoreboard:
  Include workflow storage rows for raw SQLite, Lix SQLite, and Lix RocksDB.

Decision:
  Keep, revert, or follow-up.
```
