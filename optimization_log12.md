# Optimization Log 12: Changelog Physical Layout Cutover Baseline

Goal: measure the current CRUD/version/merge shape before the hard cut to the
new changelog physical layout:

```text
logical:
  Commit / CommitBody / Change

physical:
  Segment / SegmentCommit / SegmentChange

publication:
  CommitVisibility

derived:
  tracked_state root and changelog by_* indexes
```

This log is intentionally forked from the log8 JSON-pointer CRUD benchmark
surface. The purpose is to isolate log12 evidence from log8 while keeping the
same product-like workload: JSON pointer rows, CRUD, version creation, fast
forward merge, divergent merge, and storage-size accounting.

## Benchmark Surface

Benchmark target:

```text
packages/engine/benches/log12_physical_layout/main.rs
```

Storage guardrail:

```text
packages/engine/tests/log12_physical_layout_storage.rs
```

Commands:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench log12_physical_layout
cargo test --manifest-path packages/engine/Cargo.toml --features storage-benches --test log12_physical_layout_storage -- --ignored --nocapture --test-threads=1
```

Fixture:

```text
source: packages/engine/benches/fixtures/pnpm-lock.fixture.json
shape: flattened JSON pointer rows
sizes:
  baseline = 100 rows
  smoke    = 1,000 rows
```

Log12-only fixture note: the copied benchmark excludes the root JSON pointer
row with empty path. Lix's JSON pointer schema derives identity from `/path`,
and an empty primary-key value is rejected. The original log8 benchmark files
were left untouched.

## 10k Run Status

The full run was stopped before a complete 10k scoreboard. It completed raw
SQLite, raw storage SQLite, raw storage RocksDB, and the non-merge Lix SQLite
10k rows, then stopped making completed Criterion estimates during the first
10k Lix merge row.

Observed before stopping:

```text
elapsed: about 13 minutes
bench process CPU: about 98%
completed 10k scale rows: 41
stalled row: likely lix_sqlite/scale/merge_version_fast_forward_10pct_updates/10k
```

Treat this as a baseline signal: 10k merge is currently too expensive or has a
superlinear cliff. The 100 and 1k rows below are the stable scoreboard for the
hard cut.

## CRUD / Version / Merge Scoreboard

Times are Criterion median estimates in milliseconds.

### Raw SQLite

| row                   |   100 |    1k |
| --------------------- | ----: | ----: |
| insert_all_rows       | 0.467 | 1.681 |
| select_all_path_value | 0.357 | 0.788 |
| select_one_by_pk      | 0.360 | 0.770 |
| update_all_values     | 0.410 | 1.042 |
| update_one_by_pk      | 0.405 | 0.749 |
| delete_all_rows       | 0.389 | 0.766 |
| delete_one_by_pk      | 0.365 | 0.769 |

### Lix SQLite

| row                                      |    100 |       1k |
| ---------------------------------------- | -----: | -------: |
| insert_all_rows                          |  5.951 |   42.562 |
| select_all_path_value                    |  1.790 |    6.384 |
| select_one_by_pk                         |  1.420 |    2.806 |
| update_all_values                        |  3.712 |   17.840 |
| update_one_by_pk                         |  2.759 |    5.389 |
| delete_all_rows                          |  3.733 |   18.398 |
| delete_one_by_pk                         |  2.538 |    5.584 |
| create_version                           |  2.788 |    4.985 |
| merge_version_fast_forward_10pct_updates | 28.231 |  510.000 |
| merge_version_divergent_10pct_updates    | 49.792 | 1094.568 |

### Lix RocksDB

| row                                      |    100 |      1k |
| ---------------------------------------- | -----: | ------: |
| insert_all_rows                          |  5.690 |  39.631 |
| select_all_path_value                    |  1.603 |   5.430 |
| select_one_by_pk                         |  1.317 |   2.001 |
| update_all_values                        |  3.600 |  13.729 |
| update_one_by_pk                         |  2.938 |   4.471 |
| delete_all_rows                          |  3.576 |  15.470 |
| delete_one_by_pk                         |  2.584 |   4.374 |
| create_version                           |  2.678 |   3.825 |
| merge_version_fast_forward_10pct_updates | 26.902 | 472.740 |
| merge_version_divergent_10pct_updates    | 49.024 | 984.456 |

## Raw Storage Scoreboard

Times are Criterion median estimates in milliseconds. These rows isolate the
tracked-state physical layer from SQL/provider/session overhead.

### Raw Storage SQLite

| row                              |   100 |    1k |
| -------------------------------- | ----: | ----: |
| write_root_all_rows              | 1.047 | 3.320 |
| get_many_exact_keys              | 0.502 | 2.264 |
| get_many_missing_keys            | 0.455 | 1.535 |
| scan_keys_only                   | 0.441 | 1.323 |
| scan_headers_only                | 0.452 | 1.352 |
| scan_full_rows                   | 0.504 | 1.732 |
| prefix_scan_schema               | 0.486 | 1.696 |
| prefix_scan_schema_file_null     | 0.466 | 1.721 |
| write_delta_10pct_updates        | 0.472 | 1.469 |
| write_tombstone_10pct_deletes    | 0.457 | 1.392 |
| changed_keys_update_10pct        | 0.538 | 1.767 |
| changed_keys_delta_chain_10x1pct | 0.562 | 1.861 |
| materialize_delta_chain_10x1pct  | 1.017 | 3.552 |

### Raw Storage RocksDB

| row                              |   100 |    1k |
| -------------------------------- | ----: | ----: |
| write_root_all_rows              | 2.333 | 3.668 |
| get_many_exact_keys              | 0.507 | 1.908 |
| get_many_missing_keys            | 0.403 | 1.038 |
| scan_keys_only                   | 0.359 | 0.845 |
| scan_headers_only                | 0.365 | 0.906 |
| scan_full_rows                   | 0.392 | 1.215 |
| prefix_scan_schema               | 0.396 | 1.232 |
| prefix_scan_schema_file_null     | 0.416 | 1.272 |
| write_delta_10pct_updates        | 0.385 | 0.935 |
| write_tombstone_10pct_deletes    | 0.379 | 0.890 |
| changed_keys_update_10pct        | 0.398 | 1.071 |
| changed_keys_delta_chain_10x1pct | 0.456 | 1.037 |
| materialize_delta_chain_10x1pct  | 0.519 | 2.117 |

## Storage Scoreboard

Bytes on disk from `log12_physical_layout_storage_accounting`.

| backend / state                        | rows |   bytes | bytes/row |
| -------------------------------------- | ---: | ------: | --------: |
| raw SQLite / inserted                  |  100 |  139632 |    1396.3 |
| Lix SQLite / inserted                  |  100 |  148136 |    1481.4 |
| Lix SQLite / after create_version      |  100 |  156376 |    1563.8 |
| Lix SQLite / after fast-forward merge  |  100 |  321176 |    3211.8 |
| Lix SQLite / after divergent merge     |  100 |  856776 |    8567.8 |
| Lix RocksDB / inserted                 |  100 |   98161 |     981.6 |
| Lix RocksDB / after create_version     |  100 |   99617 |     996.2 |
| Lix RocksDB / after fast-forward merge |  100 |  112067 |    1120.7 |
| Lix RocksDB / after divergent merge    |  100 |  141373 |    1413.7 |
| raw SQLite / inserted                  | 1000 |  903720 |     903.7 |
| Lix SQLite / inserted                  | 1000 |  547776 |     547.8 |
| Lix SQLite / after create_version      | 1000 |  560136 |     560.1 |
| Lix SQLite / after fast-forward merge  | 1000 | 2970336 |    2970.3 |
| Lix SQLite / after divergent merge     | 1000 | 4791920 |    4791.9 |
| Lix RocksDB / inserted                 | 1000 |  486567 |     486.6 |
| Lix RocksDB / after create_version     | 1000 |  488023 |     488.0 |
| Lix RocksDB / after fast-forward merge | 1000 |  604693 |     604.7 |
| Lix RocksDB / after divergent merge    | 1000 |  882308 |     882.3 |

