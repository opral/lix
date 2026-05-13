# Optimization Log 13: Changelog Segment Optimization

Goal: optimize the new changelog physical segment layer before wiring it into
the transaction path.

Scope:

```text
changelog.segment codec
segment validation and directory/index construction
stage_segment / stage_publish_commit
visible and physical commit/change reads
mandatory index rebuild
GC mark/sweep over changelog segments
```

Out of scope for this log:

```text
tracked_state prolly root mutation/diff
transaction integration
SQL/provider routing
real fsync or object-store behavior
```

## Baseline Scoreboard

Date: 2026-05-13

Command:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml \
  --features storage-benches \
  --bench changelog_scorecard
```

Notes:

```text
CPU rows are median of 10 in-process samples.
Backend rows are single-shot smoke timings to keep this loop cheap.
Backends:
  mem_unit         in-memory ordered map
  sqlite_tempfile  SQLite temp file
  rocksdb_tempdir  RocksDB temp directory
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | baseline_ms |
| --------------------------------------- | ----------: |
| encode_segment / 1c_1000ch              |       0.163 |
| decode_segment / 1c_1000ch              |       4.000 |
| validate_segment_shape / 1c_1000ch      |       4.016 |
| build_decoded_segment_index / 1c_1000ch |       7.781 |
| build_by_change / 1c_1000ch             |       0.808 |
| build_by_change_membership / 1c_1000ch  |       0.034 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.548 |              3.069 |              4.194 |
| stage_segment / 1c_1000ch                            |       7.745 |             10.855 |             10.749 |
| stage_publish_commit / 1c_1ch                        |       0.044 |              0.049 |              0.055 |
| stage_publish_commit / 1c_100ch                      |      50.801 |             59.635 |             50.830 |
| stage_publish_commit / 1c_1000ch single-shot         |    7526.135 |           7502.516 |           7509.135 |
| load_commits_visible_batched / 1c_100ch              |       0.589 |              0.545 |              0.536 |
| load_changes_visible_batched / 1c_100ch              |      58.200 |             54.658 |             54.413 |
| load_changes_visible_batched / 1c_1000ch             |    8073.494 |           7774.836 |           7768.603 |
| load_changes_physical_scattered / 100seg_100c_1000ch |       7.427 |              7.408 |              7.432 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     491.364 |             84.243 |             67.760 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       6.282 |              7.792 |              6.586 |
| plan_gc / live_50pct_mixed_segments                  |       8.221 |              6.528 |             11.339 |
| collect_garbage / live_50pct_mixed_segments          |       6.853 |              7.234 |              7.043 |

## Baseline Read

The immediate bottleneck is not backend IO. The worst rows are nearly identical
across memory, SQLite, and RocksDB:

```text
stage_publish_commit / 1c_1000ch:       ~7.5s
load_changes_visible_batched / 1c_1000ch: ~7.8s
```

That points at changelog-layer visible-change proof and publication closure
validation, not storage. The first optimization should target repeated
whole-segment decode/validation and repeated membership scans while proving
visibility.

The second hotspot is CPU segment handling:

```text
decode_segment / 1c_1000ch:               4.0ms
validate_segment_shape / 1c_1000ch:       4.0ms
build_decoded_segment_index / 1c_1000ch:  7.8ms
```

Segment encode is cheap by comparison, and index row construction is not yet
the limiting cost.

## Entries

Each optimization entry below should:

```text
1. describe the physical change
2. rerun `cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard`
3. paste the full scorecard
4. call out rows that improved/regressed materially
5. keep or revert based on structural correctness first, timings second
```

