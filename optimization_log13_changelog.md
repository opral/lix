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

## Entry 1: SegmentView And Real Object Byte Ranges

Change:

```text
SegmentView now parses only the segment header and directory.
Segment object locations are real encoded byte_offset + byte_len ranges.
Canonicalization computes commit/change byte ranges from the encoded segment.
Validation checks directory byte ranges against the encoded object stream.
```

Measured with:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | entry_1_ms |
| --------------------------------------- | ---------: |
| encode_segment / 1c_1000ch              |      0.173 |
| decode_segment / 1c_1000ch              |      3.968 |
| view_segment / 1c_1000ch                |      0.636 |
| validate_segment_shape / 1c_1000ch      |      7.559 |
| build_decoded_segment_index / 1c_1000ch |     11.112 |
| build_by_change / 1c_1000ch             |      0.693 |
| build_by_change_membership / 1c_1000ch  |      0.032 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.518 |              3.461 |              4.242 |
| stage_segment / 1c_1000ch                            |      18.432 |             21.840 |             20.838 |
| stage_publish_commit / 1c_1ch                        |       0.053 |              0.064 |              0.067 |
| stage_publish_commit / 1c_100ch                      |      79.292 |             79.355 |             87.995 |
| stage_publish_commit / 1c_1000ch single-shot         |   10719.812 |          10761.576 |          10757.437 |
| load_commits_visible_batched / 1c_100ch              |       0.853 |              0.932 |              0.821 |
| load_changes_visible_batched / 1c_100ch              |      84.318 |             83.075 |             80.455 |
| load_changes_visible_batched / 1c_1000ch             |   11393.271 |          11270.454 |          11162.575 |
| load_changes_physical_scattered / 100seg_100c_1000ch |      10.298 |             10.309 |             10.254 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     529.561 |            115.931 |             99.827 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       9.169 |             11.451 |             10.389 |
| plan_gc / live_50pct_mixed_segments                  |      11.858 |             10.145 |              9.709 |
| collect_garbage / live_50pct_mixed_segments          |       9.922 |             15.618 |              9.983 |

Read:

```text
view_segment is now a cheap directory-only parse: 0.636ms for 1c_1000ch.

The raw directory view improved structurally, but validation and DecodedSegmentIndex
regressed because they now recompute/verify encoded byte ranges on top of the full
decode path. Keep the semantic cut; the next performance cut should make readers
use SegmentView + byte ranges directly instead of decoding and validating the whole
segment for every locator proof.
```

## Entry 2: Decode Requested Objects From SegmentView Byte Ranges

Change:

```text
Batch commit/change readers now load segment bytes once, parse SegmentView once,
validate locator equality against the directory, and decode only the requested
SegmentCommit or SegmentChange byte slice.

This avoids whole-segment decode + whole-segment validation on normal locator-backed
read paths.
```

Measured with:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | entry_2_ms |
| --------------------------------------- | ---------: |
| encode_segment / 1c_1000ch              |      0.153 |
| decode_segment / 1c_1000ch              |      3.790 |
| view_segment / 1c_1000ch                |      0.592 |
| validate_segment_shape / 1c_1000ch      |      7.642 |
| build_decoded_segment_index / 1c_1000ch |     11.150 |
| build_by_change / 1c_1000ch             |      0.810 |
| build_by_change_membership / 1c_1000ch  |      0.032 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.556 |              3.384 |              4.253 |
| stage_segment / 1c_1000ch                            |      17.422 |             20.760 |             21.492 |
| stage_publish_commit / 1c_1ch                        |       0.051 |              0.064 |              0.071 |
| stage_publish_commit / 1c_100ch                      |      88.490 |             83.630 |             78.473 |
| stage_publish_commit / 1c_1000ch single-shot         |   10971.050 |          10847.920 |          10832.477 |
| load_commits_visible_batched / 1c_100ch              |       0.271 |              0.233 |              0.229 |
| load_changes_visible_batched / 1c_100ch              |      26.255 |             22.906 |             22.141 |
| load_changes_visible_batched / 1c_1000ch             |    2364.192 |           2018.527 |           2046.168 |
| load_changes_physical_scattered / 100seg_100c_1000ch |       3.721 |              3.679 |              3.832 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     451.776 |             45.379 |             31.451 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       9.162 |             10.552 |              9.203 |
| plan_gc / live_50pct_mixed_segments                  |      10.962 |              9.213 |              9.342 |
| collect_garbage / live_50pct_mixed_segments          |       9.696 |             10.154 |              9.909 |

Read:

```text
The hot read paths moved hard:

load_commits_visible_batched / 1c_100ch:
  ~0.82-0.93ms -> ~0.23-0.27ms

load_changes_visible_batched / 1c_100ch:
  ~80-84ms -> ~22-26ms

load_changes_visible_batched / 1c_1000ch:
  ~11.2-11.4s -> ~2.0-2.4s

load_changes_physical_scattered:
  ~10.3ms -> ~3.7ms

The remaining 1c_1000ch visible-change cost is now mostly repeated membership
proof work, not whole-segment decoding. The next cut should batch membership
proofs by candidate commit and reuse decoded commit membership once per visible
commit instead of once per change_id.
```

## Entry 3: Batch Visible Change Membership Proofs By Commit

Change:

```text
Visible change reads now prove requested change_ids as one batch.

The reader scans by_change_membership candidates for all requested changes,
groups proof work by candidate commit_id, loads each visible candidate commit
once, scans its CommitBody.membership once, and marks all requested change_ids
proven by that commit. The fallback path also scans visible commits once for
the remaining unproven changes.
```

Measured with:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | entry_3_ms |
| --------------------------------------- | ---------: |
| encode_segment / 1c_1000ch              |      0.167 |
| decode_segment / 1c_1000ch              |      4.336 |
| view_segment / 1c_1000ch                |      0.613 |
| validate_segment_shape / 1c_1000ch      |      7.690 |
| build_decoded_segment_index / 1c_1000ch |     11.454 |
| build_by_change / 1c_1000ch             |      0.809 |
| build_by_change_membership / 1c_1000ch  |      0.035 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.515 |              3.406 |              4.087 |
| stage_segment / 1c_1000ch                            |      18.083 |             20.663 |             20.969 |
| stage_publish_commit / 1c_1ch                        |       0.050 |              0.061 |              0.094 |
| stage_publish_commit / 1c_100ch                      |      80.784 |             94.250 |             79.890 |
| stage_publish_commit / 1c_1000ch single-shot         |   11264.882 |          11141.916 |          11117.231 |
| load_commits_visible_batched / 1c_100ch              |       0.262 |              0.278 |              0.228 |
| load_changes_visible_batched / 1c_100ch              |       1.902 |              0.886 |              0.704 |
| load_changes_visible_batched / 1c_1000ch             |     134.949 |             18.746 |              6.960 |
| load_changes_physical_scattered / 100seg_100c_1000ch |       3.818 |              3.713 |              3.920 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     179.248 |             19.738 |              7.797 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       9.031 |             10.435 |              9.151 |
| plan_gc / live_50pct_mixed_segments                  |      10.831 |              9.316 |              9.555 |
| collect_garbage / live_50pct_mixed_segments          |       9.553 |              9.787 |              9.751 |

Read:

```text
The repeated proof cliff is mostly gone:

load_changes_visible_batched / 1c_1000ch:
  entry 2: ~2.0-2.4s
  entry 3: ~7-135ms depending backend smoke variance

load_changes_visible_batched / 1c_100ch:
  entry 2: ~22-26ms
  entry 3: ~0.7-1.9ms

load_changes_visible_scattered:
  entry 2: ~31-452ms
  entry 3: ~8-179ms

The remaining large Unit variance likely comes from the benchmark harness and
in-memory backend behavior, not physical IO. The next structural cliff is now
stage_publish_commit / 1c_1000ch, which still validates publication closure in
a per-change way.
```
