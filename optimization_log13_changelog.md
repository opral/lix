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

## Entry 4: Batch Publish Membership Closure Validation

Change:

```text
stage_publish_commit now validates membership closure as one batch.

The writer collects all CommitBody.membership change_ids, resolves staged
changes by scanning staged segments once, resolves stored changes through
by_change grouped by physical segment, and only falls back to one full segment
scan for unresolved changes.
```

Measured with:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | entry_4_ms |
| --------------------------------------- | ---------: |
| encode_segment / 1c_1000ch              |      0.174 |
| decode_segment / 1c_1000ch              |      4.976 |
| view_segment / 1c_1000ch                |      0.603 |
| validate_segment_shape / 1c_1000ch      |      7.557 |
| build_decoded_segment_index / 1c_1000ch |     11.273 |
| build_by_change / 1c_1000ch             |      0.702 |
| build_by_change_membership / 1c_1000ch  |      0.032 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.517 |              3.368 |              4.114 |
| stage_segment / 1c_1000ch                            |      18.046 |             20.361 |             20.342 |
| stage_publish_commit / 1c_1ch                        |       0.056 |              0.051 |              0.059 |
| stage_publish_commit / 1c_100ch                      |       1.167 |              1.244 |              1.223 |
| stage_publish_commit / 1c_1000ch single-shot         |      14.898 |             14.540 |             15.028 |
| load_commits_visible_batched / 1c_100ch              |       0.246 |              0.230 |              0.221 |
| load_changes_visible_batched / 1c_100ch              |       1.840 |              0.907 |              0.722 |
| load_changes_visible_batched / 1c_1000ch             |     123.311 |             18.612 |              7.530 |
| load_changes_physical_scattered / 100seg_100c_1000ch |       3.788 |              3.634 |              4.037 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     198.086 |             19.525 |              7.762 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       9.273 |             11.254 |              9.291 |
| plan_gc / live_50pct_mixed_segments                  |      11.081 |              9.356 |              9.398 |
| collect_garbage / live_50pct_mixed_segments          |       9.498 |              9.870 |              9.699 |

Read:

```text
The publish cliff collapsed:

stage_publish_commit / 1c_1000ch single-shot:
  entry 3: ~11.1s
  entry 4: ~14-15ms

stage_publish_commit / 1c_100ch:
  entry 3: ~80-94ms
  entry 4: ~1.2ms

This is the intended Big-O shift: staged publication closure now scans staged
changes once instead of resolving each membership change through a repeated
segment search.
```

## Entry 5: Stage Segment Uses Construction-Time Validation

Change:

```text
stage_segment now uses validate_stage_segment_shape after canonicalize_segment.

validate_stage_segment_shape checks O(K) construction invariants: header counts,
duplicate ids, commit membership/directory cover, payload directory cover, and
segment directory cover. Full validate_segment_shape remains the repair/debug/GC
validator and still re-encodes the segment, verifies byte ranges, and recomputes
checksums.
```

Measured with:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | entry_5_ms |
| --------------------------------------- | ---------: |
| encode_segment / 1c_1000ch              |      0.198 |
| decode_segment / 1c_1000ch              |      4.005 |
| view_segment / 1c_1000ch                |      0.645 |
| validate_segment_shape / 1c_1000ch      |      7.791 |
| build_decoded_segment_index / 1c_1000ch |     11.764 |
| build_by_change / 1c_1000ch             |      0.809 |
| build_by_change_membership / 1c_1000ch  |      0.032 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.551 |              4.109 |              4.889 |
| stage_segment / 1c_1000ch                            |      11.090 |             13.998 |             14.072 |
| stage_publish_commit / 1c_1ch                        |       0.143 |              0.060 |              0.061 |
| stage_publish_commit / 1c_100ch                      |       1.234 |              1.192 |              1.209 |
| stage_publish_commit / 1c_1000ch single-shot         |      14.784 |             14.554 |             14.669 |
| load_commits_visible_batched / 1c_100ch              |       0.254 |              0.233 |              0.230 |
| load_changes_visible_batched / 1c_100ch              |       1.848 |              0.890 |              0.711 |
| load_changes_visible_batched / 1c_1000ch             |     122.225 |             19.177 |              6.954 |
| load_changes_physical_scattered / 100seg_100c_1000ch |       3.804 |              3.605 |              4.009 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     177.936 |             19.481 |              7.828 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       9.318 |             10.500 |              9.541 |
| plan_gc / live_50pct_mixed_segments                  |      11.165 |             10.068 |              9.336 |
| collect_garbage / live_50pct_mixed_segments          |       9.838 |             10.078 |              9.889 |

Read:

```text
stage_segment improved but did not reach raw write cost:

stage_segment / 1c_1000ch:
  entry 4: ~18-20ms
  entry 5: ~11-14ms

The remaining stage cost is now mostly canonicalize_segment itself: checksums,
payload directories, segment directory construction, and two encodes/views to
compute stable byte ranges. Next cut should remove the second encode/view pass
or make directory byte-range patching happen during encode.
```

## Entry 6: Record Segment Object Ranges During Encode

Change:

```text
encode_segment_with_object_locations writes the segment and records each
SegmentCommit / SegmentChange byte range while writing.

canonicalize_segment now uses those recorded ranges instead of encoding and
then viewing/skipping the encoded segment to discover object offsets. It also
avoids the second byte-range discovery pass because header checksum and locator
fields have fixed encoded widths.
```

Measured with:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | entry_6_ms |
| --------------------------------------- | ---------: |
| encode_segment / 1c_1000ch              |      0.269 |
| decode_segment / 1c_1000ch              |      4.516 |
| view_segment / 1c_1000ch                |      0.653 |
| validate_segment_shape / 1c_1000ch      |      7.394 |
| build_decoded_segment_index / 1c_1000ch |     11.206 |
| build_by_change / 1c_1000ch             |      0.686 |
| build_by_change_membership / 1c_1000ch  |      0.032 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.528 |              3.409 |              3.988 |
| stage_segment / 1c_1000ch                            |       5.029 |              7.724 |              7.798 |
| stage_publish_commit / 1c_1ch                        |       0.060 |              0.063 |              0.063 |
| stage_publish_commit / 1c_100ch                      |       1.159 |              1.198 |              1.215 |
| stage_publish_commit / 1c_1000ch single-shot         |      14.988 |             14.796 |             14.656 |
| load_commits_visible_batched / 1c_100ch              |       0.277 |              0.229 |              0.226 |
| load_changes_visible_batched / 1c_100ch              |       1.857 |              0.965 |              0.696 |
| load_changes_visible_batched / 1c_1000ch             |     122.428 |             18.041 |              6.788 |
| load_changes_physical_scattered / 100seg_100c_1000ch |       3.635 |              3.661 |              3.671 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     175.585 |             19.343 |              7.740 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       9.326 |             10.659 |              9.411 |
| plan_gc / live_50pct_mixed_segments                  |      11.219 |              9.362 |              9.339 |
| collect_garbage / live_50pct_mixed_segments          |       9.780 |              9.986 |             13.098 |

Read:

```text
stage_segment is now close to the original target:

stage_segment / 1c_1000ch:
  entry 5: ~11-14ms
  entry 6: ~5-8ms

The remaining gap to raw segment write is mostly canonical checksum and index
construction cost. The next cut should avoid logical checksum recomputation via
byte-native object checksums or add borrowed views for checksum/identity fields.
```

## Entry 7: Remove Diagnostic Allocation From Segment Views

Change:

```text
Segment view and object-slice walking now use fast ByteCursor methods that
report offset-only errors instead of formatting rich field names on successful
reads.

This follows the reference-system pattern from byte-native engines: hot readers
walk compact records with cheap static errors; rich diagnostic context stays on
debug/repair paths.
```

Measured with:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | entry_7_ms |
| --------------------------------------- | ---------: |
| encode_segment / 1c_1000ch              |      0.150 |
| decode_segment / 1c_1000ch              |      3.712 |
| view_segment / 1c_1000ch                |      0.026 |
| validate_segment_shape / 1c_1000ch      |      4.840 |
| build_decoded_segment_index / 1c_1000ch |      8.516 |
| build_by_change / 1c_1000ch             |      1.019 |
| build_by_change_membership / 1c_1000ch  |      0.035 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.667 |              5.686 |              4.223 |
| stage_segment / 1c_1000ch                            |       4.730 |              8.665 |              8.221 |
| stage_publish_commit / 1c_1ch                        |       0.055 |              0.050 |              0.059 |
| stage_publish_commit / 1c_100ch                      |       0.905 |              0.910 |              0.904 |
| stage_publish_commit / 1c_1000ch single-shot         |      11.865 |             12.185 |             12.277 |
| load_commits_visible_batched / 1c_100ch              |       0.209 |              0.265 |              0.187 |
| load_changes_visible_batched / 1c_100ch              |       1.758 |              0.804 |              0.619 |
| load_changes_visible_batched / 1c_1000ch             |     124.134 |             17.247 |              5.868 |
| load_changes_physical_scattered / 100seg_100c_1000ch |       3.127 |              3.091 |              3.179 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     174.873 |             18.227 |              6.440 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       6.564 |              7.635 |              6.524 |
| plan_gc / live_50pct_mixed_segments                  |       8.463 |              6.859 |              6.737 |
| collect_garbage / live_50pct_mixed_segments          |       7.166 |              7.256 |              7.316 |

Read:

```text
view_segment is now cheap enough to stop being a structural concern:

view_segment / 1c_1000ch:
  entry 6: 0.653ms
  entry 7: 0.026ms

The broader decode/validate/index rows also improved because object-slice
walking no longer allocates diagnostic field names in tight loops. The next cut
should move owned SegmentCommit/SegmentChange decode toward the same fast cursor
path or make checksum/index construction consume borrowed object views directly.
```

## Entry 8: Borrow Segment Directory Locations During Validation

Change:

```text
directory_commit_location_ref and directory_change_location_ref return borrowed
SegmentObjectLocation values for validation paths.

Owned directory lookup wrappers remain for APIs that must return or persist a
locator, but pure validation no longer clones locators just to compare them.
```

Measured with:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | entry_8_ms |
| --------------------------------------- | ---------: |
| encode_segment / 1c_1000ch              |      0.137 |
| decode_segment / 1c_1000ch              |      3.421 |
| view_segment / 1c_1000ch                |      0.027 |
| validate_segment_shape / 1c_1000ch      |      4.775 |
| build_decoded_segment_index / 1c_1000ch |      8.504 |
| build_by_change / 1c_1000ch             |      0.791 |
| build_by_change_membership / 1c_1000ch  |      0.033 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.528 |              5.738 |              4.235 |
| stage_segment / 1c_1000ch                            |       4.841 |              8.197 |              7.688 |
| stage_publish_commit / 1c_1ch                        |       0.053 |              0.042 |              0.054 |
| stage_publish_commit / 1c_100ch                      |       0.871 |              0.895 |              0.987 |
| stage_publish_commit / 1c_1000ch single-shot         |      11.610 |             11.597 |             11.741 |
| load_commits_visible_batched / 1c_100ch              |       0.196 |              0.164 |              0.173 |
| load_changes_visible_batched / 1c_100ch              |       1.726 |              0.758 |              0.593 |
| load_changes_visible_batched / 1c_1000ch             |     122.412 |             17.104 |              6.050 |
| load_changes_physical_scattered / 100seg_100c_1000ch |       3.059 |              3.073 |              3.093 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     180.548 |             18.731 |              6.636 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       6.338 |              7.806 |              6.698 |
| plan_gc / live_50pct_mixed_segments                  |       8.422 |              6.807 |              7.660 |
| collect_garbage / live_50pct_mixed_segments          |       7.911 |              8.466 |              7.496 |

Read:

```text
This was a small constant-factor cut. It removes the cloned locator/free frames
seen in the sampled validation profile, while keeping the public owned lookup
shape for index-entry construction.

The remaining CPU profile is still dominated by:
  - full owned decode into SegmentChange strings
  - EntityIdentity JSON serialization/deserialization
  - checksum construction over logical objects
```

## Entry 9: Encode EntityIdentity As Tuple Parts

Change:

```text
Changelog SegmentChange encoding now stores EntityIdentity as its canonical
string tuple parts instead of JSON-array text.

checksum_change hashes the entity identity parts directly, avoiding
EntityIdentity::as_json_array_text and serde_json serialization in checksum hot
paths.

Owned decode uses a fast tuple reader for entity identity parts instead of
EntityIdentity::from_json_array_text.
```

Measured with:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | entry_9_ms |
| --------------------------------------- | ---------: |
| encode_segment / 1c_1000ch              |      0.147 |
| decode_segment / 1c_1000ch              |      4.402 |
| view_segment / 1c_1000ch                |      0.029 |
| validate_segment_shape / 1c_1000ch      |      4.816 |
| build_decoded_segment_index / 1c_1000ch |      8.095 |
| build_by_change / 1c_1000ch             |      0.806 |
| build_by_change_membership / 1c_1000ch  |      0.033 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.459 |              3.289 |              4.134 |
| stage_segment / 1c_1000ch                            |       4.482 |              7.391 |              7.202 |
| stage_publish_commit / 1c_1ch                        |       0.048 |              0.044 |              0.053 |
| stage_publish_commit / 1c_100ch                      |       0.857 |              0.868 |              0.827 |
| stage_publish_commit / 1c_1000ch single-shot         |      11.059 |             10.931 |             11.101 |
| load_commits_visible_batched / 1c_100ch              |       0.196 |              0.177 |              0.168 |
| load_changes_visible_batched / 1c_100ch              |       1.766 |              0.772 |              0.593 |
| load_changes_visible_batched / 1c_1000ch             |     123.992 |             17.187 |              5.569 |
| load_changes_physical_scattered / 100seg_100c_1000ch |       2.772 |              2.871 |              3.011 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     176.019 |             17.954 |              6.257 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       6.060 |              7.152 |              5.966 |
| plan_gc / live_50pct_mixed_segments                  |       7.784 |              6.378 |              6.182 |
| collect_garbage / live_50pct_mixed_segments          |       6.693 |              6.886 |              6.629 |

Read:

```text
The cut improved checksum and maintenance-shaped paths, especially stage,
publish, scattered loads, rebuild, and GC.

decode_segment did not improve because full owned decode still materializes
SegmentChange strings and EntityIdentity Vec allocations. That confirms the next
larger structural cut: avoid full owned decode for DecodedSegmentIndex and read
hot fields through borrowed segment object views.
```

## Entry 10: Make DecodedSegmentIndex Byte-Backed

Change:

```text
DecodedSegmentIndex no longer decodes the entire Segment and validates every
logical object while building the index.

It now builds from SegmentView directory refs, stores segment bytes, and decodes
individual SegmentCommit / SegmentChange objects only when callers request them.
Lookup-only paths use contains_commit / contains_change and stay directory-only.
```

Measured with:

```sh
cargo bench --manifest-path packages/engine/Cargo.toml --features storage-benches --bench changelog_scorecard
```

### CPU Segment Scoreboard

Times are milliseconds.

| row                                     | entry_10_ms |
| --------------------------------------- | ----------: |
| encode_segment / 1c_1000ch              |       0.130 |
| decode_segment / 1c_1000ch              |       4.420 |
| view_segment / 1c_1000ch                |       0.028 |
| validate_segment_shape / 1c_1000ch      |       4.971 |
| build_decoded_segment_index / 1c_1000ch |       0.233 |
| build_by_change / 1c_1000ch             |       0.791 |
| build_by_change_membership / 1c_1000ch  |       0.032 |

### Backend Smoke Scoreboard

Times are milliseconds.

| row                                                  | mem_unit_ms | sqlite_tempfile_ms | rocksdb_tempdir_ms |
| ---------------------------------------------------- | ----------: | -----------------: | -----------------: |
| stage_segment_raw_no_indexes / 1c_1000ch             |       0.443 |              3.398 |              3.979 |
| stage_segment / 1c_1000ch                            |       4.543 |              7.204 |              6.941 |
| stage_publish_commit / 1c_1ch                        |       0.075 |              0.036 |              0.037 |
| stage_publish_commit / 1c_100ch                      |       0.448 |              0.472 |              0.461 |
| stage_publish_commit / 1c_1000ch single-shot         |       4.535 |              4.280 |              4.735 |
| load_commits_visible_batched / 1c_100ch              |       0.192 |              0.171 |              0.171 |
| load_changes_visible_batched / 1c_100ch              |       1.707 |              0.752 |              0.572 |
| load_changes_visible_batched / 1c_1000ch             |     122.194 |             16.967 |              5.629 |
| load_changes_physical_scattered / 100seg_100c_1000ch |       2.855 |              2.909 |              2.979 |
| load_changes_visible_scattered / 100seg_100c_1000ch  |     184.597 |             17.941 |              6.225 |
| rebuild_mandatory_indexes / 100seg_100c_1000ch       |       6.199 |             10.422 |              8.677 |
| plan_gc / live_50pct_mixed_segments                  |      10.648 |              7.031 |              6.270 |
| collect_garbage / live_50pct_mixed_segments          |       6.484 |              7.592 |              6.594 |

Read:

```text
This is the large structural cut the profiler asked for:

build_decoded_segment_index / 1c_1000ch:
  entry 9: 8.095ms
  entry 10: 0.233ms

stage_publish_commit / 1c_1000ch:
  entry 9: ~11ms
  entry 10: ~4.3-4.7ms

Publish now pays for the requested commit object plus closure validation instead
of full segment decode. Remaining visible-change read cliffs are not in
DecodedSegmentIndex; they sit in visible proof / change materialization paths.
```
