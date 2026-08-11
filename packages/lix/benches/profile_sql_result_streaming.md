# SQL result streaming profile

This benchmark compares the current eager public result path with two internal
prototypes. It does not change `ExecuteResult` or expose streaming publicly.
Every mode uses the same fixture and SQL statement, and the profile scope
includes snapshot acquisition, execution, and result consumption.

The fixture query is an unordered `UNION ALL` over eight distinct registered
row tables. Do not add a global `ORDER BY` when measuring early stop: a sort
must consume all input before it can emit a first batch and would intentionally
hide cancellation. Distinct child tables create independent DataFusion output
batches so Lix's identical-scan cache cannot merge them back into one eager
scan.

The modes are:

- `full`: current behavior; convert and retain every public `Row`.
- `stream`: collect all DataFusion batches as the eager engine does, then
  convert one row at a time and drop it immediately. This isolates retained
  public-row allocation from execution and is the collected-batch control.
- `live`: pull the benchmark-only DataFusion batch stream before collection;
  dropping after a row limit drops the underlying scan inside the scoped
  storage read.
- `count_only`: skip scalar conversion; use it as the execution ceiling.

Run the same command shape for each mode:

```text
LIX_SQL_PROFILE_RESULT_MODE=full \
  LIX_SQL_PROFILE_ROWS=131072 LIX_SQL_PROFILE_ROUNDS=15 \
  cargo bench -p lix --no-default-features --features storage-benches \
  --bench profile_sql_result_streaming

LIX_SQL_PROFILE_RESULT_MODE=stream \
  LIX_SQL_PROFILE_ROWS=131072 LIX_SQL_PROFILE_ROUNDS=15 \
  cargo bench -p lix --no-default-features --features storage-benches \
  --bench profile_sql_result_streaming

LIX_SQL_PROFILE_RESULT_MODE=stream LIX_SQL_PROFILE_ROW_LIMIT=100 \
  LIX_SQL_PROFILE_ROWS=131072 LIX_SQL_PROFILE_ROUNDS=15 \
  cargo bench -p lix --no-default-features --features storage-benches \
  --bench profile_sql_result_streaming

LIX_SQL_PROFILE_RESULT_MODE=live LIX_SQL_PROFILE_ROW_LIMIT=100 \
  LIX_SQL_PROFILE_ROWS=131072 LIX_SQL_PROFILE_ROUNDS=15 \
  cargo bench -p lix --no-default-features --features storage-benches \
  --bench profile_sql_result_streaming

LIX_SQL_PROFILE_RESULT_MODE=count_only \
  LIX_SQL_PROFILE_ROWS=131072 LIX_SQL_PROFILE_ROUNDS=15 \
  cargo bench -p lix --no-default-features --features storage-benches \
  --bench profile_sql_result_streaming
```

Interpret the output as follows:

- `arrow_execution_us` changes only if the engine itself can stop producing
  batches. The collected `stream` mode is expected not to lower this number;
  the live mode should.
- `public_result_materialization_us` is the row conversion cost. A streaming
  win should reduce this for early stop and avoid the full-result allocation.
- `result_rows_materialized` counts owned scalar rows: all rows for `full`,
  consumed rows for `stream`/`live`, and zero for `count_only`.
- `result_rows_retained` counts rows kept through consumption: all rows for
  `full` and zero for cursor modes.
- `scan_rows` in `live` is bounded by the first batch that contains the row
  limit; it need not equal the exact row limit.
- `wall_median_us` is the end-to-end number to use for a ship/no-ship decision.

For CPU attribution, build once and profile the same binary with `samply` or
Instruments. For allocation and peak-memory attribution, use the benchmark
crate's `system-allocation-profiler` feature or run the binary under
`heaptrack`; compare `full` and `stream` with identical row counts and warmups.

## Decision matrix with RSS

Use the fresh-process matrix when evaluating whether a future explicit,
read-only `stream()` capability is worthwhile. It randomizes
scenario order for every repetition, runs each scenario in a separate process,
captures maximum RSS with `/usr/bin/time -l`, and emits CSV suitable for a
spreadsheet or statistical analysis:

```text
LIX_SQL_PROFILE_ROWS=32768 \
LIX_SQL_PROFILE_REPETITIONS=5 \
LIX_SQL_PROFILE_WARMUPS=1 \
scripts/profile_sql_streaming_matrix.sh > streaming-matrix.csv
```

The matrix includes a `fixture_only` RSS baseline plus `full_all`,
`full_early`, `collected_all`, `collected_early`, `live_all`, `live_early`, and
`count_all`. Every non-count query scenario validates a deterministic checksum
over all projected values before emitting its row. Compare medians and spread
by scenario. Evidence for a separate streaming capability requires a material
end-to-end win for `live_early`, lower query-over-baseline `max_rss_bytes` for
retained result cases, and no checksum or row-count failures. RSS is process
maximum RSS, so allocator high-water behavior can still mask deltas; keep
fixture size, warmups, build mode, and host conditions fixed.

The A/B is meaningful only at the same consumption point. `full` calls the
normal eager `execute()` and then `rows()`. `stream` takes the ordinary
collect-all-batches path without retaining public rows. `live` consumes the
pull-based DataFusion stream directly. The live result and its borrowed row
cursor are dropped before the storage-read closure returns, so the benchmark
does not let the existing lifetime erasure escape.
