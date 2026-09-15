# Indexed import profile

This workload verifies query results while measuring the import and query paths
behind inlang issue 4417. It creates fresh in-memory repositories with one bundle,
seven messages and seven variants per key. It measures staging and commit
separately, then cold and warm equality queries, equivalent full scans, and nested
joins. Schema registration, fixture string construction, and result assertions
are outside the reported operation timers.

## Run

Build matching native and JavaScript SDK artifacts first:

```sh
cd packages/js-sdk
npm ci
npm run build:native
npm run build:ts
node scripts/profile-indexed-import.mjs > indexed-import.json
```

The default run uses 34, 35, 256 and 2,048 bundles, one warmup round and five
measured rounds. Sizes rotate between rounds. Override `LIX_PROFILE_BUNDLES`,
`LIX_PROFILE_ROUNDS` or `LIX_PROFILE_WARMUPS`; `LIX_PROFILE_SDK` selects another
built SDK's `dist/index.js` for comparisons. `LIX_PROFILE_TRACE=1` includes engine
telemetry spans, whose overhead should be excluded from latency comparisons.

For CPU sampling on Linux:

```sh
LIX_PROFILE_BUNDLES=2048 LIX_PROFILE_ROUNDS=12 \
  perf record -F 199 -g --call-graph dwarf -o indexed-import.perf.data -- \
  node scripts/profile-indexed-import.mjs > indexed-import-profile.json
DEBUGINFOD_URLS= perf report --stdio --no-children --call-graph none \
  -i indexed-import.perf.data
```

## Measurements

Measured 2026-09-15 on AMD Ryzen 9 9950X, Linux x64, Node v22.23.2, Rust
nightly-2026-05-21, optimized release native builds and canonical in-memory
storage. The baseline is `feba0dd51`, containing the original packed-index
publication fix. Both revisions pass every result assertion.

Two adjacent runs per revision used the order baseline, final, final, baseline.
Each run had one warmup and five measured rounds with rotating sizes. The tables
combine the ten measured rounds. These are local samples on a shared development
machine; small differences may include host noise.

Median import time in milliseconds (staging plus commit for each sample):

| Bundles | Imported rows | Baseline | Final PR | Difference |
| --- | --- | --- | --- | --- |
| 34 | 510 | 6.565 | 6.652 | +1.3% |
| 35 | 525 | 5.438 | 5.613 | +3.2% |
| 256 | 3,840 | 32.810 | 33.737 | +2.8% |
| 2,048 | 30,720 | 291.614 | 297.118 | +1.9% |

Final warm-query medians in milliseconds (equality and full scan return the same
seven rows; the nested join returns every imported bundle/message/variant):

| Bundles | Indexed equality | Equivalent full scan | Nested join |
| --- | --- | --- | --- |
| 34 | 0.154 | 0.513 | 0.985 |
| 35 | 0.225 | 0.608 | 1.402 |
| 256 | 0.250 | 1.290 | 5.518 |
| 2,048 | 0.560 | 7.291 | 48.066 |

At the largest size, the selective indexed query is approximately 13 times
faster than its equivalent scan. Median staging and commit times are 67.371 ms
and 229.801 ms respectively; commit remains the largest import phase. The
baseline warm nested join is 47.918 ms, compared with 48.066 ms in the final build.

The JSON output retains every sample and p95 values. Keep instrumented CPU runs
separate from uninstrumented latency runs when comparing revisions.

The final CPU capture contains 1,902 samples with no lost samples. Leading
named samples are memory copying, allocation/free, packed-row scanning,
foreign-key target comparisons, and commit-member validation. Samples are spread
across these paths.

## Compatibility costs

Older completeness markers are not trusted. Existing index generations can use
scans until completeness is independently established; opening does not eagerly
rebuild them. Selective queries on those collections can therefore be slower.
Fresh complete indexes remain usable, as verified by the workload above.

An amendment adding defaults writes affected existing rows atomically, so its
work grows with the collection size. The import timings above cover fresh data,
not a default-adding schema amendment.
