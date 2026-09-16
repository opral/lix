# E15: finite-ID selection without a complete filesystem index

**Rejected: statistically significant cold/warm regressions and an indexed-read invariant failure.** This report retains
an isolated prototype; it does not apply its production changes to the stack.

The hypothesis was that finite canonical file IDs should use the existing
selective live-state reader without first constructing the complete filesystem
path index. Noncanonical literals retain the previous index/filter route. The
prototype adds no protocol, storage format, migration, or public API change.

## Paired results

Ten alternating pairs per fixture, real HTTP with zero injected latency, matching
persisted synthetic snapshots and identical harness d81746db943e11a513516a16297745e9a1bc1341083e576733d8c02e1d7c5cb9.
Each invocation compares complete ordered file rows to its authority and repeats
the file lookup offline. Paired validation matches mode, row count, snapshot and
history result fields; it does not compare a separate file-row digest between
engines. History uses LIMIT 1. Positive improvement means faster. Confidence
intervals are seeded 10,000-resample bootstrap intervals for median paired change.

| Exact-ID selection | Baseline → candidate median | Paired improvement, 95% interval | Native requests |
| --- | --- | --- | --- |
| dense64 cold | 37.896 → 53.919 ms | −46.87% [−51.37%, −42.06%] | 17 → 23 |
| dirs1600 cold | 362.807 → 916.679 ms | −152.59% [−156.96%, −149.73%] | 62 → 77 |
| wide16000 cold | 6464.050 → 169.994 ms | +97.39% [+97.33%, +97.43%] | 256 → 57 |
| dense64 warm | 0.880 → 2.482 ms | −184.99% [−198.75%, −172.63%] | offline |
| dirs1600 warm | 1.236 → 6.788 ms | −446.95% [−485.21%, −415.91%] | offline |
| wide16000 warm | 1.556 → 2.700 ms | −72.62% [−77.09%, −67.32%] | offline |

Wide transfer falls 2,429,284 → 141,200 bytes, but dense transfer grows
11,742 → 17,526 bytes. Subsequent LIMIT-1 history improves 19.8–34.4% across
these fixtures, which does not compensate for the file-selection regressions.
All raw measurements, including unfavorable controls, are retained.

Eight additional single-run query-form controls cover exact/missing IDs,
16/256-ID batches, exact/missing paths, prefix and invalid-ID queries on all
three fixtures. They are diagnostics, not additional statistical claims.
The 1,600-directory fixture explicitly asserts its directory count before export.

## Cause and next abstraction

`plan_scan` normally obtains `FilesystemPathIndex` before applying finite IDs.
The prototype bypasses it. The existing selective helper pins file owner IDs,
but still requests both descriptor and blob schemas and scans every directory.
It also loses the existing revision cache on warm reads. This replaces one
expensive broad path with another and is not an acceptable general policy.

A follow-up should scope the existing index to requested files and their ancestor
directory closure. Scope must survive cache keys and retained dependency recipes,
including refresh and transaction overlays. Cache invalidation and sequential
ancestor depth are costs to measure. Exact-path lookup remains a separate problem;
this experiment does not provide a searchable persistent path index.

## Validation and reproducibility

The measured patch is `measured-prototype.patch.gz`, based on accepted E14
0b46fb063ac28610cef9ee11d9dc528fdcef7eb5. Its two new canonical-Memory simulation
cases pass (base and tracked rebuild): finite IDs, ancestor moves, residual
filters, absence and noncanonical literals.

The original broad gate ran 2,863 of 4,327 tests: 2,862 passed, one failed and
1,464 were canceled. The failing unit fixture supplied file rows only through a
static path index and an empty underlying reader, conflicting with the changed
route. Its full log is retained. A test-only correction supplies underlying rows and verifies bounded file IDs.
The subsequent no-fail-fast run completes all 4,327 tests: 4,326 pass, one fails,
and 84 are skipped. It exposes a second test: indexed content capture
requires that cached index reads do not scan unrelated HOT rows. The prototype
violates that requirement. This invariant is retained, not weakened. No full
correctness pass is claimed. Timing binaries remain unchanged; test-only edits
are archived separately from the measured patch.

This is a completed negative experiment based on ten paired measurements, not an
attempt counted merely because compilation or a test failed. The material
regressions independently reject the policy; the retained failing invariant
provides further evidence. The non-improvement streak becomes 1 after E14.
All 60 paired openings used two foreground requests and zero native reads.

Baseline binary b7723475afe63a10ed8ad1c757eaa0cf598682f9f7a89218a74c66857a7e5743;
candidate f94e351097f6586de7addd90df13bd94f7dfedd1e1442835e93a9a8f9695e476.
Use `paired_file_selection.py` with these matching artifacts, the persisted
fixtures, `LIX_PROFILE_FILE_LOOKUP=id`, and `LIX_PROFILE_HISTORY_LIMIT=1`.
Do not substitute the earlier diagnostic harness for matched paired runs.

The intermediate fixture edit initially failed to compile because its test constant
needed a module qualifier. That log is retained as `e15-final-nextest.log.gz`;
it is not a successful gate or a separate experiment.

Ten doctests and scoped `cargo fmt -p lix -- --check` pass on the audited
prototype. The broad engine gate remains a rejection, not a green validation.
