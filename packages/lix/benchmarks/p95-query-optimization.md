# Seven-query optimization evidence

Companion draft: https://github.com/opral/lixray/pull/516. Full synthetic logs, frozen production target provenance, comparison scripts and independent GPT-6 Luna xhigh review ledger: https://github.com/opral/lixray/tree/codex/p95-query-optimization/perf/p95-optimization.

## Answer-scoped filesystem point reads

Exact native ID reads now request one file owner and its directory ancestry. Transaction overlay readers honor the same scope. Late content, size and ranged hydration carry IDs bound from projection source expressions and keep their path predicate. Aliases do not determine identity. Path-only APIs retain their path semantics; invalid optional IDs retain the fallback. Plugin rendering, view acknowledgement, global/untracked visibility and partial-read registration remain in the shared pipeline.

Real-engine, canonical Memory, unoptimized test build: 128 files, 128 checkpoints, complete result equality, 100 warm samples/query/trial, 20 fresh-engine/session first executions per arm. Counterbalanced A B B A order; distinct copied executables with SHA-256 checks. Lazy public rows are consumed inside timing. Cold session means first SQL on the same seeded Memory storage; engine opening is excluded. Index rows count materialization into the engine, not physical storage pages/objects.

| Workload | Baseline ms | Scoped ms | Result |
| --- | ---: | ---: | --- |
| First-session content ID, p95 | 4.612 | 1.202 | -73.9%; index rows 277 → 3 |
| First-session file row ID, p95 | 5.716 | 2.480 | -56.6%; index rows 279 → 5 |
| Same-ID content, median trial warm p95 | 0.334 | 0.335 | Approximately flat |
| Same-ID file row, median trial warm p95 | 0.709 | 0.717 | Approximately flat |
| Changing-ID content, median trial warm p95 | 0.333 | 0.735 | +120.8%; scoped-cache miss tradeoff |

The changing-ID regression remains an actionable QA finding. Shared descriptor decoding/cache reuse is a separate candidate so each architectural change can be measured independently. These measurements do not predict deployed production p95 or establish the production cold/warm mix.

Validation: 4,615 all-simulation tests passed (79 skipped), including fresh-engine work bounds at 8/400 files, qualified/aliased source IDs, path-as-ID alias misdirection, staged/global/untracked/tombstone parity and late-content plugin acknowledgement. Ten doctests passed. All seven families also pass explicit observation-SQL mode (six observed families, account ordinary execute) and 128-dirty-file density experiments. Observation-SQL timing excludes the observer invalidation wait, lifecycle and delivered-view acknowledgement.

Independent GPT-6 Luna xhigh reviewers `implement_history` and `audit_cardinality` approved correctness and the cold-tail benefit with the changing-ID tradeoff retained. Reviewer `audit_points` approved the consumed-result observation profiler and its measurement scope.

## Bounded checkpoint-status and columnar log windows

Log scans now collect at most 64 ordered first-parent nodes, resolve their retirement states in one batch at the pinned anchor, and emit a stable columnar batch. History diffs keep one-node windows and the original frontier. A generic physical-fetch callback preserves ordinary ordered-page laziness; repeated optimizer rebinding preserves the original planned cap and its physical cache key. This changes shared scan execution, with no query fingerprint recognizer or maintained count shortcut.

Compared with the preceding point-scope commit, 20 independent trials per arm, 100 warm executions/trial, 128 files/checkpoints, using observation SQL for six production-observed families and execute for account:

| Checkpoint summary | Baseline ms | Batched ms | Change |
| --- | ---: | ---: | ---: |
| Median trial warm p95 | 7.488 | 1.863 | -75.1% |
| Fresh-session p95 | 8.715 | 2.491 | -71.4% |

Warm trial p95 ranges are 7.326–8.193 versus 1.844–1.907 ms. Scan batches fall 128 → 3 and Arrow bytes 45,056 → 5,780; graph nodes remain 131 and retirement keys remain 128, resolved in three batches. All complete-result oracles pass. Other warm medians change by at most about 1%, with overlapping ranges; file-row candidate trial p95 range includes one 0.869 ms outlier versus baseline maximum 0.725 ms, with medians 0.714 versus 0.710 ms. Changing-ID point-read regression from the prior optimization remains open.

Validation: 4,618 all-simulation tests passed (79 skipped), and ten doctests passed. Coverage includes 64-row window boundaries, pinned undo/redo and fork status, plain zero-column count, full-count page consumption, page work independent of older history, LIMIT 0, successive fetch widening/narrowing/removal and effective cache keys. Independent GPT-6 Luna xhigh code and data review challenges preserved in the companion audit ledger. Failed intermediate page-work experiments are retained there and excluded from comparative results. These canonical Memory SQL measurements do not predict deployed production p95 or network/observer delivery time.

## Directory-only filesystem indexes

Directory listing now requests a directory-only index through a shared scope enum. The scope controls schema demand, cache identity and advancement, transaction overlays, and durable partial-replica read interests. Writes still resolve the complete filesystem namespace. This is a shared index/read-demand change rather than a query-fingerprint shortcut.

Compared with the preceding checkpoint-window engine, each fixture has 20 counterbalanced trials per arm and 100 warm samples per query/trial, with matching protocol-enabled build features and complete result equality:

| Directory first-session p95 | Baseline ms | Scoped ms | Engine descriptor rows |
| --- | ---: | ---: | --- |
| 128 files, 19 directories | 3.403 | 1.766 | 148 → 19; -48.1% latency |
| 400 files, 53 directories | 7.048 | 2.266 | 454 → 53; -67.9% latency |

Cold ranges do not overlap: 2.875–3.657 versus 1.472–1.796 ms, and 6.451–7.096 versus 2.100–2.347 ms. Warm median trial p95 is approximately unchanged (0.390 → 0.387 ms at 128 files; 0.559 → 0.548 ms at 400), with overlapping ranges. Other query-family warm medians change by at most 0.8%. These are engine row counts, not physical storage I/O or byte reductions. The larger fixture also has more output directories; it does not establish constant cost as directory count grows. The changing-ID point-read tradeoff remains open.

Validation: 4,878 tests passed with all simulations, storage benchmarks, server protocol and protocol client enabled (91 skipped), and ten doctests passed. Tests check selected-directory output and actual work with 400 unrelated files, cache scope isolation and committed/staged advancement, partial directory demand, and rejection of old interest journals. Initial fixture/bootstrap, OpenAPI and journal write-admission failures were corrected and retained in the companion audit records. Independent GPT-6 Luna xhigh reviewers approved the scope and data.

The serialized interest shape changes intentionally: sync protocol 22 and partial-interest journal version 3 are required. Client and server must use matching sync versions; existing version-2 journals are rejected rather than silently interpreted or erased. No migration or deployment is included in this draft.

## Typed filesystem descriptor demand

Filesystem index scans and ancestor reads now request typed snapshots and metadata without derived full-row JSON. File, directory and blob fields use schema-validated typed values; the existing JSON fallback remains. Blob consumers still receive their canonical DTO. A focused real-engine test proves zero whole-row JSON renders on the typed path; this is not a measured baseline conversion count.

Two independent comparisons against the directory-scope commit use the same features, production execution kinds, 20 ABBA trials per arm and 100 warm samples per query/trial. All eight complete-result oracles pass.

| Cold directory build | Baseline median ms | Typed median ms | Physical-planning median ms |
| --- | ---: | ---: | --- |
| 128 files, 19 descriptors | 1.523 | 1.465 | 0.755 → 0.700 |
| 400 files, 53 descriptors | 2.191 | 2.024 | 1.219 → 1.066 |

At 400 files, every ABBA block improves cold directory mean time (4.2–16.2%); median first-execution time falls 7.6%. The observed first-execution p95 is 2.569 → 2.137 ms, but trial ranges overlap (2.083–2.612 versus 1.958–2.375), so this is evidence for reduced cold build cost, not a proven general tail reduction. The physical-phase difference scales at approximately 2.9 microseconds per descriptor across the two fixtures; that is an inferred consistency check. At 128 files the p95 is essentially unchanged (1.681 → 1.669 ms). Warm directory and other same-ID query timings remain approximately flat. Changing-ID content improves only about 2%, with overlapping ranges; the original scoped-cache tradeoff remains open.

Validation: 4,881 tests passed (91 skipped), plus ten doctests. Typed-only scan/parent projection, UUID/null fields, canonical blob parity and invalid blob size are covered. Two fixture compilation failures and one existing cross-fixture collection-delete byte-bound failure are retained. The unchanged full suite rerun and six isolated repetitions of that test passed; its assertion was not weakened. Independent GPT-6 Luna xhigh reviewers approved source and the narrow cold-build evidence. These are canonical Memory results, with no deployed production prediction.
