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
