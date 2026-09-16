# Plugin developer workflows and sparse edits

Baseline: main `3ce4c9abf` (CSV QA merged). Measurements are development-machine observations, not portable guarantees. This change targets ordinary row edits, shared ordering and developer feedback; it does not redesign the transport.

## Method

The same portable probes were added to a detached baseline worktree and this branch. Native probes use the SDK harness; SQL probes use actual compiled Wasm plugins, the public SQL API and memory storage. Component compilation is warmed on a separate empty file before import. SQL samples alternate short and long replacement values on a known middle-row ID, discard three warmups and report 21 samples. Row selection and byte verification are outside the edit timer. Every SQL edit checks exact bytes; close/reopen is followed by another genuine edit. The native text probe also checks exact bytes; the native Markdown timing probe checks replacement presence, with exact-byte behavior covered separately by adapter regression tests.

SQL allocation counters are process-wide Rust allocator deltas around execution (including the allocation scope's final measurement helper), not guest linear-memory peaks. Native harness timings include fixture copying and must not be interpreted as bounded host execution.

## SQL/Wasm

The tables below are sequential baseline/candidate runs after compilation and test workers stopped. An earlier candidate run during compilation had a 100k text p50 of 11.465 ms, illustrating why these are observations, not guarantees.

Baseline:

| Plugin | Rows | Import ms | Point p50 ms | Point p95 ms | Allocated bytes p50 | Peak live delta p50 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Text | 1,000 | 14.196 | 2.852 | 2.985 | 2,961,052 | 339,741 |
| Text | 10,000 | 100.390 | 7.985 | 8.059 | 5,268,142 | 589,339 |
| Text | 100,000 | 1180.804 | 68.326 | 68.762 | 22,902,591 | 2,850,587 |
| Markdown | 1,000 | 27.554 | 6.437 | 6.895 | 4,313,418 | 425,672 |
| Markdown | 10,000 | 220.238 | 49.770 | 50.370 | 14,882,423 | 2,904,513 |

Updated:

| Plugin | Rows | Import ms | Point p50 ms | Point p95 ms | Allocated bytes p50 | Peak live delta p50 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Text | 1,000 | 14.269 | 2.306 | 3.157 | 3,001,791 | 334,844 |
| Text | 10,000 | 102.302 | 2.307 | 2.358 | 4,194,726 | 600,023 |
| Text | 100,000 | 1201.136 | 4.376 | 4.548 | 8,102,334 | 2,857,475 |
| Markdown | 1,000 | 30.760 | 2.270 | 2.311 | 3,264,969 | 342,480 |
| Markdown | 10,000 | 233.485 | 2.054 | 2.091 | 4,093,942 | 607,463 |

Text at 100k improves about 15.6x; Markdown at 10k about 24.2x. Host allocation traffic falls about 65% and 72%, respectively. Text's peak live delta is largely unchanged: less allocation traffic is not the same as less peak memory. At 1k, fixed engine overhead dominates text and its p95 did not improve in this run.

Baseline Markdown import at 100,000 paragraphs failed with Wasm out-of-memory while emitting typed rows. The matched SQL profile therefore caps Markdown at 10,000; native Markdown still exercises 100,000. This is a recorded pre-existing import limit, not evidence for or against a transport redesign.

## Native Text

Five warmups and 21 measured alternating variable-length edits:

| Lines | Baseline p50 ms | Updated p50 ms | Updated p95 ms | Baseline import ms | Updated import ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1.701 | 0.101 | 0.133 | 2.774 | 3.267 |
| 10,000 | 17.886 | 0.355 | 0.360 | 23.638 | 26.827 |
| 100,000 | 195.899 | 0.809 | 0.857 | 238.065 | 267.567 |

The index adds about 12% import time at 100k in this run. Regression tests assert bounded affected-line reads, page-sized state work, exact grouped edits across page boundaries and zero state writes for same-length content changes.

## Native Markdown

Nine alternating paragraph edits per size, same probe on both revisions:

| Paragraphs | Baseline p50 ms | Updated p50 ms | Baseline import ms | Updated import ms |
| --- | ---: | ---: | ---: | ---: |
| 1,000 | 6.441 | 0.048 | 15.869 | 17.670 |
| 10,000 | 68.501 | 0.142 | 155.933 | 176.178 |
| 100,000 | 702.969 | 4.339 | 1970.736 | 2037.173 |

At 100k, p95 was 709.202 ms before and 5.005 ms after. The first 1k sparse edit read 24 file bytes and 482 state bytes, wrote 262 state bytes, and used seven state reads and two state writes. A repeated edit read 33/503 file/state bytes and wrote 240 state bytes. Full fallback reads at least the 25,999-byte file.

## Ordering

Five optimized native process runs, 20,000 allocations after an initial key: append median 28.802 -> 3.972 ms; prepend 33.195 -> 3.960 ms. Total key characters fell from 50,025,000 to 680,000; longest key from 5,002 to 34. The stride optimization already existed in text; this change shares it with other plugins and SQL, rather than claiming a new algorithm.

## Boundaries

- Text handles content-only batches up to 64 stable rows with paged lengths and
  prefix sums. Same-length SQL edits write no state. Structural edits and absent indices retain the existing full-document path.
- Same-length text file edits preserve the existing zero-state-write behavior.
  Variable-length file edits can rebuild the index; they are not the optimized SQL workload.
- Markdown handles single ordinary top-level literal paragraphs. Its existing
  syntax and lexical guards decide when full validation/rendering is required.
- The UUID index is disposable derived state. Building it sorts/materializes
  IDs; it is not a general persistent ordered-tree implementation.
- SQL selection, import and structural changes are not claimed to be
  file-size-independent. No guest-memory or ABI-copying reduction is inferred from host allocation counters.

## Validation

- `cargo nextest run -p lix --features all-simulations --no-fail-fast`:
  3,965 passed, 74 skipped.
- `cargo test -p lix --doc`: 10 passed.
- All five native plugin suites passed (text, Markdown, CSV, JSON, Excalidraw),
  including 35 text and 213 Markdown tests.
- Compiled text SQL/Wasm suite: eight passed, one manual probe ignored.
  New point-edit/reopen workflow and benchmark metric guard: two passed.
- Independent sub-agent reviews covered ordering, lifecycle tooling, sparse
  adapters and profiling assertions. Findings fixed included native SQL function registration and null parity, index rewrites on same-length file edits, and a no-op reopen test. No remaining actionable review findings.
- Root formatting and diff checks passed.

## Reproduction

```sh
cargo test -p plugin_text text_matched_point_profile -- --ignored --nocapture
cargo test -p plugin_markdown profile_matched_sparse_sql -- --ignored --nocapture
cargo test -p plugin_markdown profile_sparse_sql_paragraph_edits -- --ignored --nocapture
cargo test --manifest-path tooling/Cargo.toml -p lix_e2e --no-default-features --features sdk-tests --test plugin_api_benchmarks profile_sql_point_edits -- --ignored --nocapture
rustc --edition=2024 -O packages/plugin-utils/profile_order_key.rs -o /tmp/profile-order-key
/tmp/profile-order-key
```

For matched baseline runs, apply only the portable profiling tests/module declarations to the baseline, not production or structural-counter assertions.
