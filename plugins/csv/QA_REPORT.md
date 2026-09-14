# CSV QA report

Qualification performed on 2026-09-14, starting from main `d99c3f37a`.
Independent review rounds covered roundtripping, SQL mapping/merging, and scale.
Fixes were followed by repeat reviews; the final round reported no further
concrete issues in the reviewed scope.

## Correctness and editability

Regression coverage includes encoding versus literal BOMs, mixed CR/LF/CRLF,
missing final terminators, empty records, quoted multiline values, dialect
changes, arbitrary UUIDs beyond the compact namespace capacity, streamed cold
checkpoints, malformed checkpoint lengths, disjoint cell merges, and sparse
batches that preserve unchanged formatting and identities. CSV records map to
positional string arrays, preserving headers as data, duplicate headers, ragged
rows, leading zeroes, and empty strings. The README documents SQL operations and
ordering constraints.

## Actual SQL/Wasm measurements

Single-run development engine with release Wasm, in-memory storage, four-cell
45-byte records. Component compilation is warmed separately and excluded from
import (first warmup 6.03 s). These are observations, not portable latency limits.
Every tested operation checks exact output; both sizes pass exact reopening.

| Operation | 100k rows / 4.5 MB | 1M rows / 45 MB |
|---|---:|---:|
| Import | 4.13 s | 43.63 s |
| Ordered selection of first two rows | 752 ms | 7,947 ms |
| First SQL cell edit | 20.5 ms | 90.7 ms |
| Repeated SQL cell edit | 16.3 ms | 88.7 ms |
| Two-row SQL update | 16.4 ms | 87.8 ms |
| Variable-length SQL edit, growing/shrinking | 20.1 / 20.3 ms | 123.7 / 125.3 ms |
| File edit after SQL | 26.6 ms | 170 ms |
| Delete | 148 ms | 1.38 s |
| Reorder | 220 ms | 2.12 s |
| Insert | 217 ms | 2.10 s |
| Update 4,097 alternating rows | 2.50 s | 4.39 s |

Before the fixes, the 100k second edit took 1,031 ms and the two-row update
1,744 ms. The 1M import exceeded the aggregate output budget. Subsequent review
rounds exposed structural memory exhaustion, a host WASI diagnostic-flush panic,
and large-batch timeout/splice-count cliffs; the final matrix passes these cases.

Reproduce using [the probe instructions](qa_scale/README.md), with
`CSV_QA_ROWS=100000,1000000 CSV_QA_STRUCTURAL=1 CSV_QA_BULK_STRIDE=2`.
The native core probe separately measures parser/index/checkpoint costs; its
sub-millisecond point edits must not be presented as end-to-end SQL timings.

## Validation and limits

- Engine: `cargo nextest run -p lix --features all-simulations`: 3,940 passed,
  74 skipped by the suite.
- Documentation: `cargo test -p lix --doc`: 10 passed.
- CSV unit and integration suites pass, including added edge-case regressions.
- Actual SQL/Wasm qualification: both sizes pass every operation and exact reopen.

Imports and ordered queries remain proportional to row count in these measurements.
Structural operations are broader than point edits, and the 1M bulk case remains
close to the hot-transition time budget. The fixture is representative of narrow
rows, not every record width or hardware configuration. UTF-8, cell-count, file-size,
and host resource bounds still apply. SQL range comparisons on order keys are
currently unsupported; the probe selects IDs before its bound-ID bulk update.

The separate [plugin API report](PLUGIN_API_IMPROVEMENTS.md) proposes accepted-row
lookup, indexed-state helpers, normalization feedback, ordering helpers, query
planning improvements, and observable resource budgets.

## Full local CI follow-up

The follow-up validation removed one obsolete synchronous-WASI import that
failed Clippy after the asynchronous host migration. On Linux, all three CI
Clippy commands passed with `-D warnings`: the root workspace, tooling workspace,
and E2E targets with CI's explicit feature list.

The broader CI checks also passed:

- Root all-feature nextest suite: 5,080 passed, 89 skipped.
- Tooling nextest suite: 69 passed; E2E suite: 228 passed, 27 skipped.
- Root and tooling doctests, default-feature test compilation, Rust 1.93 consumer
  compilation, and an external WASIp2 plugin built from the packaged crates.
- Native SDK: 281 passed; filesystem package: 3 passed.
- Release-Wasm browser SDK: 49 passed, 1 skipped; OPFS: 56 passed.
- SDK/storage typechecks and builds, package validation, both packed Vite
  production checks, and 74 CI/release-script tests.

These are local executions of the Linux CI checks. Platform-specific hosted
jobs and artifact publication remain separate CI results.
