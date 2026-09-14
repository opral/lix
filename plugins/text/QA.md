# Text plugin qualification

Baseline: `main` at `4c29bd555`. Four review rounds covered
correctness/roundtripping, SQL editing/merge behavior, and performance. Randomized
core reviews passed in rounds 2 and 3; the schema integration audit then found
and fixed the late-NUL fallback edge case. The final scaling review confirmed
the memory fix and found no further algorithmic defect.

## Regressions covered

- Nonfinal unterminated SQL rows reject before corrupting persisted identities.
- UTF-8, invalid UTF-8, BOM bytes, CR, LF, mixed endings, empty files and missing
  final LF preserve exact bytes. NUL within the first 8,000 bytes remains raw; later NUL uses the byte fallback
  because SQL text columns reject NUL.
- SQL insert/update/delete/reorder, failed-update rollback, file edits after
  SQL edits, cold reopen and historical byte reconstruction.
- Concurrent gap insertions retain both IDs and deterministic UUID tie order.
- Encoding-switch merges retain exactly one payload representation; both
  branch orientations and both sides switching are exercised.
- A conflicting final-LF removal plus append fails without changing the target.
- A 4 MiB single line succeeds through the real Wasm plugin, including a large
  file-range replacement. This test was previously ignored.
- Duplicate matching consumes each old line once; sequential end allocations
  remain bounded; sparse deletion sends no unchanged bytes; repeated edits do
  not retain full historical buffers; content edits write no identity pages.

The permanent randomized test runs 10,000 deterministic byte edits and checks
row replay, rendered splices, identities and hydration. Reviewers also ran
100,000 random byte-splice cases and 20,000 tied-order cases per final review
round in independent scratch harnesses.

Final validation: **32 plugin tests, 9 compiled-plugin tests, and 50 schema
tests passed**, including both opt-in scaling probes. The fourth review found
no remaining actionable issue and additionally checked all 256 byte values
after the text-classification prefix.

## Reproduce

```sh
cargo test -p plugin_text --lib
cargo test --manifest-path tooling/Cargo.toml -p lix_e2e \
  --no-default-features --features sdk-tests --test git_text_plugin

# Native core with real SDK rows; median five; plugin compiled optimized.
cargo test -p plugin_text --lib \
  --config 'profile.test.package.plugin_text.opt-level=3' \
  text_core_scaling_probe -- --ignored --nocapture

# Actual SQL + Wasm + in-memory storage; median three updates, one import.
cargo test --manifest-path tooling/Cargo.toml -p lix_e2e \
  --no-default-features --features sdk-tests --test git_text_plugin \
  text_sql_scaling_probe -- --ignored --nocapture
```

Probes print timings rather than enforce machine-dependent thresholds. Normal
regressions assert exact bytes, IDs, bounded retained storage and edit payloads.
Plugin installation is outside the integration probe's timed regions.

## Before/after algorithm measurements

Rust 1.97.0-nightly, x86_64, Cargo release, median five invocations, 100,000
36-byte lines (3.6 MB). Actual baseline and measured revision `ff848a05e` core/model/order
sources were tested with the same minimal BTreeMap row shim and pinned
base64/UUID dependencies. Raw results: [baseline](benchmarks/core-baseline.csv)
and [current](benchmarks/core-current.csv). These compare core algorithms;
they exclude the real SDK row representation, Wasm, SQL and storage. That
revision predates the final late-NUL fallback and LF-default fixes; the current
real-SDK and end-to-end probes below run the final implementation.

| Operation | Main, ms | Revised, ms |
| --- | ---: | ---: |
| Open document | 4.940 | 4.928 |
| Open and emit every row | 31.834 | 41.390 |
| Local file edit | 11.711 | 7.414 |
| One row edit | 23.771 | 13.667 |
| Repeated-line reconciliation | 1,236.636 | 8.171 |
| Two distant row deletions | 22.776 | 17.777 |

Duplicate reconciliation is about 151× faster in this workload. Sparse deletion
previously sent 3,599,821 unchanged bytes; now it sends two deletions and zero
insert bytes. Twenty thousand end allocations require at most 680,000 order
characters instead of 50,020,000. After 100 edits to different lines in a
360 KB file, retained backing capacity falls from 36,589,824 to 953,424 bytes.

The five-column readable SQL schema costs about 30% more to materialize in
this shim than the old three-column base64 schema. This is an explicit
usability/performance tradeoff; cold document construction itself is unchanged.
Do not interpret these shim figures as end-to-end timings.

## Current native core with real SDK rows

The committed probe at `6a7910275`, optimized plugin and default test-profile dependencies,
produced the following medians on this machine. Inputs have 35-byte lines;
`open + emit` includes constructing all actual typed rows.

| Lines | Bytes | Open + emit, ms | File edit, ms | Row edit, ms | Duplicates, ms | Sparse delete, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 35,000 | 0.578 | 0.083 | 0.137 | 0.105 | 0.159 |
| 10,000 | 350,000 | 6.131 | 0.845 | 1.557 | 1.024 | 1.837 |
| 100,000 | 3,500,000 | 103.551 | 8.761 | 17.926 | 10.443 | 22.933 |

Every sparse deletion case inserts zero bytes. Native row materialization
numbers differ from the shim because these use the actual SDK representation
and different dependency optimization settings.

## Current end-to-end SQL and Wasm probe

The committed integration probe, default test profile and canonical in-memory
storage, produced the following results. Import is one sample; updates are the
median of three single-line changes. The qualification suite ran concurrently,
so treat these as local scaling observations, not dedicated release benchmarks.

| Lines | Bytes | Import, ms | File update, ms | SQL update, ms |
| --- | ---: | ---: | ---: | ---: |
| 1,000 | 35,000 | 14.661 | 1.783 | 1.849 |
| 10,000 | 350,000 | 87.062 | 7.459 | 7.797 |
| 100,000 | 3,500,000 | 897.763 | 66.457 | 70.524 |

These include actual SQL execution, compiled Wasm projection, commits and
in-memory storage. They are current-only measurements; the native before/after
speedup must not be extrapolated to this whole pipeline. Different storage
adapters and release profiles require separate measurements.

## Scope and remaining API constraints

This establishes lossless Git-style line segmentation and editable SQL rows,
not complete equivalence to Git's merge UI or unlimited input sizes. Matching,
merge conflict reporting and engine resource budgets remain Lix contracts.
Local edits still hydrate and process the complete document; complexity is
approximately linear in document size, not constant-time. Independent removal
of a final LF and append can violate a cross-row invariant and is rejected
atomically instead of inventing a newline. Rows exceeding the engine's bounded
record/page budget still need an API-level solution. See the separate
[plugin API proposals](PLUGIN_API_IMPROVEMENTS.md) for evidence and proposed
refactors.
