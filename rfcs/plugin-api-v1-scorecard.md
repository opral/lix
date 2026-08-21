# Plugin API v1 scorecard

> Historical design record, superseded by
> [Universal plugin API v2 contract](universal-plugin-api.md). The gates,
> pending lanes, and decision figures below describe an earlier prototype and
> are not acceptance evidence for the selected `lix:plugin@2.0.0` API.

Measured on 2026-07-29 from `origin/main` at `57d619aad`. Release results are
from a local Linux x86-64 build and are intended for relative design guidance,
not as stable cross-machine latency thresholds.

## v2 production baselines

These use the production Wasmtime Component runtime and real format plugins.

| workload | input | transition | elapsed | guest high water | boundary |
| --- | ---: | --- | ---: | ---: | ---: |
| CSV, 220,000 rows | 10,680,000 B | initial import | 1.62 s test total | 53,018,624 B | paged full import |
| CSV, 220,000 rows | 10,680,000 B | cold restore | 0.86 s combined test | 61,997,056 B | paged rows/render |
| CSV, 220,000 rows | 10,680,000 B | one-row warm edit after restore | included above | 61,997,056 B | one row |
| JSON, 39,870 properties | 10,485,760 B | initial import | 1,688.546 ms | 26,673,152 B | full source |
| JSON, one scalar | 10,485,760 B | verified warm edit | 5.500 ms engine | 26,673,152 B | 418 B |
| JSON, one scalar | 10,485,760 B | ordinary public-SQL warm edit, 7 samples | 6.724 ms p50 / 8.602 ms p95 | 26,673,152 B control | full-byte request, sparse plugin update |
| Markdown, `vscode-docs` `d5badf` | 3,276,550 changed B | one-commit replay | 8.95–9.22 s | 102,367,232 B | see format profile |

The JSON hot path is the important control: v2 already crosses only 418 bytes
and does not reparse or rerender the document. A v3 implementation must reduce
total ownership while completing the verified-splice transition in at most
2.750 ms. The independently sampled ordinary public-SQL path has an 8.602 ms
p95, making its v3 target 4.301 ms. Its median hot transition allocated
10,996,407 bytes and reached a 10,595,159-byte peak live allocation delta.
Counting only the 10,485,760-byte accepted host blob plus the measured
26,673,152-byte guest high water gives a conservative v2 total of 37,158,912
bytes, so the v3 peak must be at most 12,386,304 bytes.

Reproduction:

```sh
cargo test -p lix --lib \
  csv_v2_initial_import_retains_64_mib_efficiency_invariant \
  --release -- --ignored --nocapture

cargo test -p lix --lib \
  csv_v2_cold_open_and_warm_edit_retain_64_mib_efficiency_invariant \
  --release -- --ignored --nocapture

cargo test -p lix_e2e --test e2e \
  v2_json_ten_mib_real_wasm_edit_stays_sparse_and_bounded \
  --release -- --ignored --nocapture

cargo test -p lix_e2e --test e2e \
  v2_json_ten_mib_ordinary_sql_byte_edit_benchmark \
  --release -- --ignored --nocapture
```

## v3 arena ownership model

The deterministic scorecard applies one one-byte edit, one complete row
replacement, and one opaque state-page replacement to unique-content fixtures.
The v2 retained estimate counts only two exact accepted byte owners plus the
changed values; it deliberately excludes allocator and parsed-index overhead.
The v3 number is the actual unique content-page storage after both roots.

| format fixture | bytes | conservative v2 retained | v3 unique pages | reduction | modeled v3 boundary |
| --- | ---: | ---: | ---: | ---: | ---: |
| Markdown | 8,388,660 | 16,777,408 | 8,388,749 | 50.0% | 82 B |
| CSV | 10,680,040 | 21,360,158 | 10,680,119 | 50.0% | 72 B |
| JSON | 8,388,669 | 16,777,418 | 8,388,750 | 50.0% | 74 B |
| Excalidraw | 8,388,673 | 16,777,438 | 8,388,766 | 50.0% | 86 B |

The host rope operation itself does not introduce a latency concern. This
release run used 100 warmups and 5,000 measured edits per format, alternated the
v2/v3 measurement order, and reports per-operation distributions:

| format | whole-`Vec` p50 / p95 | arena p50 / p95 | p95 speedup | retained reduction | gate |
| --- | ---: | ---: | ---: | ---: | --- |
| Markdown | 162.550 / 181.749 µs | 32.470 / 40.460 µs | 4.492× | 2.000× | fail memory |
| CSV | 235.589 / 275.049 µs | 41.500 / 53.599 µs | 5.132× | 2.000× | fail memory |
| JSON | 160.260 / 177.839 µs | 32.779 / 40.240 µs | 4.419× | 2.000× | fail memory |
| Excalidraw | 161.040 / 177.889 µs | 32.710 / 40.530 µs | 4.389× | 2.000× | fail memory |

Reproduction:

```sh
cargo test -p lix --test plugin_arena_scorecard -- --nocapture
cargo test -p lix --test plugin_arena_scorecard --release \
  four_format_warm_edit_latency_scorecard -- --ignored --nocapture

# The same benchmark exits nonzero if any lane misses 2x latency or 3x memory.
LIX_PLUGIN_V1_ENFORCE_ACCEPTANCE=1 \
  cargo test -p lix --test plugin_arena_scorecard --release \
  four_format_warm_edit_latency_scorecard -- --ignored --nocapture
```

## Hard acceptance gates

Every row is an independent pass/fail gate. “Pending” is not a pass.

| format | workload | v2 p95/control | required v3 p95 | conservative v2 total memory | required v3 total memory |
| --- | --- | ---: | ---: | ---: | ---: |
| JSON | 10 MiB verified-splice warm scalar edit | 5.500 ms control | ≤2.750 ms | 37,158,912 B | ≤12,386,304 B |
| JSON | 10 MiB ordinary public-SQL warm scalar edit | 8.602 ms p95 | ≤4.301 ms | 37,158,912 B | ≤12,386,304 B |
| Markdown | `vscode-docs` one-commit replay | 9.220 s | ≤4.610 s | ≥105,643,782 B | ≤35,214,594 B |
| CSV | 10.68 MiB warm row edit | pending isolated p95 | pending | ≥72,677,056 B | ≤24,225,685 B |
| Excalidraw | large element/file edit | pending baseline | pending | pending baseline | pending |

The final scorecard also requires cold import, warm row edit, eviction
restore, and merge lanes for all four formats. It measures process peak delta
and separately reports host unique arena bytes and guest high water so a lower
Wasm heap cannot hide host duplication.

## Decision

Accepted as the production hard cut after the Wasmtime binding, fused push
sinks, universal row pages, certified storage batches, and sparse arena state were
connected end to end. The measured matched lanes were:

| format | v2 p50 | v3 p50 | speedup | memory result |
| --- | ---: | ---: | ---: | --- |
| JSON, 10 MiB import | 618.340 ms | 287.018 ms | 2.15× | host allocation 352.4 MB → 107.3 MB |
| Excalidraw transition | 243.150 ms | 3.507 ms | 69.3× | guest high water 42.14 MB → 25.17 MB |
| Markdown transition | 34.090 ms | 6.236 ms | 5.47× | peak host allocation down 66.6% |
| CSV, 10.68 MiB import | 5.169 s | 2.066 s | 2.50× | peak live host allocation 929.7 MB → 282.4 MB |

The final CSV storage-layout cut measured 146.824 ms p50 against the preceding
141.625 ms lane, a 3.7% difference below the 10% “keep profiling” threshold.
Correctness gates cover exact bytes, exact semantic rows, atomic rollback,
cold reopen, sparse successors, history/time travel, and conflict composition
for Markdown, CSV, JSON, and Excalidraw.
