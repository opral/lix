# Plugin API v3 prototype scorecard

Measured on 2026-07-29 from `origin/main` at `57d619aad`. Release results are
from a local Linux x86-64 build and are intended for relative design guidance,
not as stable cross-machine latency thresholds.

## v2 production baselines

These use the production Wasmtime Component runtime and real format plugins.

| workload | input | transition | elapsed | guest high water | boundary |
| --- | ---: | --- | ---: | ---: | ---: |
| CSV, 220,000 rows | 10,680,000 B | initial import | 1.62 s test total | 53,018,624 B | paged full import |
| CSV, 220,000 rows | 10,680,000 B | cold restore | 0.86 s combined test | 61,997,056 B | paged entities/render |
| CSV, 220,000 rows | 10,680,000 B | one-row warm edit after restore | included above | 61,997,056 B | one row |
| JSON, 39,870 properties | 10,485,760 B | initial import | 1,688.546 ms | 26,673,152 B | full source |
| JSON, one scalar | 10,485,760 B | verified warm edit | 5.500 ms engine | 26,673,152 B | 418 B |
| Markdown, `vscode-docs` `d5badf` | 3,276,550 changed B | one-commit replay | 8.95–9.22 s | 102,367,232 B | see format profile |

The JSON hot path is the important control: v2 already crosses only 418 bytes
and does not reparse or rerender the document. A v3 implementation must reduce
total ownership while completing the same transition in at most 2.750 ms.
Counting only the 10,485,760-byte accepted host blob plus the measured
26,673,152-byte guest high water gives a conservative v2 total of 37,158,912
bytes, so the v3 peak must be at most 12,386,304 bytes.

Reproduction:

```sh
cargo test -p lix_sdk --lib \
  csv_v2_initial_import_retains_64_mib_efficiency_invariant \
  --release -- --ignored --nocapture

cargo test -p lix_sdk --lib \
  csv_v2_cold_open_and_warm_edit_retain_64_mib_efficiency_invariant \
  --release -- --ignored --nocapture

cargo test -p lix_sdk_tests --test e2e \
  v2_json_ten_mib_real_wasm_edit_stays_sparse_and_bounded \
  --release -- --ignored --nocapture
```

## v3 arena ownership model

The deterministic scorecard applies one one-byte edit, one complete entity
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

The host rope operation itself does not introduce a latency concern:

| format | whole-`Vec` successor mean | arena successor mean | ratio |
| --- | ---: | ---: | ---: |
| Markdown | 333.408 µs | 32.564 µs | 0.098 |
| CSV | 392.008 µs | 40.465 µs | 0.103 |
| JSON | 166.892 µs | 32.466 µs | 0.195 |
| Excalidraw | 238.517 µs | 32.434 µs | 0.136 |

Reproduction:

```sh
cargo test -p lix_plugin_arena --test scorecard -- --nocapture
cargo test -p lix_plugin_arena --test scorecard --release \
  four_format_warm_edit_latency_scorecard -- --ignored --nocapture
```

## Hard acceptance gates

Every row is an independent pass/fail gate. “Pending” is not a pass.

| format | workload | v2 p95/control | required v3 p95 | conservative v2 total memory | required v3 total memory |
| --- | --- | ---: | ---: | ---: | ---: |
| JSON | 10 MiB warm scalar byte edit | 5.500 ms | ≤2.750 ms | 37,158,912 B | ≤12,386,304 B |
| Markdown | `vscode-docs` one-commit replay | 9.220 s | ≤4.610 s | ≥105,643,782 B | ≤35,214,594 B |
| CSV | 10.68 MiB warm row edit | pending isolated p95 | pending | ≥72,677,056 B | ≤24,225,685 B |
| Excalidraw | large element/file edit | pending baseline | pending | pending baseline | pending |

The final scorecard also requires cold import, warm entity edit, eviction
restore, and merge lanes for all four formats. It measures process peak delta
and separately reports host unique arena bytes and guest high water so a lower
Wasm heap cannot hide host duplication.

## Decision

The immutable host arena design passes its prototype correctness gates. It is
**not yet accepted as the production v3 hard cut**: its conservative retained
byte model improves by only 2×, below the required 3×, and the scorecard does
not include a Wasmtime v3 binding or format-specific page codecs. The rope-only
latency result cannot prove the required 2× end-to-end p95.

Accordingly this prototype keeps the production v2 runtime selected. The next
implementation step is to bind the parsed WIT transaction/root resources in
Wasmtime and move each declared format state page behind those resources.
Changing plugin manifests to v3 before that evidence would violate the stated
acceptance rule.
