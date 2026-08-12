# Plugin document-scale profile

This profile guards the invariant that an atomic plugin-backed import is
bounded by the configured live-Store working set, not by its document count.
The executable conformance fixture is
`tests/plugin_document_scale.rs`.

## Reproduce

Build the release fixture once, then time the executable directly so Cargo and
compilation are outside the measurement:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 \
  cargo test --release -p lix_e2e \
  --test plugin_document_scale --no-run

PLUGIN_DOCUMENT_SCALE_BIN="$(find target/release/deps -maxdepth 1 \
  -type f -executable -name 'plugin_document_scale-*' -print -quit)"

/usr/bin/time -v "$PLUGIN_DOCUMENT_SCALE_BIN" \
  --exact atomic_markdown_import_scales_to_1184_documents \
  --nocapture --test-threads=1

perf stat -x, \
  -e task-clock,cycles,instructions,branches,branch-misses,cache-misses \
  "$PLUGIN_DOCUMENT_SCALE_BIN" \
  --exact atomic_markdown_import_scales_to_1184_documents \
  --nocapture --test-threads=1
```

The fixture installs the production Markdown component before timing, then
executes one public `execute_batch` statement containing 1,184 fresh Markdown
documents. It verifies the committed file count, semantic rows, and exact bytes
from the first and last files. CSV, Git-text, and Excalidraw run the same
capacity-plus-one conformance check. A sequential Git-text fixture imports 17
files, then updates them in deterministic path order to prove retained session
observations survive cache eviction between atomic batches.

## Results

Measured on Linux x86-64 with Rust nightly 1.97.0 and the release profile.
Seven warm-cache runs used fresh processes and the same 30,932-byte logical
fixture:

| Metric | Result |
| --- | ---: |
| Documents | 1,184 |
| Atomic import p50 | 594.637 ms |
| Atomic import p95 | 604.307 ms |
| p50 per document | 0.502 ms |
| Warm whole-process wall time | 680 ms |
| Warm peak RSS | 148,848 KiB |
| Swaps | 0 |

A profiled warm run completed the measured import in 577.842 ms with these
counters:

| Counter | Result |
| --- | ---: |
| task-clock | 775.27 ms |
| cycles | 2,349,944,681 |
| instructions | 4,583,514,106 |
| branches | 839,576,968 |
| branch misses | 14,714,570 |
| cache misses | 41,646,839 |

The exact `origin/main` baseline (`f860b30ea`) rejects the capacity-plus-one
fixture before commit with `LIX_ERROR_PLUGIN_RESOURCE_LIMIT`: its 16 live
Stores are held by the first 16 fresh documents. The candidate keeps that same
16-Store resource ceiling, retires completed fresh- or existing-document Stores
as pressure requires, preserves actively contested same-file leases, and
retains the most recent bounded working set for reuse.
