# Excalidraw editing and scale

The plugin uses native element IDs and a paged ID-to-span lookup for warm SQL
content updates. File splices are grouped by affected element. Both paths preserve
unrelated raw bytes, including embedded images. Indexed element no-ops avoid state writes.

The native regression suite checks 100, 1,000, and 10,000 elements with a 1 MB
attachment. A single SQL element edit reads fewer than 128 file bytes and 16 KiB
of index state in those fixtures. Repeated growth/shrink edits write at most 36
bytes of shift state. These are guest-facing transition counters, excluding
harness setup, host row lookup, storage, transaction work, and output validation.

Import, structural edits, and cold serialization stream row records instead of
retaining whole-scene JSONB row collections. A compiled regression imports
100,000 elements, edits the beginning/middle/end, appends and deletes an element,
then reopens and performs another real SQL edit. This workload previously failed
with guest allocation exhaustion during import. It now passes without increasing
runtime memory limits. A separate 4,096-to-4,097 changed-element regression checks
overlay compaction and exact reconstruction of attachment offsets.

## Measurement method

`packages/e2e/tests/plugin_api_benchmarks.rs` uses the real SQL API, compiled Wasm
component, and in-memory storage. The ignored Excalidraw profile covers 100,
1,000, and 10,000 elements with either no attachment or a 1,000,000-byte data URL.
Each edit series has three warmups and 21 measured samples, alternating value
length and cycling through the first, middle, and last element. Expected fixture
serialization and exact-byte assertions are outside the edit timing scope.

Import measurements exclude initial component compilation through an empty-file
warmup. Structural append and SQL deletion are timed separately. Reopen timing
includes opening and reading; a separate timing covers the first real SQL edit
after reopening. The profile asserts exact bytes after every operation, including
that cold edit. It does not count a no-op reopen write as successful editing.

Allocation counters measure **host allocator traffic** and **host peak live-byte
delta** during the operation. They exclude Wasm linear memory and are not process
RSS or retained workspace size. All profiles use the repository's test profile
and default Wasm limits; these are not release-build latency claims.

The matched baseline is `d0bac8228` (main after PR #1788), whose tree matches the
local integration base `d012b4911`. The same test executable and host runtime can
load a baseline plugin archive with `LIX_EXCALIDRAW_PROFILE_ARCHIVE`, holding the
host implementation constant. Both plugin components use Cargo's artifact
dependency test profile.

## Matched results

Measured on 2026-09-14 on Linux x86_64, AMD Ryzen 9 9950X (32 logical
CPUs), nightly-2026-05-21, with no concurrent builds. Candidate: `24eaeda18`.
[Raw measurements and all point samples](excalidraw-measurements.json) include
host allocation counters, import, structural, and cold timings. Times below are
milliseconds; each cell shows p50 / p95.

| Elements | Attachment bytes | Baseline SQL | Current SQL | Baseline file | Current file |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 0 | 2.850 / 2.971 | 1.612 / 1.661 | 1.919 / 1.941 | 1.527 / 1.536 |
| 100 | 1,000,000 | 11.608 / 11.896 | 1.969 / 2.176 | 1.927 / 1.971 | 1.932 / 1.961 |
| 1,000 | 0 | 7.561 / 7.623 | 1.749 / 1.818 | 1.618 / 1.676 | 1.641 / 1.670 |
| 1,000 | 1,000,000 | 17.243 / 18.008 | 2.168 / 2.405 | 2.073 / 2.129 | 2.120 / 2.202 |
| 10,000 | 0 | 61.351 / 61.962 | 2.257 / 2.557 | 2.065 / 2.163 | 2.105 / 2.376 |
| 10,000 | 1,000,000 | 71.864 / 72.463 | 3.133 / 3.493 | 2.954 / 3.252 | 2.970 / 3.238 |

At 10,000 elements, warm SQL median latency improves about 27× without an
attachment and 23× with the 1 MB attachment. Single-element file edit timings
remain similar; the file-path changes chiefly address batched edits and fallback
scaling. These are single-run distributions, not cross-machine guarantees.

The baseline cannot import the 100,000-element fixture under the same Wasm
limits. The current component completes both larger lifecycle profiles:

| Attachment bytes | Import | SQL p50 / p95 | File p50 / p95 | Append | Delete | First cold SQL edit |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 2083.8 | 6.546 / 7.194 | 6.165 / 6.646 | 875.9 | 704.4 | 1739.8 |
| 1,000,000 | 2020.9 | 7.381 / 7.757 | 6.780 / 6.940 | 889.2 | 684.3 | 1677.0 |

Import, append, delete, and cold figures are single observations. At 100,000
elements, import host allocation traffic is about 1.46–1.48 GB and peak live-byte
delta is about 278–281 MB; streaming removes the guest allocation failure but
does not make large-scene import cheap.

## Reproduce

Run the regular qualification (including the 100,000-element lifecycle):

```sh
cargo test --manifest-path tooling/Cargo.toml -p lix_e2e \
  --features sdk-tests,plugin-tests,storage-benches,slatedb \
  --test plugin_api_benchmarks excalidraw -- --nocapture
```

Run distributions for 100–10,000 elements:

```sh
cargo test --manifest-path tooling/Cargo.toml -p lix_e2e \
  --features sdk-tests,plugin-tests,storage-benches,slatedb \
  --test plugin_api_benchmarks profile_excalidraw_point_edits \
  -- --ignored --nocapture
```

Set `LIX_EXCALIDRAW_PROFILE_ROWS=100000` for the larger profile, or provide a
comma-separated list. `LIX_EXCALIDRAW_PROFILE_ARCHIVE=/absolute/baseline.lixplugin`
selects an independently built baseline component with its matching manifest and
schemas. Without that variable, the harness packages the current Cargo artifact.
A second way to reproduce a comparison is to copy the benchmark source into a
worktree at the baseline commit and run the same commands there. Keep compiler
versions and profiles equal and avoid concurrent compilation during measurements.

## Boundaries

Import and structural changes still scale with scene size. The first SQL edit
after a cold reopen reconstructs from durable rows; it is substantially slower
than an indexed warm edit. A bounded overlay records up to 4,096 distinct
element-length changes before rebuilding indexes. Rebuilds apply those shifts in
a linear sweep. Attachment row edits use the full-document path.

The tested elements are small rectangles. A scene with 100,000 complex freehand
paths or large per-element custom objects has a different memory envelope. The
100,000-element result is a qualified fixture, not an unlimited-size guarantee.
Host transaction and storage costs remain visible even when guest reads are
bounded, so the SQL interface does not have file-size-independent latency.
