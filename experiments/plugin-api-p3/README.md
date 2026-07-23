# Lix plugin API Preview 3 candidate

This is an executable, deliberately isolated candidate for one question:

> Can Component Model async make large-file plugin authorship simpler without
> giving up the persistent-document and sparse-update gains of plugin API v2?

It does not replace the production v2 runtime. The experiment runs a real
WebAssembly component, indexes a normal top-level JSON object, changes one
property in a roughly 10 MiB file, and compares:

- a v1-shaped full-file reopen;
- a whole-buffer `list<u8>` cold open;
- a Preview 3 `stream<u8>` cold open;
- sparse edits on persistent documents created by either cold path; and
- matched sync/async variants of the hot edit ABI, random reads, and sparse
  output.

## Candidate boundary

The hybrid boundary in [`wit/plugin.wit`](wit/plugin.wit) keeps only genuinely
long-running or large transfers asynchronous:

```wit
open-list: async func(file: list<u8>) -> result<open-result, plugin-error>;
open-stream: async func(
  file: own<byte-source>,
) -> result<open-result, plugin-error>;

resource document {
  fork: func() -> own<document>;
  stats: func() -> document-stats;
  file-changed: func(
    before: own<byte-source>,
    after: own<byte-source>,
    edits: list<input-splice>,
  ) -> result<file-transition, plugin-error>;
}
```

The host-backed source offers synchronous random reads for a localized warm
edit and a P3 byte stream for cold sequential hydration. Large output is a
typed stream paired with an authoritative terminal future:

```wit
record entity-summary-stream {
  count: u64,
  items: stream<entity-summary>,
  done: future<result<_, plugin-error>>,
}
```

That stream-plus-terminal-future shape follows the native P3 pattern: stream
closure and operation success are separate, so the receiver must drain or drop
the stream and still await `done`.

The executable WIT also contains three explicitly benchmark-only
`file-changed-async-*` methods and one `read-async` import. They are retained so
the sync/async decision is reproducible; they are not part of the recommended
candidate boundary.

## What this tests

The guest uses generated `wit-bindgen` bindings directly. There is no Lix SDK
facade in this experiment. This makes the authorship result honest:

- P3 removes custom `next-page`/pollable resource protocols from cold
  transfers.
- A plugin can consume input with ordinary Rust `async`/`await`.
- Producing an output stream still requires a spawned producer, backpressure
  handling, cancellation handling, and a terminal future.
- For a resident source and one changed 262-byte JSON member, warm sparse edits
  are measurably cheaper as synchronous calls. Making the complete interface
  async adds ABI/scheduling machinery without improving this operation's
  algorithm.

The executable WIT uses typed entity summaries to measure the simplest raw
author experience. A production Lix v3 should retain v2's bounded packet-v1
pages inside the stream if typed per-entity Canonical ABI lowering proves
materially slower.

## JSON model and limits

The candidate treats each member of a top-level JSON object as one entity.
Entity identity is derived from the decoded property key, and the incremental
index retains only property ranges, IDs, and hashes—not a second complete copy
of the file. The parser handles strings and escapes plus nested object/array
values across arbitrary input chunk boundaries.

Cold-stream memory is proportional to the transport chunk plus the largest
top-level property because this proof buffers one complete member while
validating it. The retained fixture has 262-byte members; a single multi-
megabyte property would therefore use materially more guest memory. A
production parser can remove that dependency by retaining incremental syntax
and hash state.

The warm proof intentionally accepts one equal-length edit contained in one
existing property. It imports and validates only that property's before/after
bytes, returns one complete changed property snapshot, and produces an
immutable successor. Structural insert/delete/reorder, length-changing edits,
nested entity identity, merge groups, transition budgets, packet-v1, and
production actor recovery remain v2 responsibilities and are outside this
small ABI experiment.

## Toolchain

The manifests pin the latest published experiment dependencies used by this
candidate:

- Wasmtime and `wasmtime-wasi` 47.0.2;
- `wit-bindgen` 0.60.0; and
- Rust nightly 2026-07-23.

Rust recognizes `wasm32-wasip3`, but rustup does not distribute its standard
library yet. The guest is therefore built as a `wasm32-wasip2` reactor that
exports the genuine async Canonical ABI (`async func`, `stream<T>`, and
`future<T>`). Wasmtime hosts that ABI with its experimental P3 implementation.
This is an executable interface candidate, not a claim that the Rust-native
WASI P3 application target or Wasmtime's P3 library is production-ready.

## Build and run

From the repository root:

```sh
cargo build --manifest-path experiments/plugin-api-p3/Cargo.toml \
  -p plugin-api-p3-guest --target wasm32-wasip2 --release

wasm-tools validate \
  --features=component-model,cm-async \
  target/wasm32-wasip2/release/plugin_api_p3_guest.wasm

cargo run --manifest-path experiments/plugin-api-p3/Cargo.toml \
  -p plugin-api-p3-host --release -- \
  target/wasm32-wasip2/release/plugin_api_p3_guest.wasm
```

The host validates transport equivalence, exact entity summaries, stable
identity, immutable fork/successor behavior, fail-closed truncated streams,
full-data streams whose terminal future fails, bounded warm source reads, a
host-capped entity output count, output-receiver cancellation with terminal
completion, and the configured per-linear-memory ceiling before printing
timings. The harness defaults to 64 MiB for comparison with PR2, but that is
not an API limit; set
`LIX_P3_GUEST_LINEAR_MEMORY_LIMIT_MIB` to test a larger or smaller deployment
budget. `LIX_P3_MAX_ENTITY_SUMMARIES` independently configures the host output
budget and defaults to one million. The matched hot benchmark defaults to
2,400 warmup rounds and 24,000 measured rounds per arm. Override those with
`LIX_P3_HOT_WARMUPS` and `LIX_P3_HOT_SAMPLES`; set
`LIX_P3_HOT_PRINT_RAW=1` to print every paired sample.

## Retained result

On the retained 10,485,811-byte / 39,870-property JSON fixture, the P3 cold
stream was latency-neutral versus `list<u8>` (109.146 versus 109.749 ms p50)
while reducing the largest guest linear memory from 15.625 MiB to 6.625 MiB.
Sequential synchronous persistent edits read only the affected 262-byte
property from each source and measured 0.006031 ms p50, versus 109.441 ms for
the v1-shaped full-reopen control. Cold and microsecond-scale p95s remain noisy
in this VM run and are not an adoption signal.

The hot-path decision uses five fresh processes pinned to one CPU, with 24,000
paired samples per arm in each process and all 24 execution orders:

| Hot variant | Process-median p50 | Process-median p95 |
|---|---:|---:|
| Sync export, sync reads, inline change | 6.632 µs | 7.595 µs |
| Async export, sync reads, inline change | 7.064 µs | 8.076 µs |
| Async export, ready async reads, inline change | 7.855 µs | 9.228 µs |
| Async export, sync reads, streamed change | 13.135 µs | 18.324 µs |

Relative to its matched baseline, async export added a process-median 0.491 µs
(7.6%), ready async reads added another 0.773 µs (10.9%), and streaming the
single change added 6.030 µs (86.0%). All five processes observed each async
variant as slower. Across fresh-process p50 deltas, ready-async reads and
one-item streaming were materially beyond the 0.5 µs margin; async export's
95% interval crossed that margin. Because async export adds no capability to
this resident hot path, the selected boundary is still the synchronous export
with synchronous random reads and inline sparse output.

The reported costs are paired per-process deltas, not subtraction of the
separately aggregated arm medians in the table.

That decision is deliberately narrow. A source that actually waits on remote
or disk I/O may benefit from async concurrency, and larger or multi-entity
outputs need a payload-size/memory break-even sweep before choosing inline
lists over streams.

See [`RESULTS.md`](RESULTS.md) for the complete p50/p95 matrix, counters,
limitations, and retained raw output.

See [`AX-EVAL.md`](AX-EVAL.md) for the exploratory no-docs authorship cohort
and the resulting recommendation to test a small transport helper before
introducing a broad SDK.

## Decision rule

P3 is useful here only if it preserves the v2 sparse warm mechanism and makes
cold large-file transfer measurably more bounded. Adoption should remain a
dual-version experiment until all of the following hold:

1. the P3 stream open has materially lower guest high-water than `list<u8>`;
2. documents opened by list and stream have equivalent warm p50/p95 in a
   paired, sufficiently powered run;
3. a one-property warm edit stays proportional to one JSON member;
4. production packet validation, cancellation, actor retirement, and the full
   v2 correctness matrix are ported; and
5. the Rust guest target and Wasmtime P3 host surface are supportable in
   production.

See the [WASI 0.3 launch](https://bytecodealliance.org/articles/WASI-0.3) and
the [Component Model async design](https://component-model.bytecodealliance.org/design/async.html)
for the standard's async and stream semantics.
