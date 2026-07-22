# Persistent CSV Wasm proof

This isolated experiment measures one architectural change without modifying the
production CSV plugin: keep a parsed document inside a Wasm instance and exchange
small splices, instead of passing and parsing complete file views on every plugin
call.

The comparison always crosses a real Wasmtime 45 core-Wasm boundary. The guest is
compiled Rust `wasm32-unknown-unknown`; the host applies Wasmtime's `StoreLimits`
linear-memory ceiling. It does not benchmark a native implementation disguised as
a Wasm result.

## Paths under test

`stateless` is an intentionally optimistic lower bound for the current API shape:

1. Copy the complete previous and new blobs into Wasm.
2. Parse both as CSV, including quoted commas, escaped quotes, and quoted newlines.
3. Hash decoded cells and identify changed rows and cells.
4. Copy a complete rendered blob back out of Wasm.

It omits current production costs such as component canonical ABI lowering,
entity JSON serialization/deserialization, UUID/order-key handling, and the full
row diff. This biases the experiment in favor of stateless calls.

`persistent` does this:

1. Cold-hydrate a complete blob once, retaining compact row/cell metadata in the
   guest.
2. On each edit, compute a common-prefix/suffix splice in the native host.
3. Send only the inserted bytes plus scalar splice arguments to Wasm.
4. Update the retained document, parse and compare only the touched row, and
   return a 64-byte splice descriptor plus inserted bytes.
5. Apply that returned patch in the host and byte-compare the result with the
   expected full CSV.

Every measured iteration asserts exact output, exactly one changed row, and
exactly one changed decoded cell.

## Reproduce

Run from the `vendor/lix` directory at commit `115350f9` or later:

```sh
cargo build \
  --manifest-path experiments/persistent-csv-wasm/guest/Cargo.toml \
  --target wasm32-unknown-unknown \
  --release

cargo build \
  --manifest-path experiments/persistent-csv-wasm/Cargo.toml \
  --release

# Production-like memory limit. The 10 MiB stateless case is expected to trap.
target/release/persistent-csv-wasm-bench \
  --size-mib all \
  --iterations-1mib 200 \
  --iterations-10mib 100 \
  --memory-mib 64

# Relax only the ceiling so a 10 MiB stateless lower-bound timing is measurable.
target/release/persistent-csv-wasm-bench \
  --size-mib 10 \
  --iterations-10mib 120 \
  --memory-mib 128
```

The repository's `.cargo/config.toml` places experiment artifacts in the shared
`vendor/lix/target` directory. The source tree remains isolated under this
directory.

## Results

Environment: aarch64 macOS 26.3.1, Rust 1.97.0-nightly
`b954122b`, Wasmtime 45.0.3, release LTO, Cranelift `OptLevel::Speed`. The Wasm
guest is 26,724 bytes. Warmups were four stateless or six persistent alternating
edits. Timings are wall-clock and were collected in one process; no other Lix
storage layer was involved.

The files slightly exceed the requested binary size because the generator emits
complete rows. Both edit one byte in the middle row of CSV with 6 decoded cells.

| File / limit | Path | Warm p50 | Warm p95 | Guest linear-memory high-water | Cold hydrate |
|---|---|---:|---:|---:|---:|
| 1,048,631 B / 64 MiB | stateless, full Wasm round trip | 2.073 ms | 3.127 ms | 7.8125 MiB | n/a |
| 1,048,631 B / 64 MiB | persistent, Wasm boundary only | 0.0062 ms | 0.0098 ms | 5.125 MiB | 1.305 ms |
| 1,048,631 B / 64 MiB | persistent, host scan + Wasm + full host materialize | 0.291 ms | 0.379 ms | 5.125 MiB | 1.305 ms |
| 10,485,841 B / 64 MiB | stateless | **traps during warmup** | — | needs more than 64 MiB | n/a |
| 10,485,841 B / 64 MiB | persistent, Wasm boundary only | 0.062 ms | 0.091 ms | 38.875 MiB | 22.658 ms |
| 10,485,841 B / 64 MiB | persistent, host scan + Wasm + full host materialize | 2.980 ms | 3.758 ms | 38.875 MiB | 22.658 ms |
| 10,485,841 B / 128 MiB | stateless, full Wasm round trip | 21.642 ms | 29.852 ms | 64.0625 MiB | n/a |
| 10,485,841 B / 128 MiB | persistent, Wasm boundary only | 0.065 ms | 0.081 ms | 38.875 MiB | 15.492 ms |
| 10,485,841 B / 128 MiB | persistent, host scan + Wasm + full host materialize | 3.080 ms | 3.548 ms | 38.875 MiB | 15.492 ms |

The repeated 10 MiB run uses the 128 MiB row for like-for-like speed ratios:

- Within this isolated prototype, the host scan + guest call + host
  materialization interval is **7.0x faster at p50** and **8.4x faster at
  p95**. This is not a Lix SQL/storage end-to-end result.
- The actual Wasm apply/return portion is **333x faster at p50**. The remaining
  persistent time is almost entirely the host's scalar full-file prefix/suffix
  scan (2.817 ms p50) and full output allocation/copy (0.180 ms p50).
- Hydration plus the first persistent edit (18.57 ms at medians) is already below
  one stateless edit (21.64 ms) in this optimistic baseline. Warm reuse is not
  needed to break even on this workload, though real cache behavior must still be
  measured.
- At 10 MiB, stateless transfers 20,971,682 input bytes and 10,485,841 output
  bytes per edit. Persistent transfers 1 input byte and 65 output bytes after a
  one-time 10,485,841-byte hydrate: about **476,600x fewer payload bytes per
  edit**. Scalar core-ABI arguments are not included in either payload count.
- The 64 MiB stateless lower bound misses the ceiling by at least one 64 KiB Wasm
  page. The real plugin's strings, entity JSON, and component-ABI temporaries are
  not represented, so this should be read as an early warning rather than as a
  precise production OOM threshold.

The two 10 MiB hydrate values differ because they came from separate runs under
shared-machine load. Hydration is a cold/cache-miss metric, not part of warm apply
percentiles.

## Architectural conclusion

The high-leverage change is a stateful/delta plugin ABI, not WASI async by itself.
For large blobs, the present full-state boundary forces O(file size) copies,
parsing, rendering, and temporary memory for a one-cell edit. A Wasm resource (or
opaque host-owned document handle tied to a guest instance) with operations like
`hydrate`, `apply-splice`, `apply-entity-changes`, and `render-patch` can retain
plugin semantics while making the edit path proportional to the affected region.

WASI Preview 3 async may improve concurrency, cancellation, and isolation around
guest calls, but it cannot remove these O(N) payload and parse costs. It is
orthogonal to the speedup measured here.

The next largest gains exposed by this proof are:

1. Accept an editor/client delta or use chunk hashes so Lix does not scan the full
   incoming blob to discover a one-byte splice.
2. Store/materialize files as chunks or a rope so a patch does not allocate and
   copy a complete 10 MiB output. If the next consumer accepts a patch, the
   10 MiB guest portion is already roughly 0.065 ms p50.
3. Define lifecycle/version semantics for persistent guest resources: cache by
   plugin/document/version, serialize mutations, invalidate on merge or plugin
   upgrade, and fail closed or rehydrate after eviction.
4. Keep a stateless compatibility path for plugins that do not implement the
   delta/resource interface.

## Limitations

- This is a dedicated core ABI, not the production component WIT. A follow-up must
  test an equivalent WIT resource to measure canonical-ABI overhead and resource
  lifecycle costs.
- The retained prototype supports one-row splices. A production implementation
  needs an affected-window parser and a rope/piece-table/B-tree for multi-row,
  insert/delete, and length-changing edits. This prototype's contiguous `Vec`
  makes length-changing edits O(file size), and adjusting later row metadata is
  O(row count).
- The measured edit is same-length, one byte, in the middle of the file. That is
  deliberately harsh for scalar prefix/suffix discovery but favorable to the
  guest's in-place update. Larger rows, quoted multiline edits, inserts, deletes,
  reorder detection, and batches need their own matrix.
- The parser meaningfully understands CSV structure but does not implement the
  production plugin's dialect sniffing, legacy encodings, UUID/order-key row
  identity, general row diff, JSON snapshots, or merge semantics.
- Guest state is volatile and per instance. This experiment does not solve
  session routing, concurrent rebasing, eviction, restart recovery, or malicious
  compilation isolation.
- The benchmark intentionally excludes RocksDB/SlateDB. It isolates the plugin
  boundary so storage cannot conceal or amplify the effect; the parent benchmark
  matrix should compose this design with both requested backends.
