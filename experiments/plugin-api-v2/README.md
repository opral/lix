# Wasm plugin API mechanism benchmark

This experiment isolates one architectural question: what has to cross the
host/guest boundary, and what work must a plugin repeat, when one byte changes
in a large file?

Every candidate implementation runs in the same `wasm32-unknown-unknown`
module under Wasmtime 45. The native host only creates fixtures, copies ABI
buffers, calls the Wasm exports, times the call, and checks the result. There is
no native fallback for parsing, stable-ID reconciliation, checkpoint reduction,
or splice application.

## Candidates

| Candidate | Per-edit input | State location | Expected property |
|---|---|---|---|
| v1 stateless | compact checkpoint + full new file; small change output | host | optimistic lower bound for the current full-state API |
| A persistent/full-file | full new file | Wasm instance | tests whether persistence alone is enough |
| B persistent/splice | offset, delete length, inserted bytes | Wasm instance | tests persistent state plus an incremental edit API |
| B2 guest index/host source | offset, delete length, inserted bytes | offsets, IDs, and hashes in Wasm; immutable bytes in host | tests B ergonomics without retaining a guest file copy |
| C copied checkpoint reducer | compact checkpoint + splice | copied through Wasm memory | separates incremental parsing from persistent resources |
| D fine host context | splice + point-index and per-byte source imports | host-owned source/index | measures a deliberately chatty context API |
| D batched host context | splice + point-index and source-range imports | host-owned source/index | tests batching without packing semantic KVs |

The checkpoint is intentionally packed: the file bytes plus 16 bytes per
entity. Current rich Component Model records would transfer more data, so v1
and C are best-case baselines rather than caricatures. The timed v1 call returns
only a 32-byte change summary. The checkpoints representing the next stateless
fixture state are built outside the timed interval.

Candidate D keeps semantic entity records as individual host-side records. It
does not pack KVs into application-level storage pages. Both D lanes perform
the same point lookup in a host-owned private offset index; only source access
changes. The fine lane imports one byte at a time. The batched lane imports the
affected before/after entity ranges in two calls. The report separately counts
direct ABI bytes, host-import calls, host-import bytes, and private-index bytes.

Candidate B2 is intentionally different from D: the host exposes only immutable
before/after byte sources. The guest hydrates and owns its format-specific
offset/ID/hash index by streaming 64 KiB source ranges, then imports only the
affected before/after entity bytes for a localized edit. There is no host-owned
semantic index, application-level packed KV, or retained guest copy of the
file. Its timed output contains the exact changed entity bytes, not merely a
hash.

The measured Candidate B facade was later refined after the controlled AX
cohort and a source-level correctness review. The sparse edit/source data flow
is unchanged, but the final compileable facade adds explicit byte/entity cold constructors,
minimal merge groups, complete upserts versus keyed deletes, composite retry-
stable IDs, and no guest `Send`/`Sync` requirement. The refined WIT also names
large input ranges and guest lazy outputs instead of forcing full `list<u8>`
payloads. These post-cohort changes are not presented as another benchmark
lane or retroactively assigned an AX score.

## Fixtures and correctness

The harness generates approximately 100 KiB, 1 MiB, and 10 MiB fixtures for:

- CSV rows, Markdown paragraphs, and text lines, with generated semantic IDs
  reconciled across a localized edit.
- Top-level JSON object properties, whose decoded property key is treated as
  identity only inside this generated mechanism fixture. This is not the
  production identity design: nested object slots need stable parent identity,
  and array items need opaque IDs plus independent order keys rather than JSON
  Pointer/index primary keys.
- Excalidraw elements, whose native `id` is the intrinsic identity.

Nested JSON and JSON arrays are not mechanism-tested by this generated fixture;
their required identity break is specified and correctness-gated separately.

The large CSV/text cases contain 200,000 entities. Large Markdown, JSON, and
Excalidraw cases contain 50,000 entities. JSON and Excalidraw fixtures are
parsed by `serde_json` before benchmarking to prove that the generated files
are valid JSON.

Each timed write changes one value in the middle entity, alternating between
one byte and 17 bytes. This forces both a 16-byte growth and shrink rather than
special-casing same-length replacement. After timing, the host validates the
candidate's measured output:

1. the target entity ID is unchanged;
2. the plugin reports that exact ID as changed; and
3. v1/A/B/C reconstruct the complete requested file exactly; B2 returns the
   exact affected-entity bytes from the requested source view; and D's
   detection-only lane returns the exact affected-entity hash.

B2 deliberately isolates incremental detection and source access; it does not
exercise a full renderer. A production B2 port must additionally prove that
applying its semantic delta and cold-rendering all entities reproduces the
canonical requested file.

## Run

From this directory:

```sh
cargo build -p plugin-api-v2-guest --target wasm32-unknown-unknown --release
cargo build -p plugin-api-v2-host --release
./target/release/plugin-api-v2-host \
  --sizes-kib 100,1024,10240 \
  --formats csv,markdown,json,excalidraw,text \
  --warmups 5 \
  --iterations 30 \
  --output results/mechanism-benchmark.json
```

Use `--quick` for a five-iteration, 100 KiB smoke run. Without `--output`, the
harness writes the JSON report to standard output.

The measured interval includes guest allocation, the direct host-to-guest copy,
all Wasm execution and host imports, the guest-to-host result copy, and guest
deallocation. Fixture generation, request assembly in host memory,
initialization/hydration, and final correctness checks are reported or
performed separately.

## Interpretation limits

- This is a mechanism benchmark, not an end-to-end Lix benchmark. It excludes
  RocksDB/SlateDB, transactions, merges, and rendering a separate shared view.
- The format scanners understand the generated fixture shapes; they are not
  replacements for the production CSV, Markdown, JSON, text, or future
  Excalidraw plugins. Their purpose is to make entity count, identity, and
  bytes processed comparable across API shapes.
- Candidate B mutates one thread-local document in place. It does not allocate
  an immutable successor, retain accepted/successor versions through commit,
  or model eviction, abort, multi-session structural sharing, or restart.
- Candidate B2 mutates one thread-local index in the same way. Its streamed
  hydration cost is reported separately from warm edits. The measurements are
  a localized detection/source-access lower bound, not a measured immutable
  resource lifecycle.
- Candidate D models a warm host-owned source and private index. Its point
  lookup and range reads are in memory; storage-engine latency and cache misses
  belong in the separate RocksDB/SlateDB engine benchmark.
- The edit is contained within one semantic entity. Structural edits that add,
  remove, or reorder entities need an incremental regional reparse and broader
  correctness suites in a production implementation.
- Candidate B uses a flat byte vector. A length-changing edit shifts bytes and
  the following flat offsets. A
  production implementation should use a rope/piece tree and relative entity
  positions so localized edits remain logarithmic.
- `peak_guest_linear_memory_bytes` is Wasm linear-memory high-water as observed
  after calls. Wasm memory does not shrink, but a transient peak wholly inside
  a call may be higher.
- The raw core-Wasm ABI is deliberately packed. A WIT resource API should hide
  an equivalently packed transport behind an ergonomic SDK; these numbers do
  not measure Canonical ABI lifting/lowering of rich record lists.

Do not infer that a persistent resource alone is sufficient. Candidate A still
receives and scans the full blob; Candidate B tests the combination of retained
document state and a localized edit description.

## Recorded matrix

`results/mechanism-benchmark-macos-aarch64.json` contains 105 records: seven lanes,
five formats, and three sizes, with five warmups and 30 measured calls per
record. It was recorded on 2026-07-22 on an Apple M5 Pro with 64 GiB RAM,
macOS 26.3.1 (25D771280a), Rust nightly 1.97, and Wasmtime 45.0.3.

At the approximately 10 MiB scale, the cross-format ranges were:

| Candidate | p50 | p95 | p50 versus v1 | Largest guest high-water |
|---|---:|---:|---:|---:|
| v1 stateless | 18.41–19.08 ms | 20.22–27.79 ms | 1.0x | 54.44 MiB |
| A persistent/full-file | 17.45–19.16 ms | 19.16–21.92 ms | 1.00–1.05x | 45.63 MiB |
| B persistent/splice | 0.123–0.326 ms | 0.126–0.358 ms | 58.3–149.1x | 61.06 MiB |
| B2 guest index/host source | 0.0126–0.0710 ms | 0.0227–0.0718 ms | 264.9–1462.6x | 13.44 MiB |
| C copied checkpoint | 1.10–1.80 ms | 1.80–4.01 ms | 10.54–16.69x | 59.00 MiB |
| D fine host context | 0.000375–0.000959 ms | 0.000375–0.003375 ms | access-only probe | 1.13 MiB |
| D batched host context | 0.000333–0.000625 ms | 0.000375–0.000666 ms | access-only probe | 1.13 MiB |

Candidate A does not clear the project's 20% performance threshold. Candidate
B clears it in every format. Candidate C clears it in every format but still
copies 10.05–12.78 MiB in each direction per edit, so its latency result does
not solve boundary bandwidth, checkpoint storage, or Canonical ABI pressure.

Candidate B2 is the strongest localized detection/source-access mechanism in
this experiment.
It is 4.50–9.81x faster than B while reducing guest linear-memory high-water by
77.99–91.93%. At 10 MiB it retained a 1.14 MiB index for 50,000 entities or a
4.58 MiB index for 200,000 entities, used exactly two source imports carrying
116–426 bytes per edit, and returned 82–237 exact entity bytes. Streamed cold
hydration took 20.35–26.15 ms, 20–56% slower than B's full-file hydration; that
is a one-time/eviction tradeoff rather than a per-edit cost.

The flat B2 offset vector still shifts following offsets for length-changing
edits, explaining the 0.070 ms CSV/text results versus 0.013–0.020 ms for the
50,000-entity formats. A relative-offset tree can target this remaining cost
without changing the author-facing document/splice/source facade.

The D timings are intentionally labeled access-only: the host already owns the
before/after source views and private index. They show the warm lower bound for
identifying and reading one affected entity, not an end-to-end write. Fine
access used 123 imports for CSV/text and 411–433 for the longer structured
entities. Batched access used nine imports for every format while transferring
the same 148–458 bytes. Batching improved p50 by 1.53–1.60x for Markdown, JSON,
and Excalidraw, but only 1.13x for short CSV/text entities. The absolute values
are below a microsecond and should be treated as a throughput probe, not a
scheduler-level latency promise.

The host-owned private offset index occupied 1.14 MiB for 50,000 entities and
4.58 MiB for 200,000 entities in this deliberately unpacked 24-byte-record
representation. This is transient in-memory index cost; it is not a proposal
to pack semantic KVs into storage pages.
