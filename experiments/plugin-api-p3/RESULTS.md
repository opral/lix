# Preview 3 JSON candidate results

Measured July 23, 2026 on a four-core/eight-thread AMD EPYC Milan Linux VM
with 30 GiB RAM, Rust 1.99.0-nightly, Wasmtime 47.0.2, and `wit-bindgen`
0.60.0. The guest was a 191,870-byte WebAssembly component with SHA-256
`9c903961004fd3186c16dd97786cb6c11d437a458bbdfc4a12b95dc3eac38248`.

The deterministic fixture was one valid 10,485,811-byte minified JSON object
with 39,870 top-level properties. One byte in the middle property's 262-byte
member changed. Timings include Component ABI transfer, guest parsing/indexing
or sparse update work, and output draining. Fixture construction, component
compilation, instantiation, and host input ownership setup are outside the
timer.

The complete retained output is
[`results/2026-07-23-linux-x86_64.tsv`](results/2026-07-23-linux-x86_64.tsv).

## Measurements

| Operation | p50 | p95 | Largest guest linear memory |
|---|---:|---:|---:|
| Cold open, `list<u8>` | 112.837 ms | 113.403 ms | 15.312 MiB |
| Cold open, P3 `stream<u8>` | 113.102 ms | 115.574 ms | 6.312 MiB |
| Stateless full reopen after one property edit | 112.771 ms | 114.474 ms | 15.312 MiB |
| Sequential persistent sparse edit, list-opened document | 0.006172 ms | 0.012013 ms | 15.312 MiB |
| Sequential persistent sparse edit, stream-opened document | 0.006162 ms | 0.012463 ms | 6.312 MiB |

The P3 cold stream was effectively latency-neutral at p50 (0.998x) and reduced
the largest observed guest linear memory by 9 MiB, or 58.8%. Its p95 was 2.171
ms worse in this unpaired run. The retained file includes every sample so the
tail is visible rather than hidden by aggregates.

The persistent document made the measured sequential one-property operation
roughly 18,300x faster at p50 than a v1-shaped full reopen. Each sample
advanced to the immutable successor and alternated the property byte, rather
than repeatedly editing revision zero. This is still a mechanism result, not
an end-to-end Lix speedup: it excludes database reconciliation, durable commit,
shared rendering, filesystem publication, and actor lifecycle work. The
speedup comes from v2's persistent/sparse architecture, not from making the
call async.

## Relationship to the existing production JSON path

PR2 already retained a separate full-engine diagnostic for the current v1 JSON
plugin: one changed property in an exact 10,000,000-byte / 220,000-property
file measured 7,831.846/7,993.483 ms p50/p95 on RocksDB LocalFilesystem and
10,119.876/10,601.274 ms on cached SlateDB. That run required a diagnostic
256 MiB guest ceiling. See
[`incremental-csv-v2-pr2-2026-07-22.md`](../../perf-results/plugin-api-v2/incremental-csv-v2-pr2-2026-07-22.md#10-mb-json-diagnostic).

Those end-to-end values must not be divided by this experiment's six-
microsecond guest call: the fixtures, entity counts, memory caps, and measured
boundaries differ. Together they identify the next vertical slice, however:
port JSON to the persistent document + splice + sparse semantic output path,
use P3 only for cold hydration, then rerun the paired RocksDB/SlateDB gate.

## Mechanism checks

- Each measured sparse edit performed exactly two source `len` calls and two
  bounded source reads totaling 524 bytes. It performed no full-file read and
  opened no stream.
- The list-opened and stream-opened documents had equivalent sparse p50 in
  this run. Their microsecond-scale p95s have flipped ordering across reruns,
  so a paired gate is still required.
- A source that advertised the complete length but emitted one byte less was
  rejected, and no partial document was accepted.
- A source that emitted every byte but resolved its authoritative terminal
  future with an error was also rejected.
- Dropping the entity-output receiver made the guest producer resolve its
  authoritative terminal future as cancelled.
- A fork and its accepted document remained at revision zero after producing a
  revision-one successor.
- This retained run used a 64 MiB limit on each guest linear memory for
  comparison with PR2. The value is configurable and is not an API invariant.
  The reported high-water is the largest linear memory, not aggregate component
  or process memory.
- Guest-declared and actually streamed entity counts are capped by a separate,
  configurable host output budget; the retained run used one million.

## Interpretation

Preview 3 helps the cold edge: `stream<T>`, `future<T>`, and `async func`
replace a hand-built cursor/poll protocol and bound large input transfer. It
does not improve the already-local synchronous edit path, so that path should
remain synchronous.

Raw output production is still intricate in Rust: the plugin must create a
stream and terminal future, spawn a producer, honor backpressure, handle a
dropped receiver, and resolve completion. P3 therefore simplifies the WIT
shape more than it simplifies all author code. This candidate does not justify
shipping a new Lix SDK yet; it identifies the small transport adapter an SDK
could eventually hide if repeated plugin implementations show the same
friction.

The input parser buffers one complete top-level member, so its cold memory is
O(transport chunk + largest property), not O(transport chunk) for every JSON
shape. The cold output is also a typed four-integer entity summary rather than
production packet-v1/snapshots. These constraints make this an executable JSON
indexing and ABI mechanism benchmark, not the finished production JSON plugin.
