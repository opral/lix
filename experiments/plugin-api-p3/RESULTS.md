# Preview 3 JSON candidate results

Measured July 23, 2026 on a four-core/eight-thread AMD EPYC Milan Linux VM
with 30 GiB RAM, Rust 1.99.0-nightly, Wasmtime 47.0.2, and `wit-bindgen`
0.60.0. The guest was a 224,501-byte WebAssembly component with SHA-256
`cc3ffcd1fae32e4152295ea9c76787e7918450886e8196d0adf560a6dd19b79d`.

The deterministic fixture was one valid 10,485,811-byte minified JSON object
with 39,870 top-level properties. One byte in the middle property's 262-byte
member changed. Timings include Component ABI transfer, guest parsing/indexing
or sparse update work, and output draining. Fixture construction, component
compilation, instantiation, and host input ownership setup are outside the
timer.

The complete current output is
[`results/2026-07-23-hot-decision-linux-x86_64.tsv`](results/2026-07-23-hot-decision-linux-x86_64.tsv).
All raw block summaries from the five fresh-process hot repeats are retained
separately in
[`results/2026-07-23-hot-sync-async-repeats-raw-linux-x86_64.tsv`](results/2026-07-23-hot-sync-async-repeats-raw-linux-x86_64.tsv).
The earlier cold-only run remains available as
[`results/2026-07-23-linux-x86_64.tsv`](results/2026-07-23-linux-x86_64.tsv).

## Measurements

| Operation | p50 | p95 | Largest guest linear memory |
|---|---:|---:|---:|
| Cold open, `list<u8>` | 109.749 ms | 114.536 ms | 15.625 MiB |
| Cold open, P3 `stream<u8>` | 109.146 ms | 109.800 ms | 6.625 MiB |
| Stateless full reopen after one property edit | 109.441 ms | 109.988 ms | 15.625 MiB |
| Sequential persistent sparse edit, list-opened document | 0.006031 ms | 0.010370 ms | 15.625 MiB |
| Sequential persistent sparse edit, stream-opened document | 0.006082 ms | 0.010150 ms | 6.625 MiB |

The P3 cold stream was effectively latency-neutral at p50 (1.006x) and reduced
the largest observed guest linear memory by 9 MiB, or 57.6%. The retained file
includes every cold and sequential sample so the tail is visible rather than
hidden by aggregates.

The persistent document made the measured sequential one-property operation
roughly 18,100x faster at p50 than a v1-shaped full reopen. Each sample
advanced to the immutable successor and alternated the property byte, rather
than repeatedly editing revision zero. This is still a mechanism result, not
an end-to-end Lix speedup: it excludes database reconciliation, durable commit,
shared rendering, filesystem publication, and actor lifecycle work. The
speedup comes from v2's persistent/sparse architecture, not from making the
call async.

## Hot sync versus async decision

The hot comparison has four matched arms:

1. sync WIT export, sync host reads, inline `list<entity-change>`;
2. async WIT export, the exact sync edit method, and inline output;
3. async WIT export, sequential ready-async host reads, and inline output; and
4. async WIT export, the exact sync edit method, and a one-item P3 output
   stream plus authoritative terminal future.

Every call used the same immutable revision-zero index, two 262-byte member
reads, one changed byte, the same guest validation/update kernel, one
revision-one successor, and one 262-byte change. Input-resource creation,
result validation, successor stats/drop, counter checks, and host-table leak
checks were outside the timer. The stream arm's timer did include complete
stream draining and its terminal future.

Each fresh process ran 2,400 warmup and 24,000 measured rounds per arm on CPU
0. Execution rotated through all 24 arm permutations. The following values are
the median of the five process-level p50/p95 values (120,000 measured samples
per arm overall):

| Arm | Process-median p50 | Process-median p95 |
|---|---:|---:|
| Sync export + sync reads + inline output | 6.632 µs | 7.595 µs |
| Async export + sync reads + inline output | 7.064 µs | 8.076 µs |
| Async export + ready async reads + inline output | 7.855 µs | 9.228 µs |
| Async export + sync reads + streamed output | 13.135 µs | 18.324 µs |

| Isolated comparison | Process-median p50 cost | Slowdown | Slower processes |
|---|---:|---:|---:|
| Async export over sync export | +0.491 µs | +7.6% | 5/5 |
| Ready async reads over sync reads | +0.773 µs | +10.9% | 5/5 |
| One-item stream over inline output | +6.030 µs | +86.0% | 5/5 |

The costs are medians of paired, within-process p50 differences; they are not
arithmetic differences between the separately aggregated arm medians above.

Before the retained runs, the conservative policy gate was defined as:

- p50 cost no greater than both 0.5 µs and 10% of baseline;
- p95 cost no greater than both 1.0 µs and 15% of baseline; and
- a round-level IID paired-mean confidence bound fitting inside the p50
  margin.

A post-run audit found serial autocorrelation in the raw blocks. The harness
therefore still prints the originally specified within-process IID bound, but
labels it as a conservative diagnostic rather than independent-sample
inference. The workload and margins were not changed. The final analysis uses
each fresh process's p50 delta as the unit, with a two-sided Student-t 95%
interval across the five processes:

| Comparison | Mean process p50 cost | 95% interval |
|---|---:|---:|
| Async export over sync export | +0.463 µs | +0.362 to +0.564 µs |
| Ready async reads over sync reads | +0.776 µs | +0.734 to +0.817 µs |
| One-item stream over inline output | +6.071 µs | +5.949 to +6.193 µs |

Ready-async reads and one-item streaming are materially slower than the 0.5 µs
margin. Async export is the close case: it was slower in all five processes,
but its interval crosses that margin, so the benchmark establishes neither
non-inferiority within 0.5 µs nor material inferiority beyond 0.5 µs. The
engineering decision is nevertheless sync: async adds a consistently observed
cost and no capability to this resident, sequential hot path.

This does not reject async I/O generally. These host reads resolve immediately,
so the benchmark measures async ABI/scheduling cost without a blocking-latency
benefit. A delayed or real storage source needs a separate concurrency sweep.
Likewise, the one-item stream carries only 262 payload bytes. This run did not
measure per-arm allocation, and its stream arm materializes the item before
streaming it. Larger and multi-change outputs need a 4 KiB/64 KiB/1 MiB
latency-and-memory break-even sweep with a true producing stream before
selecting their transport.

## Relationship to the production recursive JSON path

The preceding stack layers now provide the production recursive JSON v2
vertical slice that this experiment originally called for. On the exact
10,000,000-byte / 220,000-leaf RocksDB fixture, its warm edit performs one
semantic change with zero full-file reads, imports, reparses, or renderer
calls. Direct interned hydration reduced guest high-water to 101,056,512 bytes
for flat JSON and 101,515,264 bytes for nested JSON. See
[`json-v2-pareto-stack-2026-07-23.md`](../../perf-results/plugin-api-v2/json-v2-pareto-stack-2026-07-23.md).

Those end-to-end values still must not be divided by this experiment's
microsecond guest call: the fixtures, recursive entity count, memory model, and
measured boundaries differ. In particular, the 6.625 MiB P3 result uses a
coarser top-level range index and typed summaries, not the production recursive
entity graph and packet-v1 snapshots. It proves the cold transport mechanism;
it does not predict that the recursive production plugin will use 6.625 MiB.

## Mechanism checks

- Each measured sparse edit performed exactly two source `len` calls and two
  bounded source reads totaling 524 bytes. It performed no full-file read and
  opened no input/source stream.
- All four hot arms produced the same revision-one successor and 262-byte
  semantic change, kept the base document at revision zero, matched the
  expected source counters, and left no host-table entries.
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
replace a hand-built cursor/poll protocol and bound large input transfer. The
matched benchmark shows that it does not improve the already-local synchronous
edit path, so that path should remain synchronous.

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
