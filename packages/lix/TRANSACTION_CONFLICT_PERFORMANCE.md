# Transaction conflict-resolution performance

Release-mode microbenchmarks isolate stale-commit discovery work from plugin
execution, storage commit latency, and observation delivery. End-to-end
capacity results remain in
`packages/e2e/REALTIME_COLLABORATION_CAPACITY.md`.

## Indexed overlap discovery

The removed production path compared every prepared key with every concurrent
key and then rebuilt the overlap set inside reconciliation. The replacement
hashes concurrent identities once and preserves prepared-row order.

| Prepared keys | Concurrent keys | Overlaps | Before p50 | After p50 | Speedup |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5,000 | 5,000 | 2,500 | 112,658 µs | 698 µs | 161.30× |

The benchmark executes both algorithms over identical typed keys and asserts
identical membership and order before accepting a sample.

## Committed-generation write set

Normal commits already persist compact, immutable per-generation identity
deltas. Stale transaction admission now unions those write sets directly for
a first-parent interval. It no longer performs endpoint reconstruction,
ancestor point reads, or payload hydration merely to discover touched keys.
Non-first-parent and non-journal histories retain the general diff fallback.

| Touched identities | Before: general diff p50 | After: generation write set p50 | Speedup |
| ---: | ---: | ---: | ---: |
| 5,000 | 19,136 µs | 1,946 µs | 9.83× |

The write set is deliberately conservative: an identity touched and then
restored to its opening payload remains present. This prevents stale admission
from treating intervening writes as if they never happened. Duplicate touches
across generations are returned once.

The optimization targets large stale intervals, not the five-write capacity
wave. Five fresh-process 100-client JSON capacity repetitions after this cut
had a worst convergence p95 of 16.062 ms, versus the established pre-series
six-process worst of 18.095 ms. The real-time path therefore remains flat
within run-to-run variance and far below the 100 ms gate while high-cardinality
discovery improves by an order of magnitude.

## Row-first reconciliation

The file-scoped resolver and render-replay path measured by the earlier version
of this document no longer exists. Lix now composes ordinary row snapshots
column by column in the host. Only overlapping changes to the same column are
eligible for an optional plugin `ColumnMerger`; rows without one use
deterministic column-based LWW without entering Wasm. File bytes are serialized
only after the accepted rows are known.

Those older resolver-call and render-transition measurements are therefore not
comparable to the row-first API. The matched public-workflow benchmark in
`packages/e2e/tests/plugin_api_benchmarks.rs` measures file projection and
same-row Markdown/CSV column merging against the frozen pre-cut implementation.

## Bounded commit admission

Explicit transaction commits previously contended directly on one Tokio mutex.
That queue had no engine-owned bound, reacquired the collaboration gate for
every transaction, and let task-local benchmark clocks start only after a task
was polled, which could omit mutex queue time. A private FIFO coordinator now
applies backpressure at 256 queued commits and drains at most 16 commits under
one gate acquisition. Each transaction still revalidates, resolves, persists,
and reports its own deterministic result; this is admission batching, not a
change to atomicity or durability.

This maintainability cut is throughput-neutral by design. On eight release
runs of the 100-writer, 100%-overlap B3.1 stress fixture, whole-cohort p50 moved
from 150.612 ms to 151.588 ms (+0.65%, within run variance). The corrected
request timer now includes queue residence and reports 157.627 ms p95 for that
pathological same-scalar workload, making the remaining serialization visible
instead of presenting the old 1.983 ms undercount. On the practical 100-client,
10%-overlap capacity workload, convergence p95 moved from 16.201 ms to
16.420 ms and remains far below the 100 ms gate.

## Reproduction

```bash
cargo test -p lix --release \
  stale_overlap_discovery_benchmark_probe --lib -- --ignored --nocapture

cargo test -p lix --release \
  generation_write_set_benchmark_probe --lib -- --ignored --nocapture

cargo test -p lix_e2e --release --test e2e \
  stale_plugin_replay_batch_benchmark_probe -- --ignored --nocapture

LIX_CRDT_B3_CLIENTS=100 LIX_CRDT_SAMPLES=8 \
cargo test -p lix_e2e --release --test crdt_benchmarks_baseline \
  crdt_benchmarks_b3_1_json_concurrent_map_sets -- --ignored --exact --nocapture
```

The probes emit versioned machine-readable records or stable key/value output
and assert that the optimized path beats its exact predecessor on the same
fixture.
