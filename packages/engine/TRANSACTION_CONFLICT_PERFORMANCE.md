# Transaction conflict-resolution performance

Release-mode microbenchmarks isolate stale-commit discovery work from plugin
execution, storage commit latency, and observation delivery. End-to-end
capacity results remain in
`packages/rs-sdk-tests/REALTIME_COLLABORATION_CAPACITY.md`.

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

## Reproduction

```bash
cargo test -p lix_engine --release \
  stale_overlap_discovery_benchmark_probe --lib -- --ignored --nocapture

cargo test -p lix_engine --release \
  generation_write_set_benchmark_probe --lib -- --ignored --nocapture
```

Both probes emit versioned machine-readable records or stable key/value output
and assert that the optimized path beats its exact predecessor on the same
fixture.
