# Workload isolation benchmark

This native benchmark measures one foreground point-query client while a second
session continuously executes `SELECT 1` against the same engine. It compares:

- `baseline`: no background worker;
- `loaded-default`: an unclassified saturating background worker;
- `loaded-priority`: the same worker with `priority: Background`.

The foreground and background threads are pinned to separate CPUs. Each process
warms the foreground path for 500 queries, then records 2,000 queries. Run each
mode in a fresh process and alternate mode order. For example:

```sh
cargo +nightly bench -Z bindeps -p lix_sdk --no-default-features \
  --bench workload_isolation --no-run
BENCH=$(find target/release -type f -name 'workload_isolation-*' -executable | head -1)
LIX_BENCH_CPUS=14,15 "$BENCH" baseline
LIX_BENCH_CPUS=14,15 "$BENCH" loaded-default
LIX_BENCH_CPUS=14,15 "$BENCH" loaded-priority
# Or run the complete 21-triple alternating suite and seeded bootstrap:
LIX_BENCH_CPUS=14,15 "$BENCH" suite
```

## Service policy

Foreground admission is not delayed by the workload coordinator. Background
admission is FIFO, capped at 64 queued calls per engine, and limited to one
active background statement or atomic batch. A call queued while the engine is
foreground-idle waits for a 250 microsecond quiet window. A call that initially
observes active foreground work is rechecked at the 5 millisecond suppression
deadline so foreground traffic cannot starve it. This deadline is a scheduling
target rather than a hard end-to-end wait bound. Queue overflow returns
`LIX_WORKLOAD_QUEUE_FULL`.

The acceptance gate for this workload is a median paired foreground p95
regression below 20%, with background progress in every process. The benchmark
also reports p50/p95 admission wait separately from execution time and measures
cooperative shutdown latency.

## Reference result

On Linux x86-64 (AMD EPYC-Genoa), 21 fresh alternating process triples pinned to
CPUs 14 and 15 produced these medians:

| mode | foreground p50 | foreground p95 | foreground p99 | background queries/s |
| --- | ---: | ---: | ---: | ---: |
| baseline | 498.8 us | 550.1 us | 621.2 us | 0 |
| loaded-default | 647.5 us | 735.9 us | 899.4 us | 2,276.3 |
| loaded-priority | 512.2 us | 618.9 us | 678.5 us | 161.4 |

The median paired p95 regression was 29.7% for unclassified background work and
11.2% with background priority. A seeded 20,000-resample bootstrap put the latter
median's 95% interval at 5.7% to 13.8%. In priority mode, median background queue
p50/p95 was 5.150/5.194 ms, execution p50/p95 was 1.042/1.318 ms, and cooperative
shutdown was 3.087 ms.

This is a QoS tradeoff, not a total-throughput optimization: priority mode cut
background throughput by about 93% under saturation to preserve foreground tail
latency. Scheduling is cooperative at statement and batch boundaries; it does
not preempt a long statement already executing, and separate engines do not
share a coordinator.
