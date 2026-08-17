# Sync strategy scorecard

This benchmark replays the same deterministic row/file traces through three
protocol strategies:

1. transaction-pack admission plus canonical event pull;
2. transaction-pack admission plus canonical commit-pack pull/fast-forward;
3. commit-pack admission and canonical commit-pack pull.

The simulator is deliberately transport- and storage-adapter-neutral. It
asserts convergence, branch isolation, and idempotent retry behavior for every
trace, reports deterministic wire/storage/replay counters as JSON lines, and
uses Criterion for runtime measurements. The crash scenario models a process
death after server admission and before the acknowledgement is persisted; the
real durable-receipt behavior remains covered by the storage/e2e tests.

Run it with:

```text
cargo bench -p lix --bench sync_strategy_scorecard -- --noplot
```

The JSON scorecard is the comparable data set. Criterion's p50/p95 timings are
machine-dependent, so compare strategies from the same run and hardware. The
JSON also reports server operations/second, catch-up bytes per accepted
operation, and separate p95 timings for fast-forward and divergent overlay
application. The initial decision gates are zero correctness failures, no
branch leakage or duplicate canonical commits, fast-forward latency within 10%
of the best *semantically equivalent* fast path, and catch-up bytes
proportional to changed rows rather than total repository size.

The `sync_mode` and `sync_prototype` e2e tests remain required because this
scorecard does not execute real plugins or storage adapters.
