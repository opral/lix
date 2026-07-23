# JSON v2 Pareto stack — 2026-07-23

This report records the benchmark gate for each independently reviewable layer
of the recursive JSON v2 optimization stack. Results use the production
Component v2 runtime and RocksDB filesystem backend with an exact 10,000,000
byte JSON document containing 220,000 editable leaf properties.

## Layer 1: uncontended successor adoption

The engine can adopt the already validated `file-changed` successor when the
actor lease and durable semantic root prove that the acknowledged observation
is current. A stale observation still takes the renderer path so concurrent
changes compose.

The latency comparison pooled two runs in opposite baseline/candidate order.
Both used sparse splice provenance, 4 warmups in the second run, and 36 measured
samples per shape in total.

| Shape | Metric | Recursive v2 baseline | Candidate | Change |
| --- | ---: | ---: | ---: | ---: |
| flat | edit p50 | 64.566 ms | 65.118 ms | +0.9% |
| flat | edit p95 | 70.659 ms | 77.184 ms | +9.2% |
| nested | edit p50 | 68.789 ms | 70.520 ms | +2.5% |
| nested | edit p95 | 75.460 ms | 76.277 ms | +1.1% |

The flat candidate p95 contains a single 89.182 ms scheduler/storage outlier;
its next-highest sample is 77.184 ms. End-to-end edit latency remains dominated
by the durable RocksDB transaction, so latency is treated as neutral rather
than as evidence for this layer.

The deterministic hot-path work counters improve on every measured edit:

| Shape | Metric | Recursive v2 baseline | Candidate | Change |
| --- | ---: | ---: | ---: | ---: |
| flat | packet pages / records | 2 / 2 | 1 / 1 | -50% / -50% |
| flat | component imports | 1 | 0 | -100% |
| flat | component boundary bytes | 458 B | 204 B | -55.5% |
| nested | packet pages / records | 2 / 2 | 1 / 1 | -50% / -50% |
| nested | component imports | 1 | 0 | -100% |
| nested | component boundary bytes | 622 B | 286 B | -54.0% |

All rounds retained one durable semantic change and zero source reads, full
semantic materializations, full reparses, full renderer invocations, and
filesystem-sync full renders. Guest high-water memory was unchanged, as
expected for a warm-path-only change.

Example command (run once per shape and binary):

```sh
LIX_PROFILE_FORMAT=json \
LIX_PROFILE_JSON_API=v2 \
LIX_PROFILE_JSON_SHAPE=nested \
LIX_PROFILE_SPLICE_PROVENANCE=1 \
LIX_PROFILE_WARMUPS=4 \
LIX_PROFILE_ROUNDS=21 \
profile_plugin_large_file rocksdb-fs edit <fixture-dir>
```
