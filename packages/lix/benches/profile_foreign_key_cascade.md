# Declared foreign-key deletion actions

The shared referential-action planner runs against the statement or resolved
merge candidate. It batches each frontier by referencing schema, domain, and FK
columns. Scalar indexed columns use declared-column equality probes; composite
keys use the existing collection-scoped scan. A visited identity set bounds
cycles and diamonds. Leaf deletions do not need another snapshot conversion or
frontier. Final delete validation uses the same scalar index probes.

Commit preparation checks the coherent publication snapshot again. This is
necessary even for already cascaded intermediate parents: a concurrent writer
may have added a new grandchild. Pending inserts/updates are not silently erased
by that pass. When concurrent plugin reconciliation would overlap pending row
or file work, publication returns a retryable transaction conflict.

Merge preparation hydrates selected changes in batches per historical commit,
limited to schemas reachable through cascading edges. Incoming schema
registrations require rebuilding the candidate catalog. Both branch-live
preimages can seed a winning delete when a referenced unique key changed;
ordinary descendants are still deleted once. Collection-generation markers use
endpoint diffs restricted to referenced schema/file scopes to recover proven
parent deletions suppressed from authored-member diffs. Both durability catalogs
are prepared from the candidate, preserving selected schema change identities
while generated rows resolve their declarations. Historical changes stay immutable.

## Reproduce

```sh
cargo build -p lix --release --example profile_cascade
# Arguments: total children, children referencing the deleted parent.
target/release/examples/profile_cascade 10000 100
LIX_CASCADE_TRACE=1 target/release/examples/profile_cascade 10000 100
```

The in-memory workload registers a parent/child schema, inserts two parents and
children in batches of 1,000, then times one autocommitted parent DELETE. Setup,
remaining-row verification, and close are excluded. The assertion checks that
exactly the requested fan-out disappeared. `LIX_CASCADE_TRACE` reports indexed
and scan probe counts, candidates, frontier levels, and generated tombstones.

For CPU samples of the DELETE only (Linux perf):

```sh
mkfifo /tmp/lix-cascade-control
LIX_CASCADE_PERF_CONTROL=/tmp/lix-cascade-control \
  perf record -e cycles:u -F 997 -g --call-graph dwarf,16384 \
  --delay=-1 --control=fifo:/tmp/lix-cascade-control \
  -o /tmp/lix-cascade.data -- \
  target/release/examples/profile_cascade 10000 10000
perf report -i /tmp/lix-cascade.data --stdio --no-children
rm /tmp/lix-cascade-control
```

## Measurement environment

AMD Ryzen 9 9950X (16 cores / 32 threads), Linux 6.17.0-23, Rust
1.97.0-nightly (`b954122bb`, 2026-05-20), ordinary Cargo release profile and
canonical Memory storage. Measurements were collected on a shared development
host with local compilation activity; wall-clock timings are observations, not
a latency guarantee. This workload measures scalar fan-out, not composite-key
fallbacks, plugin materialization, or merge hydration.

## Observations (2026-09-21)

| Total child rows | Cascade fan-out | Repetitions | DELETE median | Observed range |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 100 | 3 | 3.548 ms | 3.480–3.675 ms |
| 10,000 | 100 | 3 | 6.990 ms | 6.825–7.421 ms |
| 10,000 | 10,000 | 3 | 205.830 ms | 203.954–221.047 ms |
| 100,000 | 100 | 1 | 22.459 ms | single observation |

The 10,000-row / 100-child trace recorded two indexed probes (the eligible
tracked/untracked source domains), zero scan probes, 100 candidates and 100
generated deletes. The coherent commit pass recorded two indexed probes and
zero remaining candidates/deletes. This bounds planner candidate materialization
by matches in this scalar case; end-to-end latency still includes storage,
normalization, constraint checks, history publication, and allocation costs.

The isolated 10,000-child perf sample took 207.529 ms and collected 205 cycle
samples. Self time was dominated by allocation (`malloc` 15.95%, `_int_free`
6.91%), copying (`memmove` 9.93%), comparison (`memcmp` 8.49%, primary-key
comparison 1.56%), and shared byte-buffer ownership (clone 8.16%, drop 6.43%). These are coarse samples of the complete DELETE, not a
microbenchmark of the planner. No whole-database scan is introduced: fallback
work remains scoped to the referencing collection and domain.
