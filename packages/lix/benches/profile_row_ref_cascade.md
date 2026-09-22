# File-qualified row-reference cascade profiling

The workload places equal parent primary keys in two different files. Children
in the second file point either to the first parent (the measured fan-out) or
the second parent (unrelated rows). Deleting the first parent must remove only
its references. Setup is excluded; the timer includes the DELETE and its
autocommit. The remaining-row count is asserted.

```sh
cargo build -p lix --release --example profile_row_ref_cascade
LIX_CASCADE_TRACE=1 target/release/examples/profile_row_ref_cascade 10000 100
LIX_CASCADE_TRACE=1 target/release/examples/profile_row_ref_cascade 100000 100
```

Measurements use canonical Memory storage and the normal release profile on a
shared AMD Ryzen 9 9950X development host. Concurrent compilation affects wall
time; these are observations, not latency guarantees. This fixture measures
cross-file lookup and cascade publication, not plugin materialization or merge
hydration.

## Fallback found during implementation

The first implementation sent an entire frontier as one declared-column index
probe. Frontiers above the index's value limit silently fell back to scanning
the referencing collection. A trace reporting an indexed *request* therefore
did not prove an indexed execution.

| Total children | Fan-out | DELETE | Statement candidates | Commit candidates |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 100 | 100.098 ms | 10,100 | 9,900 |
| 100,000 | 100 | 1,413.071 ms | 100,100 | 99,900 |

Each row is one baseline observation. Both traces reported zero explicit scan
requests; candidate counts exposed the fallback underneath them.

## Value-probe batching alone

Splitting frontiers at the 64-value probe limit removed the first fallback.
Three observations at fan-out 100 gave 28.146–31.458 ms for 10,000 children
and 310.129–317.894 ms for 100,000 children. Both sizes returned exactly 100
statement candidates and zero commit candidates. At 100,000 children,
fan-outs 1,000 and 10,000 took 1,872.434 ms and 15,109.645 ms respectively.

A DELETE-only `perf record -e cpu-clock -F 499 --call-graph dwarf,16384`
capture collected 157 samples with no losses. Row-key equality accounted for
38.85% of sampled self time, storage range iteration 18.47%, and key decoding
7.01%. The declared-column index produced primary keys without their file
identity; hydrating those candidates across files still fell back to a full
collection scan. Candidate counts alone therefore did not prove bounded
storage reads. These are intermediate results, not the final implementation.

## Correlated candidate identities

The final path stores file identity with each indexed primary key and hydrates
one correlated exact batch. Transaction overlays merge pending rows and
recheck each row's current reference value. New index entry/witness tags keep
old row-PK-only indexes from being mistaken for complete correlated indexes;
unwitnessed collections retain one authoritative scan fallback for the entire
predicate, rather than repeating a scan per value chunk.

| Total children | Fan-out | DELETE observations | Statement candidates | Commit candidates |
| ---: | ---: | --- | ---: | ---: |
| 10,000 | 100 | 9.407 / 9.142 / 8.794 ms | 100 | 0 |
| 100,000 | 100 | 15.649 / 12.329 / 13.079 ms | 100 | 0 |
| 100,000 | 1,000 | 151.389 ms | 1,000 | 0 |
| 100,000 | 10,000 | 955.922 ms | 10,000 | 0 |

The 100,000-row / 100-dependent median is 13.079 ms, compared with the
1,413.071 ms initial observation. The 10,000-dependent observation improved
from 15,109.645 ms after value batching alone to 955.922 ms. These comparisons
identify the eliminated scans, not a guaranteed speedup across workloads.
Compilation was running concurrently on the shared host.

A separate DELETE-only CPU-clock profile of 100,000 rows / 10,000 dependents
collected 370 samples with no losses; elapsed time was 744.250 ms. The largest
sampled costs were byte comparisons (14.32%), shared-buffer cloning (6.76%),
and allocation/freeing. The former row-PK filtering hotspot no longer
dominated. The profile includes generated row publication, not setup.

To isolate DELETE in `perf`, create a control FIFO and pass its path through
`LIX_CASCADE_PERF_CONTROL`, using `perf record -D -1 --control fifo:PATH`.
The example enables recording immediately before DELETE and disables it
after autocommit. Use `-e cpu-clock -F 499 --call-graph dwarf,16384` when
hardware performance events are unavailable.
