# E06: bounded first-parent metadata selection

Hypothesis: exact-address batches cannot combine parent addresses the client
has not discovered yet. Let the authenticated authority follow a bounded
immutable first-parent path, then install the selected metadata through the
existing local admission and epoch checks.

The SQL provider emits a semantic history-demand annotation on typed metadata
misses. The sync owner chooses a walk of at most 16 commits and optional state
headers. The server returns at most 32 records / 256 KiB decoded payload under
the existing native baseline lease. The client validates repository, epoch,
anchor, parent continuity, generations and header association before the
existing immutable installer performs its admission/CAS checks. No local read
is held over network I/O. Normal awaited SQL retries internally.

Opening does not trigger this selector. Arbitrary point reads keep exact-address
hydration. A partial prefix is never proof of complete history or absent data.
Speculative missing/corrupt ancestors or optional headers stop the server walk;
required anchor or subsequent exact reads still report their original failure.
This prevents prefetch from making a short query depend on older corrupt data.

## Validation and compatibility

- 4,315 all-simulation and server-protocol tests passed; 84 skipped.
- 10 doctests passed. The subsequent source change only fixes setup of the new
  cfg(test) corruption fixture; no production code changed after the benchmark
  binary was built. The manifest records the distinction; `built-engine.patch.gz`
  preserves the exact benchmark-build source patch.
- 45 SDK unit tests, TypeScript checking and one Chromium SharedWorker admission
  regression passed. That browser test uses a stub engine; native query timings
  below are not browser-production latency measurements.
- Fresh opening remains two foreground requests, no native reads and <=8 KiB
  response bodies, including 16,000 unrelated files and an unopened 8 MiB blob.
- Actual protocol-17 partial RocksDB replicas created by the accepted baseline
  reopened with protocol 18, returned equal ordered history and worked offline
  when warm, with no local reset. Cached opening used zero foreground requests.
- Protocol 18 requires matching SDK/server deployment. Storage format 81 and
  partial receipt v2 are unchanged; no repository-data migration is required.
  The SDK's existing durable admission cache includes the protocol epoch, so
  upgrade requires online re-admission before that cache authorizes offline use.

The upgrade control also reproduced an existing expired-recovery descriptor
churn bug on the unchanged baseline: dense history made 320 fresh descriptor
requests, sparse 152. E06 reduces them incidentally with fewer native demands;
the lifecycle bug is addressed in the following prototype, not hidden here.

## Measurements

Ten alternating fresh-replica pairs per comparison, identical exported fixture
snapshots and benchmark harness, real canonical loopback HTTP, RocksDB, optimized
engine with dev dependencies, zero injected RTT, no task compiler/test workload.
Results below report median paired improvement and seeded bootstrap 95% intervals.

| Query | Native requests before → after | Median cold time before → after | Paired improvement, 95% interval |
| --- | --- | --- | --- |
| Dense64 | 320 → 262 | 11.799 s → 9.519 s | 19.29% [18.55%, 19.67%] |
| Sparse64 | 152 → 94 | 2.491 s → 1.526 s | 38.82% [38.07%, 40.00%] |
| Dense64 LIMIT1 | 8 → 8 | 55.663 ms → 55.334 ms | 0.19% [-0.95%, 5.13%] |
| Sparse64 LIMIT1 | 22 → 17 | 166.283 ms → 132.230 ms | 20.74% [18.71%, 21.62%] |
| Dense4 | 20 → 19 | 136.566 ms → 129.176 ms | 5.55% [4.09%, 6.40%] |

In both full 64 cases, 62 exact metadata requests become 4 segment requests.
The other object and metadata dependencies remain; this does not approach the
known-dependency oracle's 14/8 requests yet. The wide16000 smoke changes only
347→346 native history requests: this optimization targets history metadata
discovery, not the remaining wide-tree dependency traversal.

Full-query native response bodies decline from 232125→227415 bytes dense and
154913→150203 bytes sparse. Short queries expose the overfetch tradeoff:
sparse LIMIT1 increases 20733→29508 bytes (+42.32%, an extra 8775 bytes), while
dense4 increases 13186→14689 bytes (+11.40%, an extra 1503 bytes). Dense LIMIT1
uses the same 5035 bytes. These are HTTP body bytes, excluding headers/TLS.

Opening remains bounded. In the full64 pairs, opening medians are 9.225→9.172 ms
dense and 8.658→8.584 ms sparse. Warm medians are 67.874→67.227 ms and
26.759→26.937 ms. The 95% intervals for opening, file selection and warm reads across all five
comparisons exclude a 10% regression. Sparse LIMIT1 warm reads have a small
measured paired slowdown of 1.21% [0.16%,1.73%]; no warm speedup is claimed.

## Decision

Accept: the lower 95% latency improvement bound exceeds 10% for both full 64
histories and sparse LIMIT1, alongside deterministic native-request reductions
of 18.13% and 38.16% on full histories. Correctness, bounded opening, existing
replica reopening and offline warm controls pass. Consecutive non-improvements
remain **0**.

Tradeoffs: a new bounded protocol selector, coordinated SDK/server upgrade,
and disclosed speculative metadata bytes in short queries. This is a
performance acceptance, not an architecture-simplification claim. Remaining
SQL replay and tree/locator discovery work are still targets for this series.
