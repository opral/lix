# E05: normalize native metadata demands

Hypothesis: a singleton and a batch are the same operation with different
cardinality. Maintaining separate runtime variants duplicates residency,
ancestry, hydration and baseline-write-frontier handling.

The parser now normalizes both existing diagnostic formats to one bounded
vector. Residency, immutable input validation, epoch fencing, CAS guards and
baseline-write-frontier preparation use the existing batch implementation.
The original error remains available for full-replica fallback. There is no
protocol, storage or application API change.

## Architecture inventory

| Production mechanism | Before | After |
| --- | ---: | ---: |
| Native metadata demand variants | 2 | 1 |
| All production demand variants | 9 | 8 |
| Metadata residency dispatch branches | 2 | 1 |
| Metadata hydration dispatch branches | 2 | 1 |
| Additional state owners, caches or protocol routes | 0 | 0 |

This removes duplicated control flow rather than removing validation. The
single-input path already allocated a vector inside the hydrator; normalization
moves that allocation to the boundary. Performance is a regression gate, not
a claimed reason to accept this change.

## Validation

All 4,074 all-simulation tests passed (74 skipped); all 10 documentation tests
passed. Regression coverage verifies both single and one-element batch wire
diagnostics normalize to the same demand and retain the original error.
Existing resident-corruption, pinned transaction, local contention and epoch
change tests remain in the full suite.

Dense4, dense64, sparse64, 16,000-unrelated-file and unopened-8-MiB-blob smoke
controls preserve ordered authority results, offline warm reads and opening
with one handshake, one descriptor and zero native reads. Smoke timing was
collected alongside compilation and is not latency evidence.

## Paired performance check

Ten alternating pairs against accepted E03, identical fixture snapshots, fresh
RocksDB replicas, engine opt-level=2, real loopback HTTP and no injected RTT.
No concurrent task compiler or test workload ran during these measurements.

| Case | Native requests | Median cold history before → after | Paired improvement, 95% interval |
| --- | --- | --- | --- |
| Dense64 | 320 → 320 | 11.734 s → 11.808 s | -0.78% [-1.35%, -0.29%] |
| Sparse64 | 152 → 152 | 2.508 s → 2.523 s | -0.57% [-1.43%, 0.15%] |

Dense cold history has a small measured slowdown; this is not a speedup.
Opening medians improve from 9.350 to 9.064 ms and 8.744 to 8.626 ms, without
a >10% supported claim. Warm medians move from 67.626 to 68.048 ms and 26.660
to 26.856 ms; their paired intervals include zero and exclude a 10% regression.
File selection intervals also exclude a 10% regression. Complete metrics and
all observations are retained alongside this report.

Decision: accept for reduced control flow with the disclosed sub-1% median
cold-history cost; no material regression at the series' 10% threshold. No
performance improvement is claimed. Consecutive non-improvements reset to **0**.

