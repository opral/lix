# E04: group both diff endpoint descriptors — rejected

Hypothesis: after E03 groups a commit's header and graph record, grouping both
endpoints and sharing their graph reader could expose more missing inputs per
retry and reduce local read overhead.

The prototype replaces two descriptor calls with one pair operation, reads both
topologies concurrently, and reads graph nodes in one exact batch. It validates
all hard failures before producing a bounded missing frontier. An initial test
caught overbroad legacy history diagnostics: a resident endpoint was included
in `commitIds`. That bug was fixed before the final comparison. The corrected
prototype passes 4,074 all-simulation tests (74 skipped), including the explicit
transaction test and the new frontier/corruption tests.

## Measurement

Ten alternating pairs against accepted E03 on identical snapshots, fresh RocksDB
partial replicas, real loopback HTTP, engine opt-level=2, no injected RTT, and
no concurrent compiler/test workload. All runs compare ordered results against
the authority and check the covered query offline.

| Case | Native requests | Median history before → after | Median paired improvement, 95% interval |
| --- | --- | --- | --- |
| Dense 64 checkpoints | 320 → 320 | 11.774 s → 11.673 s | 0.81% [0.19%, 1.43%] |
| Sparse 64 checkpoints | 152 → 152 | 2.513 s → 2.508 s | 0.24% [-0.72%, 1.42%] |

No measured opening, file-selection, history, or warm-read metric reaches a
10% supported improvement. Opening remains two requests and no native reads.
Request counts are identical in every run. The result indicates that the other
endpoint is generally already resident when these workloads miss an input;
grouping known identities alone does not imply an additional useful batch.

This is also not an architecture simplification: a shared temporary graph
reader replaces two local reader instances, but there is no removed component
or state owner. The helper adds a loop, result vector, missing-commit collection,
and conditional bookkeeping. The small CPU change does not justify that cost.

Decision: reject. Keep E03 as the baseline. Consecutive non-improvements: **1**.
The patch is retained as an experiment artifact, not applied to production code.
The first compile error and initial diagnostic-test failure were development
failures; neither was counted as an optimization result.
