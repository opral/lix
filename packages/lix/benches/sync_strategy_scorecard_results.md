# Protocol scorecard sample

This is a quick Criterion run on the protocol simulator, captured with:

```text
cargo bench -p lix --bench sync_strategy_scorecard -- --noplot \
  --warm-up-time 0.05 --measurement-time 0.1 --sample-size 10
```

All 21 strategy/scenario runs converged, preserved branch isolation, reported
no lost disjoint writes, and returned idempotent retry receipts. Across the
six scenarios, the aggregate wire counters were:

| Strategy | Upload bytes | Download bytes | Minimum fast-forwards |
| --- | ---: | ---: | ---: |
| tx + event | 120,303 | 318,317 | 16 |
| tx + commit | 120,303 | 298,603 | 62 |
| commit both ways | 124,969 | 298,603 | 62 |

The per-scenario deterministic counters were:

| Scenario | Strategy | Upload bytes | Download bytes | Fast-forwards | Overlap rows |
| --- | --- | ---: | ---: | ---: | ---: |
| disjoint rows | tx + event | 19,086 | 51,464 | 32 | 0 |
| disjoint rows | tx + commit | 19,086 | 48,284 | 128 | 0 |
| disjoint rows | commit both ways | 19,918 | 48,284 | 128 | 0 |
| hot conflicts | tx + event | 18,722 | 50,736 | 32 | 60 |
| hot conflicts | tx + commit | 18,722 | 47,556 | 128 | 60 |
| hot conflicts | commit both ways | 19,554 | 47,556 | 128 | 60 |
| offline queue | tx + event | 28,388 | 76,176 | 48 | 88 |
| offline queue | tx + commit | 28,388 | 71,396 | 192 | 88 |
| offline queue | commit both ways | 29,649 | 71,396 | 192 | 88 |
| plugin rows | tx + event | 14,754 | 39,456 | 24 | 42 |
| plugin rows | tx + commit | 14,754 | 37,076 | 96 | 42 |
| plugin rows | commit both ways | 15,378 | 37,076 | 96 | 42 |
| filesystem projection | tx + event | 15,682 | 37,968 | 16 | 28 |
| filesystem projection | tx + commit | 15,682 | 35,684 | 64 | 28 |
| filesystem projection | commit both ways | 15,746 | 35,684 | 64 | 28 |
| branch isolation | tx + event | 9,347 | 24,501 | 16 | 27 |
| branch isolation | tx + commit | 9,347 | 22,971 | 62 | 27 |
| branch isolation | commit both ways | 9,763 | 22,971 | 62 | 27 |
| crash after ack loss | tx + event | 14,324 | 38,016 | 24 | 42 |
| crash after ack loss | tx + commit | 14,324 | 35,636 | 96 | 42 |
| crash after ack loss | commit both ways | 14,961 | 35,636 | 96 | 42 |

For the crash-after-ack-loss scenario, every strategy recorded exactly one
server-side duplicate admission and one recovery, with one canonical event
rather than two. The offline scenario similarly recorded one idempotent retry.

The sample supports canonical commit-pack replication: it removes roughly
6% of pull bytes and exposes a parent commit, allowing every canonical
delivery in the simulator to use the fast-forward path. Commit-pack admission
adds roughly 4% upload bytes without improving convergence or storage counts.
The quick Criterion run measured these central-time ranges: tx + event
189–866 µs, tx + commit 190–850 µs, and commit both ways 196–876 µs. The
simulator's runtime is not a production decision: transaction-event pull wins
on the disjoint and branch-isolation cases, while transaction-commit pull wins
on the other four. The next gate is an adapter-backed run using the real Lix
server and FilesystemStorage/RocksDB counters before declaring a final format
choice.
