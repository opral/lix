# E07: retain descriptor discovery across hydration

The descriptor long poll was canceled and recreated for each foreground native
miss. Preserve only its network future, keyed by the full local admission and
watch cursor. Reconciliation remains cancellable; no storage read or operation
gate is held over a retained network request. Explicit refresh, admission/cursor
changes and shutdown invalidate the pending request. Completed responses still
pass the normal lease deadline and publication checks.

The reopen control exposed a second bug: expired-lease recovery published fresh
coordinates but left the forced-refresh flag set. Subsequent native misses
fetched the same descriptor repeatedly. Clear that flag when successful recovery
has changed the admission; local edits and explicit refresh retain their existing
invalidation path.

## Correctness and compatibility

All 4,315 all-simulation/server-protocol tests and 10 doctests pass (84 tests
skipped). The watch regression completes eight foreground demands while one
watch remains blocked, then verifies that a remote change publishes a retained
negative scope and that adopting the new admission starts a new watch. The
public awaited-SQL expiry regression requires exactly one fresh descriptor
fetch during clean recovery.

Three alternating reopen pairs use separate copies of the same prepared E06
RocksDB replica and restored authority. All return equal ordered history and
work offline when warm. Dense recovery descriptor reads fall from 262 to 1;
sparse from 94 to 1. Each candidate also starts one pending watch. Native reads
remain 263 and 95, including the expired attempt. These runs overlapped compiler
work: their timing is not acceptance evidence. Raw request counts are preserved
in reopen-controls.jsonl.

Fresh opening also passes the 16,000-unrelated-file and unopened 8 MiB blob
controls: two foreground requests, no native reads, at most 8 KiB response
bodies. Ordered history and offline warm checks pass in both fixtures.

No protocol, SDK, repository storage or receipt changes. No migration/reset.
This adds one pending-network-future state slot; it is a performance/correctness
candidate, not a claim of fewer architectural moving parts.

## Native dependency trace

Dedicated synthetic loopback HTTP capture, request direction only; parsed
native counts must exactly match the benchmark counters. Committed summaries
contain categories only; packet captures and ephemeral test session headers
are not included. Dense history has 262 native requests: 64 change locators,
66 state headers, 64 tree chunks, 64 mutation catalogs and four metadata walks.
Sparse has 94: eight locators, ten headers, 64 tree chunks, eight catalogs and
four walks. Metadata walk response records are not counted as request addresses.
This experiment leaves these native dependencies unchanged.

## Fresh paired measurement

Ten alternating fresh-replica pairs, identical exported fixtures and harness,
real canonical HTTP/RocksDB, engine opt-level 2 with dev dependencies, zero
injected RTT and no concurrent task compiler/test workload. Seeded bootstrap
95% intervals describe paired timing improvement.

| Case | Total history HTTP attempts | Native attempts | Median history time before → after | Paired latency improvement [95% CI] |
| --- | --- | --- | --- | --- |
| Dense64 | 524 → 262 | 262 → 262 | 9.542 s → 9.435 s | 0.94% [0.33%, 1.46%] |
| Sparse64 | 188 → 94 | 94 → 94 | 1.522 s → 1.510 s | 0.08% [-0.90%, 1.45%] |

All ten pairs have the same request counts: a deterministic 50% reduction in
history HTTP attempts. The eliminated requests were canceled descriptor watches,
not native data requests. One watch starts before history and remains pending;
it is not newly counted in the history phase. Response bodies stay identical at
227415 bytes dense and 150203 bytes sparse. Body counts exclude headers/TLS and
cannot quantify the transport-byte savings from fewer requests.

No substantial history-latency improvement is claimed. Opening, file selection
and warm-read intervals exclude a 10% regression for both full64 cases. Ten alternating pairs each also cover LIMIT1 and dense4:

| Control | Total history HTTP attempts | Paired history latency improvement [95% CI] |
| --- | --- | --- |
| Dense64 LIMIT1 | 16 → 8 | -0.65% [-3.03%, 2.57%] |
| Sparse64 LIMIT1 | 34 → 17 | -1.47% [-3.42%, 2.74%] |
| Dense4 | 38 → 19 | -0.67% [-1.88%, 1.46%] |

Native request counts and response bytes are unchanged in these controls.
Opening, file selection and offline warm-read intervals also exclude a 10%
regression. Small negative timing point estimates are disclosed; there is no
short-query speedup claim.

## Decision

Accept for the deterministic 50% reduction in history HTTP attempts, reproduced
across all 50 fresh baseline/candidate pairs, and the recovered-replica refresh
bug fix. Correctness, opening and warm-read controls pass. This does not qualify
as a >10% latency gain or architecture simplification. Consecutive substantive
non-improvements remain **0**.

The next optimization should reduce native dependency-discovery depth or SQL
replay work. This change removes redundant descriptor traffic but leaves both
of those costs in place.

## Reproduction

Build baseline ef38c1b15 and this candidate with the same benchmark harness and
engine opt-level 2 as described in ../public-history-benchmark.md. Use
../paired_history.py with --pairs 10 --cases dense64,sparse64 --rtt 0, then repeat
with LIX_PROFILE_HISTORY_LIMIT=1 and separately --cases dense4. Supply a shared
--fixtures directory and separate --output paths. Do not run compilers or other
benchmarks concurrently with latency pairs.

For recovery controls, use the baseline benchmark with
LIX_PROFILE_PREPARE_REPLICA=1 and LIX_PROFILE_PERSISTED_REPLICAS pointing to an
empty directory. Then run compare_reopen.py BASELINE CANDIDATE PREPARED PREFIX
--fixtures FIXTURES --pairs 3. It creates independent copies for each run;
PREFIX and the copied-store paths must not already exist. Recovery is measured
separately from fresh-replica timings.
