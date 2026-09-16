# E08: bounded native dependency bundles

Accepted: exact locator responses include their validated owner header and
mutation catalog. Dense64 native requests fall 262→134 and cold history improves
48.05% [47.83%, 48.31%]. Sparse64 requests fall 94→78 and cold history improves
15.37% [14.35%, 16.08%]. Applications continue ordinary awaited Lix queries;
opening remains bounded and independent of these history downloads.

## Why batching alone left these requests

E07's synthetic wire trace shows dense64 has 64 locator reads, 66 header reads,
64 mutation catalog reads, 64 tree reads and four metadata walks. Sparse64 has
8 locator, 10 header, 8 catalog, 64 tree reads and four walks. Exact address
batching cannot combine an owner/catalog address the client has not learned yet.
The authority already resolves canonical locator ownership.

The response now carries an optional bundle using existing native record formats.
The client derives permitted owner IDs from exact locator bytes, then catalog
addresses/digests from validated owner headers. It rejects unrelated records,
duplicates, incorrect digests and excess payloads. Metadata and objects install
atomically through existing immutable write paths and the exact durable-admission
epoch guard. No network await holds a local storage read.

The combined response is bounded to 32 records and 256 KiB decoded bytes.
Optional absence, corrupt bytes or payload overflow omit companions; normal exact
demand remains the fallback. Required reads retain their errors. Optional storage
I/O errors still propagate. Bundles certify no logical absence or completeness.
Empty bundles are omitted from the wire.

## Measurements

Canonical loopback HTTP authority, fresh RocksDB replicas, same exported fixtures
and harness, engine opt-level 2 with dev dependencies, zero injected RTT. No
compiler, test suite or other benchmark ran during the timing pairs. Medians and
seeded bootstrap 95% intervals are from alternating baseline/candidate runs.
All runs check equal ordered authority results and offline warm behavior.

| Query | Pairs | Native requests before → after | Median cold time before → after | Paired improvement [95% interval] |
| --- | ---: | --- | --- | --- |
| Dense64 | 10 | 262 → 134 | 9.450 s → 4.905 s | 48.05% [47.83%, 48.31%] |
| Sparse64 | 20 | 94 → 78 | 1.502 s → 1.273 s | 15.37% [14.35%, 16.08%] |
| Dense64 LIMIT1 | 10 | 8 → 4 | 54.903 ms → 31.548 ms | 43.22% [40.65%, 45.44%] |
| Sparse64 LIMIT1 | 10 | 17 → 13 | 130.156 ms → 101.006 ms | 22.58% [20.64%, 25.50%] |
| Dense4 | 10 | 19 → 11 | 130.159 ms → 80.992 ms | 37.43% [33.98%, 39.05%] |

Native response bodies decline 227415→218391 bytes dense64 and 150203→149075
sparse64. Short-query bodies also decline: 5035→4753 dense LIMIT1, 29508→29226
sparse LIMIT1 and 14689→14125 dense4. These are HTTP body bytes, not headers/TLS.
Descriptor requests are zero in both versions during history; relative change
is undefined and the report uses absolute counts.

The initial ten sparse pairs had a wide file-selection improvement interval
[-10.57%, 1.31%]. We added ten sparse pairs to resolve that control, retaining
all initial and additional raw data. Across twenty pairs it is [-9.05%, 0.29%],
with a -1.33% paired point estimate. All opening, file-selection and warm-read
intervals across the final comparisons exclude a 10% regression. Sparse warm
reads have a small measured slowdown: 1.11% [0.40%, 1.95%], median 26.628→26.888 ms.
Sparse LIMIT1 warm slowdown is 0.46% [0.07%, 1.77%]. No warm speedup is claimed.
The adaptive sample-size extension and original ten-pair summary are preserved.

The first summary attempt divided by zero on the eliminated descriptor count.
The runner now reports undefined relative change for zero baselines and supports
--summarize-only. The successful raw benchmark runs were retained and summarized;
no failed or unfavorable measurements were replaced.

## Validation and compatibility

- 4,318 all-simulation/server-protocol tests pass; 84 skipped. All three new
  bundle tests cover derived addresses, corruption, duplicates, response limits,
  optional fallback and atomic epoch-fenced installation.
- 10 doctests, 19 SDK admission tests, TypeScript checking and one Chromium
  SharedWorker admission regression pass. The browser test uses a stub engine;
  host benchmark latency is not production browser latency.
- Three alternating pairs reopen actual protocol18 partial RocksDB replicas
  through protocol19 without reset, return equal ordered history and work offline
  when warm. Native requests fall 263→135 dense and 95→79 sparse, including the
  expired attempt. Native bodies fall 227577→218553 and 150365→149237 bytes.
  These control timings overlapped compilation and are not latency evidence.
- Wide16000 and unopened 8 MiB blob controls open with two foreground requests,
  zero native reads and at most 8 KiB response bodies. Ordered history and offline
  warm checks pass. The wide history still uses 338 native requests versus E07's
  346, and file selection takes 6.34 s in this smoke: wide-tree discovery remains
  unresolved. This does not turn all repository operations into constant work.

Protocol19 requires coordinated SDK/server upgrade. Repository format81 and
partial receipt v2 are unchanged, so no repository-data migration is needed.
The SDK admission epoch changes to19; its existing cache requires online
re-admission before using credentials admitted under protocol18.

## Tradeoffs and decision

Accept: the lower 95% cold-history improvement bound exceeds 10% in every query
comparison, with deterministic request reductions and passing controls. The
consecutive substantive non-improvement count remains **0**. This is a performance
acceptance, not a claim of fewer architectural components.

Costs: a bounded selection/validation module and response field; coordinated
protocol upgrade; potential duplicate or unused companions outside measured
queries; and small disclosed warm timing costs. Selection rereads companion
bytes after canonical locator resolution. The storage API reads whole values
internally, so the response cap is not a bound on peak authority allocation for
an individual stored object. No storage layout, cache or application prefetch
API was introduced.

The remaining per-checkpoint trees and repeated SQL prefix work are the next
experimental targets. A bounded discovery pass across already-known history
checkpoints may reuse existing native batches; that is an unmeasured hypothesis.

## Reproduction and provenance

Build E07 commit51c6e10bb and this candidate using the unchanged benchmark harness
and engine opt-level 2 in ../public-history-benchmark.md. Use ../paired_history.py
with --pairs 10 --cases dense64,sparse64 --rtt 0 and separate output paths; repeat
with LIX_PROFILE_HISTORY_LIMIT=1 and separately --cases dense4. The additional
sparse comparison uses --pairs 10 --cases sparse64. The combined twenty-pair file
retains original sparse pairs0..9 and assigns additional pairs10..19. Summarize
that file using --summarize-only --pairs 20 --cases sparse64.

For upgrade controls, prepare a protocol18 replica using
LIX_PROFILE_PREPARE_REPLICA=1 and an empty LIX_PROFILE_PERSISTED_REPLICAS path.
Run compare_reopen.py BASELINE CANDIDATE PREPARED PREFIX --fixtures FIXTURES
--pairs 3. It copies the prepared store separately for each variant. Native counts
may differ, but snapshots and ordered results must match. Fresh timing runs never
use persistent replica paths. Use separate wide16000,blob8m opening controls.

manifest.json records source/binary/harness hashes and SDK/OpenAPI provenance.
built-engine.patch.gz decompresses to the measured engine patch with the hash in
the manifest. Raw measurements are synthetic; private HAR and packet captures
are not included. Nothing in this draft series is merged or deployed.
