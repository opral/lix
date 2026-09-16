# On-demand partial replica optimization series

Goal: repeatedly experiment, profile and accept changes that improve a relevant
metric by more than 10% with supporting evidence, or simplify architecture
(fewer independently managed states, branches or components) without material
performance regression. Continue until five consecutive substantive experiments
produce neither kind of improvement. An accepted change resets that count.
Bug fixes discovered along the way are in scope and require regression coverage.

## Invariants

- Opening transfers bounded repository coordinates, independent of repository
  rows, history depth, branches and unopened content size. No eager inventory or
  history download to hide subsequent query cost.
- Applications use ordinary awaited Lix operations. No application-managed
  prefetch, internal recovery codes, or query retry loop.
- Cold results and order match the authority; warm covered reads work offline.
- Pending edits, transaction snapshots, lease expiry and publication remain
  correct. Never replay a durable commit as a hydration retry.
- Breaking changes are allowed. Storage changes must use Lix-owned migration;
  protocol-only changes must not require a storage reset or manual migration.

## Acceptance method

Record baseline and candidate source revisions, environment, fixture seed,
request/byte counts, query work, latency and opening/warm controls. Test dense
and sparse file histories, starting at 64 checkpoints, plus smaller/larger
controls. Establish the public awaited-query baseline before replacing the
initial known-dependency oracle with a real optimization.

For latency, use independent fresh replicas, alternating paired baseline and
candidate runs on the same fixture, at least 10 pairs where feasible. Report
medians and a seeded bootstrap confidence interval for the paired improvement.
Accept a latency claim when the lower 95% bound exceeds 10%; increase samples
when inconclusive. For deterministic work counts, verify equivalent results
and repeat across fixtures/runs; report all variability rather than treating
repeated identical counters as noisy timing samples. Opening and warm-read
regressions are separate vetoes, not hidden in an aggregate score.

Architecture acceptance requires an explicit before/after inventory of state,
branches and ownership. Deleting checks without preserving their invariants is
not simplification. Test/build failures are unfinished experiments, not failed
optimization attempts. The five-failure stopping rule applies to completed,
substantive experiments against the latest accepted stack.

## Ledger

| ID | Experiment | Evidence / decision | Consecutive non-improvements |
| --- | --- | --- | ---: |
| E00 | Known-dependency oracle, 64 checkpoints | Dense: 394 → 14 fetches, 12,359 → 64 diff plans. Sparse (1/8 checkpoints relevant): 226 → 8 fetches, 7,032 → 64 diff plans. Three runs, equal ordered rows and payload per pair. Diagnostic only: address discovery excluded. Not an accepted implementation. | 0 |
| E01 | Preserve credential verification on canceled requests | Chromium SharedWorker, real HTTP/RPC with stub engine: initial attachment plus five canceled requests used 6 admissions before, 1 after (83.3% less). Rotation, identity drift, rejection and offline behavior remain covered. Accepted correctness + deterministic request reduction; no production-latency claim. | 0 |
| E02 | Public awaited-history baseline and opening controls | Canonical HTTP authority + RocksDB replica: dense64 uses 381 native requests; sparse64 uses 213. Ordered results match the authority and warm reads work offline. Opening uses two requests and zero native reads, including 16,000 unrelated files and an unopened 8 MiB blob. Diagnostic unoptimized timing includes a retained 30-second deadline failure; optimized paired timing follows. See [benchmark method](public-history-benchmark.md). | 0 |
| E03 | Discover independent commit metadata together | Accepted: dense64 requests 381 → 320; sparse64 213 → 152. Ten paired runs: cold history improves 16.45% [15.86%, 17.00%] and 29.11% [28.26%, 29.46%]. Full simulations and doctests pass; opening and offline warm controls preserved. [Evidence](e03/README.md). | 0 |
| E04 | Group both diff endpoint descriptors | Rejected: unchanged native requests; paired history improves only 0.81% [0.19%, 1.43%] dense and 0.24% [-0.72%, 1.42%] sparse. More local bookkeeping, no removed component. Corrected prototype passes 4,074 simulation tests. [Evidence](e04/README.md). | 1 |

PRs are drafts and stacked. Nothing is deployed or merged by this series.

First accepted implementation: [Lix #1810](https://github.com/opral/lix/pull/1810).
Related UI bug fix: [Atelier #174](https://github.com/opral/atelier/pull/174).
