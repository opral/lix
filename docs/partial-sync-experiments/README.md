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
| E05 | Normalize singleton and batch metadata demand | Accepted architecture simplification: metadata variants 2 → 1, residency/hydration branches 2 → 1 each. Requests unchanged; paired cold history -0.78% dense and -0.57% sparse. Disclosed small cost, no material regression. 4,074 tests and 10 doctests pass. [Evidence](e05/README.md). | 0 |
| E06 | Bounded first-parent metadata selection | Accepted: dense64 native requests 320 → 262 and cold history 19.29% faster [18.55%, 19.67%]; sparse64 152 → 94 and 38.82% faster [38.07%, 40.00%]. Short-query overfetch disclosed. 4,315 tests pass; existing v17 replicas reopen with v18 without reset. [Evidence](e06/README.md). | 0 |
| E07 | Retain pending descriptor watch; clear completed recovery refresh | Accepted: total history HTTP attempts 524 → 262 dense and 188 → 94 sparse, identical counts in ten pairs. Native requests unchanged; no substantial latency gain claimed. Expired-recovery descriptor reads 262/94 → 1. 4,315 tests and 10 doctests pass. [Evidence](e07/README.md). | 0 |
| E08 | Bundle locator owner/header/catalog dependencies | Accepted: dense64 native requests 262 → 134 and cold history 48.05% faster [47.83%, 48.31%]; sparse64 94 → 78 and 15.37% faster [14.35%, 16.08%]. Short queries improve; warm costs disclosed. 4,318 tests pass; existing v18 replicas reopen v19 without reset. [Evidence](e08/README.md). | 0 |
| E09 | Cross-checkpoint native frontier | Rejected policy: full-history latency improves 82.82% dense / 84.95% sparse, but dense LIMIT1 is 162.45% slower and transfers 11.4× bytes. Promising discovery result, unacceptable unconditional lookahead. 4,322 tests and 10 doctests pass. [Evidence](e09/README.md). | 0 |
| E10 | Consumption-driven history lookahead | Rejected: full dense/sparse history gains, but wide history is 15.10% slower [14.46%, 16.01%]. Discovery prepares metadata-excluded commits. [Evidence](e10/README.md). | — |
| E11 | Restrict discovery to selected checkpoints | Accepted: dense cold history improves 80.09% and sparse 55.68% with 25 ms injected delay; wide regression removed. First covered dense query is about 10.7 ms slower, explicitly accepted and not hidden by later warm queries. [Evidence](e11/README.md). | 0 |
| E12 | Grow discovery with selected checkpoints traversed | Accepted: sparse full history improves 36.97% [36.33%, 37.68%] at zero injected delay; sparse LIMIT2/4 improve too. Small limited-query byte increases disclosed. [Evidence](e12/README.md). | 0 |
| E13 | Schedule discovery with completed checkpoints | Accepted: one counter and a conditional branch removed; sparse limited history improves 20–26%, empty history 82%. Wide-sparse LIMIT1 improves 36.88% with 37.88% more bytes. Full engine gate and final latency controls pass. [Evidence](e13/README.md). | 0 |
| E14 | Prune tree subtrees using query scope | Accepted: ordinary wide history 95.81% faster, wide-sparse 94.29% faster; requests 337 → 21 and 404 → 46. Wide-sparse LIMIT1 is 4.88% slower, explicitly disclosed. 4,325 tests and 10 doctests pass. [Evidence](e14/README.md). | 0 |
| E15 | Finite-ID scan without complete path index | Rejected: wide cold +97.39%, but dense cold −46.87%, many-directory cold −152.59%, and warm −72.62% to −446.95%; ten paired runs each. Retained indexed-read invariant also fails. No engine changes accepted. [Evidence](e15/README.md). | 1 |
| E16 | Scoped filesystem index and ancestor-read variants | Rejected: cold finite-ID gains up to 99.56%, but every follow-up variant regresses wide offline warm history by 22.65–25.85% after latency-injected hydration. Combined deep64 regression is below 10%; the warm-history failure remains. All four follow-up variants pass 4,331 tests and 10 doctests. [Evidence](e16/README.md). | 2 |
| E17 | Attribute warm-history timing and complete E16 validation | Accepts E16 combined after CPU-state controls: identical measured query-work counts; matched-state first-read interval excludes a 10% regression, steady warm reads +1.54%. Cold finite-ID gains up to 99.56%, with deep64 −6.09% explicitly retained. Full integration engine gate and 32 query-form controls pass. [Evidence](e17/README.md). | 0 |

Accepted E01–E08, E11–E14, and the combined E16 implementation validated by E17 are consolidated in [Lix #1817](https://github.com/opral/lix/pull/1817), the sole merge target. E09/E10/E15 and the original E16 decision retain their research evidence; E17 explicitly revises E16 after CPU-state attribution and completed controls. Research PRs preserve the experiment history. Nothing has been deployed or merged as part of this series.

First accepted implementation: [Lix #1810](https://github.com/opral/lix/pull/1810).
Related UI bug fix: [Atelier #174](https://github.com/opral/atelier/pull/174).
