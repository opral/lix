# E09: bounded cross-checkpoint dependency discovery

**Rejected for a material short-query regression.** Batching across known history
checkpoints removes substantial repeated work, but an unconditional 16-checkpoint
lookahead is the wrong policy for consumers that need only the latest change.
The implementation is preserved as a compressed patch, not enabled in this branch.

## Architecture and hypothesis

Ordinary history execution walks checkpoints and stops at the first missing
native input. Metadata walks make several parent coordinates available, but
individual trees and change locators still emerge one checkpoint at a time.
E09 inspects up to 15 additional known parents after a real diff-stream miss,
using shared native diff preparation without executing SQL expressions or UDFs.
It combines missing addresses of the same kind in the existing 32-address batch.

```mermaid
flowchart LR
  SQL[Await ordinary SQL history] --> Miss[Required native input missing]
  Miss --> Discover[Prepare known parent checkpoint inputs]
  Discover --> Batch[Required prefix plus optional suffix]
  Batch --> HTTP[Existing native batch endpoint]
  HTTP --> Install[Validate and atomically install]
  HTTP -->|Speculative failure| Exact[Retry required prefix only]
  Exact --> Install
  Install --> Retry[Retry ordinary SQL]
```

Optional remote absence/corruption must not fail an otherwise answerable short
query. A typed required-prefix annotation therefore follows the demand through
residency and transaction refresh. A failed combined batch falls back to exact
required inputs; lease, repository, and protocol errors retain their meaning.
Pinned snapshot refresh admits valid available optional inputs and skips invalid
or absent optional records. Required corruption still fails normally.

This adds a discovery module, required-prefix diagnostic, fallback branch, and
optional pinned-refresh handling. It does **not** simplify the architecture.
No public API, transport endpoint, protocol version, or storage format changes:
server protocol 11, sync protocol 19, storage format 81 remain unchanged.

## Paired results

Ten alternating E08/E09 pairs per case, persisted identical synthetic fixtures,
fresh RocksDB replicas, real HTTP, and unchanged public awaited SQL harness.
No builds or tests ran during timing. Each invocation compares ordered rows with
the authority and verifies that the warm query works with the server offline.
Reported improvements use paired medians with 10,000 bootstrap resamples; the
intervals below are 95% intervals. These are local synthetic results, not
production or high-RTT measurements.

| Query | Native requests E08 → E09 | Median latency E08 → E09 | Paired improvement [95% interval] |
| --- | ---: | ---: | ---: |
| Dense 64, full history | 134 → 24 | 4,880.7 → 840.0 ms | 82.82% [82.62%, 82.96%] |
| Sparse 64, full history | 78 → 14 | 1,275.9 → 192.1 ms | 84.95% [84.83%, 85.03%] |
| Dense 4, full history | 11 → 5 | 83.4 → 38.7 ms | 52.93% [51.10%, 55.47%] |
| Dense 64, LIMIT 1 | 4 → 7 | 31.7 → 84.5 ms | **−162.45% [−179.69%, −157.21%]** |
| Sparse 64, LIMIT 1 | 13 → 6 | 100.7 → 48.6 ms | 51.81% [50.78%, 53.17%] |

Dense full-history native bytes decrease 218,391 → 206,185; sparse decreases
149,075 → 144,018. However, dense LIMIT1 grows **4,753 → 54,197 bytes (11.4×)**,
and sparse LIMIT1 grows 29,226 → 54,018 bytes (+84.83%). The dense short-query
regression is well beyond measurement noise; full-query improvements do not
justify enabling this policy for all ordinary SQL consumers.

Warm full-history paired changes were −0.70% dense [−1.49%, +0.20%], −1.83%
sparse [−5.36%, +0.19%], and +2.88% dense4 [−0.87%, +6.34%]. All raw opening,
file-selection, warm, request and byte metrics are retained in the summaries.

Opening controls with 16,000 unrelated files and an unopened 8 MiB blob each
use two foreground requests, zero native requests, and less than 2.3 KB of
response data. Their single-run opening latencies were 9.89 and 9.41 ms;
these are smoke measurements, not statistical proof of size independence.
Wide-tree history still needs 324 native requests: this prototype does not solve
wide-tree discovery.

## Correctness and decision

Final-source engine gate: 4,322 tests pass with `all-simulations,server-protocol`
(84 skipped), plus 10 doctests. Tests cover bounded discovery, original required
errors, optional missing/corrupt HTTP responses, required failure propagation,
pinned refresh, and invalid required-prefix diagnostics. An earlier fault fixture
omitted the protocol error envelope; the fixture was corrected without relaxing
its expected error. Earlier smoke timings overlapped compilation and are excluded
from paired results.

Do not merge the prototype. Keep E08 as the accepted baseline. The next candidate
should grow lookahead from actual downstream consumption, then test LIMIT1,
LIMIT2, and LIMIT4 to ensure it does not merely move the overfetch cliff. Inexact
SQL filters can prevent a global LIMIT from reaching the scan, so blindly relying
on the provider's pushed limit is insufficient.

The five-consecutive-no-improvement stopping streak remains **0**: E09 has real
statistically meaningful gains and identifies a promising refinement, even though
the policy is rejected for its regression. It must not be counted as evidence that
further meaningful improvement is exhausted.

## Reproduce

Apply `rejected-prototype.patch.gz` to E08 (`e0d831a3`) in a separate worktree.
Build the `partial_replica_history` bench through `tooling/Cargo.toml` with
`--features server-protocol`, `profile.dev.package.lix.opt-level=2`, and
`CARGO_INCREMENTAL=0`. Run `../paired_history.py` against saved E08 and candidate
executables with ten pairs and `--cases dense4,dense64,sparse64`; rerun with
`LIX_PROFILE_HISTORY_LIMIT=1` and `--cases dense64,sparse64`. Use the same
`--fixtures` directory. Manifest, exact patch, artifact metadata, raw pairs,
summary JSON, and opening controls accompany this report.
