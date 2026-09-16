# E10: consumption-driven checkpoint discovery

**Rejected for a wide-history regression.** Growing lookahead from actual downstream
consumption preserves LIMIT1 behavior and substantially improves long histories.
However, the final policy slows history on a 16,000-file repository by 15.10%
(95% interval: 14.46–16.01%). The prototype is retained as a compressed patch;
no runtime changes are enabled in this report branch. E08 remains the accepted baseline.

## Architecture and variants

E09 always looked ahead up to 16 known checkpoints. E10 starts with no lookahead
and adds one checkpoint for each nonempty batch that downstream consumes and then
resumes, capped at 16. Counting resumed batches rather than produced rows avoids
inflating the window when a single checkpoint produces many rows. The engine still
accepts ordinary awaited SQL; applications do not discover or fetch dependencies.

Discovery prepares native diff inputs across known first-parent checkpoints and
batches missing addresses of the same kind. It never evaluates speculative SQL
expressions or UDFs. Required-prefix annotations distinguish inputs necessary to
answer the query from optional lookahead. Optional failures fall back to required
inputs, and pinned snapshot refresh tolerates unavailable optional records.

Three policies were measured during development:

1. Adaptive consumption alone retained strong full-history gains, but optional
   graph records triggered nested metadata walks and excessive LIMIT2 bytes.
2. Suppressing the nested walk reduced that overfetch, but fetching optional graph
   records individually suppressed ordinary required graph misses. Dense-history
   gains fell to an inconclusive 10.66% [9.97%, 11.44%].
3. The final variant stops discovery at unknown graph topology. Required graph
   misses retain the existing bounded metadata walk. This restores dense-history
   gains and keeps dense LIMIT2 requests and bytes equal to E08.

The final variant still adds discovery, annotations, fallback, and refresh branches.
It is a performance experiment, not an architectural simplification. Public API,
server protocol 11, sync protocol 19, and storage format 81 are unchanged.

## Final paired measurements

Fresh RocksDB partial replicas, identical persisted synthetic fixtures, real HTTP,
zero injected RTT, and the existing public SQL harness. Every run compares ordered
rows against the authority and verifies a warm query with the server offline.
No compilers, tests, or other benchmarks ran during paired timing. Intervals use
10,000 seeded bootstrap resamples of paired relative improvements. Positive values
mean faster; negative values mean slower. Local synthetic measurements do not
establish production or high-RTT performance.

| Query | Native requests E08 → E10 | Median history ms E08 → E10 | Paired improvement [95% interval] |
| --- | ---: | ---: | ---: |
| dense4, full | 11 → 10 | 81.8 → 75.2 | 7.99% [6.05%, 12.04%] |
| dense64, full | 134 → 29 | 4893.1 → 871.9 | 82.27% [82.12%, 82.43%] |
| sparse64, full | 78 → 37 | 1275.6 → 504.9 | 60.27% [60.00%, 60.48%] |
| dense64, limit1-20 | 4 → 4 | 31.9 → 31.8 | 0.33% [-2.15%, 2.85%] |
| sparse64, limit1-20 | 13 → 13 | 100.9 → 100.9 | 0.24% [-0.75%, 0.91%] |
| dense64, limit2 | 7 → 7 | 54.2 → 55.0 | -1.81% [-3.73%, -0.23%] |
| sparse64, limit2 | 22 → 18 | 194.7 → 156.8 | 19.56% [17.85%, 22.03%] |
| dense64, limit4 | 12 → 10 | 101.4 → 84.3 | 17.51% [14.70%, 18.39%] |
| sparse64, limit4 | 41 → 26 | 473.9 → 276.7 | 41.49% [41.17%, 41.84%] |
| wide16000, opening-10 | 338 → 337 | 3719.6 → 4293.0 | -15.10% [-16.01%, -14.46%] |
| blob8m, opening-10 | 11 → 10 | 80.7 → 74.7 | 7.93% [4.09%, 10.25%] |

Full histories and LIMIT2/4 each use ten alternating pairs. LIMIT1 uses two
blocks of ten (20 pairs total), expanding an initially inconclusive file-opening
control; all observations are retained. Wide/blob controls use an initial block
of three plus seven additional pairs after observing the regression (six pairs
baseline-first and four candidate-first). This adaptive sampling is disclosed;
no unfavorable observations were removed.

Dense LIMIT1 keeps 4 native requests and 4,753 bytes; sparse LIMIT1 keeps 13 and
29,226. Dense LIMIT2 keeps 7 requests and 7,995 bytes, with a small 1.81% latency
cost [0.23%, 3.73%]. Dense LIMIT4 transfers 29,948 versus 27,942 bytes (+7.18%).
Full dense bytes fall 218,391 → 206,786; sparse falls 149,075 → 145,831.

Wide/blob repository opening still takes two foreground requests, zero native
reads, and 2,280/2,274 response bytes. Wide opening latency changes by −0.71%
[-2.36%, 1.71%]; file-selection latency by −0.24% [-1.21%, 0.44%]. These controls
support preserved opening behavior at the tested sizes, not a universal timing
proof. The wide regression occurs specifically during history: 338 → 337 requests
barely changes network work while latency rises from 3.72 to 4.29 seconds.

## Correctness, decision, and next experiment

The exact final source passed 4,322 engine tests with
`cargo nextest run -p lix --features all-simulations,server-protocol` (84 skipped)
and 10 doctests with `cargo test -p lix --doc`. The graph-absence fixture uses the
sanctioned changelog deletion helper; an earlier direct-delete fixture was fixed
without weakening the repository writer-site guard. Tests cover bounded discovery,
required/optional errors, invalid annotations, unknown topology, and pinned refresh.

Do not enable E10. Large long-history gains do not justify a material wide-history
regression. The next experiment should measure local work spent in speculative
native preparation and bound unproductive discovery. Repeated tree traversal is a
hypothesis being profiled, not yet a demonstrated root cause. CPU-instrumented runs
are diagnostic only and excluded from the latency samples above.

The conservative five-consecutive-no-meaningful-improvement streak remains **0**:
E10 finds large statistically meaningful gains but fails a separate regression
veto. It does not establish that further improvement is exhausted.

## Reproduce

Apply `rejected-prototype.patch.gz` to E08 (`e0d831a3037a92e8ced7bab8bd5f0b264de219af`)
in a separate worktree. Build the `partial_replica_history` benchmark through
`tooling/Cargo.toml` with `--features server-protocol`,
`profile.dev.package.lix.opt-level=2`, and `CARGO_INCREMENTAL=0`. Use
`../paired_history.py` against saved E08 and candidate executables with the same
fixture directory. Cases: `dense4,dense64,sparse64`; limit controls use
`LIX_PROFILE_HISTORY_LIMIT=1`, `2`, or `4` and `dense64,sparse64`; opening controls
use `wide16000,blob8m`. Preserve each sampling block before combining pair indices.
Raw final pairs, summaries, exact patch, hashes, and artifact metadata accompany
this report. Earlier policy variants remain external research artifacts and are
not the source of the final table.
