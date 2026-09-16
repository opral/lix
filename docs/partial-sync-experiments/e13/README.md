# E13: completed-checkpoint discovery scheduling

**Accepted: sparse/empty-history gains and simpler scheduling.** E12 tracks selected checkpoints visited and separately
tracks progress exposed after a nonempty result batch is resumed. E13 replaces
that logic with one counter advanced when the selected checkpoint's diff stream
finishes. Reaching that point means the consumer is still polling for history,
including when the diff produced no rows.

This removes one counter, a nonempty-batch Boolean, and a conditional branch.
It is a narrow simplification of discovery scheduling; it does not remove SQL
retries, native dependency layers, or required/optional input handling. The
16-checkpoint bound and checkpoint-selection constraint are unchanged. No public
API, protocol, storage format, or migration behavior changes.

## Standard paired measurements

Ten alternating
E12/E13 pairs use identical persisted synthetic fixtures, fresh RocksDB partial
replicas, real HTTP, and ordinary awaited SQL. LIMIT1 includes twenty pairs after
an initial file-opening control was inconclusive; all initial and extra observations
are retained. Each invocation verifies ordered results against the authority and
runs a covered query with native fetching disabled. No builds or tests ran during
timing. Intervals are 95% seeded bootstrap intervals for paired relative improvement.

| Query | Native requests E12 → E13 | Median history ms E12 → E13 | Improvement [95% interval] |
| --- | ---: | ---: | ---: |
| dense4, full, 0 ms | 10 → 10 | 75.4 → 74.3 | 0.34% [-3.61%, 3.26%] |
| dense64, full, 0 ms | 29 → 29 | 852.8 → 862.4 | -0.74% [-1.65%, 1.68%] |
| sparse64, full, 0 ms | 25 → 20 | 320.8 → 287.3 | 10.63% [9.53%, 11.82%] |
| dense64, limit1-20, 0 ms | 4 → 4 | 31.6 → 31.6 | 0.27% [-2.25%, 1.62%] |
| sparse64, limit1-20, 0 ms | 13 → 9 | 100.8 → 74.8 | 25.61% [24.67%, 26.86%] |
| dense64, limit2, 0 ms | 7 → 7 | 57.1 → 54.7 | 4.44% [-0.60%, 9.03%] |
| sparse64, limit2, 0 ms | 15 → 11 | 127.5 → 100.6 | 21.56% [19.91%, 22.63%] |
| dense64, limit4, 0 ms | 10 → 10 | 84.4 → 83.4 | 1.76% [-0.85%, 4.62%] |
| sparse64, limit4, 0 ms | 19 → 14 | 189.3 → 151.2 | 20.35% [18.37%, 21.48%] |
| wide16000, opening, 0 ms | 337 → 337 | 3736.4 → 3727.1 | 0.13% [-0.14%, 0.79%] |
| blob8m, opening, 0 ms | 10 → 10 | 74.8 → 74.9 | -1.72% [-4.44%, 2.71%] |
| dense64, full, 25 ms | 29 → 29 | 1884.8 → 1890.5 | -0.17% [-0.93%, 0.48%] |
| sparse64, full, 25 ms | 25 → 20 | 1096.2 → 917.4 | 16.24% [15.73%, 17.31%] |

Sparse LIMIT1 transfers 29,226 → 32,291 native bytes (+10.49%). This is an
explicit bandwidth tradeoff for fewer round trips and lower latency. Dense LIMIT1
remains four native requests and 4,753 bytes. Full sparse history at zero injected
delay does not clear the strict 10% lower-confidence-bound threshold; sparse limited
queries and the delayed full-history case do.

All standard opening, file-selection, and warm-query controls exclude a slowdown
greater than 10% after the additional LIMIT1 observations. The initial dense LIMIT1
file-selection interval was [-12.66%, +2.40%]; combining all twenty pairs gives
[-7.07%, +2.01%]. Wide and blob opening retains two foreground requests and zero
native reads. These are tested-size controls, not universal timing guarantees.
The E11 first-covered-query cost remains part of the accepted baseline.

## Extended controls and validation

Full extended history measurements pass: wide-sparse history is 1.58% faster
[1.36%, 2.40%], with 424 → 404 requests; absent-file history is 82.20% faster
[82.07%, 82.42%], with 70 → 14 requests. All opening, file-selection and warm
controls exclude a slowdown greater than 10%. These ten pairs use the separate
extended harness. Wide history still takes approximately 14 seconds.

Extended wide-sparse LIMIT1 is 36.88% faster [36.37%, 37.33%], with
39 → 23 native requests. Bytes grow 173,158 → 238,747 (+37.88%). This bandwidth
tradeoff is accepted for the measured latency reduction; it is not a universal
win for bandwidth-limited clients. The absent-file LIMIT1 query also improves.
Its initial opening interval slightly crossed the 10% regression threshold,
so ten additional pairs were collected after all compilation finished. All initial
and extra observations are retained in the twenty-pair summary.

The combined absent-file opening improvement is -0.25% [-2.32%, 1.79%]. All final latency controls exclude a slowdown greater than 10%.

The exact engine source passed 4,323 tests (84 skipped) with
`cargo nextest run -p lix --features all-simulations,server-protocol`, plus ten
doctests with `cargo test -p lix --doc`. Engine-package formatting and whitespace checks pass.
The initial workspace-wide formatter found pre-existing differences in unchanged
server and filesystem-adapter files in the controlled baseline. That failure is
retained in the validation record; the changed engine package passes formatting
in both the controlled and report worktrees.
The existing canonical-Memory bounded-discovery and required/optional-error tests
remain in place; real-HTTP limited and empty-result controls exercise changed
consumer demand. No protocol or storage format changes are introduced.

This removes one scheduler counter, a Boolean and a conditional branch. It is a
narrow scheduling simplification, not removal of SQL retries or transport layers.
The five-consecutive-non-improvement streak remains zero. The broader goal remains
active, particularly the remaining wide-history cost.

## Reproduce

Use `../paired_history.py` with the recorded E12/E13 binaries, ten alternating
pairs, shared persisted synthetic fixtures and fresh RocksDB partial replicas.
Build through `tooling/Cargo.toml`, `lix_e2e`, `server-protocol`,
`profile.dev.package.lix.opt-level=2`, `CARGO_INCREMENTAL=0`. Set
`LIX_PROFILE_HISTORY_LIMIT` for limited cases; `--rtt 25` injects network delay.
Standard and extended harnesses are measured separately. The extended harness is
archived here and must be identical for both compared engines. Combined controls
retain the initial ten pairs plus ten additional pairs with offset pair indices.
Artifact metadata and the manifest record exact source and executable hashes.
