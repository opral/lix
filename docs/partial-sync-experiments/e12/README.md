# E12: grow discovery with selected checkpoints traversed

**Accepted for statistically meaningful sparse-history improvements.** E11 grows its dependency-discovery window with nonempty
result batches consumed. A sparse file history may traverse many selected
checkpoints before finding another changed row, so that counter understates work
already traversed on the consumer's behalf.

E12 counts metadata-selected checkpoints visited, including empty diffs, and
exposes that count as the lookahead budget only after the consumer resumes past a
nonempty batch. This is a producer-side signal: residual SQL filters above the
history scan may discard that batch. The LIMIT1 controls use an exact file ID.
It does not count excluded graph nodes or increase the 16-checkpoint cap.
Required/optional dependency handling, normal awaited SQL, and protocol/storage
versions remain unchanged.

## Paired measurements

Ten alternating E11/E12 pairs per case, identical persisted synthetic fixtures,
fresh RocksDB partial replicas, real HTTP, and ordinary awaited SQL. Every
invocation compares ordered rows with the authority and verifies a covered query
with native fetching disabled. No compilers, tests, or other benchmarks ran during
timing. Positive values mean faster; 95% intervals use 10,000 seeded bootstrap
resamples of paired relative improvements.

| Query and injected delay | Native requests E11 → E12 | Median history ms E11 → E12 | Paired improvement [95% interval] |
| --- | ---: | ---: | ---: |
| dense4, full, 0 ms | 10 → 10 | 74.0 → 74.2 | -0.59% [-3.09%, 0.57%] |
| dense64, full, 0 ms | 29 → 29 | 865.8 → 853.9 | 0.95% [0.65%, 1.80%] |
| sparse64, full, 0 ms | 37 → 25 | 507.7 → 319.0 | 36.97% [36.33%, 37.68%] |
| dense64, limit1, 0 ms | 4 → 4 | 31.3 → 32.2 | -2.55% [-4.26%, -0.10%] |
| sparse64, limit1, 0 ms | 13 → 13 | 101.5 → 103.6 | -2.69% [-6.21%, 4.38%] |
| dense64, limit2, 0 ms | 7 → 7 | 54.4 → 55.2 | -1.09% [-3.01%, 0.76%] |
| sparse64, limit2, 0 ms | 18 → 15 | 155.9 → 127.9 | 18.24% [16.70%, 18.81%] |
| dense64, limit4, 0 ms | 10 → 10 | 86.6 → 83.9 | 2.95% [-0.10%, 4.63%] |
| sparse64, limit4, 0 ms | 26 → 19 | 275.8 → 188.6 | 31.42% [30.60%, 31.78%] |
| wide16000, opening, 0 ms | 337 → 337 | 3741.8 → 3720.5 | 0.36% [-0.39%, 0.94%] |
| blob8m, opening, 0 ms | 10 → 10 | 74.7 → 75.0 | -0.26% [-2.60%, 2.26%] |
| dense64, full, 25 ms | 29 → 29 | 1903.0 → 1894.7 | 0.71% [0.00%, 0.94%] |
| sparse64, full, 25 ms | 37 → 25 | 1664.3 → 1098.7 | 34.04% [33.51%, 34.28%] |

Sparse full-history native bytes decrease 145,831 → 144,944. Sparse LIMIT2 bytes
grow 40,029 → 40,976 (+2.37%); sparse LIMIT4 grows 76,424 → 78,186 (+2.31%).
Dense and LIMIT1 request/byte counts remain unchanged. The small limited-query
byte increases are explicit tradeoffs for fewer round trips and lower latency.

All measured opening, file-selection, and warm-query intervals exclude a slowdown
greater than 10%. Wide history is unchanged within measurement uncertainty, and
wide/blob opening retains two foreground requests and zero native reads. These
are tested-size controls, not universal timing guarantees. The earlier E11
first-covered-query cost is part of the accepted baseline; E12 does not claim
to explain or remove it.

## Validation and decision

The performance threshold is met for sparse full history and sparse LIMIT2/4.
The exact source passed 4,323 engine tests with
`cargo nextest run -p lix --features all-simulations,server-protocol` (84 skipped)
and 10 doctests with `cargo test -p lix --doc`. Formatting, diff whitespace,
changenote, and server-protocol documentation checks pass.
The existing canonical-Memory tests exercise bounded/zero-demand discovery and
required/optional errors; the real-HTTP limited-query measurements exercise the
consumer stopping behavior changed by this candidate.

This adds one scalar counter; it is a performance experiment, not an architectural
simplification. No public API, transport version, or storage format changes. The
overall optimization goal remains active. The five-consecutive-no-meaningful-
improvement streak remains zero because E12 finds substantial gains over E11.

## Reproduce

Build the `partial_replica_history` benchmark through `tooling/Cargo.toml` with
`--features server-protocol`, `profile.dev.package.lix.opt-level=2`, and
`CARGO_INCREMENTAL=0`. Compare saved E11 and E12 executables using
`../paired_history.py`, ten pairs, and identical fixture directories. Full cases:
`dense4,dense64,sparse64`; limit cases `dense64,sparse64` with
`LIX_PROFILE_HISTORY_LIMIT=1`, `2`, or `4`; opening cases `wide16000,blob8m`.
Use `--rtt 25` for the delayed-network full cases `dense64,sparse64`.
Raw measurements, summaries, and artifact metadata accompany this report.
