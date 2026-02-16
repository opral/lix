# Optimization Notebook

## Baseline (current)
Hypothesis: Establish stable baseline before applying pipeline optimizations.
Implementation: Run `BENCH_REPLAY_COMMITS=25 pnpm --filter nextjs-replay-bench run bench` and capture throughput + phase breakdown.
Result: Replay duration `15950.20ms`, commit throughput `1.32 commits/s`, execute phase `15298.06ms`.

## optimize 1: strict-id fast path for mutation rewrite prefetch
Hypothesis: `mutation.file_ids_matching_update` spends most time expanding `lix_file_by_version`; exact `WHERE id = ...` updates can fetch `id/untracked` directly from `lix_state_by_version` with pushdown.
Implementation: Add a strict predicate parser for `id`/`version_id` equality and route those updates to a direct `lix_state_by_version` query. Fall back to previous path for all other predicates.
Result: Replay duration `9827.51ms` (from `15950.20ms`, `-38.4%`), commit throughput `2.14 commits/s` (from `1.32`, `+62.1%`). `cargo test -p lix_engine mutation_rewrite` passed.
