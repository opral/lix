# Optimization Notebook

## Baseline (current)
Hypothesis: Establish stable baseline before applying pipeline optimizations.
Implementation: Run `BENCH_REPLAY_COMMITS=25 pnpm --filter nextjs-replay-bench run bench` and capture throughput + phase breakdown.
Result: Replay duration `15950.20ms`, commit throughput `1.32 commits/s`, execute phase `15298.06ms`.

## optimize 1: strict-id fast path for mutation rewrite prefetch
Hypothesis: `mutation.file_ids_matching_update` spends most time expanding `lix_file_by_version`; exact `WHERE id = ...` updates can fetch `id/untracked` directly from `lix_state_by_version` with pushdown.
Implementation: Add a strict predicate parser for `id`/`version_id` equality and route those updates to a direct `lix_state_by_version` query. Fall back to previous path for all other predicates.
Result: Replay duration `9827.51ms` (from `15950.20ms`, `-38.4%`), commit throughput `2.14 commits/s` (from `1.32`, `+62.1%`). `cargo test -p lix_engine mutation_rewrite` passed.

## optimize 2: pending update prefetch cache fast path
Hypothesis: `pending.collect_update_writes` still dominates after optimize 1 because it expands `lix_file_by_version` for each id-scoped update; using file path/data caches should bypass that expansion for common replay updates.
Implementation: Add exact-id fast path in `pending_file_writes` that reads before-path from `lix_internal_file_path_cache` and before-data from `lix_internal_file_data_cache`. Populate/maintain path cache in engine postprocess and invalidate on deletes.
Result: Replay duration `3349.14ms` (from `9827.51ms`, `-65.9%`; from original baseline `15950.20ms`, `-79.0%`). Commit throughput `6.27 commits/s` (`+193%` vs optimize 1). `cargo test -p lix_engine pending_file_writes` passed.
