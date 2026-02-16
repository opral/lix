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

## optimize 3: reuse plugin wasm instances across detect loop
Hypothesis: detect pipeline still pays heavy non-SQL cost because wasm plugin components are instantiated repeatedly (per write/per statement).
Implementation: Keep an instance cache by plugin key during detect execution and reuse initialized components across statement-level detect calls.
Result: Replay duration `3250.95ms` (from `3349.14ms`, `-2.9%`). Below 10% threshold, no commit.

## optimize 4: cache instantiated wasm component in node runtime
Hypothesis: first-write latency is dominated by host-side wasm component initialization; re-instantiating the same plugin component each execute call is unnecessary.
Implementation: In `js-sdk` node wasm runtime, cache initialized component instances by wasm hash and reuse for `initComponent` calls.
Result: Replay duration `3284.56ms` (worse than optimize 3 by `+1.0%`). Reverted.

## optimize 5: engine-lifetime plugin component cache
Hypothesis: plugin instance reuse only within a single `execute` still re-initializes component instances across statements/commits; promoting cache to engine lifecycle should reduce per-statement fixed cost.
Implementation: Add engine-owned plugin component cache keyed by plugin key, reuse across detect calls, clear cache when plugin SQL invalidates installed plugin cache.
Result: Replay duration `2908.15ms` (from `3349.14ms`, `-13.2%`; from original baseline `15950.20ms`, `-81.8%`), confirmed across repeated run (`2918.10ms`).

## optimize 6: skip existing-entity reconciliation for text-lines detect
Hypothesis: detect pipeline still spends significant time loading existing plugin entities from DB even though `plugin_text_lines` already emits full add/remove diff.
Implementation: For `plugin_text_lines`, bypass the `load_existing_plugin_entities` reconciliation path in detect (keep fallback path for other plugins).
Result: Replay duration `1990.29ms` (from `2908.15ms`, `-31.6%`; from original baseline `15950.20ms`, `-87.5%`). `cargo test -p lix_engine plugin_install` passed.

## optimize 7: use release wasm artifact for text-lines plugin in benchmark
Hypothesis: first detect call is dominated by JCO transpiling a large debug wasm artifact (~7.1MB); loading release wasm (~238KB) should significantly reduce one-time component init cost.
Implementation: Update replay benchmark plugin loader/build to prefer `target/wasm32-wasip2/release/plugin_text_lines.wasm` and build with `cargo build --release` fallback.
Result: Replay duration `1590.48ms` (from `1990.29ms`, `-20.1%`; from original baseline `15950.20ms`, `-90.0%`), crossing the 10x target on this 25-commit run.

## optimize 8: profile slowest commit with raw backend trace + EXPLAIN
Hypothesis: remaining replay gap is likely in engine SQL path; tracing should show if missing indexes or expensive queries still dominate.
Implementation: Run `packages/nextjs-replay-bench/src/profile-slowest-commit.js` on current slowest commit and inspect generated `results/nextjs-replay.slowest-commit.explain.txt`.
Result: target commit wall-time `415.00ms`, but traced backend SQL totaled only `17.08ms` (largest SQL: plugin lookup `13.55ms`). Remaining time is host-side wasm/component overhead (JCO path), not an index/planner bottleneck in engine SQL for this commit.

## optimize 9: persistent node transpile cache (attempt, reverted)
Hypothesis: persisting JCO transpile output to disk across process runs should reduce first-commit latency.
Implementation: Add node runtime cache file keyed by wasm hash (`packages/js-sdk/src/wasm-runtime/node.js`) and load transpiled files from disk before calling `transpile()`.
Result: No meaningful win (`1728.99ms` first run, `1668.69ms` second run; still around current range). Indicates current bottleneck is mostly component instantiation/call path, not transpile output generation. Reverted.

## optimize 10: id-only projection for delete prefetch
Hypothesis: `pending.collect_delete_targets` still uses `lix_file_by_version` expansion even when delete predicates only need `id/lixcol_version_id`, causing unnecessary directory/path/file data work.
Implementation: In `pending_file_writes`, detect delete predicates that only reference `id`/version columns and run prefetch against an id-only projection from `lix_state_by_version` (`schema_key='lix_file_descriptor'`) instead of `lix_file_by_version`.
Result: Warm bench (`25 measured + 5 warmup`) improved from `1222.62ms` to `1142.19ms` (`-6.6%`). Outlier statement dropped (`88b01... stmt0` from `44.18ms` to `18.74ms`). Below 10% threshold, no commit yet.

## optimize 11: iterative writer-key rewrite routing (stack safety)
Hypothesis: stack overflow risk is from recursive SQL rewrite routing; switching tail-recursive routing to an iterative pass loop should reduce stack growth and catch rewrite cycles.
Implementation: Refactor `rewrite_statement_with_writer_key` in `sql/route.rs` from self-recursive rewrites to a bounded iterative loop (`MAX_REWRITE_PASSES=32`) with explicit `continue` transitions.
Result: Plugin cache invalidation tests pass on default stack again; replay throughput unchanged within noise (`~1376ms` measured for 25 commits vs previous `~1351ms`). Safety/maintainability win, no >10% speedup.
