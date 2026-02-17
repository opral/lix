# Optimization Trial Log 3

## Scope
Evaluate a minimal "structured bulk ingestion" prototype for snapshot writes in `vtable_write`:
- Stage large snapshot batches into a temp table.
- Merge staged rows into `lix_internal_snapshot`.

Goal: reduce parse/dispatch overhead for very large multi-row snapshot inserts.

## Trial 1: Temp-stage snapshot merge (prototype)

### Change
- In `packages/engine/src/sql/steps/vtable_write.rs`:
  - For large snapshot row batches (`>= 64`), emit:
    - `CREATE TEMP TABLE IF NOT EXISTS lix_internal_snapshot_stage ...`
    - `DELETE FROM lix_internal_snapshot_stage`
    - `INSERT INTO lix_internal_snapshot_stage ... VALUES ...`
    - Merge into `lix_internal_snapshot` (prototype iterations used upsert-like merge, then DELETE+INSERT SELECT)

### Observed issues
- Initial profile run failed due stale wasm artifacts (no rebuild).
- After rebuild, profile succeeded.
- Throughput regression in replay benchmark.

### Benchmark result (with staging prototype)
Command:
```bash
pnpm -C packages/nextjs-replay-bench bench:100
```
Result:
- Commit throughput: **27.07 commits/s**
- Replay duration (measured): **3693.96ms**
- Slowest statement: **20.30ms** (`commit=89f96cc16019`, `stmt=0`)

## Revert and validation

### Action
- Reverted staged-ingestion prototype from `vtable_write` (restored direct snapshot upsert path).

### Benchmark result (after revert)
Command:
```bash
pnpm -C packages/nextjs-replay-bench bench:100
```
Result:
- Commit throughput: **31.52 commits/s**
- Replay duration (measured): **3172.36ms**
- Slowest statement: **19.22ms** (`commit=3c88a3d7a8a2`, `stmt=1`)

## Conclusion
- The minimal temp-stage ingestion prototype is **rejected**.
- Net impact was strongly negative (about **-14% throughput** vs reverted baseline).
- Keep current direct snapshot insert path.

## Next candidates
- Focus on high-cost commit/update path internals (statement fanout and redundant descriptor/state reads), not snapshot staging.
- Use slow-commit profile + EXPLAIN-guided pruning for targeted >30% wins.

## Trial 2: Multi-statement fallback optimization

### Change
- In `packages/engine/src/engine.rs`:
  - Added shared script execution helpers so all multi-statement fallback paths reuse one execution flow:
    - `execute_statement_script_with_options`
    - `execute_statement_script_with_options_in_transaction`
  - Routed `execute_multi_statement_sequential_with_options*` through that shared flow.
  - Reused existing optimizations from transaction scripts for fallback scripts:
    - statement coalescing (`coalesce_lix_file_transaction_statements` / `coalesce_vtable_inserts_in_statement_list`)
    - deferred side-effect collection and flush in one transaction.

### Benchmark result
Command:
```bash
pnpm -C packages/nextjs-replay-bench bench:100
```
Result:
- Commit throughput: **28.70 commits/s**
- Replay duration (measured): **3484.44ms**
- `executeStatementsMs`: **1732.62ms** (down from prior high multi-statement fallback behavior)
- Slowest statement: **15.77ms** (`commit=418fb844752d`, `stmt=0`)

### Conclusion
- Multi-statement execution is substantially faster and now near the previous high-throughput regime again.
- Keep this refactor; it removes a slow duplicate path and consolidates script execution behavior.

## Trial 3: Single-parse/single-preprocess execution path

### Change
- In `packages/engine/src/engine.rs`:
  - Parse SQL once at execute entry and reuse parsed statements for:
    - read-only detection
    - file-cache refresh detection
    - file-read materialization scope detection
    - side-effect collection
    - preprocess/rewrite pipeline
  - Added statement-slice helpers and routed transaction/deferred side-effect path to statement-based collectors.
- In `packages/engine/src/sql/pipeline.rs`:
  - Added `preprocess_parsed_statements_with_provider_and_detected_file_domain_changes(...)`
  - Existing SQL-string entrypoint now delegates to parsed-statement entrypoint.
- In `packages/engine/src/filesystem/pending_file_writes.rs`:
  - Reused parsed-statement collectors (`*_from_statements`) from engine path.

### Validation
Command:
```bash
cargo test -p lix_engine --lib
```
Result:
- **222 passed, 0 failed**

### Benchmark result
Command:
```bash
pnpm -C packages/nextjs-replay-bench bench:100
```
Result:
- Commit throughput: **29.61 commits/s**
- Replay duration (measured): **3376.99ms**
- `executeStatementsMs`: **1626.34ms**
- Slowest statement: **20.99ms** (`commit=07b95ae08015`, `stmt=0`)
- Former hotspot tracked: `commit=3c88a3d7a8a2`, `stmt=1` now **18.88ms**

### Conclusion
- Keep this refactor.
- It preserves behavior and improves execute-phase cost by removing duplicate parse/traversal/rewrite setup across side-effect and preprocess stages.
