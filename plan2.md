# Plan: Unify File Reads/Writes Behind One Router

## Goal
Consolidate file-table semantics into one internal reader/writer interface so `lix_file`, `lix_file_by_version`, and `lix_file_history` stop duplicating planner logic.

## Why
Current file behavior is split across multiple planners and helpers:
1. `packages/engine/src/filesystem/select_rewrite.rs` builds separate large SQL shapes per view and fast-path shape.
2. `packages/engine/src/filesystem/mutation_rewrite.rs` independently resolves path/version/ancestor semantics for writes.
3. `packages/engine/src/filesystem/pending_file_writes.rs` reparses SQL and re-infers write intent again for plugin prefetch.
4. History semantics are split between `lix_state_history` and filesystem-specific history projection layers.

This causes duplicate optimization work, drift risk, and slower performance tuning cycles.

## Architectural Direction
Keep file views as shape aliases only. Move semantics to one internal router:
1. `FileReadOp` (semantic read planner)
2. `FileWriteOp` (semantic write planner)
3. `DirectoryPathResolver` (support component used by file ops for path/ancestor resolution)

All three are logical components, not physical backend tables.

## Semantic Invariants (must not change)
1. `lix_file` is active-version scoped effective view.
2. `lix_file_by_version` remains explicit-version scoped effective view.
3. `lix_file_history` remains root-commit + depth history semantics.
4. Path normalization, collision checks, and ancestor auto-create behavior remain identical.
5. Writer-key and tracked/untracked semantics remain unchanged.

## Out Of Scope
1. Plugin runtime API changes.
2. Non-file entity/state view architecture.
3. Changing user-facing SQL contracts for existing filesystem views.
4. Converting `lix_directory*` into first-class `DirectoryReadOp`/`DirectoryWriteOp` in this pass.

## Target Interface

### Reader
1. `FileReadOp` classification:
   - mode: `active`, `by_version`, `history`
   - projection: `descriptor_only`, `descriptor_plus_data`, `count_only`
   - filters: path/id/version/root_commit/depth predicates
2. Planner emits one canonical SQL pipeline per mode with shared pushdown rules.

### Writer
1. `FileWriteOp` normalization:
   - target scope resolution
   - path normalization
   - directory ancestor planning via `DirectoryPathResolver`
   - collision validation
   - descriptor/data/cache mutation plan
2. `pending_file_writes` consumes `FileWriteOp` plan outputs instead of re-inferring by reparsing final SQL.

## Migration Plan

## Phase 0: Baseline + Guardrails
1. Keep and run:
   - `cargo bench -p lix_engine --bench lix_file -- --noplot --quick`
   - `cargo bench -p lix_engine --bench lix_file_history -- --noplot --quick`
2. Add explain-plan snapshots for file-history hot queries:
   - count by root commit
   - timeline scan for one file/root.
3. Keep correctness guards in:
   - `packages/engine/tests/filesystem_view.rs`
   - `packages/engine/tests/file_history_view.rs`
   - `packages/engine/tests/file_materialization.rs`

Exit criteria:
1. Baseline numbers are captured.
2. Query plans for hot shapes are documented.

## Phase 1: Introduce `FileReadOp`
1. Add internal read classifier in filesystem read step.
2. Normalize alias and shape handling in view rewrites.
3. Centralize predicate extraction/pushdown rules for id/path/version/root/depth.

Exit criteria:
1. View rewrites become thin wrappers around `FileReadOp`.
2. Existing filesystem read tests pass unchanged.

## Phase 2: Canonical Active/By-Version Read SQL
1. Replace separate `lix_file` and `lix_file_by_version` large SQL assembly with one generator.
2. Keep fast-path eligibility detection but route into the same planner contract.
3. Pushdown should happen before expensive recursive path CTEs.

Exit criteria:
1. No duplicate active/by-version descriptor path logic remains.
2. `lix_file` bench read cases do not regress.

## Phase 3: Canonical History Read SQL
1. Move `lix_file_history` query shape into one reusable history planner.
2. Share root-commit/depth predicate pushdown rules with `lix_state_history` layer.
3. Add count-fast-path for history count queries where safe.

Exit criteria:
1. `lix_file_history` benchmarks improve or remain stable with reduced plan complexity.
2. `file_history_view` tests pass on sqlite/materialization/postgres.

## Phase 4: Introduce `FileWriteOp`
1. Move insert/update/delete target scope + path normalization + ancestor planning into one write-op builder.
2. Make mutation rewrite and pending write collection use the same normalized write plan.
3. Remove duplicate exact-id/version inference code paths.

Exit criteria:
1. Mutation rewrite and pending-write collection share one semantic source of truth.
2. `file_materialization` + `filesystem_view` + `writer_key` suites stay green.

## Phase 5: Cleanup + Hardening
1. Remove dead helper paths in filesystem rewrite modules.
2. Add explicit planner tests for pushdown and placeholder-order safety.
3. Capture before/after explain snapshots for hot file-history and file-read shapes.

Exit criteria:
1. No duplicated planner fragments remain for filesystem reads/writes.
2. Bench deltas are documented and accepted.

## Query Planning Rules To Enforce
1. Apply scope and key filters before recursive directory/path expansion.
2. Avoid repeated materialization of descriptor-history unions.
3. Emit count-only shapes when projection is `COUNT(*)` and semantics allow.
4. Resolve active-version once per query plan.
5. Keep placeholder-order safety guarantees for bare `?` placeholders.

## Test Matrix
1. Correctness:
   - `filesystem_view`
   - `file_history_view`
   - `file_materialization`
2. Performance:
   - `lix_file`
   - `lix_file_history`
3. Planner behavior:
   - explain snapshots for root-count and timeline scans.

## Success Metrics
1. Single semantic read planner for file views (`lix_file*`).
2. Single semantic write planner reused by mutation rewrite + pending write collection.
3. No behavior regressions in existing filesystem integration tests.
4. Meaningful latency reduction for file-history reads.

## Implementation Status (2026-02-17)
1. Phase 1 done: introduced filesystem read planner module at `packages/engine/src/filesystem/planner/read.rs` with `FilesystemReadOp` classification (`infer_filesystem_read_op`).
2. Phase 2 (partial) done: `select_rewrite` now delegates filesystem projection assembly to planner read module; rewrite step remains focused on AST shape transformation and fast-path detection.
3. Phase 3 (partial) done: `lix_file_history` projection SQL is routed through planner read module (same entry point as other filesystem views), removing embedded history SQL assembly from `select_rewrite`.
4. Phase 4 (partial) done: shared write-intent helpers now live in `packages/engine/src/filesystem/planner/write.rs` and are reused by both `pending_file_writes` and `mutation_rewrite` for exact update target inference and file write-scope routing.
5. Phase 4 pending: full `FileWriteOp` object model (path normalization + ancestor planning + mutation plan emission) is not fully migrated yet.
6. Phase 5 pending: planner-specific cleanup/hardening and explain snapshots not yet captured in this pass.

## Benchmark Baseline (2026-02-16)

Commands run:
1. `cargo bench -p lix_engine --bench lix_file -- --noplot --quick`
2. `cargo bench -p lix_engine --bench lix_file_history -- --noplot --quick`

### `lix_file` Baseline

| Benchmark | Baseline (current) | After (post-plan) |
|---|---:|---:|
| `lix_file_insert_no_plugin` | 1.7527 ms | 1.7647 ms |
| `lix_file_insert_plugin_json` | 6.4962 ms | 6.3620 ms |
| `lix_file_exact_delete_missing_ids` | 757.98 µs | 721.45 µs |
| `lix_file_exact_update_missing_id` | 2.9732 s | 2.8669 s |
| `lix_file_read_scan_path_data_no_plugin` | 600.85 ms | 574.97 ms |
| `lix_file_read_scan_path_data_plugin_json` | 495.37 ms | 705.95 ms |
| `lix_file_read_point_path_data_no_plugin` | 716.37 ms | 698.84 ms |
| `lix_file_read_point_path_data_plugin_json` | 644.56 ms | 726.70 ms |

### `lix_file_history` Baseline

| Benchmark | Baseline (current) | After (post-plan) |
|---|---:|---:|
| `lix_file_history_count_by_root_commit` | 2.3719 s | 2.3634 s |
| `lix_file_history_file_timeline_scan` | 2.3651 s | 2.3197 s |
