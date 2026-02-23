# Query Planner Hardening Plan (Updated)

## Design Goal

Build a sound query planner with minimal duplicate work.

Primary design goal: **separate concerns and establish a file structure that enables parallel improvements without merge conflicts**.

## Current-State Findings

1. The previous summary is partially outdated relative to current code layout.
2. `lix_file_history` currently derives from `lix_state_history` in `packages/engine/src/filesystem/select_rewrite.rs`; this is already layered, but the layering logic is embedded in filesystem rewrite SQL instead of a shared history module.
3. History timeline maintenance is still triggered inside `lix_state_history` rewrite (`packages/engine/src/sql/steps/lix_state_history_view_read.rs`), coupling rewrite and maintenance responsibilities.
4. Read-side plugin cache materialization is triggered from execution by statement analysis (`packages/engine/src/sql/analysis.rs` + `packages/engine/src/execute/side_effects.rs`), which is better than raw SQL substring matching but still not an explicit history-requirements contract.
5. Plugin history materialization in `packages/engine/src/plugin/runtime.rs` performs its own descriptor discovery/query flow from `lix_file_history`, which duplicates history-path concerns.
6. The old proposed folders (`sql/planner`, `sql/read_views`, `execute/pipeline/run.rs`) do not match current repository structure.

## Non-Negotiable Rules

1. `lix_state_history` is the canonical history semantic source.
2. History materialization is driven by explicit planner requirements, not inferred by downstream heuristics when requirements are available.
3. Plugin history materialization consumes the same root/depth abstraction used by read query rewriting.
4. Rewrite logic, maintenance logic, and plugin runtime logic must each own separate modules with narrow interfaces.
5. Large file moves are deferred until behavior is locked to reduce merge conflicts.

## Updated Phased Plan

### Phase 0: Re-baseline and lock semantics

1. Keep current `lix_file_history` layering behavior as baseline (do not refactor structure yet).
2. Add regression tests that lock current root/depth/path behavior for:
   1. descriptor-backed history rows.
   2. content-only root cases.
   3. non-active root commits.
3. Add differential assertions against `next` for key `lix_file_history` query shapes.

Exit criteria:

1. We have executable tests defining current intended behavior before structural refactors.
2. `lix_file_history` behavior is proven equivalent to `next` for covered scenarios.

### Phase 1: Introduce explicit history requirements (no behavior change)

1. Add a shared requirements type in `packages/engine/src/sql/history/requirements.rs`.
2. Extend planner/rewrite outputs to carry:
   1. requested root commits.
   2. required max depth.
   3. file-history cache materialization requirement.
3. Keep existing statement-analysis triggers as compatibility fallback while requirements are rolled out.

Exit criteria:

1. Execution receives explicit history requirements from planner output.
2. Existing behavior remains unchanged with fallback path still active.

### Phase 2: Move maintenance out of rewrite path

1. Extract root resolution + timeline ensure helpers from `lix_state_history_view_read.rs` into:
   1. `packages/engine/src/sql/history/requests.rs`
   2. `packages/engine/src/sql/history/maintenance.rs`
2. Execute maintenance once in execution flow based on requirements, not inside rewrite.
3. Keep rewrite functions pure (AST transform + requirement emission only).

Exit criteria:

1. `lix_state_history` rewrite no longer performs side-effectful materialization.
2. Root-driven maintenance works for active and non-active roots.

### Phase 3: Consolidate file-history layering helpers

1. Introduce shared file-history layer helpers in `packages/engine/src/sql/history/file_history_layer.rs`.
2. Make `packages/engine/src/filesystem/select_rewrite.rs` consume these helpers instead of carrying all history SQL assembly inline.
3. Keep SQL output stable against Phase 0 tests.

Exit criteria:

1. Filesystem history rewrite depends on shared history-layer builders.
2. No behavior drift from locked Phase 0 tests.

### Phase 4: Plugin/history unification

1. Move plugin file-history descriptor selection to shared history-layer helpers.
2. Define one depth contract for plugin apply-changes input selection and enforce it centrally.
3. Remove duplicate history descriptor/query construction from `packages/engine/src/plugin/runtime.rs` where shared helpers exist.

Exit criteria:

1. Query path and plugin path use the same `(file_id, root_commit_id, depth)` contract.
2. Plugin runtime contains only plugin execution concerns, not history-shape assembly.

### Phase 5: Hardening and cleanup

1. Remove compatibility fallbacks once requirements plumbing is fully adopted.
2. Add structural guard tests preventing direct history-semantic table use outside `sql/history/*`.
3. Add canary tests for root/depth regressions in query + plugin cache materialization flows.

Exit criteria:

1. CI fails on semantic drift across planner/rewrite/execute/plugin boundaries.
2. No duplicate history pipeline remains.

## Conflict-Resistant File Structure (Target)

This structure is additive-first (minimal moves early), so parallel work can proceed with low conflict risk.

```text
packages/engine/src/sql/
  history/
    mod.rs
    requirements.rs       # shared contract emitted by rewrite/planner
    requests.rs           # root/depth extraction + normalization
    maintenance.rs        # ensure_* materialization helpers
    file_history_layer.rs # shared SELECT/descriptor helpers for filesystem + plugin
  pipeline/
    ...                   # existing planner/rewrite pipeline stays in place initially
  steps/
    lix_state_history_view_read.rs  # reduced to rewrite + requirement emission
```

```text
packages/engine/src/execute/
  entry.rs                # consumes requirements, orchestrates maintenance
  side_effects.rs         # plugin/materialization side effects use requirements first
```

```text
packages/engine/src/filesystem/
  select_rewrite.rs       # consumes sql::history::file_history_layer
  mutation_rewrite.rs
```

```text
packages/engine/src/plugin/
  runtime.rs              # plugin calls + cache writes; delegates history descriptor discovery
```

## PR Slicing (Parallel-Friendly)

1. PR1: Phase 0 tests + differential checks (no structural refactor).
2. PR2: introduce `sql/history/requirements.rs` + wiring (fallback retained).
3. PR3: move maintenance to `sql/history/maintenance.rs` and call from execute.
4. PR4: extract `file_history_layer.rs` and switch filesystem rewrite.
5. PR5: switch plugin runtime to shared history helpers and remove duplicate logic.
6. PR6: remove fallbacks + add structural guardrails.
