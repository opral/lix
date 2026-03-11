# Engine Filestructure Alignment Plan

## Goal

Align the engine package around actual responsibilities instead of migration-history names.

Primary goals:

- Remove the `query_*` vs `sql_*` vs `sql2` split at the top level.
- Avoid introducing `src/engine/` inside the engine crate.
- Keep churn low by using canonical new module paths plus temporary aliases.
- Preserve public API behavior while the tree is being moved.

## Design Rules

- Keep engine-facing orchestration files at `src/` root.
- Introduce `src/sql/` as the umbrella for all SQL parsing, planning, execution, analysis, and SQL string builders.
- Introduce `src/state/` as the umbrella for internal state, commit/materialization/checkpoint, and state stream/timeline helpers.
- Keep already-coherent domains where they are: `filesystem`, `plugin`, `schema`, `version`, `account`, `key_value`, `cel`, `functions`.
- Use one canonical module path per subsystem, then keep old names as temporary aliases.
- Prefer moving small shim modules first and high-churn hubs later.

## Target Tree

```text
packages/engine/src/
  lib.rs

  engine.rs
  api.rs
  transaction.rs
  transaction_exec.rs
  statement_scripts.rs
  boot.rs
  backend.rs
  error.rs
  errors/
  types.rs
  wire.rs

  sql/
    mod.rs
    public/         # current sql2
    execution/      # current query_runtime
    analysis/       # current query_semantics + history-read analysis
    storage/        # current query_storage
    ast/            # current sql_ast
    common/         # current sql_shared

  state/
    mod.rs
    internal/       # current internal_state
    commit/         # current commit
    materialization/# current materialization
    checkpoint/     # current checkpoint
    timeline.rs     # current history_timeline.rs
    stream.rs       # current state_commit_stream.rs
    snapshot.rs     # current snapshot.rs
    validation.rs   # current validation.rs

  schema/
    mod.rs
    builtin/        # current builtin_schema
    registry.rs     # current schema_registry.rs

  filesystem/
  plugin/
  version/
  account/
  key_value/
  cel/
  functions/
  init/
  deterministic_mode/
```

## Current To Target Mapping

| Current path | Target path | Notes |
| --- | --- | --- |
| `src/query_runtime/` | `src/sql/execution/` | Main execution hub. Keep `contracts/` intact initially. |
| `src/query_semantics/` | `src/sql/analysis/` | This is lightweight analysis/requirements logic, not a peer runtime. |
| `src/query_storage/` | `src/sql/storage/` | SQL builders/constants belong under SQL. |
| `src/query_history/plugin_inputs.rs` | `src/sql/analysis/history_reads.rs` | This is read-shape/history analysis. |
| `src/query_history/commit_runtime.rs` | remove wrapper; use `state::commit` directly | It is only a shim over `commit`. |
| `src/sql2/` | `src/sql/public/` | Keep internal `planner/catalog/core/runtime` shape at first. |
| `src/sql_ast/` | `src/sql/ast/` | Parser/lowering/walk utilities. |
| `src/sql_shared/` | `src/sql/common/` | Shared SQL helpers, mostly transitional. |
| `src/internal_state/` | `src/state/internal/` | Internal vtable/read/write/postprocess machinery. |
| `src/commit/` | `src/state/commit/` | Matches existing empty `src/state/commit`. |
| `src/materialization/` | `src/state/materialization/` | Matches existing empty `src/state/materialization`. |
| `src/checkpoint/` | `src/state/checkpoint/` | Matches existing empty `src/state/checkpoint`. |
| `src/history_timeline.rs` | `src/state/timeline.rs` | State/history runtime helper. |
| `src/state_commit_stream.rs` | `src/state/stream.rs` | State stream bus and helpers. |
| `src/snapshot.rs` | `src/state/snapshot.rs` | State persistence concern. |
| `src/validation.rs` | `src/state/validation.rs` | State write validation. |
| `src/builtin_schema/` | `src/schema/builtin/` | Matches existing empty `src/schema/builtin`. |
| `src/schema_registry.rs` | `src/schema/registry.rs` | Belongs under schema. |
| `src/engine_transaction.rs` | `src/transaction.rs` | Transaction lifecycle + handle management. |
| `src/engine_in_transaction.rs` | `src/transaction_exec.rs` | Transactional execution path. |
| `src/api.rs` | `src/api.rs` | Keep at root. |
| `src/statement_scripts.rs` | `src/statement_scripts.rs` | Keep at root. |

## Canonical Names

The new canonical module paths should be:

- `crate::sql::execution`
- `crate::sql::analysis`
- `crate::sql::storage`
- `crate::sql::public`
- `crate::sql::ast`
- `crate::sql::common`
- `crate::state::internal`
- `crate::state::commit`
- `crate::state::materialization`
- `crate::state::checkpoint`

Old names should survive temporarily as aliases:

- `crate::query_runtime`
- `crate::query_semantics`
- `crate::query_storage`
- `crate::sql2`
- `crate::sql_ast`
- `crate::sql_shared`
- `crate::internal_state`
- `crate::commit`
- `crate::materialization`
- `crate::checkpoint`

## Alias Strategy

Use the new names as the real modules, and expose old names as `pub(crate) use` aliases during the transition.

Example shape:

```rust
// lib.rs
pub(crate) mod sql;
pub(crate) mod state;

pub(crate) use sql::analysis as query_semantics;
pub(crate) use sql::ast as sql_ast;
pub(crate) use sql::common as sql_shared;
pub(crate) use sql::execution as query_runtime;
pub(crate) use sql::public as sql2;
pub(crate) use sql::storage as query_storage;

pub(crate) use state::checkpoint as checkpoint;
pub(crate) use state::commit as commit;
pub(crate) use state::internal as internal_state;
pub(crate) use state::materialization as materialization;
```

This keeps import churn low because:

- Canonical paths become available immediately.
- Existing imports continue to compile.
- Large mechanical import rewrites can happen incrementally.

## Phase Plan

### Phase 0: Lock In The Direction

- Keep the guardrails that ban legacy rewrite-engine and bridge code.
- Remove the guardrails that forbid a generic `src/sql` namespace and `crate::sql::*` imports.
- Do not add new guardrails until canonical module names exist.

### Phase 1: Introduce Umbrella Modules Without Moving Files

- Add `src/sql/mod.rs`.
- Add `src/state/mod.rs`.
- Point these modules at existing files using `#[path = "..."]` where needed.
- Make `sql/*` and `state/*` the canonical declarations in `lib.rs`.
- Alias old module names back to the new canonical modules.

Expected churn:

- Small.
- Mostly `lib.rs`, new `mod.rs` files, and a few path declarations.

### Phase 2: Normalize Root Engine-Orchestration Files

- Rename `engine_transaction.rs` to `transaction.rs`.
- Rename `engine_in_transaction.rs` to `transaction_exec.rs`.
- Update `engine.rs` path-module declarations accordingly.
- Leave `engine.rs`, `api.rs`, and `statement_scripts.rs` at root.

Rationale:

- These files are crate entry orchestration, not subdomains.
- This avoids the awkward `src/engine/` directory.

### Phase 3: Move Small SQL Shim Modules First

- Move `query_storage/` to `sql/storage/`.
- Move `sql_shared/` to `sql/common/`.
- Move `query_history/plugin_inputs.rs` to `sql/analysis/history_reads.rs`.
- Delete `query_history/commit_runtime.rs` and switch remaining call sites to direct `state::commit` imports.

Rationale:

- These are small and mostly adapter/shim code.
- They give quick wins on naming clarity with low risk.

### Phase 4: Move Core SQL Runtime Modules

- Move `query_semantics/` to `sql/analysis/`.
- Move `query_runtime/` to `sql/execution/`.
- Move `sql_ast/` to `sql/ast/`.

Important rule:

- Keep `query_runtime/contracts/` together in the first move.
- Do not try to redesign `contracts` vs `core/contracts` during the same phase.

### Phase 5: Rehome `sql2` As Public SQL

- Move `sql2/` to `sql/public/`.
- Preserve its internal substructure:
  - `catalog/`
  - `core/`
  - `planner/`
  - `runtime/`
  - `backend/`

Rationale:

- `sql2` is really “public SQL surfaces and planning”.
- The `sql2` label is migration history, not a durable responsibility name.

### Phase 6: Consolidate State Modules

- Move `internal_state/` to `state/internal/`.
- Move `commit/` to `state/commit/`.
- Move `materialization/` to `state/materialization/`.
- Move `checkpoint/` to `state/checkpoint/`.
- Move `history_timeline.rs` to `state/timeline.rs`.
- Move `state_commit_stream.rs` to `state/stream.rs`.
- Move `snapshot.rs` to `state/snapshot.rs`.
- Move `validation.rs` to `state/validation.rs`.

Rationale:

- These modules are tightly coupled around stored state and commit/materialization flow.
- The repo already has empty `src/state/*` directories, which suggests this direction was already intended.

### Phase 7: Finish Schema Cleanup

- Move `builtin_schema/` to `schema/builtin/`.
- Move `schema_registry.rs` to `schema/registry.rs`.
- Update imports and keep reexports in `lib.rs`.

### Phase 8: Remove Transitional Aliases

- Once call sites have been migrated, remove aliases for:
  - `query_runtime`
  - `query_semantics`
  - `query_storage`
  - `sql2`
  - `sql_ast`
  - `sql_shared`
  - `internal_state`
  - `commit`
  - `materialization`
  - `checkpoint`
- Delete dead wrapper modules and compatibility files.

## What Not To Do

- Do not add `src/engine/`.
- Do not rename `sql2` internals in the same phase as moving `sql2` under `sql/public`.
- Do not fold `query_runtime/contracts` into `sql/common` in the first pass.
- Do not rewrite behavior while moving files.
- Do not remove old import paths until the canonical paths are in place and compiling.

## Expected Benefits

- One obvious place for SQL-related code: `src/sql/`.
- One obvious place for state/commit/materialization code: `src/state/`.
- Root-level files become the actual crate entry/orchestration surface.
- New contributors no longer need to guess the difference between `query_*`, `sql_ast`, `sql_shared`, and `sql2`.

## Suggested Execution Order For Actual PRs

1. Guardrails cleanup.
2. `sql/` and `state/` umbrella modules plus aliases.
3. Root transaction file renames.
4. `query_storage`, `sql_shared`, `query_history` cleanup.
5. `query_semantics`, `query_runtime`, `sql_ast` move.
6. `sql2 -> sql/public`.
7. State module moves.
8. Schema cleanup.
9. Alias removal and import cleanup.

## Validation After Each Phase

- `cargo test -p lix_engine --test sql_guardrails`
- `cargo test -p lix_engine`
- If a phase is mostly path churn, also run a quick `rg` pass for old canonical names that should be shrinking:
  - `query_runtime`
  - `query_semantics`
  - `query_storage`
  - `sql2`
  - `sql_ast`
  - `sql_shared`

## Notes

- The highest-value naming fix is the SQL umbrella, not the transaction file rename.
- The lowest-risk first move is adding canonical modules plus aliases before any physical file moves.
- The most disposable bucket is `query_history`; it should not survive as a top-level subsystem.
