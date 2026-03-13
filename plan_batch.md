# Batch Execution Plan

## Goal

Make one logical commit execute as one database roundtrip.

For the `lix_file` hot path, the engine already knows the full set of SQL mutations before execution. The current bottleneck is not only SQL shape, but the fact that the engine still executes many prepared statements one by one.

The target architecture is:

- engine compiles one logical write into one SQL batch/script
- backend `execute(...)` semantics are batch-native
- one commit path = one batch execution call inside one transaction

No backward compatibility constraints. The backend API contract can change.

## Problem Today

Current state:

- `build_statement_batch_from_generate_commit_result(...)` in `packages/engine/src/state/commit/runtime.rs` already chunks some inserts and returns a `StatementBatch`
- `bind_statement_batch_for_dialect(...)` lowers that batch into `Vec<PreparedStatement>`
- `execute_generated_commit_result(...)` in `packages/engine/src/sql/execution/shared_path.rs` loops:

```rust
for statement in prepared {
    transaction.execute(&statement.sql, &statement.params).await?;
}
```

- `append_observe_tick_in_transaction(...)` is not part of the generated batch and is executed separately

So today:

- batching exists at SQL generation time
- batching does not exist at DB-call execution time
- one logical update still means many `transaction.execute(...)` calls

## First-Principles Contract

The backend API should model the real unit of work:

- `execute(...)` means execute a SQL script/batch
- the script may contain one or more statements
- placeholders are bound across the entire script
- execution is atomic with respect to the current transaction

This replaces the implied old contract:

- `execute(...)` means execute one statement

## Backend API Refactor

Refactor `packages/engine/src/backend.rs`.

### New semantics

Keep the name if desired, but redefine it explicitly:

```rust
#[async_trait(?Send)]
pub trait LixBackend: Send + Sync {
    fn dialect(&self) -> SqlDialect;

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError>;
}

#[async_trait(?Send)]
pub trait LixTransaction {
    fn dialect(&self) -> SqlDialect;

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError>;

    async fn commit(self: Box<Self>) -> Result<(), LixError>;

    async fn rollback(self: Box<Self>) -> Result<(), LixError>;
}
```

But with new documented meaning:

- `sql` may be a multi-statement script
- `params` bind across the whole script
- statement separators are valid input

### Result semantics

Define one of these clearly:

1. `QueryResult` represents only the final statement result
2. `QueryResult` carries one entry per statement

For correctness and future debugging, prefer:

- one entry per statement

This keeps script execution inspectable and avoids hidden result truncation.

### Backend implementation requirements

SQLite:

- execute a semicolon-separated script in one call path
- bind parameters globally across the script

Postgres:

- execute a semicolon-separated script in one batch call
- bind parameters globally across the script

The important invariant is:

- one engine call into backend
- not one call per statement

## Engine Refactor

Keep batching generic in the engine.

Do not move commit lowering logic into backend-specific code.

### Replace `Vec<PreparedStatement>` with one prepared batch

Current:

- `StatementBatch { statements: Vec<Statement>, params: Vec<Value> }`
- `bind_statement_batch_for_dialect(...) -> Vec<PreparedStatement>`

Target:

- `StatementBatch { statements: Vec<Statement>, params: Vec<Value> }`
- `bind_statement_batch_for_dialect(...) -> PreparedBatch`

Proposed shape:

```rust
pub(crate) struct PreparedBatch {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
}
```

The engine will:

1. lower each statement to dialect SQL
2. renumber placeholders globally
3. concatenate statements with `;`
4. flatten parameters into one batch-wide parameter list

The result is:

- one script
- one parameter vector

### Move `observe_tick` into the generated batch

Today `observe_tick` is appended separately during transaction commit.

That violates the one-roundtrip goal.

Refactor so that:

- observe tick becomes part of the same generated batch as commit/live-state writes
- any mandatory runtime bookkeeping required per commit is emitted into the batch builder

The commit batch must include:

- snapshots
- changes
- live-state upserts
- commit ancestry
- observe tick
- any required deterministic/runtime bookkeeping

No trailing extra `transaction.execute(...)` after batch execution.

### Replace per-statement execution loop

Current:

```rust
for statement in prepared {
    transaction.execute(&statement.sql, &statement.params).await?;
}
```

Target:

```rust
let prepared = bind_statement_batch_for_dialect(...)?;
transaction.execute(&prepared.sql, &prepared.params).await?;
```

That is the central behavioral change.

## Commit Logic Refactor

### Stage 1: Produce one complete commit batch

Refactor `build_statement_batch_from_generate_commit_result(...)` in `packages/engine/src/state/commit/runtime.rs` so it produces the full commit script contents.

This includes:

- snapshot inserts
- change inserts
- live-state upserts
- ancestry writes
- observe tick insert

The builder should own all commit-path SQL generation.

### Stage 2: Batch-wide placeholder numbering

Today placeholders are effectively statement-local after per-statement binding.

For one script execution:

- placeholders must be globally numbered across the entire batch

Refactor binder logic so the final script is valid as one parameterized batch.

### Stage 3: Script-safe statement ordering

Preserve required ordering:

1. prerequisite runtime bookkeeping
2. snapshots/blob metadata
3. change rows
4. live-state rows
5. ancestry rows
6. observe tick

The order must ensure:

- foreign-key or semantic dependencies are satisfied
- result remains identical to current behavior

### Stage 4: Single execution call

Refactor:

- `execute_generated_commit_result(...)`
- append-commit paths
- followup/internal state commit paths

so they call `transaction.execute(...)` once per generated commit batch.

## QueryResult Considerations

Some paths currently may assume one statement per execute.

Audit and update:

- execution result contracts
- tracing
- public read/write instrumentation
- debugging helpers

If `QueryResult` becomes multi-statement-aware, make that explicit everywhere.

## Scope Clarification

This plan is specifically for the internal commit/write path.

Public SQL can still be:

- parsed as one statement or many
- validated as today

The key change is:

- internal engine-generated commit work is executed as one script roundtrip

## Validation

### Functional

- existing commit/write tests still pass
- state commit stream still works
- observe tick behavior unchanged
- deterministic mode unchanged semantically

### Performance

Re-run:

- `cargo bench -p lix_engine --bench lix_file_update`
- `LIX_BENCH_TRACE_UPDATE=1 cargo bench -p lix_engine --bench lix_file_update -- --nocapture`
- `cargo bench -p lix_engine --bench lix_file_recursive_update`

Expected performance wins:

- fewer `transaction.execute(...)` calls
- reduced fixed per-update overhead
- reduced runtime bookkeeping bucket
- lower transaction boundary cost

## Success Criteria

For one `metadata_only` file update:

- exactly one commit batch execution call
- no per-statement execution loop in commit application
- `observe_tick` included in the same batch
- materially fewer traced DB operations

For the architecture:

- backend contract explicitly supports multi-statement script execution
- engine owns generic batching/chunking logic
- one logical commit maps to one DB roundtrip

## Progress log

- 2026-03-13: Initial plan drafted
- 2026-03-13: Started implementation. Audited backend execute semantics, commit batch builder, append-commit path, shared-path commit application, and followup callers that still fan out statement-by-statement.
- 2026-03-13: Refactored batch binding to emit one prepared SQL script for engine-generated commit work. Append-commit, shared-path generated commits, and internal followup batches now execute through one `transaction.execute(...)` call instead of looping per statement.
- 2026-03-13: Validated the batch execution refactor with `cargo check -p lix_engine` and targeted append-commit tests. Traced `lix_file/update_existing_row/metadata_only` dropped from 42 ops / 25 tx executes to 33 ops / 16 tx executes, but wall-clock regressed to roughly 6.0-6.8 ms. The next optimization target is engine-side batch construction plus the remaining runtime bookkeeping outside the generated commit batch.
- 2026-03-13: Ran `cargo fmt -p lix_engine` and re-checked the crate after the refactor. The commit batch path is now one generated SQL script per execute call; further gains need to come from reducing script construction cost or moving more runtime bookkeeping into the same batched path.
