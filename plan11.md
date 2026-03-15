# Plan 11: Simplify Backend API To Execute + ExecuteBatch

## Goal

Replace the current mixed backend contract with the smallest clean transaction API that still supports the engine's first-principles execution model:

- `BEGIN`
- one `preflight`
- one `write batch`
- `COMMIT`

This plan ignores backward compatibility.

## Final API

```rust
struct PreparedStatement {
    sql: String,
    params: Vec<SqlParam>,
}

struct PreparedBatch {
    steps: Vec<PreparedStatement>,
}

trait LixBackend {
    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction>, LixError>;
}

trait LixTransaction {
    async fn execute(&mut self, stmt: PreparedStatement) -> Result<QueryResult, LixError>;
    async fn execute_batch(&mut self, batch: PreparedBatch) -> Result<BatchResult, LixError>;
    async fn commit(self: Box<Self>) -> Result<(), LixError>;
    async fn rollback(self: Box<Self>) -> Result<(), LixError>;
}
```

## Principles

- The backend does not know about filesystem semantics, append semantics, or transaction programs.
- The engine owns planning.
- The backend owns only transaction lifetime and prepared SQL execution.
- `execute(...)` is the single-statement primitive used for transaction-wide preflight.
- `execute_batch(...)` is the multi-statement primitive used for the transaction-wide write phase.
- There is no semicolon-script special case in the API contract.
- There is no top-level non-transaction `execute(...)` on `LixBackend`.
- All engine DB work goes through a pinned transaction.

## Why This Is Simpler Than Today

Today the API is ambiguous:

- backend and transaction both expose execution primitives
- semicolon scripts sometimes execute as batches
- parameterized writes sometimes require one call per statement
- the engine has to care about backend execution quirks

The target API removes that ambiguity:

- one way to start work: `begin_transaction()`
- one way to do the read phase: `tx.execute(preflight)`
- one way to do the write phase: `tx.execute_batch(write_batch)`
- one way to end successfully: `commit()`
- one way to abort: `rollback()`

## Required Architecture Changes

### 1. Remove top-level backend execution

Delete direct `LixBackend::execute(...)` usage.

Anything that currently does out-of-transaction execution must instead:

- open a transaction
- run `execute(...)`
- optionally run `execute_batch(...)`
- commit or rollback

The engine should no longer rely on non-pinned backend calls.

### 2. Make preflight a `PreparedStatement`

The engine must lower transaction-wide preflight into exactly one `PreparedStatement`.

That statement may be large and may use parameters, but it is still one statement.

### 3. Make write phase a `PreparedBatch`

The engine must lower transaction-wide writes into exactly one `PreparedBatch`.

The batch may contain multiple physical statements, but the backend sees it as one batch execution request.

### 4. Delete semicolon-script execution semantics from the backend contract

The backend API must not rely on:

- `sql.contains(';')`
- `params.is_empty()` hacks
- literal inlining as a required execution path

If the engine wants batching, it uses `PreparedBatch`.

### 5. Make SQLite and Postgres implementations native to the batch abstraction

SQLite and Postgres backends must each implement:

- pinned transaction execution for one prepared statement
- pinned transaction execution for one prepared batch

The batch implementation may internally loop statements, pipeline them, or use driver-specific batching, but that behavior is hidden behind the backend boundary.

### 6. Make the engine runtime obey one transaction model

All mutating flows must use the same runtime shape:

- `begin_transaction()`
- `execute(preflight)`
- `execute_batch(write_batch)`
- `commit()`

No runtime path may issue additional transactional SQL outside those phases.

## Explicit Non-Goals

- No backend knowledge of files, commits, or schemas.
- No temp-table API in the backend surface.
- No writable-CTE API in the backend surface.
- No fallback to raw SQL scripts as an execution contract.

## Success Criteria

- `LixBackend` only exposes `begin_transaction()`.
- `LixTransaction` only exposes `execute(...)`, `execute_batch(...)`, `commit()`, and `rollback()`.
- The engine no longer depends on semicolon-script behavior.
- Transaction traces show exactly:
  - one preflight execute
  - one batch execute
  - one commit
- Physical batching details are fully hidden behind the backend implementation.

## Progress Log

- Created the plan with the target backend API and final-state constraints.
- Promoted engine `PreparedStatement` and `PreparedBatch` as backend-facing types. `PreparedBatch` is now a real list of prepared statements instead of one semicolon-concatenated SQL string.
- Added `LixTransaction::execute_batch(...)` with a default per-statement implementation and native overrides in the SQLite and Postgres backends.
- Removed semicolon-script special casing from the concrete SQLite and Postgres execution paths. Batch execution now flows through `execute_batch(...)` instead of `sql.contains(';')` behavior.
- Updated write-program execution to call `transaction.execute_batch(...)` for prepared batches, so the engine's write phase now uses the backend batch primitive instead of flattening batches back into ad hoc statement loops.
- Validation passed with:
  - `cargo check -p lix_engine -p lix_rs_sdk -p lix_cli`
  - `cargo test -p lix_engine --no-run`
  - `cargo test -p lix_engine --test transaction_execution -- --nocapture`
- One deliberate compromise remains: `LixBackend::execute(...)` still exists as a default convenience wrapper implemented in terms of `begin_transaction() -> tx.execute() -> commit()`. The concrete backends no longer own top-level execution behavior, but the method is still present on the trait until the remaining read-side call sites are cut over.
- Collapsed backend batch execution to one physical SQL call per `PreparedBatch` in the SQLite and Postgres implementations by literalizing the batch once and executing the collapsed SQL text once.
- Collapsed write-program execution to one backend batch call for the whole write program instead of one backend batch call per write step.
- Folded tracked filesystem payload CAS persistence into append-time write-program assembly so the replayed tracked write path no longer emits a separate payload batch before append.
- Removed two remaining transaction-script overheads on the combined public mutating path:
  - no pending public append session is built after an already-coalesced one-shot transaction write
  - no second observe-tick batch is appended at transaction end when the tracked append batch already emitted one
- Traced replay validation for commit `1c6301ae55a853b266024f604fb6936668e3acc1` now shows:
  - `BEGIN`
  - `5` transaction `execute(...)` calls, all read-side preflight work
  - `1` transaction `execute_batch(...)` call for the entire write phase
  - `COMMIT`
- The remaining gap to the strict final-state trace is now entirely on the read side: the combined filesystem insert planning path still performs multiple descriptor lookup statements before the append preflight statement. The write side is down to one backend batch call.
- Latest validation passed with:
  - `cargo check -p lix_engine -p lix_rs_sdk -p lix_cli`
  - `cargo test -p lix_engine --test transaction_execution -- --test-threads=1 --nocapture`
