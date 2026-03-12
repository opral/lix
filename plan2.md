# Engine Separate-Call Transaction Support Plan

## Objective

Implement session-scoped SQL transaction control in `packages/engine` so one `Lix` instance can support:

- `execute("BEGIN")`
- one or more later `execute(...)` calls
- `execute("COMMIT")` or `execute("ROLLBACK")`

Then update `packages/js-kysely` to use that capability through the JS SDK's `execute()` API instead of requiring `lix.beginTransaction()`.

## Scope

This plan is intentionally for:

- `packages/engine`
- `packages/js-sdk`
- `packages/js-kysely`

This plan explicitly ignores `packages/sdk`.

## Current State

### Engine

The engine supports:

- single-call wrapped transaction scripts such as `BEGIN; ... COMMIT;`
- explicit Rust transaction objects internally

The engine currently rejects standalone transaction control in public `execute()` calls. Separate calls like `execute("BEGIN;")` and later `execute("COMMIT;")` do not form a transaction.

Relevant files:

- `packages/engine/src/api.rs`
- `packages/engine/src/sql/execution/statement_scripts.rs`
- `packages/engine/src/state/internal/script.rs`
- `packages/engine/tests/transaction_execution.rs`

### JS SDK

The JS SDK public `Lix` surface now centers on `execute()`, but the backend adapter layer still knows about `beginTransaction()`.

Relevant files:

- `packages/js-sdk/src/types.ts`
- `packages/js-sdk/src/open-lix.ts`
- `packages/js-sdk/src/open-lix.test.ts`
- `packages/js-sdk/wasm-bindgen.rs`

### JS Kysely

`packages/js-kysely` currently requires `lix.beginTransaction(options)` and transaction objects with:

- `execute`
- `commit`
- `rollback`

It also emits savepoint SQL for nested transactions.

Relevant files:

- `packages/js-kysely/src/create-lix-kysely.ts`
- `packages/js-kysely/tests/transaction.test.ts`

## Contract Goal

The desired steady-state contract is:

- engine-owned `Lix.execute()` supports top-level SQL transaction control across separate calls on the same `Lix` instance
- `packages/js-kysely` can run transactions against a plain `Lix` with `execute(sql, params, options)`
- `packages/js-kysely` no longer requires `lix.beginTransaction()`

## Non-Goals

- Do not reintroduce public transaction wrapper objects on `Lix` in this phase.
- Do not implement full nested transaction support in the first pass unless the engine work naturally makes it low-risk.
- Do not solve every engine internal/public cleanup issue here.
- Do not migrate unrelated packages.

## First-Principles Decision

The minimal SDK surface does not include transaction objects right now. Given that choice, `execute()` must behave like a session-bound SQL connection for transaction control, otherwise the public SQL surface is incomplete compared to normal SQL expectations.

That means separate-call `BEGIN` and `COMMIT` support is the correct bridge for now.

## Key Design Decisions

### 1. Transaction scope is per `Lix` instance

One open SQL transaction belongs to one `Lix` instance at a time.

Implications:

- later `execute()` calls on that same `Lix` instance run inside the open SQL transaction
- concurrent callers on the same `Lix` instance must be serialized or rejected while a transaction is active

### 2. First pass supports only minimal top-level transaction control

Support in phase one:

- `BEGIN`
- `COMMIT`
- `ROLLBACK`

Defer for now:

- `BEGIN DEFERRED`
- `BEGIN IMMEDIATE`
- `BEGIN EXCLUSIVE`
- `SAVEPOINT`
- `RELEASE SAVEPOINT`
- `ROLLBACK TO SAVEPOINT`

Reason:

- the engine does not currently expose savepoint/session state for separate public `execute()` calls
- `packages/js-kysely` is the only immediate consumer, and it can be changed to avoid nested transactions until savepoints are intentionally designed
- supporting only plain `BEGIN` keeps parsing, validation, and backend behavior tighter for the first pass

### 3. Public non-SQL methods are blocked during an open SQL transaction

While a separate-call SQL transaction is open, the following should fail fast:

- `create_version`
- `switch_version`
- `create_checkpoint`
- `install_plugin`
- `export_snapshot`

Reason:

- these operations carry their own state and persistence semantics
- mixing them with an open session transaction would be ambiguous in the first implementation
- fail-fast errors are the MVP behavior because they are simpler and safer than blocking or queueing

### 4. Writer key semantics must be explicit

`packages/js-kysely` already passes execute options such as `writerKey`.

Recommended rule:

- the `BEGIN` call may set `writerKey`
- later statements in the same open SQL transaction must either use the same `writerKey` or omit it
- conflicting `writerKey` values during one open SQL transaction should return a clear error

## Implementation Phases

## Phase 1: Add Engine Session Transaction State

### Goal

Introduce transaction state owned by `Lix`/engine so public `execute()` calls can share one backend transaction across calls.

### Tasks

1. Add per-instance transaction session state inside the engine-owned `Lix` implementation.
2. Store the active backend transaction handle plus the minimum metadata needed to finalize engine side effects correctly.
3. Track whether a public SQL transaction is open.
4. Track the transaction's active version context and writer key.
5. Ensure state is cleaned up on `COMMIT`, `ROLLBACK`, and error paths.

### Likely State To Carry

- backend transaction object
- active version id snapshot
- writer key
- pending effect/finalization context now tied to explicit engine transaction flows

## Phase 2: Route Public `execute()` Through The Session Transaction

### Goal

Change `execute()` so transaction control statements manipulate the session transaction instead of being rejected.

### Tasks

1. Detect single-statement transaction control commands in `packages/engine/src/api.rs`.
2. Replace the current blanket rejection for standalone transaction control with explicit routing:
   - `BEGIN` opens session transaction state
   - `COMMIT` finalizes and clears it
   - `ROLLBACK` aborts and clears it
3. When a session transaction is open, route normal SQL statements through that active transaction.
4. Preserve the current behavior for wrapped single-call scripts like `BEGIN; ... COMMIT;`.
5. Return clear errors for invalid transitions:
   - `BEGIN` while already in a transaction
   - `COMMIT` with no open transaction
   - `ROLLBACK` with no open transaction

## Phase 3: Refactor Shared Finalization Logic

### Goal

Avoid duplicating commit/rollback side-effect logic between existing engine transaction paths and the new session transaction path.

### Tasks

1. Extract commit finalization logic from existing transaction execution paths into shared helpers.
2. Extract rollback cleanup logic into shared helpers.
3. Reuse the same logic for:
   - explicit wrapped transaction scripts
   - any remaining internal explicit transaction API
   - new separate-call session transactions
4. Make transaction error normalization consistent across all paths.

## Phase 4: Define Concurrency Behavior

### Goal

Make transaction behavior predictable when multiple async callers use one `Lix` instance.

### Recommended First Pass

- if no session transaction is open, ordinary `execute()` calls behave as they do today
- once a session transaction is open, other public operations on that `Lix` instance are serialized behind it

### Tasks

1. Audit existing JS-side queueing assumptions in `packages/js-sdk/src/open-lix.ts`.
2. Decide whether engine alone should guarantee serialization, or whether engine plus JS wrapper should both enforce it.
3. Add tests for concurrent calls against one `Lix` while a session transaction is open.

## Phase 5: Update JS SDK To Expose The New Capability Cleanly

### Goal

Keep the JS SDK surface minimal while making the new engine behavior available to callers.

### Tasks

1. Ensure `lix.execute("BEGIN", [], options)` reaches the engine unchanged.
2. Remove error messages and docs that claim raw SQL transaction control is unsupported.
3. Remove public `beginTransaction` support from the JS SDK in this phase.
4. Delete or rewrite any JS SDK adapter code that still assumes transaction objects in:
   - `packages/js-sdk/src/types.ts`
   - `packages/js-sdk/src/open-lix.ts`
   - `packages/js-sdk/wasm-bindgen.rs`

## Phase 6: Rewrite `packages/js-kysely` To Use Raw SQL Transaction Control

### Goal

Make `packages/js-kysely` depend only on `lix.execute(...)`, not `lix.beginTransaction(...)`.

### Tasks

1. Simplify `LixLike` in `packages/js-kysely/src/create-lix-kysely.ts` to require:
   - `execute(sql, params, options)`
   - optional `db` shortcut only if intentionally retained
2. Remove `LixSqlTransactionLike` and `LixBeginTransactionLike`.
3. Rewrite `LixDriver` transaction hooks:
   - `beginTransaction()` issues `BEGIN`
   - `commitTransaction()` issues `COMMIT`
   - `rollbackTransaction()` issues `ROLLBACK`
4. Reject savepoint operations with a clear error in the first pass.
5. Keep `writerKey` flowing through all calls made by the driver.
6. Update the error message that currently says raw SQL transaction fallback is not supported.

## Phase 7: Test Matrix

### Engine

Add or update tests for:

- `BEGIN`, write, write, `COMMIT` across separate calls persists atomically
- `BEGIN`, write, `ROLLBACK` discards writes
- `COMMIT` without `BEGIN` fails
- `ROLLBACK` without `BEGIN` fails
- double `BEGIN` fails
- wrapped `BEGIN; ... COMMIT;` still works
- non-transaction methods fail while SQL transaction is open
- conflicting writer keys inside one open SQL transaction fail clearly

### JS SDK

Add or update tests for:

- separate-call `BEGIN` and `COMMIT` through `openLix()`
- rollback behavior
- concurrent `execute()` calls while a transaction is open

### JS Kysely

Add or update tests for:

- `qb(lix).transaction()` works with plain `openLix()`
- concurrent transactions on one `Lix` behave predictably
- nested transaction/savepoint usage fails with the intended first-pass error

## Recommended Implementation Order

1. Engine session transaction state and `execute()` routing.
2. Shared commit/rollback finalization helpers.
3. Engine tests covering new behavior.
4. JS SDK error/message cleanup.
5. `packages/js-kysely` driver rewrite away from `beginTransaction`.
6. JS SDK and JS Kysely tests.
7. Follow-up removal of obsolete JS SDK transaction-object plumbing if no longer needed.

## Risks

- transaction finalization in the engine likely touches more internal state than plain backend `commit()` and `rollback()`
- serialization bugs can create very subtle cross-call corruption on one `Lix` instance
- nested transactions will remain unsupported until savepoints are designed intentionally
- removing JS transaction-object shims in the same phase reduces duplicate paths, but increases migration pressure on `packages/js-kysely`

## Open Questions

1. If backend users submit `BEGIN IMMEDIATE` or `BEGIN EXCLUSIVE`, should the engine reject them explicitly or normalize them to unsupported transaction-control variants in the same error family?
2. Should savepoints be a follow-up only after `packages/js-kysely` no longer requires nested transactions anywhere in its test matrix?

## Progress Log

- 2026-03-12: Created `plan2.md` for engine-level separate-call SQL transaction support and the `packages/js-kysely` migration away from `lix.beginTransaction()`.
- 2026-03-12: Fixed first-pass decisions in `plan2.md`: support only plain `BEGIN`, make non-`execute()` methods fail fast during open SQL transactions, remove the JS `beginTransaction` API, and document no nested transactions for now.
- 2026-03-12: Moved separate-call public SQL transaction state into `packages/engine/src/sql/execution/transaction_session.rs` and routed `Engine::execute_with_options()` through it. `Lix` is now a thin passthrough wrapper over `Engine`.
- 2026-03-12: Added engine coverage in `packages/engine/tests/transaction_execution.rs` for separate-call `BEGIN`/`COMMIT`, separate-call `BEGIN`/`ROLLBACK`, unsupported transaction variants, and blocking non-`execute()` engine APIs while a public SQL transaction is open.
- 2026-03-12: Updated `packages/rs-sdk/tests/transaction.rs` and `packages/js-sdk/src/open-lix.test.ts` to match the engine-owned transaction behavior. Verified `cargo test -p lix_engine --test transaction_execution`, `cargo test -p lix_rs_sdk --test transaction`, and `pnpm --filter @lix-js/sdk test -- open-lix.test.ts`.
- 2026-03-12: Rewrote `packages/js-kysely/src/create-lix-kysely.ts` to use raw `BEGIN`/`COMMIT`/`ROLLBACK`, reject savepoints, normalize JS SDK `{ statements: [...] }` execute results, and serialize concurrent top-level transactions on one `Lix` instance.
- 2026-03-12: Added a Lix-specific Kysely query compiler that emits unquoted identifiers because the current engine public SQL write path does not accept quoted targets like `INSERT INTO \"lix_file\" ...`. Verified `pnpm --filter @lix-js/kysely typecheck` and `pnpm --filter @lix-js/kysely test -- tests/transaction.test.ts`.
