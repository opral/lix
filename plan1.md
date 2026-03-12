# Lix Type And SDK Exposure Plan

## Objective

Define one minimal `Lix` contract in `packages/engine` and make both `rs-sdk` and `js-sdk` expose that same contract.

The purpose is to stop API drift by making one engine-owned API authoritative while keeping environment bootstrapping thin and platform-specific.

## Desired Public Contract

The authoritative public object is `Lix`.

Target methods:

- `open`
- `init`
- `execute`
- `observe`
- `create_version`
- `switch_version`
- `create_checkpoint`
- `install_plugin`
- `export_snapshot`

Target DTOs and related types:

- `Lix`
- `LixConfig`
- `InitResult`
- `Value`
- `QueryResult`
- `ExecuteResult`
- `ExecuteOptions`
- `ObserveQuery`
- `ObserveEvent`
- `ObserveEvents`
- `CreateVersionOptions`
- `CreateVersionResult`
- `CreateCheckpointResult`
- `LixBackend`
- `LixTransaction`
- `SqlDialect`
- `WasmRuntime`
- `WasmComponentInstance`
- `WasmLimits`

## Design Rules

1. `packages/engine` owns the authoritative raw SDK contract.
2. `packages/rs-sdk` must expose that contract directly or with near-zero wrapper logic.
3. `packages/js-sdk` may add bootstrapping defaults, but must not redefine semantics or DTO shapes.
4. Any type shared across Rust and JS must be defined once in `engine` and reused, not re-described by hand.
5. The public contract should be minimal. Internal planning, materialization, commit generation, and stream internals must not leak into the default SDK surface.
6. Bootstrapping is allowed to remain environment-specific.
7. Transaction object APIs are intentionally out of scope for this phase.
8. If transaction APIs remain out of scope, decide explicitly whether `execute("BEGIN") ... execute("COMMIT")` must be supported and test it.

## Non-Goals

- Do not preserve the current broad `engine` export surface as the SDK contract.
- Do not hand-maintain a second JS product API that drifts from engine-owned DTOs.
- Do not introduce WIT as the primary contract language in this phase.
- Do not solve every internal-public split in one pass if tests and benches still depend on internals.
- Do not add new product features while reshaping the contract.

## Current State Summary

### Engine

`packages/engine` already owns the real behavior for:

- open/init
- execute
- explicit transactions
- observe
- version creation and switching
- checkpoint creation
- plugin installation
- snapshot export and restore

It also exposes many advanced or internal-ish APIs that are not part of the desired minimal SDK surface.

### Rust SDK

`packages/rs-sdk` is currently a thin wrapper over `engine`, but it only exposes a subset of engine behavior.

### JS SDK

`packages/js-sdk` already calls into engine through wasm-bindgen, but it also maintains a second handwritten API layer with:

- bootstrapping defaults
- value adaptation
- lifecycle helpers
- queueing behavior
- JS-specific convenience methods

This layer should be reduced to thin bootstrapping and adaptation only.

## Target Architecture

### Engine Layer

Create one explicit SDK-focused module or facade in `packages/engine` that defines the public `Lix` contract.

Two viable shapes:

1. `engine::sdk` module with public `Lix` types re-exported from crate root.
2. Keep `Engine` as the implementation type and expose a smaller crate-root API that treats it as the public `Lix`.

Preferred initial approach:

- Use `engine` as the implementation.
- Define the minimal public contract in `packages/engine/src/lib.rs`.
- Keep broad internals accessible only inside the engine crate where possible.
- Delay aggressive privatization until SDKs and tests migrate.

### Rust SDK Layer

Reshape `packages/rs-sdk` so it does one of the following:

1. Mostly re-export the engine-owned `Lix` contract.
2. Provide only Rust-specific default bootstrapping helpers around the same contract.

Preferred initial approach:

- Keep `rs-sdk` as the package users import.
- Re-export the engine-owned DTOs and methods.
- Limit wrapper code to default backend and default wasm runtime provisioning.

### JS SDK Layer

Reshape `packages/js-sdk` into two layers:

1. Raw binding layer for the engine-owned contract.
2. Thin JS bootstrap layer that provides defaults.

The JS layer may continue to:

- create the default SQLite wasm backend
- load the default plugin wasm runtime
- convert between JS runtime values and engine wire values
- expose `openLix()` and `initLix()` convenience functions

The JS layer must stop:

- hand-defining DTO shapes that already exist in engine
- inventing semantic differences from the engine contract
- carrying extra product behavior unless explicitly desired

## Implementation Phases

## Phase 1: Freeze The Minimal Contract

### Goals

- Decide the exact authoritative public API in `packages/engine`.
- Decide naming and ownership of every DTO.
- Decide transaction semantics for this phase.

### Tasks

1. Write down the exact method list and DTO list in engine code comments or module docs.
2. Decide whether the public type is literally `Engine` or a `Lix` facade.
3. Decide whether `restore_snapshot` is intentionally omitted from the minimal contract or included.
4. Decide whether multi-call SQL transaction control must be supported if transaction objects remain excluded.
5. Mark current engine exports as one of:
   - keep public
   - move behind advanced/internal boundary
   - remove from public surface later

### Exit Criteria

- One agreed method list.
- One agreed DTO list.
- One agreed transaction stance.

## Phase 2: Define The Engine-Owned `Lix` Contract

### Goals

- Make `packages/engine` the single source of truth for the minimal SDK contract.

### Tasks

1. Introduce or refine the public `Lix` type boundary in engine.
2. Ensure `open` and `init` are the explicit public lifecycle entrypoints.
3. Ensure all public DTOs used by SDKs are owned by engine.
4. Move JS-facing wire conversion helpers behind the minimal contract where practical.
5. Remove accidental public exports from `lib.rs` where doing so does not block migration.

### Notes

- If using `Engine` directly as `Lix`, keep the public API narrow at crate root even if implementation methods remain broader internally.
- If a separate facade is introduced, keep it as thin delegation over engine behavior.

### Exit Criteria

- Engine crate root exposes exactly the intended minimal SDK contract plus any temporarily required migration shims.

## Phase 3: Unify DTO Ownership

### Goals

- Eliminate duplicated DTO definitions and manual drift between engine, rs-sdk, and js-sdk.

### Tasks

1. Audit all DTOs currently defined separately in JS and Rust wrappers.
2. Make engine-owned DTOs the source of truth.
3. Replace handwritten duplicated result shapes in JS where possible.
4. Normalize naming inconsistencies such as snake_case vs camelCase bridges with one explicit conversion layer.
5. Minimize special-case result remapping in wrappers.

### High-Risk Areas

- `CreateCheckpointResult`
- observe event/result shapes
- execution result/value encoding
- boot key value field names

### Exit Criteria

- No product DTO is authored independently in more than one layer.

## Phase 4: Reshape `rs-sdk`

### Goals

- Make `rs-sdk` a thin package around the engine-owned contract.

### Tasks

1. Replace wrapper-owned DTOs with re-exports where possible.
2. Keep only Rust-specific bootstrapping defaults:
   - default SQLite backend
   - default Wasmtime plugin runtime
3. Expose `open` and `init` with the same semantics as engine.
4. Add coverage for all methods in the minimal contract, not just execute and explicit transaction cases.

### Exit Criteria

- `rs-sdk` behavior is functionally a re-exposed engine contract plus default provisioning.

## Phase 5: Reshape `js-sdk`

### Goals

- Make `js-sdk` a thin bootstrap layer over the engine-owned contract.

### Tasks

1. Define a raw binding entrypoint for the engine-owned contract.
2. Keep `openLix()` and `initLix()` as convenience wrappers only.
3. Restrict JS wrapper logic to:
   - default backend creation
   - default wasm runtime loading
   - JS value adaptation
   - resource cleanup
4. Remove or collapse wrapper-owned DTO definitions where possible.
5. Decide whether queueing remains a bootstrap concern or becomes engine contract behavior.
6. Add tests that prove the JS SDK behavior matches the engine-owned contract.

### Exit Criteria

- `js-sdk` no longer acts as an independently authored product API.

## Phase 6: Transaction Decision

### Goals

- Resolve the gap introduced by omitting transaction object APIs from the initial minimal contract.

### Decision Branch A: Support Multi-Call SQL Transactions

Tasks:

1. Change engine public `execute()` semantics to allow:
   - `execute("BEGIN")`
   - later writes
   - `execute("COMMIT")`
2. Define how this behaves per `Lix` instance and per backend connection.
3. Add explicit tests for:
   - successful multi-call transaction commit
   - rollback
   - nested begin rejection
   - interleaved observe behavior
   - error handling and failed transaction state

### Decision Branch B: Keep Transaction Control Restricted

Tasks:

1. Keep public `execute()` limited to:
   - normal statements
   - wrapped transaction scripts in a single call
2. Reintroduce a minimal explicit transaction API sooner than originally planned.
3. Make the restriction explicit in docs and tests.

### Exit Criteria

- One supported model.
- Clear tests covering the chosen behavior.

## Phase 7: Conformance Test Matrix

### Goals

- Make drift detectable immediately.

### Tasks

1. Build one shared scenario matrix for the minimal `Lix` contract.
2. Run the same scenarios against:
   - engine-level contract
   - rs-sdk
   - js-sdk
3. Cover at minimum:
   - `init`
   - `open`
   - execute scalar reads
   - execute writes
   - `observe`
   - `create_version`
   - `switch_version`
   - `create_checkpoint`
   - `install_plugin`
   - `export_snapshot`
4. Add contract tests for failure cases and validation behavior.

### Exit Criteria

- CI fails if SDK behavior diverges from the engine-owned contract.

## Phase 8: Privatize Non-SDK Surface

### Goals

- Shrink the public engine surface to the minimal SDK contract plus explicitly advanced APIs, if any.

### Tasks

1. Remove public re-exports for commit-generation internals.
2. Remove public re-exports for materialization/debug internals.
3. Remove public re-exports for state commit stream unless intentionally kept as advanced API.
4. Remove temporary migration shims.
5. Update engine tests and benches to use internal modules instead of public crate-root exports where needed.

### Exit Criteria

- `packages/engine/src/lib.rs` clearly communicates the actual intended public API.

## Open Questions

1. Should `restore_snapshot` be in the minimal public contract now or deferred?
2. Should `install_plugin` remain on `Lix` or move behind a capability-specific API later?
3. If JS keeps queueing semantics, are those part of the contract or just current implementation behavior?
4. Should engine expose `Lix` as a new facade type or simply narrow the public `Engine` API?
5. Should the raw JS binding use canonical tagged values directly, or should the binding accept runtime JS scalars and bytes?

## Recommended Order Of Work

1. Freeze the contract and transaction stance.
2. Define the engine-owned public `Lix` surface.
3. Unify DTO ownership.
4. Migrate `rs-sdk`.
5. Migrate `js-sdk`.
6. Add cross-SDK conformance tests.
7. Privatize advanced engine exports.

## Deliverables

- Engine-owned minimal `Lix` contract.
- Thin `rs-sdk` wrapper with default provisioning only.
- Thin `js-sdk` wrapper with default provisioning only.
- Shared contract test matrix.
- Reduced engine crate-root public exports.

## Plan: Separate Execute Transaction Control

### Objective

Add support for transaction control across separate `execute()` calls so this flow works on one `Lix` / `Engine` instance:

- `execute("BEGIN;")`
- `execute("INSERT ...")`
- `execute("INSERT ...")`
- `execute("COMMIT;")`

The goal is to make transaction scope session-like instead of single-call-only, while keeping the current explicit transaction-script path (`BEGIN; ... COMMIT;` in one call) working.

### Important Architecture Note

The current Kysely environment driver in `packages/sdk` already emits raw transaction SQL:

- `begin`
- `commit`
- `rollback`
- nested `savepoint` / `release savepoint` / `rollback to savepoint`

That code lives in `packages/sdk/src/database/sqlite/environment-dialect.ts`.

However, that driver currently talks to the TypeScript engine through `lix_execute_sync` in `packages/sdk/src/engine/functions/register-builtins.ts`, not directly to the Rust engine.

That means there are two distinct integration options:

1. Implement the feature in the Rust engine and also move JS Kysely to a Rust-backed execution path.
2. Implement the same feature in the TypeScript engine if the immediate goal is to fix the current `packages/sdk` Kysely path.

If the goal is specifically the newer Rust-backed JS SDK, the Rust engine plan below is the right one. If the goal is the current `packages/sdk` Kysely runtime, the same semantics must also exist in the TypeScript engine or the driver must be rewired.

### Current Confirmed Behavior

The current Rust engine behavior is now covered by tests:

- separate `BEGIN` is rejected
- separate `COMMIT` is rejected
- writes between them commit independently
- single-call `BEGIN; ... COMMIT;` scripts still work

### Scope Decision

First pass should support top-level transaction control only:

- `BEGIN`
- `COMMIT`
- `ROLLBACK`

Do not make nested transactions part of the first implementation.

For nested Kysely transactions:

- either explicitly reject them in the driver for now
- or add `SAVEPOINT` / `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT` in a follow-up

### Phase 1: Introduce Session Transaction State In The Rust Engine

Add engine-owned state for one active session transaction per `Engine` / `Lix` instance.

That state should hold:

- the backend transaction object
- current active version id
- starting active version id
- pending state commit stream changes
- pending SQL2 append session
- plugin cache invalidation flag
- any other commit-finalization state currently tracked by `EngineTransaction`

This should be stored in a synchronization primitive so separate async `execute()` calls can reuse the same open transaction safely.

### Phase 2: Serialize Public Execute Operations Per Engine Session

Separate-call transaction control only works predictably if execution on a single session is ordered.

Add a session operation lock / queue so:

- `BEGIN`
- statements inside the open transaction
- `COMMIT` or `ROLLBACK`

are processed in a deterministic order for one `Engine` / `Lix` instance.

This aligns with session/connection semantics and matches what the JS wrapper already approximates with its operation queue.

### Phase 3: Route Transaction Control Statements In `execute_impl_sql`

Update `packages/engine/src/api.rs` so public `execute()`:

1. detects single-statement `BEGIN` / `COMMIT` / `ROLLBACK`
2. routes them to the session transaction manager instead of rejecting them
3. routes normal statements through the active session transaction when one exists
4. preserves the current one-call `BEGIN; ... COMMIT;` script behavior when no session transaction is open

Rules to enforce:

- `BEGIN` when no session transaction is open: open one
- `BEGIN` when one is already open: reject as nested transaction
- `COMMIT` / `ROLLBACK` with no open session transaction: reject
- transaction control inside a session script: reject unless explicitly supported later

### Phase 4: Reuse Existing In-Transaction Execution Helpers

Do not create a second execution path.

Reuse:

- `execute_with_options_in_transaction`
- `execute_statement_script_with_options_in_transaction`
- existing runtime side-effect flushing
- existing active-version tracking

The main refactor here is to extract commit/rollback finalization logic currently duplicated across:

- `EngineTransaction::commit`
- `execute_statement_script_with_options`

into shared helpers used by:

- explicit transaction API
- one-call transaction scripts
- new session transaction control

### Phase 5: Define Behavior For Other Public `Lix` Methods During An Open SQL Transaction

Methods like these currently manage their own transactions internally:

- `create_version`
- `switch_version`
- `create_checkpoint`
- `install_plugin`

First pass should not silently nest them inside an open SQL session transaction.

Recommended rule:

- if a session transaction is open, these methods return a clear error like "not supported while an explicit SQL transaction is open"

That avoids accidental nested transaction behavior while keeping the first implementation small and predictable.

### Phase 6: Kysely Integration Decision

For the current `packages/sdk` Kysely path:

- top-level transactions should work once the underlying engine used by `lix_execute_sync` supports separate-call transaction control
- nested transactions still need a decision

Recommended first-pass Kysely behavior:

- allow top-level `db.transaction().execute(...)`
- explicitly reject nested transactions in the driver until savepoints are supported end to end

If moving Kysely onto the Rust-backed JS SDK instead:

- create a driver/connection that maps `CompiledQuery.raw("begin")`, `commit`, and `rollback` to `lix.execute(...)`
- reuse the same session transaction semantics there

### Phase 7: Tests

Rust engine tests:

- `BEGIN` + writes + `COMMIT` persists changes
- `BEGIN` + writes + `ROLLBACK` discards changes
- `BEGIN` twice is rejected
- `COMMIT` / `ROLLBACK` without `BEGIN` is rejected
- one-call `BEGIN; ... COMMIT;` remains working
- open session transaction blocks or serializes concurrent execute calls predictably

Rust SDK tests:

- same top-level transaction behavior through `Lix`

JS SDK tests:

- same top-level transaction behavior through `openLix()`

Kysely integration tests:

- `db.transaction().execute(...)` commits on success
- `db.transaction().execute(...)` rolls back on thrown error
- nested transactions either work with savepoints or fail with an explicit not-supported error

### Recommended Implementation Order

1. Implement session transaction state and routing in the Rust engine.
2. Extract shared transaction-finalization helpers.
3. Add engine tests for separate-call `BEGIN` / `COMMIT` / `ROLLBACK`.
4. Decide and enforce first-pass behavior for nested transactions.
5. Wire the chosen JS Kysely path to the new semantics.
6. Add integration tests at the Kysely layer.

## Progress Log

- 2026-03-12: Created `plan1.md` with initial implementation plan and checkpoint log section.
- 2026-03-12: Added engine-owned `Lix`, `LixConfig`, and `InitResult` in `packages/engine/src/lix.rs`, and re-exported the new contract from `packages/engine/src/lib.rs`.
- 2026-03-12: Collapsed `rs-sdk` to a thin re-export layer over the engine-owned contract and updated Rust tests to use explicit `init` + `open` flows instead of wrapper-owned transaction APIs.
- 2026-03-12: Migrated the JS wasm binding to use engine-owned `Lix`, removed public JS transaction methods from the SDK surface, and rewrote affected JS tests around the retained `execute` transaction-script behavior.
- 2026-03-12: Verified the new contract with `cargo test -p lix_rs_sdk`, `cargo test -p lix_engine --test transaction_execution`, `pnpm --filter @lix-js/sdk typecheck`, and `pnpm --filter @lix-js/sdk test`.
- 2026-03-12: Pruned the engine crate root to remove unused SDK-facing exports (`EngineConfig`, `OpenOrInitResult`, `EngineTransactionFuture`, `observe_owned`, commit-generation exports, root `Wire*`, and root `ErrorCode`), moved JS to `lix_engine::wire`, and removed dead transaction-handle plumbing plus the unused `Engine::init` path.
- 2026-03-12: Re-verified the narrower engine surface with `cargo test -p lix_engine --tests --no-run`, `cargo test -p lix_engine --test transaction_execution`, `cargo test -p lix_rs_sdk`, and `pnpm --filter @lix-js/sdk test`. `cargo bench -p lix_engine --no-run` still fails because several bench files use a stale `ExecuteResult.rows` shape unrelated to this API-pruning pass.
- 2026-03-12: Added contract tests in engine, rs-sdk, and js-sdk showing that separate `execute("BEGIN;") ... execute("COMMIT;")` calls do not form a transaction: `BEGIN` and `COMMIT` are rejected, and intermediate writes commit independently.
