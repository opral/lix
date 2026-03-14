# Plan 4: Simplify Writes To The Core Transaction Model

## Goal

Make the tracked-write path in Lix explicitly follow this model:

- `BEGIN`
- `preflight read`
- `write batch`
- `COMMIT`

This plan is first-principles and ignores backward compatibility. The goal is not to preserve existing layering. The goal is to make the code reflect the actual desired architecture.

## Core Principle

The engine should have one obvious write transaction model:

1. prepare a tracked write transaction plan
2. open a transaction
3. run one preflight query
4. generate one write batch from the preflight result
5. commit
6. apply non-DB in-memory side effects after commit

Anything that does not fit this model should be either:

- moved into the preflight query
- moved into the write batch
- moved out of the DB transaction entirely
- or deleted as an outdated abstraction

## Desired Architecture

### 1. Thin API Layer

`api.rs` should stop being a write-orchestration hub.

Its job should be:

- parse and route SQL
- choose read path vs tracked write path vs untracked path
- call a dedicated tracked-write runner
- apply post-commit engine-only side effects

It should not directly own:

- runtime sequence persistence logic for tracked writes
- observe tick fallback logic for tracked writes
- tracked filesystem payload persistence orchestration
- tracked write transaction shaping

### 2. Explicit Tracked Write Transaction Plan

Introduce one engine-owned type, for example:

```rust
struct TrackedWriteTxnPlan {
    preflight: PreflightQuery,
    write_batch: DeferredWriteBatch,
    post_commit_effects: PostCommitEffects,
}
```

Or, if the batch depends on preflight values:

```rust
struct TrackedWriteTxnPlan {
    preflight_sql: String,
    build_write_batch: fn(&PreflightState) -> Result<PreparedBatch, LixError>,
    post_commit_effects: PostCommitEffects,
}
```

The exact types can differ, but the transaction model must be explicit.

The important part is:

- one plan object
- one preflight
- one batch
- one place that owns the transaction shape

### 3. Dedicated Tracked Write Runner

Create one runner responsible for the full tracked-write transaction:

```rust
async fn run_tracked_write_txn_plan(
    engine: &Engine,
    plan: TrackedWriteTxnPlan,
) -> Result<TrackedWriteOutcome, LixError>
```

That runner should:

1. `begin_transaction()`
2. execute `preflight`
3. materialize `write_batch`
4. execute `write_batch`
5. `commit()`
6. return post-commit effects for the engine to apply

This runner should become the only owner of tracked-write transaction flow.

### 4. Preflight Is DB State Resolution Only

Preflight should resolve all DB inputs needed to build the write batch:

- current tip
- replay/idempotency hit
- active accounts
- deterministic sequence start, if needed
- exact target file descriptor row, if needed
- any other authoritative live state needed for commit generation

Preflight should not:

- perform write-side effects
- do post-commit work
- leak planner/runtime layering concerns

### 5. Write Batch Contains All Transactional DB Writes

The write batch should contain all transactional DB writes for the tracked write:

- snapshots
- commit rows
- live-state rows
- idempotency row
- deterministic sequence persistence
- observe tick
- any other transactional bookkeeping

If a DB write happens after the main append batch but before commit, that is a design smell and should be folded into the write batch or explicitly justified as impossible.

### 6. Post-Commit Effects Are Engine-Only

Things that should happen after commit and should not be part of the transaction model:

- cache invalidation
- public surface registry refresh
- event bus / state commit stream emission
- in-memory active-version updates

These should be returned as data from the tracked write runner, not triggered ad hoc from multiple places.

## What To Remove Or Collapse

### 1. Collapse tracked-write orchestration out of `api.rs`

Today `api.rs` still owns too much transactional behavior indirectly.

Refactor so that:

- `api.rs` routes
- tracked write runner executes
- post-commit effects applier applies

### 2. Shrink `shared_path.rs`

`shared_path.rs` currently acts as a god module.

Split it into:

- preparation
- tracked write runner orchestration
- untracked write path
- post-commit effects application

The tracked-write runner should not live in a file that also owns unrelated execution preparation and merge helpers.

### 3. Narrow `append_commit.rs`

`append_commit.rs` should become the core append kernel, not a dumping ground for unrelated specialization.

It should own:

- append preflight SQL
- append replay/tip logic
- commit generation
- write batch assembly

It should not own:

- broad public runtime concerns
- generic execution plumbing
- unrelated filesystem planning concerns

### 4. Remove leftover post-append transactional side-effect paths

Anything like:

- followup transaction batches
- trailing transactional side-effect helpers
- duplicate transactional writes outside append

should be removed or folded into the write batch.

### 5. Remove tracked-write fallback branches that preserve older execution shapes

If the desired architecture is one explicit tracked-write transaction plan, do not preserve multiple equivalent tracked-write orchestration paths.

Prefer:

- one canonical tracked-write plan
- one canonical tracked-write runner

over:

- special-casing spread across API, runtime, append, and execution helpers

## Concrete Target Module Structure

One possible end state:

### `packages/engine/src/sql/public/runtime/tracked_write_plan.rs`

Owns:

- lowering from public tracked write execution into `TrackedWriteTxnPlan`
- no transaction execution

### `packages/engine/src/sql/execution/tracked_write_runner.rs`

Owns:

- begin transaction
- run preflight
- build batch from preflight result
- execute batch
- commit
- return `TrackedWriteOutcome`

### `packages/engine/src/state/commit/append_commit.rs`

Owns:

- append preflight query builder
- append replay/tip resolution
- append batch builder

### `packages/engine/src/sql/execution/post_commit_effects.rs`

Owns:

- cache invalidation
- state stream emission
- registry refresh
- active version updates

### `packages/engine/src/api.rs`

Owns:

- request routing only

## New Core Types

### `TrackedWriteTxnPlan`

```rust
struct TrackedWriteTxnPlan {
    preflight: TrackedWritePreflight,
    post_commit_effects: PostCommitEffects,
}
```

### `TrackedWritePreflight`

```rust
struct TrackedWritePreflight {
    sql: String,
    params: Vec<Value>,
}
```

### `TrackedWritePreflightState`

```rust
struct TrackedWritePreflightState {
    current_tip: Option<String>,
    replay_commit_id: Option<String>,
    active_accounts: Vec<String>,
    deterministic_sequence_start: Option<i64>,
    file_descriptor: Option<FileDescriptorState>,
}
```

### `TrackedWriteBatch`

```rust
struct TrackedWriteBatch {
    sql: String,
    params: Vec<Value>,
}
```

### `TrackedWriteOutcome`

```rust
struct TrackedWriteOutcome {
    public_result: QueryResult,
    post_commit_effects: PostCommitEffects,
    plugin_changes_committed: bool,
}
```

### `PostCommitEffects`

```rust
struct PostCommitEffects {
    state_commit_stream_changes: Vec<StateCommitStreamChange>,
    next_active_version_id: Option<String>,
    file_cache_refresh_targets: BTreeSet<(String, String)>,
    should_refresh_public_surface_registry: bool,
    should_invalidate_installed_plugins_cache: bool,
}
```

## Implementation Order

### Stage 1: Make The Transaction Model Explicit

Introduce:

- `TrackedWriteTxnPlan`
- `TrackedWriteOutcome`
- `PostCommitEffects`

without changing behavior yet.

Goal:

- tracked writes become visibly represented as one transaction plan

### Stage 2: Move Tracked Execution Into One Runner

Create `tracked_write_runner.rs` and move all tracked transaction flow there.

This stage should remove transaction orchestration from:

- `api.rs`
- `shared_path.rs`

except for thin routing glue.

### Stage 3: Move All Transactional DB Side Effects Into Batch Assembly

Ensure the tracked write batch contains:

- append writes
- idempotency
- deterministic sequence persistence
- observe tick

No trailing transactional followup writes should remain.

### Stage 4: Separate Post-Commit Effects

Return post-commit effects as data and apply them after commit in one dedicated place.

This should remove scattered side-effect handling from `api.rs` and `shared_path.rs`.

### Stage 5: Delete Redundant Tracked-Write Paths

After the explicit tracked-write model is in place:

- delete duplicate orchestration branches
- collapse helper layers that only existed to support the older shape

## Validation

### Transaction Shape

For exact tracked metadata update, trace must show:

- `BEGIN`
- one preflight read
- one write batch
- `COMMIT`

No extra transaction-local writes.

### Code Shape

There should be one obvious tracked-write execution path.

It should be possible to answer:

- where tracked writes start
- where preflight happens
- where batch assembly happens
- where commit happens
- where post-commit effects happen

by pointing to one module per concern, not five partially overlapping ones.

### Functional Parity

Must preserve:

- replay/idempotency behavior
- tip-drift checks
- deterministic sequence correctness
- observe tick correctness
- checkpoint side effects
- stored-schema bootstrap mirroring
- public surface refresh correctness

### Failure Semantics

On failure before commit:

- transaction rolls back
- no observe tick leaks
- no deterministic sequence persist leaks
- no idempotency row leaks
- no post-commit engine effects run

## Success Criteria

The architecture is successful when:

1. tracked write execution is visibly modeled as:
   - `BEGIN`
   - preflight read
   - write batch
   - `COMMIT`
2. the code has one canonical tracked-write runner
3. `api.rs` is a router, not a write-orchestration hub
4. transactional DB side effects no longer leak outside batch assembly
5. post-commit engine effects are centralized and explicit

## Progress Log

- Checkpoint 1:
  - Added an explicit `TrackedWriteTxnPlan` in `sql/public/runtime/tracked_write_plan.rs`.
  - Added a dedicated tracked-write runner in `sql/execution/tracked_write_runner.rs`.
  - Moved owned post-commit application into `sql/execution/post_commit_effects.rs`.
  - Rewired `api.rs` to delegate post-commit work instead of orchestrating it inline.
  - Rewired the tracked branch in `shared_path.rs` to build a tracked-write transaction plan and delegate execution to the runner.

- Checkpoint 2:
  - Validation passed with `cargo fmt -p lix_engine`, `cargo check -p lix_engine`, and `cargo test -p lix_engine applies_commit_when_tip_matches_expected --lib`.
  - The exact tracked metadata-update path still traces as the intended core transaction shape:
    - `BEGIN`
    - one preflight read
    - one write batch
    - `COMMIT`
  - Fresh traced benchmark shape after the refactor:
    - `total_ops=4`
    - `backend_exec=0`
    - `tx_exec=2`
  - The write architecture is now more explicit in code even though some helper logic still lives in `shared_path.rs`.

## Progress log

- 2026-03-13: Initial plan drafted
- 2026-03-13: Implemented the tracked-write transaction kernel end to end.
  - Added `TrackedWriteTxnPlan` in `packages/engine/src/sql/public/runtime/tracked_write_plan.rs`.
  - Added the dedicated runner in `packages/engine/src/sql/execution/tracked_write_runner.rs`.
  - Moved owned post-commit work into `packages/engine/src/sql/execution/post_commit_effects.rs`.
  - Rewired `packages/engine/src/sql/execution/shared_path.rs` so tracked writes build an explicit plan and delegate execution to the runner.
  - Rewired `packages/engine/src/api.rs` so owned post-commit effects are applied through the dedicated post-commit effects module instead of inline orchestration.
  - Verified the exact tracked file metadata-update path still manifests the target transaction shape:
    - `BEGIN`
    - one preflight read
    - one write batch
    - `COMMIT`
  - Verified with:
    - `cargo fmt -p lix_engine`
    - `cargo check -p lix_engine`
    - `cargo test -p lix_engine applies_commit_when_tip_matches_expected --lib`
    - traced `lix_file/update_existing_row/metadata_only`, which reported `total_ops=4`, `backend_exec=0`, and `tx_exec=2`
