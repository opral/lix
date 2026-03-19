# Plan 60: Undo / Redo Architecture

## Goal

Add a first-class `lix.undo()` and `lix.redo()` API, also exposed through the CLI, in a way that:

- works uniformly for files, state rows, entity views, and exact file payload updates
- composes with the engine's existing tracked-write and commit model
- is safe for future sync and collaboration
- preserves append-only canonical history

This plan intentionally does **not** implement undo/redo as "move the version head backward/forward". Instead, it proposes **inverse commits**.

## Decision

Undo and redo should be implemented as **new tracked commits** in a version lane.

That means:

- `undo()` creates a new commit whose changes restore the previous visible state of the last undoable commit
- `redo()` creates a new commit whose changes reapply the original forward batch that was undone

This is the right choice because future sync wants canonical, append-only history. A pure head rewind is effectively a local ref rewrite. That is acceptable for local admin tools, but it is a poor default for collaborative history.

## Why Not Head Rewrites

Rewriting `lix_version_ref` directly is attractive because it is simple, but it has bad long-term properties:

- it changes visible state without creating a new canonical change batch
- it behaves like non-fast-forward history editing
- sync would need branch-rewrite semantics instead of normal replication
- redo becomes a local cursor problem rather than shared history
- commit history, observation, and audit trails become harder to reason about

By contrast, inverse commits:

- preserve append-only history
- replicate through the same machinery as ordinary writes
- are naturally visible in change history and commit graphs
- keep observers and caches aligned with canonical writes
- make undo/redo a domain-level feature rather than a local UI trick

## Current Engine Facts

The engine already gives us the key primitives:

- tracked writes from public surfaces converge into `create_commit()` / `generate_commit()`
- canonical changes are persisted in `lix_internal_change` and linked into commits via `lix_change_set_element`
- visible state is materialized per schema and per version
- exact committed state can be loaded from authoritative live tables via `load_exact_committed_state_row_from_live_state_*`
- files are represented as ordinary tracked state rows, especially `lix_file_descriptor` and `lix_binary_blob_ref`
- merged public tracked writes inside one transaction already collapse into one commit-sized unit

Relevant files:

- `packages/engine/src/state/commit/create_commit.rs`
- `packages/engine/src/state/commit/generate_commit.rs`
- `packages/engine/src/state/commit/state_source.rs`
- `packages/engine/src/sql/execution/tracked_write_runner.rs`
- `packages/engine/src/sql/public/planner/backend/lowerer.rs`
- `packages/engine/src/state/checkpoint/create_checkpoint.rs`

## Core Model

### Undo unit

The undo unit should be **one committed head advancement in a version lane**.

That means:

- one normal tracked write that produced one commit is one undo unit
- one merged multi-statement tracked transaction that produced one commit is also one undo unit
- undo does not operate at the raw row-write granularity unless that already corresponds to one commit

This matches the engine's actual write boundary and avoids inventing a second notion of atomicity.

### Scope

V1 should support:

- tracked commits in one concrete version lane
- default lane: active version
- explicit override: specific version id

V1 should not try to solve:

- undo of untracked rows
- global-admin lane undo unless there is a concrete product need
- merge-commit undo with multiple parents

For merge commits, the safe first implementation is to reject them with a clear error until mainline semantics are designed.

## High-Level Algorithm

### Undo

Given a version lane:

1. Read the current head commit id from `lix_version_ref`.
2. Load the current head commit snapshot from `lix_commit`.
3. Read its `parent_commit_ids`.
4. Require exactly one parent in v1.
5. Load the head commit's member change ids from `change_ids`.
6. For each change id:
   - load the canonical change row from `lix_change`
   - determine the changed identity: `(entity_id, schema_key, file_id, version_id)`
   - load the previous visible committed row from the parent commit's visible state
7. Synthesize inverse `ProposedDomainChange`s:
   - if the row did not exist before the undone commit: inverse is tombstone
   - if the row existed before: inverse restores the parent-visible snapshot and metadata
8. Create a new tracked commit in the same version lane using `create_commit()`.
9. Persist redo metadata so that `redo()` can reapply the undone forward commit.

### Redo

Given a version lane:

1. Resolve the latest redo candidate for the lane.
2. Verify redo is still valid for the current head.
3. Reconstruct the original forward change batch.
4. Create a new tracked commit in the same version lane using that forward batch.
5. Advance or clear redo metadata accordingly.

Redo is therefore also append-only. It does not resurrect an old commit id as head. It reapplies the same semantic batch as a new commit.

## Required New Metadata

We need persistent redo bookkeeping. The engine currently has checkpoint metadata, but not redo lineage.

Add an internal table, for example:

`lix_internal_undo_redo`

Suggested shape:

- `version_id TEXT PRIMARY KEY`
- `undo_base_commit_id TEXT NOT NULL`
- `redo_stack_json TEXT NOT NULL`
- `updated_at TEXT NOT NULL`

Where `redo_stack_json` is an ordered JSON array of redo entries, newest-first or oldest-first, but fixed and documented.

Suggested redo entry shape:

```json
{
  "undone_commit_id": "c_123",
  "undone_parent_commit_id": "c_122",
  "forward_change_ids": ["chg_a", "chg_b"],
  "created_at": "2026-03-18T12:00:00Z"
}
```

V1 does not need to persist the full forward payload if it can deterministically reconstruct it from `forward_change_ids`. Storing ids is smaller and keeps the canonical source of truth in existing change tables.

## Invalidation Rule

Any successful tracked write in a version lane that is **not** a redo operation should clear that lane's redo stack.

Reason:

- redo only makes sense when replaying the exact branch of user intent that was just undone
- once new writes happen, the old redo sequence is no longer well-defined

This is standard editor behavior and also the simplest sync-safe invariant.

## Inverse Change Synthesis

This is the heart of the design.

For each canonical change in the undone commit:

- derive the row identity from the change row plus version lane
- load the visible committed row at the parent commit
- compare "after" state from the undone change with "before" state from parent visibility
- emit one inverse `ProposedDomainChange`

### Cases

#### Inserted row in undone commit

Before state: absent

After state: present

Inverse:

- `snapshot_content = None`
- `metadata = None`
- same `entity_id`, `schema_key`, `file_id`, `plugin_key`, `schema_version`, `version_id`

#### Updated row in undone commit

Before state: present

After state: present

Inverse:

- restore full previous `snapshot_content`
- restore previous `metadata`

#### Deleted row in undone commit

Before state: present

After state: absent

Inverse:

- restore full previous `snapshot_content`
- restore previous `metadata`

### Files

Files do not need special undo semantics at the API level because they already become tracked rows:

- `lix_file_descriptor`
- `lix_binary_blob_ref`

Undoing a file rename, metadata edit, content edit, or delete is just inverse synthesis over those schema rows.

This is a strong reason to build on canonical changes rather than inventing a filesystem-specific mechanism.

### Entity views and state views

Entity views are projections over state rows. Undo should not care whether the user originally wrote through:

- `lix_file`
- `lix_state`
- `lix_key_value`
- another entity view

By the time undo runs, the only relevant input is the canonical change batch in the commit being undone.

## How To Resolve Prior Visible State

We should not reconstruct parent-visible rows by replaying arbitrary history in a bespoke undo module. The engine already has a first-class lookup path:

- `load_exact_committed_state_row_from_live_state_with_executor`

What is missing is the ability to resolve "exact committed state row as visible from commit X", not only from the current live head of a version.

### Recommended addition

Extend `state_source.rs` with a helper like:

- `load_exact_committed_state_row_from_commit(...)`

Inputs:

- `root_commit_id` or `head_commit_id`
- `entity_id`
- `schema_key`
- `version_id`
- exact filters like `file_id`, `plugin_key`, `schema_version`

Implementation options:

- preferred: query canonical commit ancestry from the requested commit and select the first matching change
- later optimization: add a reusable authoritative projection path if this becomes hot

This should be implemented once in the commit/state layer, not inline inside `undo.rs`.

## Recommended Module Layout

Because the chosen primitive is now broader than simple version switching, a dedicated module is justified.

Recommended structure:

- `packages/engine/src/undo_redo/mod.rs`
- `packages/engine/src/undo_redo/undo.rs`
- `packages/engine/src/undo_redo/redo.rs`
- `packages/engine/src/undo_redo/types.rs`
- `packages/engine/src/undo_redo/store.rs`

Responsibilities:

- `types.rs`
  - request/result types
  - redo entry types
- `store.rs`
  - load/save/clear redo metadata
- `undo.rs`
  - undo orchestration
  - inverse change synthesis
- `redo.rs`
  - redo orchestration
  - forward replay reconstruction

This is now preferable to `src/version/` because the implementation is no longer just pointer movement. It is a history operation that creates new commits and owns redo persistence.

## Engine API

Add to `Engine`:

- `undo(options: UndoOptions) -> Result<UndoResult, LixError>`
- `redo(options: RedoOptions) -> Result<RedoResult, LixError>`

Add to `Lix`:

- `undo()`
- `redo()`
- optional overloads later with explicit `version_id` and `steps`

Suggested v1 request types:

```rust
pub struct UndoOptions {
    pub version_id: Option<String>,
}

pub struct RedoOptions {
    pub version_id: Option<String>,
}
```

Suggested results:

```rust
pub struct UndoResult {
    pub version_id: String,
    pub undone_commit_id: String,
    pub inverse_commit_id: String,
}

pub struct RedoResult {
    pub version_id: String,
    pub redone_commit_id: String,
    pub replay_commit_id: String,
}
```

`steps: usize` can be added later, but v1 should start with one-step semantics.

## CLI

Add top-level commands:

- `lix undo`
- `lix redo`

Suggested flags:

- `--version <id>` optional
- `--json` optional if CLI already adopts machine-readable output patterns later

Behavior:

- default lane is active version
- print concise result containing version id and created commit id
- surface clear errors for:
  - nothing to undo
  - nothing to redo
  - merge commit undo unsupported

## Transaction Semantics

Undo/redo must run in a single engine transaction.

Each operation should:

1. resolve lane and current head
2. load canonical history inputs
3. build new `ProposedDomainChange`s
4. call `create_commit()`
5. update redo metadata
6. commit once

This keeps the history mutation atomic and keeps observer notifications coherent.

## Observation And Dependency Semantics

Because undo/redo will create new tracked commits, most observation behavior should already work through existing state commit streams.

Still, verify these explicitly:

- active-version reads update after undo/redo
- `lix_working_changes` updates after undo/redo
- file views update after undo/redo
- history views show the new inverse/replay commits

Redo metadata itself does not need public observation in v1.

## Interaction With Checkpoints

Checkpoints should remain independent from undo/redo.

Current semantics:

- `lix_internal_last_checkpoint` marks the baseline for `lix_working_changes`

Recommended behavior:

- undo/redo does not move checkpoints
- undoing a commit that is after the checkpoint should reduce working changes naturally
- redoing it should reintroduce working changes naturally

This is a good fit with the existing `head minus checkpoint` definition.

## Sync Semantics

This design is sync-friendly because:

- undo and redo are ordinary append-only commits
- history remains monotonic
- peers can replicate inverse/replay commits using existing change transport
- there is no need for force-push or ref-rewrite semantics

If two peers both undo concurrently, normal commit graph divergence rules apply. That is acceptable and already aligned with the commit model.

## Open Design Choices

### 1. Whether to expose undo intent in commit metadata

Recommended: yes, later.

For v1, we can ship without it, but long-term it is useful to annotate undo/redo commits with metadata such as:

```json
{
  "lix_undo_redo": {
    "kind": "undo",
    "target_commit_id": "c_123"
  }
}
```

This helps UI, history inspection, and conflict diagnostics.

Possible storage:

- change metadata on synthetic changes
- commit snapshot extension
- dedicated internal table keyed by produced commit id

V1 can defer this if it slows the first implementation.

### 2. Whether redo should store change ids or full payloads

Recommended: store change ids only in v1.

Reasons:

- canonical truth already exists in `lix_change`
- payload duplication is unnecessary
- redo reconstruction can reload exact forward rows by change id

### 3. Whether undo should cross checkpoints

Recommended: yes.

Checkpoint is a baseline marker, not an undo fence. If product semantics later want "revert to checkpoint", that is a separate feature.

## Implementation Plan

### Phase 1: Core engine scaffolding

1. Add `undo_redo` module and export types from `lib.rs`.
2. Add internal redo metadata table in init/bootstrap.
3. Add store helpers to load/save/clear redo stacks.

### Phase 2: Commit ancestry lookup helpers

1. Add helper to load commit snapshot by commit id.
2. Add helper to load change ids for a commit.
3. Add helper to resolve parent-visible exact committed row from an arbitrary commit id.

### Phase 3: Undo

1. Resolve active/default version.
2. Load current head and parent.
3. Reject unsupported cases.
4. Synthesize inverse `ProposedDomainChange`s.
5. Call `create_commit()`.
6. Push redo entry.

### Phase 4: Redo

1. Load redo entry for the version.
2. Verify current head still matches the expected base.
3. Reconstruct forward `ProposedDomainChange`s from stored change ids.
4. Call `create_commit()`.
5. Pop or advance redo stack.

### Phase 5: Redo invalidation

1. Clear redo stack on ordinary tracked writes in that lane.
2. Do not clear when the write is itself a redo.

This probably belongs in the tracked-write path near commit application rather than in random public surfaces.

### Phase 6: Public APIs and CLI

1. Add `Engine::undo` / `Engine::redo`.
2. Add `Lix::undo` / `Lix::redo`.
3. Add CLI commands.

## Test Matrix

### Engine behavior

- undo of inserted entity row removes it
- undo of updated entity row restores previous snapshot
- undo of deleted entity row restores previous snapshot
- redo reapplies the undone batch
- ordinary write after undo clears redo
- undo on empty history fails clearly
- redo on empty redo stack fails clearly
- undo on merge commit fails clearly in v1

### Filesystem

- undo file create removes file
- undo file content update restores previous blob ref
- undo file rename restores previous descriptor
- undo file delete restores descriptor and blob ref
- redo re-applies each of the above

### Checkpoint interaction

- working changes count updates after undo
- working changes count updates after redo
- checkpoint pointer does not move during undo/redo

### Views and history

- `lix_state` reflects undo/redo
- entity views reflect undo/redo
- `lix_file` reflects undo/redo
- `lix_change` contains inverse/replay changes
- `lix_state_history` and file history include the new undo/redo commits

### Transactions and merge boundaries

- multiple tracked statements merged into one commit undo as one unit
- undo/redo with open public SQL transaction is rejected consistently if needed

### Observation

- observe on active entity view emits after undo
- observe emits after redo

## Recommended First Files To Change

- `packages/engine/src/init/mod.rs`
- `packages/engine/src/lib.rs`
- `packages/engine/src/api.rs`
- `packages/engine/src/lix.rs`
- `packages/engine/src/undo_redo/mod.rs`
- `packages/engine/src/undo_redo/types.rs`
- `packages/engine/src/undo_redo/store.rs`
- `packages/engine/src/undo_redo/undo.rs`
- `packages/engine/src/undo_redo/redo.rs`
- `packages/engine/src/state/commit/state_source.rs`
- `packages/cli/src/cli/root.rs`
- `packages/cli/src/commands/mod.rs`
- new CLI command files under `packages/cli/src/commands/`

## Final Recommendation

Build undo/redo as a **canonical inverse-commit system**.

Do not implement it as version-head rewrites by default.

The engine is already organized around:

- canonical domain changes
- commits and commit edges
- exact committed state lookup
- version-lane heads

Undo/redo should stay inside that model. That gives us one coherent history architecture for files, state, entity views, future sync, and observability.

## Progress log

- 2026-03-18 15:58:35 PDT: Created the initial architecture plan. Chose sync-safe inverse commits over version-head rewrites, defined redo persistence, prior-state lookup requirements, API and CLI shape, and the initial test matrix.
- 2026-03-18 16:01:00 PDT: Refined the repeated-undo design. A plain persistent redo stack is not enough for append-only inverse commits because repeated undo would otherwise target the latest inverse commit. Implementation will instead record synthetic undo/redo operations per produced commit and reconstruct semantic undo/redo stacks from version lineage plus that operation metadata.
- 2026-03-18 16:08:07 PDT: Landed the first implementation slice. Added the internal undo/redo operation table, engine `undo_redo` module scaffolding, commit ancestry helpers, public Rust API methods, rs-sdk re-exports, wasm-bindgen methods and TS types, JS SDK wrappers, and top-level CLI `lix undo` / `lix redo` command wiring.
- 2026-03-18 16:09:55 PDT: Ran the first compile pass on the implementation slice. The initial failures are integration issues around commit state-source visibility and helper promotion from test-only code; fixing those before moving on to semantic validation and tests.
- 2026-03-18 16:12:11 PDT: Generalized commit state-source helpers away from hard `dyn QueryExecutor` object signatures so undo/redo can use an in-flight transaction directly. Also removed the remaining `tx.engine` lifetime conflict in the transaction closures.
- 2026-03-18 16:17:20 PDT: Added end-to-end simulation coverage for undo/redo across inserts, updates, deletes, files, redo invalidation after new writes, version-targeted undo, and state commit stream emission. Also added CLI parse coverage for `lix undo --version` and `lix redo`.
- 2026-03-18 16:19:25 PDT: First runtime test pass exposed an architectural mismatch: undo/redo was resolving versions through public views while running on the raw backend transaction. Switched version resolution to transaction-local engine state plus internal live-row lookups, matching the `switch_version` approach.
- 2026-03-18 16:22:15 PDT: Runtime suite is green after one more lineage fix: commit ancestry reconstruction now falls back to untracked bootstrap commit rows when walking back to seeded roots. This unblocked undo/redo across sqlite, postgres, and materialization simulations.
- 2026-03-18 16:52:34 PDT: Fixed the undo-redo-undo idempotency collision. Synthetic operation commits now key idempotency by `(version_id, target_commit_id, current_head_commit_id)` instead of just `(version_id, target_commit_id)`, and added a regression test that exercises `undo -> redo -> undo`.
- 2026-03-18 16:55:03 PDT: Protected the bootstrap/root commit from undo. Semantic undo stack reconstruction now treats parentless commits as a hard boundary, and added regressions for `undo` on a fresh project plus a second undo after the last user commit has already been undone.
- 2026-03-18 16:56:29 PDT: Replaced generic empty-stack failures with explicit programmatic error codes: `LIX_ERROR_NOTHING_TO_UNDO` and `LIX_ERROR_NOTHING_TO_REDO`. Updated regression tests to assert on `error.code` in addition to the human-readable description.
- 2026-03-18 16:58:35 PDT: Clarified `--version` / `versionId` semantics in the CLI, engine option types, and JS SDK typings. The value is the actual `lix_version.id` / active `version_id`, not the `lix_active_version.id` row key.
- 2026-03-18 17:20:31 PDT: Ran `cargo fmt --all`, kept the resulting workspace formatting changes scoped to the undo/redo rollout plus one adjacent line-wrap in transaction execution code, and prepared the implementation for commit. Unrelated `plan_5.md` / `plan_6.md` remain outside the commit.
