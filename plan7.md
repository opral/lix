# Plan 7: Checkpoint APIs Must Read Canonical State

## Goal

`create_checkpoint()` and related engine-owned checkpoint logic must depend only on canonical internal state.

They must not depend on:

- `lix_label`
- `lix_entity_label`
- any other public surface
- any materialized read model
- simulation harness rematerialization timing

If public/materialized state is stale, missing, or temporarily rebuilding, checkpoint creation must still behave correctly.

## Non-Goals

This plan does not preserve the current dependency direction where engine APIs read from public views.

This plan does not optimize for minimal patch size.

This plan does not treat materialized public tables as a valid source of truth for engine invariants.

## Root Principle

There is one sound dependency direction:

1. canonical internal state is the source of truth
2. engine domain APIs read from canonical internal state
3. public/materialized surfaces are derived from canonical internal state

Never the reverse.

If a write-domain API reads from a cache or read model, the architecture is wrong.

## Root Problem

The checkpoint flow currently discovers the checkpoint label through query paths that can depend on public or materialized state.

That is unsound because:

- materialization is a cache
- rematerialization can be deferred
- public tables can be intentionally stale in tests and in future runtime modes
- engine invariants should not depend on view freshness

The materialization simulation exposed the bug correctly. The harness is not the problem. The API dependency is.

## Correct Architecture

The checkpoint label is a system-managed singleton.

That means its identity should be a reserved engine constant, not discovered by querying public labels by name.

The clean architecture is:

1. define a reserved checkpoint label ID in engine code
2. bootstrap the checkpoint label into canonical internal state with that exact ID
3. have checkpoint logic use that ID directly
4. derive `lix_label` and `lix_entity_label` from canonical state for public reads

Public surfaces may expose the checkpoint label.
They may not define it for engine logic.

## Preferred Design

Introduce a reserved, namespaced system ID for the checkpoint label.

Example:

```rust
const CHECKPOINT_LABEL_ID: &str = "lix_label_checkpoint";
```

Properties of this ID:

- engine-owned
- human-readable in debugging
- clearly namespaced to avoid collisions
- stable across repos and rebuilds
- never generated dynamically

The rule is:

- system-managed singleton identities should be constants when their identity is part of the engine model
- canonical state should contain rows that use those constants
- engine APIs should reference those constants directly

## Why This Is Better

This removes all of the bad runtime behaviors:

- no dependency on `lix_label` materialization
- no dependency on public query rewriting
- no dependency on label-name filtering in public SQL
- no dependency on simulation harness rematerialization order
- no need to search for the checkpoint label at runtime
- no need for an extra singleton lookup layer

It also makes the invariant explicit:

- checkpoint label existence is a bootstrap invariant
- checkpoint label identity is stable
- checkpoint label identity is model-defined, not data-discovered
- checkpoint creation consumes that invariant directly

## Concrete Refactor

### 1. Define a Reserved System ID

Add a constant for the checkpoint label ID.

Requirements:

- namespaced, for example `lix_label_checkpoint`
- reserved for engine use
- never generated dynamically
- documented as part of the engine model

### 2. Seed Checkpoint Label Once

During initialization:

- create or upsert the checkpoint label in canonical internal state using the reserved ID
- create the bootstrap commit label edge in canonical internal state

The bootstrap step must be idempotent.

### 3. Change Checkpoint Logic to Read Only Canonical State

`create_checkpoint()` should:

- use `CHECKPOINT_LABEL_ID` directly
- never query `lix_label`
- never query `lix_entity_label`
- never depend on public read rewriting

### 4. Keep Public Surfaces Fully Derived

`lix_label` and `lix_entity_label` remain public read models.

They should be built from canonical state and may lag temporarily.

That is acceptable because engine logic no longer depends on them.

### 5. Apply the Same Rule to Other Engine-Owned Singletons

Use reserved, namespaced constants for other system-managed identities when their identity is part of the engine model.

Examples:

- bootstrap account
- special version descriptors if they are engine-owned
- future system labels or refs

## Simulation Rule

The rematerialization simulation should remain strict.

It should continue modeling:

- stale public state
- deferred rebuilds
- cache invalidation gaps

If an engine API breaks under that simulation, that API is reading the wrong layer.

The fix is not to force rematerialization before every API call.
The fix is to make engine APIs read canonical state.

## Migration Strategy

1. define `CHECKPOINT_LABEL_ID`
2. seed the checkpoint label row with that exact ID during init
3. update checkpoint code to use the constant directly
4. remove public-surface dependency from checkpoint code
5. add tests that explicitly prove checkpoint creation works when public state is stale

## Required Tests

Add tests that assert:

- `create_checkpoint()` succeeds immediately after init without rematerializing public state
- `create_checkpoint()` succeeds when `lix_label` is stale or absent but canonical state is valid
- checkpoint creation does not perform runtime checkpoint-label discovery by name
- checkpoint label public views eventually reflect canonical state after rebuild
- public view failure does not block checkpoint-domain logic

## Final Standard

The engine must satisfy this rule:

> No engine-owned write API may depend on public or materialized state for correctness.

For checkpoints specifically:

> `create_checkpoint()` must be correct even if `lix_label` and `lix_entity_label` are empty, stale, or rebuilding.
