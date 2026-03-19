# Plan 70: Typed Canonical Identity Hardening

## Goal

Make invalid canonical identity impossible or very hard to represent inside the engine.

This hardening targets the engine-level invariant that canonical identity fields are never empty:

- `entity_id`
- `file_id`
- `version_id`
- `schema_key`
- `schema_version`
- `plugin_key`

The desired end state is:

1. external inputs may arrive as raw strings
2. boundary code validates and converts them into typed identity values
3. internal commit/state/materialization code only works with validated identity
4. storage also rejects empty canonical identity as a backstop

No backward compatibility is required.

## Why

The empty-string undo corruption bug showed that the engine currently relies on scattered validation instead of one structural invariant. Some readers already treat empty text identity as corrupt, while some writers historically allowed it. That asymmetry lets invalid history persist and fail later in unrelated code such as undo/history reconstruction.

The first-principles fix is to enforce identity validity at the type boundary and at the storage boundary.

## Scope

High-impact hardening only:

1. typed canonical identity in Rust
2. centralized identity derivation
3. engine boundary validation for plugin/materialization outputs
4. DB `CHECK` constraints for canonical identity columns
5. fail-fast invariant scan for existing corrupt history/state

## Design

### 1. Introduce typed canonical identity wrappers

Add validated newtypes for canonical identity:

- `EntityId`
- `FileId`
- `VersionId`
- `SchemaKeyValue` or similar name that does not conflict with existing schema types
- `SchemaVersionValue`
- `PluginKeyValue`

Requirements:

- constructor rejects empty string
- cheap wrapper around `String`
- `as_str()` accessor
- `Display`, `Clone`, `Eq`, `Ord`, `Hash`
- explicit conversion at trusted boundaries only

Likely location:

- `packages/engine/src/types.rs` or a dedicated `packages/engine/src/identity.rs`

### 2. Move commit/state APIs onto typed identity

Priority migration targets:

- `packages/engine/src/state/commit/types.rs`
- `packages/engine/src/state/commit/create_commit.rs`
- `packages/engine/src/state/commit/generate_commit.rs`
- `packages/engine/src/state/materialization/types.rs`
- `packages/engine/src/state/stream.rs`

Rule:

- raw `String` identity is allowed at SQL/plugin input boundaries
- canonical engine structs should use validated identity types

This removes the class of bugs where internal callers construct invalid domain changes directly.

### 3. Centralize PK-to-entity-id derivation

Today entity-id derivation logic exists in multiple places. Replace that with one shared helper that:

- rejects null primary-key components
- rejects empty string primary-key components
- produces the canonical encoded `entity_id`

Migration targets:

- `packages/engine/src/state/validation.rs`
- `packages/engine/src/sql/public/planner/semantics/state_assignments.rs`
- any plugin/materialization path deriving entity ids from structured snapshots

This avoids drift between public planning and lower-level validation.

### 4. Validate plugin/materialization outputs before commit/state insertion

Any plugin-originated or materialization-originated entity change must be rejected if canonical identity is empty.

Priority targets:

- `packages/engine/src/plugin/runtime.rs`
- file materialization / detect-changes ingestion
- any path building `LiveStateWrite` or `DomainChangeInput`

If a producer needs a logical root identity, it must use a non-empty canonical id instead of `""`.

### 5. Add DB `CHECK` constraints

Strengthen internal and live-state tables so the database also rejects empty canonical identity.

Targets:

- `packages/engine/src/init/mod.rs`
- `packages/engine/src/schema/registry.rs`

Add `CHECK` constraints for canonical identity columns on:

- `lix_internal_change`
- generated tracked live tables
- generated untracked live tables
- any other table that stores canonical state identity

This is defense in depth against future regressions or non-Rust write paths.

### 6. Fail fast on existing corruption

Because no backward compatibility is needed, add an explicit invariant scan during init/rebuild/open that aborts if any canonical identity field is empty in committed state or live state.

Priority targets:

- init/open path
- live-state rebuild path

This converts latent corruption into an immediate hard error instead of letting undo/history fail later.

## Rollout Order

1. add identity newtypes and boundary constructors
2. migrate commit-state core structs to typed identity
3. centralize entity-id derivation
4. validate plugin/materialization outputs
5. add DB `CHECK` constraints
6. add corruption scan
7. run full engine simulations before merge

## Testing

Add or expand tests for:

- direct commit construction with empty canonical identity fails
- public SQL insert with empty PK component fails
- plugin/materialization output with empty identity fails
- init/rebuild fails when corrupt rows already exist
- undo/history/state queries remain green

Before merge, run the full engine simulation matrix, not only targeted tests.
