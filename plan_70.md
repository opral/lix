# Plan 70: High-Impact Canonical Identity Hardening

## Goal

Make invalid canonical identity impossible to represent in the engine core.

Canonical identity fields:

- `entity_id`
- `file_id`
- `version_id`
- `schema_key`
- `schema_version`
- `plugin_key`

No backward compatibility is required. This should be implemented as one large clean-cut change, not a staged migration with bridge code.

## Why

The empty-string undo bug happened because the engine accepted invalid identity on write, while later readers treated empty identity as corrupt. The highest-impact fix is to enforce identity validity structurally in Rust, not with more scattered checks.

## Scope

Only two high-impact hardening items:

1. typed canonical identity in Rust
2. centralized identity derivation

## 1. Replace raw identity strings with validated types

Introduce validated wrappers for canonical identity:

- `EntityId`
- `FileId`
- `VersionId`
- `SchemaKeyValue`
- `SchemaVersionValue`
- `PluginKeyValue`

Requirements:

- constructor rejects empty string
- cheap wrapper around `String`
- used by commit/state/materialization core types in the same change

Priority files:

- `packages/engine/src/types.rs` or a dedicated identity module
- `packages/engine/src/state/commit/types.rs`
- `packages/engine/src/state/commit/create_commit.rs`
- `packages/engine/src/state/commit/generate_commit.rs`
- `packages/engine/src/state/materialization/types.rs`

Rule:

- raw strings are allowed only at external boundaries
- internal engine state should use validated identity types
- do not keep dual typed/untyped core representations around

## 2. Centralize PK-to-entity-id derivation

There should be one shared helper for deriving canonical `entity_id` from primary-key fields.

That helper must:

- reject `null`
- reject `""`
- produce the canonical encoded identity

Priority files:

- `packages/engine/src/state/validation.rs`
- `packages/engine/src/sql/public/planner/semantics/state_assignments.rs`

This removes drift between public planning and lower-level validation.

## Implementation Shape

Do this in one go:

1. add the identity newtypes
2. migrate commit/state/materialization core structs to the new types
3. replace duplicated PK-to-entity-id logic with one shared helper
4. remove old stringly-typed core paths rather than preserving compatibility shims

The point is a clean new invariant boundary, not an incremental compatibility layer.

## Testing

Add or expand tests for the final shape only:

- direct internal commit construction with empty identity fails
- public SQL insert with empty PK component fails
- undo/history regressions remain green
