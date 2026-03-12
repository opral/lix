# Public Surface Registry Effects Plan

## Objective

Replace statement-name heuristics for public-surface registry refresh with semantic write effects.

The immediate trigger is the regression in:

- `transaction_path_sql2_stored_schema_write_updates_bootstrap_for_followup_dynamic_surface_use_sqlite`
- `transaction_path_sql2_stored_schema_write_updates_bootstrap_for_followup_dynamic_surface_use_postgres`

The real goal is architectural:

- registry refresh should be driven by what a write **actually changed**
- transaction-local and post-commit registry state should use the same source of truth
- dynamic surface visibility must not depend on brittle SQL text pattern matching

## Core Principle

`Public surface registry invalidation must be derived from resolved semantic writes, not raw SQL syntax.`

That means:

- inserting a stored schema through `lix_state_by_version` must count as a registry mutation
- deleting or updating stored schema rows through public surfaces must count too
- the engine must not infer registry refresh from only the top-level target table name

## Why This Plan Exists

The current detector in [packages/engine/src/engine.rs](/Users/samuel/git-repos/lix/packages/engine/src/engine.rs) uses `should_refresh_public_surface_registry_for_statements()` and `object_name_mutates_public_surface_registry(...)`.

That logic only looks at the syntactic write target name:

- `lix_stored_schema`
- `lix_stored_schema_by_version`
- `lix_internal_state_vtable`
- `lix_internal_stored_schema_bootstrap`

This fails for semantically equivalent writes such as:

```sql
INSERT INTO lix_state_by_version (..., schema_key = 'lix_stored_schema', ...)
```

That statement mutates the public surface registry in reality, but the heuristic misses it because the table name is `lix_state_by_version`.

The observed failure mode is:

1. transaction-local registry is updated correctly
2. commit does not refresh the engine’s cached global registry
3. post-commit reads are misclassified as plain backend SQL
4. error normalization reboots a fresh registry from the backend, which makes the error message look contradictory

## Non-Goals

This plan does not optimize for backward compatibility with the current detector.

It does not try to preserve:

- `should_refresh_public_surface_registry_for_statements()` as the authoritative trigger
- statement-name heuristics as a fallback source of truth
- mixed semantics where local transaction registry uses one source and global engine registry uses another

## First-Principles Design

There should be one semantic concept:

```rust
enum PublicSurfaceRegistryEffect {
    Unchanged,
    Dirty,
}
```

Or, if we want more precision:

```rust
enum PublicSurfaceRegistryEffect {
    Unchanged,
    ReplaceDynamicSchema { schema_key: String },
    RemoveDynamicSchema { schema_key: String },
    Multiple,
}
```

The important part is not the exact enum shape. The important part is:

- it is derived from `PreparedPublicWrite` / `ResolvedWritePlan`
- it is computed after write analysis and resolution
- it is the only source used for registry refresh decisions

## Target Architecture

## 1. Derive Registry Effects From Resolved Writes

Introduce a helper in the public write/runtime layer, near where stored-schema writes are already interpreted today.

Suggested location:

- `packages/engine/src/sql/public/runtime/mod.rs`
- or a small helper module next to public write execution

Suggested API:

```rust
fn public_surface_registry_effect(
    prepared: &PreparedPublicWrite,
) -> Result<PublicSurfaceRegistryEffect, LixError>
```

This should inspect resolved intended post-state and tombstones, not raw SQL.

For stored schema changes:

- insert/update live `lix_stored_schema` row in global scope -> registry dirty
- tombstone `lix_stored_schema` row in global scope -> registry dirty
- non-global stored schema changes -> no global registry effect

## 2. Reuse The Same Semantic Logic For Local Overlay

The transaction-local registry update in:

- [packages/engine/src/sql/execution/transaction_exec.rs](/Users/samuel/git-repos/lix/packages/engine/src/sql/execution/transaction_exec.rs)

already interprets stored-schema writes semantically.

That logic should be centralized so both:

- transaction-local registry overlay
- post-commit global registry refresh decision

use the same semantic function or helper family.

The engine should not have:

- one path that reads resolved write rows
- another path that scans SQL text

## 3. Replace Commit-Time Heuristic Flags

Remove `public_surface_registry_refresh_pending` as something driven by raw parsed statements.

Instead:

- `EngineTransaction`
- public SQL session transactions
- statement-script transaction execution

should accumulate semantic registry effects while executing statements.

Suggested shape:

```rust
struct PendingRegistryEffects {
    dirty: bool,
}
```

or the more precise enum variant list if desired.

Each statement execution updates this from `PreparedPublicWrite`, not from statement text.

## 4. Refresh The Global Registry On Commit From Semantic Effects

On commit:

- if semantic registry effects are `Unchanged`, do nothing
- otherwise call `engine.refresh_public_surface_registry().await?`

This applies to:

- `EngineTransaction::commit`
- public SQL session `COMMIT`

Both transaction entrypoints must use the same rule.

## 5. Remove Statement-Name Heuristic

Delete or demote:

- `should_refresh_public_surface_registry_for_statements()`
- `statement_mutates_public_surface_registry(...)`
- `object_name_mutates_public_surface_registry(...)`

If any lightweight pre-commit shortcut remains, it must be explicitly non-authoritative.

The semantic effect must own correctness.

## 6. Keep Classification Using the Cached Registry

The cached engine registry is still the right source for early dispatch in `Engine::execute`.

What changes is how that cache is refreshed:

- not from SQL text
- from semantic registry effects

That preserves fast iteration speed and avoids drift:

- one registry cache
- one refresh source of truth
- one semantic interpretation path

## Implementation Phases

## Phase 1: Introduce Semantic Registry Effect Helper

Add:

- `PublicSurfaceRegistryEffect`
- helper to derive it from `PreparedPublicWrite`

Validation:

- unit tests over stored schema insert/update/tombstone resolved writes

## Phase 2: Centralize Registry Overlay Logic

Refactor the current transaction-local overlay helper so it shares logic with Phase 1.

Validation:

- transaction-local registry still resolves new dynamic surfaces before commit

## Phase 3: Replace Pending Refresh Flags

Change transaction state from:

- `public_surface_registry_refresh_pending: bool`

to:

- semantic registry effect accumulator

Apply to:

- `packages/engine/src/transaction.rs`
- `packages/engine/src/sql/execution/transaction_session.rs`
- `packages/engine/src/sql/execution/statement_scripts.rs`

Validation:

- no remaining correctness path depends on `should_refresh_public_surface_registry_for_statements()`

## Phase 4: Refresh Global Cache From Semantic Effects

Update both commit paths to refresh the engine registry only from accumulated semantic effects.

Validation:

- post-commit read of a newly created dynamic surface succeeds
- same for tombstoned dynamic surface removal

## Phase 5: Remove Heuristic Detector

Delete or reduce:

- `should_refresh_public_surface_registry_for_statements()`
- `statement_mutates_public_surface_registry(...)`
- `object_name_mutates_public_surface_registry(...)`

Validation:

- grep confirms these names are gone or no longer used in correctness paths

## Phase 6: Regression Coverage

Add or keep exact regressions for:

- stored schema insert through `lix_state_by_version` refreshes post-commit registry
- stored schema tombstone through a public write removes dynamic surface after commit
- both SQLite and Postgres
- both transaction entrypoints:
  - `Engine::transaction(...)`
  - public SQL `BEGIN` / `COMMIT` session path

## Validation Commands

Primary focused checks:

```bash
cargo test -p lix_engine --test transaction_execution transaction_path_sql2_stored_schema_write_updates_bootstrap_for_followup_dynamic_surface_use_sqlite -- --exact
cargo test -p lix_engine --test transaction_execution transaction_path_sql2_stored_schema_write_updates_bootstrap_for_followup_dynamic_surface_use_postgres -- --exact
```

Then broader follow-up:

```bash
cargo test -p lix_engine sqlite --no-fail-fast
```

And if the Postgres harness is available/stable in the environment:

```bash
cargo test -p lix_engine postgres --no-fail-fast
```

## Success Criteria

This plan is complete when:

- dynamic surface visibility after commit works for semantically equivalent stored-schema writes
- both transaction entrypoints behave the same
- the engine cache refresh path is semantic, not syntactic
- no correctness path relies on raw SQL target-name heuristics for public surface registry refresh

## Progress Log

- 2026-03-12: Created plan for replacing statement-name public-surface registry refresh heuristics with semantic write effects derived from resolved public writes.
- 2026-03-12: Implemented semantic registry dirtiness through execution preparation instead of raw statement-name heuristics. Public writes now derive registry mutations from resolved write rows, internal stored-schema writes mark the registry dirty through prepared execution state, and transaction-local internal stored-schema writes rebuild the local registry from the transaction backend instead of relying on SQL target-name checks.
- 2026-03-12: Added regression coverage for both transaction entrypoints and both semantic stored-schema directions: callback transaction insert, public SQL `BEGIN`/`COMMIT` insert, callback transaction tombstone, and public SQL `BEGIN`/`COMMIT` tombstone.
- 2026-03-12: Validation passed with `cargo test -p lix_engine --test transaction_execution stored_schema --no-fail-fast` and `cargo test -p lix_engine sqlite --no-fail-fast`.
