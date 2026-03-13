# Registered Schema Plan

## Goal

Rename the persisted schema-definition entity from `stored schema` to `registered schema` and remove the current naming collision where `register_schema(...)` means only "create backing tables".

The target model is:

- direct inserts into `lix_registered_schema` and `lix_registered_schema_by_version` remain valid
- `register_schema(schema)` becomes a helper, not the only registration path
- the helper should:
  - delegate to the same registration path as a direct insert into `lix_registered_schema`
  - persist it into the registered-schema entity with global semantics
  - ensure physical live-state storage exists for its `schema_key`
  - refresh dynamic public surfaces derived from that schema
- the current DDL-only helper becomes an internal primitive with a different name

No backward compatibility layer should remain.

## First-Principles Model

There are three distinct concerns and they should not share the same verb:

- schema registration:
  - making a schema definition part of the system's authoritative registered-schema set
- direct schema insertion:
  - writing to `lix_registered_schema` through normal entity-view semantics
- schema storage installation:
  - creating `lix_internal_live_v1_<schema_key>` and its indexes

After the refactor:

- direct SQL writes into `lix_registered_schema` remain a first-class path
- `register_schema(schema)` means "perform the standard registration helper flow"
- `ensure_schema_live_table(schema_key)` means table/index creation only

`register_schema(schema)` should reuse the same public insert contract as direct writes:

- it should behave like `INSERT INTO lix_registered_schema (value) ...`
- duplicate handling should not diverge from the direct SQL path
- re-registering a newer version of the same `schema_key` should reuse the same live table for that key
- the helper always writes with global semantics
- the persisted schema definition remains immutable and global

## Core Naming Decisions

Use these names consistently:

- `registered schema`: the persisted schema-definition entity
- `schema registration`: either direct insertion into the registered-schema entity or use of the registration helper
- `register schema helper`: the convenience path that writes a schema globally and ensures storage
- `schema storage`: internal live-table/index installation for a schema key

Rename:

- `lix_stored_schema`
  - to `lix_registered_schema`
- `lix_stored_schema_by_version`
  - to `lix_registered_schema_by_version`
- `lix_internal_stored_schema_bootstrap`
  - to `lix_internal_registered_schema_bootstrap`
- `lix_internal_live_v1_lix_stored_schema`
  - to `lix_internal_live_v1_lix_registered_schema`

Demote and rename the current DDL helper:

- `register_schema(backend, schema_key)`
  - to `ensure_schema_live_table(backend, schema_key)`
- `register_schema_sql(...)`
  - to `ensure_schema_live_table_sql(...)`
- `register_schema_sql_statements(...)`
  - to `ensure_schema_live_table_sql_statements(...)`

## Current Collision To Remove

Today `register_schema(...)` in `packages/engine/src/schema/registry.rs` creates:

- `lix_internal_live_v1_<schema_key>`
- standard indexes for that table

It does not persist a schema definition row.

At the same time, `lix_registered_schema` is the persisted schema-definition entity.

That split is the wording drift. The refactor should make the code read the same way the system behaves.

## Desired Registration Helper Flow

`register_schema(schema)` should perform these steps in one transactional path:

1. Route through the same write contract as `INSERT INTO lix_registered_schema (value) ...`.
2. Derive:
   - `schema_key`
   - `schema_version`
   - `entity_id = <schema_key>~<schema_version>`
3. Ensure schema storage exists for `schema_key`.
4. Insert the schema into `lix_registered_schema` at global scope.
5. Refresh or upsert dynamic entity surfaces derived from that schema.
6. Reuse the same duplicate behavior as the direct insert path.

Direct inserts should still work without using the helper:

- `INSERT INTO lix_registered_schema (value) ...`
- `INSERT INTO lix_registered_schema_by_version (value, lixcol_version_id) ...`

The helper is just the standard API for:

- "register this schema for me"
- global scope
- storage installation
- dynamic-surface refresh

Important invariant:

- physical live storage is keyed by `schema_key`
- registered schema rows are keyed by `schema_key + schema_version`

## Scope

This rename touches the active engine surface broadly.

Investigation found:

- about 43 files in `packages/engine`
- about 71 files in `packages/sdk`
- 4 ancillary files in:
  - `packages/js-benchmarks`
  - `packages/js-kysely`
  - `packages/website`
  - `skills/lix`

The engine work is the critical path because it defines the data model and runtime behavior.

## Engine Refactor Stages

## Stage 1: Split The API Boundary

Introduce two explicit operations:

- `register_schema(schema)`
- `ensure_schema_live_table(schema_key)`

Tasks:

- replace the current DDL-only meaning of `register_schema(...)`
- move physical table/index creation behind `ensure_schema_live_table(...)`
- keep direct writes into `lix_registered_schema` valid
- update all call sites that only have a `schema_key` to use `ensure_schema_live_table(...)`
- reserve `register_schema(schema)` for code that wants the helper behavior:
  - global registered-schema insert
  - storage installation
  - dynamic-surface refresh

Likely call-site classes:

- public reads that need a live table to exist
- write execution paths that pre-create schema storage before writes
- materialization and plugin paths that currently call the DDL helper directly

## Stage 2: Rename The Registered-Schema Entity

Rename the persisted schema-definition entity end-to-end:

- builtin schema key
- public relation names
- by-version relation names
- internal bootstrap table
- internal materialized live table
- all associated index names

This includes:

- builtin schema JSON file names
- schema constants
- relation binding tests
- lowerer/runtime SQL assertions
- initialization SQL
- seed SQL

## Stage 3: Rename Modules, Types, And Helpers

Rename symbols so the code no longer says `stored schema` internally.

Examples:

- `stored_schema.rs`
  - to `registered_schema.rs`
- `StoredSchemaRewrite`
  - to `RegisteredSchemaRewrite`
- `SqlStoredSchemaProvider`
  - to `SqlRegisteredSchemaProvider`
- `schema_from_stored_snapshot(...)`
  - to `schema_from_registered_snapshot(...)`
- `UpsertStoredSchemaSnapshot`
  - to `UpsertRegisteredSchemaSnapshot`

Error messages and invariant labels should also change:

- `stored schema snapshot_content invalid JSON`
  - to `registered schema snapshot_content invalid JSON`
- `stored_schema.definition_validation`
  - to `registered_schema.definition_validation`

## Stage 4: Implement Registration Helper

Add the new `register_schema(schema)` path and route real schema-registration flows through it.

That path should own:

- the same validation semantics as direct insert
- the same key/version extraction semantics as direct insert
- registered-schema persistence with global semantics
- storage installation
- dynamic-surface refresh

Potential implementation shape:

- `schema::registration::register_schema(...)`
  - high-level operation
- `schema::storage::ensure_schema_live_table(...)`
  - internal DDL primitive

The exact module split can vary, but the semantic split should be explicit in the code.

Important:

- this helper must not become the only supported way to add a schema
- direct inserts into `lix_registered_schema` should still behave correctly

## Stage 5: Rewire Existing Call Sites

Audit current uses of the DDL helper and classify them:

- callers that only need storage
  - switch to `ensure_schema_live_table(schema_key)`
- callers that truly register a schema definition
  - switch to `register_schema(schema)`
- direct SQL callers that already insert into the registered-schema entity
  - should continue to work without being funneled through the helper

Known current areas:

- SQL execution paths
- public read runtime
- materialization/apply
- filesystem pending writes
- plugin runtime/install

## Stage 6: Runtime And Catalog Rename Sweep

Update all engine subsystems that derive behavior from the registered-schema entity:

- public surface registry mutations
- dynamic entity-surface replacement
- schema provider / overlay provider
- internal vtable rewrite logic
- schema validation
- state bootstrap mirroring
- catalog binding tests
- unknown-schema diagnostics

The old term should disappear from:

- runtime code
- planner code
- validation code
- initialization code
- error messages

## Stage 7: Engine Test And Doc Sweep

Update all active engine tests and docs to the new term:

- relation names
- schema keys
- expected diagnostics
- internal table names
- helper function names

This should include:

- `packages/engine/tests/*`
- engine architecture docs

## Non-Engine Follow-Up

There is additional fallout outside `packages/engine`:

- `packages/sdk`
- `packages/js-benchmarks`
- `packages/js-kysely`
- `packages/website`
- `skills/lix`

Because `packages/sdk` has already been treated as stale in earlier work, it should be handled deliberately as a follow-up rather than accidentally dragged into the engine cut.

Recommended approach:

- finish the engine refactor first
- then decide whether non-engine packages should be renamed in one follow-up sweep or left stale temporarily

## Risks

### 1. Semantic Collision Persists

If the rename only changes `registered_schema` to `registered_schema` but leaves the current DDL helper named `register_schema`, the codebase will still have two meanings of "register schema".

That would preserve drift rather than remove it.

### 2. Partial String-Literal Rename

This subsystem currently relies on repeated hard-coded names in:

- SQL strings
- tests
- diagnostics
- runtime mutation routing
- init/seed SQL

A partial rename will leave silent old-term paths behind.

### 3. Public/Physical Name Mismatch

The public entity names and internal bootstrap/live table names are coupled in tests and runtime logic.

If only the public names change, runtime refresh and validation logic can still point at old physical storage.

### 4. Wrong API Routing

Some current callers only have a `schema_key`. They cannot call a new high-level `register_schema(schema)` because they do not possess a schema payload.

Those must be routed to `ensure_schema_live_table(schema_key)` instead.

## Invariants To Preserve

- registered schema rows are global
- registered schema rows are immutable
- `entity_id` remains derived from `schema_key~schema_version`
- live-state storage remains per `schema_key`
- registering schema version `2` for the same schema key does not create a second live table
- dynamic entity surfaces update immediately after registration
- direct inserts and helper-based registration converge on the same registered-schema semantics
- the helper always implies `global = true`

## Recommended Order

1. Rename the current DDL helper away from `register_schema`.
2. Introduce the high-level `register_schema(schema)` operation.
3. Rename `registered_schema` to `registered_schema` across the engine.
4. Rewire runtime/catalog/validation to the new names.
5. Run the engine test suite and fix stale diagnostics.
6. Decide separately whether to sweep stale non-engine packages.
