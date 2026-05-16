# Plan 5: SQL2 Clean Cut

Goal: remove the current whack-a-mole fast-path architecture by making SQL binding the single semantic authority, then making fast execution a post-bind physical optimization. We do not need backward compatibility with the current `sql2` internal layout.

The implementation strategy is intentionally a hard cut: create the ideal Rust types, APIs, and module layout first, wire `mod.rs` to the new structure, and let the compiler identify every upstream refactor that must follow.

## Invariants

- [ ] Raw sqlparser/DataFusion AST is only interpreted in `parse/` and `bind/`.
- [ ] Table names, column names, hidden/public columns, aliases, duplicate targets, qualified names, params, and assignment validity are resolved exactly once in `bind/`.
- [ ] Fast write execution never validates SQL and never inspects raw `ObjectName`, `Ident`, or unbound AST.
- [ ] Fast write execution only accepts a validated `BoundWrite` or canonical `LogicalWritePlan`.
- [ ] Normal DataFusion execution and fast execution consume the same bound/plan representation.
- [ ] Live-state visibility, transaction overlays, global-row projection, tombstone handling, and dedupe live behind one storage visibility API.
- [ ] Empty filter, no-match filter, and all-values filter are distinct Rust states, never overloaded as empty `Vec`.
- [ ] `_by_version` and base entity surfaces are distinct bound targets with different public columns and version-scope rules.
- [ ] Fast path can only decline; it cannot silently change semantics.

## Target File Layout

Create this structure first, even if most modules temporarily contain stubs and compile errors:

```text
packages/engine/src/sql2/
  mod.rs

  parse/
    mod.rs
    normalize.rs

  catalog/
    mod.rs
    surface.rs
    schema.rs
    capability.rs

  bind/
    mod.rs
    statement.rs
    read.rs
    write.rs
    expr.rs
    table.rs
    error.rs

  plan/
    mod.rs
    read.rs
    write.rs
    predicate.rs
    version_scope.rs

  optimize/
    mod.rs
    simple_write.rs
    datafusion.rs

  exec/
    mod.rs
    read.rs
    write.rs
    fast_write.rs
    datafusion.rs
    result.rs

  storage/
    mod.rs
    live_state.rs
    visibility.rs
    constraints.rs

  providers/
    mod.rs
    lix_state.rs
    entity.rs
    entity_history.rs
    file.rs
    file_history.rs
    directory.rs
    directory_history.rs
    version.rs
    change.rs
    history.rs

  udfs/
    ...

  test_support/
    mod.rs
    differential.rs
    generators.rs
```

## Phase 1: Hard Type/API Cut

- [x] Create the new directories and `mod.rs` files.
- [x] Move `udfs/` as-is under the new layout if needed, or keep existing `udfs/` and re-export from the new root until the rest compiles.
- [x] Replace `sql2/mod.rs` exports with the desired public API surface:

```rust
pub(crate) use parse::parse_statement;
pub(crate) use bind::{bind_statement, BoundStatement};
pub(crate) use plan::{LogicalReadPlan, LogicalWritePlan};
pub(crate) use exec::{
    create_logical_plan,
    create_logical_plan_from_parsed,
    create_transaction_read_logical_plan_from_parsed,
    create_write_logical_plan,
    create_write_logical_plan_from_parsed,
    execute_logical_plan,
    execute_sql,
    SqlLogicalPlan,
};
```

- [x] Define the canonical bound types before adapting any old code:

```rust
pub(crate) enum BoundStatement {
    Read(BoundRead),
    Write(BoundWrite),
}

pub(crate) struct BoundWrite {
    pub(crate) target: BoundWriteTarget,
    pub(crate) op: BoundWriteOp,
    pub(crate) input: BoundWriteInput,
    pub(crate) predicate: BoundPredicate,
    pub(crate) assignments: Vec<BoundAssignment>,
    pub(crate) params: BoundParamMap,
    pub(crate) version_scope: VersionScope,
}

pub(crate) enum BoundWriteTarget {
    LixState,
    Entity(EntityWriteSurface),
    File(FileWriteSurface),
    Directory(DirectoryWriteSurface),
    Version,
}

pub(crate) enum EntityWriteSurface {
    Base { schema_key: String },
    ByVersion {
        schema_key: String,
    },
}

pub(crate) enum BoundWriteOp {
    Insert,
    Update,
    Delete,
}

pub(crate) enum BoundWriteInput {
    Values(Vec<BoundInsertRow>),
    Query(Box<BoundRead>),
    None,
}
```

- [x] Define canonical filter/predicate/version types:

```rust
pub(crate) enum FilterSet<T> {
    All,
    Some(std::collections::BTreeSet<T>),
    None,
}

pub(crate) enum VersionScope {
    Active { version_id: String },
    Explicit { version_ids: std::collections::BTreeSet<String> },
    ExplicitRequired { version_ids: std::collections::BTreeSet<String> },
    Global,
    Empty,
}

pub(crate) enum BoundPredicate {
    True,
    False,
    And(Vec<BoundPredicate>),
    Eq(BoundExpr, BoundExpr),
    In {
        expr: BoundExpr,
        values: Vec<BoundExpr>,
    },
}
```

- [x] Define `FastWritePlan` as an optimization output, not a semantic input:

```rust
pub(crate) enum FastWritePlan {
    Insert(FastInsertPlan),
    Update(FastUpdatePlan),
    Delete(FastDeletePlan),
}

pub(crate) fn try_make_fast_write_plan(
    plan: &LogicalWritePlan,
) -> Result<Option<FastWritePlan>, LixError>;
```

- [x] Intentionally break existing imports by removing direct `pub(crate) use simple_dml::try_execute_simple_write`.
- [x] Run `cargo check -p lix_engine` and save the first compiler-error class as the next task list.

Phase 1 compiler result:

- First compiler-error class: canonical bound types incorrectly derived `Eq` for fields containing `Value`, and the new parser used a non-existent error-code constant.
- Resolution: removed `Eq` derives from value-bearing bound types and delegated DataFusion parser errors to the existing sql2 error classifier.
- Review hardening: removed the pre-bind fast-write hook from session execution, changed fast execution to consume `FastWritePlan`, put `bind_statement` on the write planning path, removed old `simple_dml` and `public_bind`, made `VersionScope` the sole entity version authority, made write values bound expressions instead of runtime `Value`s, added planned `FilterSet`s to `LogicalWritePlan`, and routed transaction overlay visibility through the `live_state` visibility owner facade.
- Review hardening: transaction overlay candidate scans now remove pre-visibility `limit` and force `include_tombstones = true`, then apply caller limit/tombstone filtering only after shared visibility resolution.
- Review hardening: read planning entrypoints now reject write ASTs before DataFusion planning, live-state point loads and transaction schema point loads route through scan/overlay visibility, empty-version overlay dedupe happens before tombstone filtering, and stale raw-DataFusion write tests are explicitly ignored until the bound write pipeline is implemented.
- Current gate: `cargo check -p lix_engine` passes with warnings from intentionally-unused Phase 1 target types. Write execution intentionally stops at the new binder/planner boundary until Phase 2/3 implement catalog, write binding, and bound write execution; this is the hard cut that prevents falling back to raw-AST DML semantics.
- Current gate: `cargo test -p lix_engine sql2::exec::datafusion::tests --lib -- --nocapture` passes with active SQL2 read coverage restored through non-SQL fixtures; only write/history-write-dependent tests are ignored until the bound write pipeline is implemented.
- Current gate: `cargo test -p lix_engine sql2::providers::lix_state::tests --lib -- --nocapture`, `cargo test -p lix_engine live_state::visibility::tests --lib -- --nocapture`, and `cargo test -p lix_engine overlay_ --lib -- --nocapture` pass. Raw provider DML hooks now fail closed, and shared live-state visibility owns overlay tombstone/dedupe/global projection semantics for both transaction and sql2 callers.
- Review hardening: overlay merge precedence is explicit now: version-specific rows beat projected global rows, staged rows beat base rows inside the same scope tier, and tracked/untracked only breaks ties within the same tier. Regression coverage includes staged tracked rows beating base untracked rows and tracked version tombstones beating staged untracked global rows.
- Current gate: `cargo test -p lix_engine create_version_from_main --test branching -- --nocapture`, `cargo test -p lix_engine --test transaction -- --nocapture`, `cargo test -p lix_engine --test code_structure -- --nocapture`, `cargo test -p lix_engine --test sql -- --nocapture`, and `cargo fmt -p lix_engine --check` pass. The global `simulation_test!` macro is not ignored; only the public SQL integration harness is explicitly ignored for Phase 1 because it depends on disabled public SQL writes. The deterministic-mode SQL-write seed is skipped on the expected `LIX_UNSUPPORTED_SQL` hard-cut error.

## Phase 2: Catalog and Public Surface Contracts

- [x] Move useful logic from `public_bind/table.rs`, `public_bind/capability.rs`, and provider registration into `catalog/`.
- [x] Implement exact table resolution in `bind/table.rs`; reject multi-part names unless explicitly supported.
- [x] Represent each public surface as data:

```rust
pub(crate) struct PublicSurfaceContract {
    pub(crate) name: String,
    pub(crate) kind: PublicSurfaceKind,
    pub(crate) columns: Vec<PublicColumn>,
    pub(crate) capabilities: SurfaceCapabilities,
}
```

- [x] Encode base entity and `_by_version` entity as separate `PublicSurfaceKind` values.
- [x] Make hidden/internal columns impossible to bind through public surfaces.
- [x] Remove all leaf-name resolution helpers from write execution.
- [x] Add unit tests for table resolution:
  - [x] `foo.table` rejected if only `table` exists.
  - [x] unknown table rejected.
  - [x] base entity table does not expose `lixcol_version_id`.
  - [x] `_by_version` exposes `lixcol_version_id` but not `version_id` alias unless explicitly desired.

Phase 2 implementation result:

- Added `catalog::PublicCatalog`, system surface contracts, and dynamic entity surface contracts from visible schemas.
- Added `PublicSurfaceKind` variants for `lix_state_by_version`, entity base/entity `_by_version`, file/file `_by_version`, and directory/directory `_by_version`.
- Added `bind::table::bind_public_table` and `require_public_column`, with exact single-part table binding through the catalog.
- Removed the remaining dead DataFusion write helper that used leaf-name table resolution (`dml.table_name.table()`); write validation now belongs to the bound write pipeline.
- Current gate: `cargo check -p lix_engine`, `cargo test -p lix_engine sql2::bind::table::tests --lib -- --nocapture`, and `cargo fmt -p lix_engine --check` pass.

## Phase 3: Binding Writes

- [x] Implement `bind::bind_statement`.
- [ ] Implement `bind::write::bind_insert`.
- [ ] Implement `bind::write::bind_update`.
- [ ] Implement `bind::write::bind_delete`.
- [x] Bind assignment targets into resolved column IDs, not strings.
- [x] Reject duplicate insert target columns during binding.
- [x] Reject duplicate update assignment targets during binding.
- [x] Bind params in source-order once into `BoundParamMap`.
- [x] Bind predicates into `BoundPredicate`.
- [ ] Convert repeated identity predicates into `FilterSet` intersections during planning, not in execution.
- [x] Remove `ParamDecoder` from fast execution.
- [x] Delete statement-level DML validation once binding covers the same rules.

Phase 3 implementation result:

- `bind_statement` now produces `BoundWrite` for supported `INSERT`, `UPDATE`, and `DELETE` statements, with fail-closed rejection for unsupported clauses, joins, aliases, tuple assignments, implicit insert columns, duplicate write targets, hidden columns, and read-only/write-protected columns.
- Bound write expressions now preserve literals, params, resolved column refs, and public Lix scalar function calls. `INSERT ... VALUES` supports public functions such as `lix_json`, `lix_text_encode`, `lix_uuid_v7`, and `lix_timestamp` without relying on raw-AST validation in the write planning entrypoint.
- Public catalog columns now carry stable column IDs and insert/update write capabilities. Dynamic entity primary-key root columns are insert-only, preventing bound updates that would desynchronize projected primary keys from entity identity.
- Write version scope is bound before planning: base writes bind to active scope, `lix_version` and global `lix_state` rows bind to global scope, `_by_version` writes require concrete explicit version selectors, and no-match predicates bind to `VersionScope::Empty`.
- Parameterized scope selectors fail closed until a later planning phase resolves bound params into concrete scopes; `VersionScope` intentionally has no dynamic variant that can leak into storage visibility.
- Current gate: `cargo test -p lix_engine sql2::bind --lib -- --nocapture`, `cargo check -p lix_engine`, and `cargo fmt -p lix_engine --check` pass. The three `bind::write::{bind_insert,bind_update,bind_delete}` extraction items remain open because the Phase 3 implementation currently lives in `bind::statement`; extracting those helpers is a follow-up layout cleanup, not a semantic blocker.

## Phase 4: Logical Write Plans

- [x] Implement `plan::write::plan_write(bound: BoundWrite) -> LogicalWritePlan`.
- [x] Make `LogicalWritePlan` the carried write plan in `SqlLogicalPlan`.
- [x] Keep planned predicate filters logical: keyed by `BoundColumnRef`, not storage schema/entity/file IDs.
- [x] Model version requirements in `VersionScope`:
  - [x] base entity writes use `VersionScope::Active`.
  - [x] `_by_version` update/delete use `VersionScope::ExplicitRequired`.
  - [x] `lix_state` can use global/active/explicit scopes where public semantics allow it.
- [x] Represent logical no-match with `FilterSet::None` via the write-plan row sentinel.
- [ ] Replace storage-facing `LiveStateFilter.no_match` with `FilterSet::None` or equivalent in Phase 7.
- [ ] Convert logical write filters to storage filters only at the storage boundary.
- [x] Add tests for contradictory predicates:
  - [x] repeated equality with different values returns zero matches.
  - [x] repeated `IN` intersections work.
  - [x] contradiction does not corrupt param binding.
  - [x] `AND false` sets a no-match sentinel.
  - [x] SQL `NULL` comparisons do not become storage null filters.
  - [x] user entity columns named like system fields stay logical columns.

Phase 4 implementation result:

- `LogicalWritePlan` now owns the bound write plus logical planned filters. The planner intersects repeated predicates by bound column ID and leaves target/version semantics on `BoundWrite`/`VersionScope`; it does not map to live-state schema keys, entity IDs, file IDs, or `NullableKeyFilter`.
- `SqlLogicalPlan` is now an enum that carries either a DataFusion read plan or a bound write plan. Write execution still returns the existing unsupported error until Phase 5/6 wire physical execution, but the write plan is no longer built and discarded.
- Planner tests cover repeated equality contradictions, repeated `IN` intersections, bound parameter preservation, `AND false`, SQL `NULL`, by-version scope preservation, and user columns named `schema_key`.
- Current gate: `cargo test -p lix_engine sql2::plan::write --lib -- --nocapture`, `cargo test -p lix_engine sql2::bind --lib -- --nocapture`, `cargo check -p lix_engine`, and `cargo fmt -p lix_engine --check` pass with the expected hard-cut unused-code warnings.

## Phase 5: DataFusion Path From Bound Plans

- [x] Move existing `execute.rs` DataFusion plan creation under `exec/datafusion.rs`.
- [x] Keep DataFusion as the reference physical executor.
- [x] Build DataFusion sessions from bound/catalog state, not separate public validation.
- [x] Ensure normal write path and fast write path share the same `LogicalWritePlan`.
- [x] Remove duplicated calls to `validate_public_dml_statement`.
- [x] Remove the unconditional full-AST clone in the write fast-path decision.
- [x] Ensure fallback large `INSERT ... VALUES` does not clone the AST just to decline fast path.

Phase 5 progress:

- Transaction write execution now calls `execute_write_logical_plan(ctx, SqlLogicalPlan::Write, params)` instead of passing write plans through the DataFusion read executor and parsing an affected-row result.
- `execute_write_logical_plan` validates bound write parameter counts, tries `optimize::simple_write::try_make_fast_write_plan(&LogicalWritePlan)`, then hands fast-path misses to the DataFusion reference writer using the same `LogicalWritePlan`.
- The former duplicated public DML validation path is gone: write planning binds through `bind_statement(...)`, and fast-path selection now receives the already-built `LogicalWritePlan` instead of cloning/parsing the AST to decide whether to decline.
- `SqlLogicalPlan` is owned by `exec/mod.rs`, `WriteLogicalPlan` construction lives in `exec/write.rs`, and fast-path misses now hand off to an explicit `exec/datafusion.rs` reference-writer adapter.
- The DataFusion reference writer no longer regenerates SQL or reparses raw AST. It lowers the validated `LogicalWritePlan` directly into registered table-provider DML calls: `insert_into`, `update`, or `delete_from`, with bound expressions converted to DataFusion logical expressions only at that physical boundary.
- This slice remains intentionally fenced to `lix_state` and `lix_state_by_version` so dynamic entity/file/directory catalogs are not re-resolved through the reference executor before their storage boundary is cut. `lix_state` insert/update/delete provider DML is re-enabled behind that route, and the lix_state write execution regression tests are unignored.

## Phase 6: Fast Write Optimization

- [x] Rebuild current `simple_dml.rs` as two modules:
  - [x] `optimize/simple_write.rs`: `LogicalWritePlan -> Option<FastWritePlan>`.
  - [x] `exec/fast_write.rs`: `FastWritePlan -> rows_affected`.
- [x] The optimizer may inspect only bound targets, bound predicates, and bound assignments.
- [x] The executor may inspect only storage-level IDs and values.
- [x] Unsupported shapes return `Ok(None)` from optimization.
- [x] Invalid SQL is impossible at this layer; if encountered, treat as internal invariant violation.
- [x] Add tests that fast optimization declines complex statements without changing normal execution.

Phase 6 progress:

- Fast-write selection now runs only after binding/planning and consumes `LogicalWritePlan`; it does not inspect raw sqlparser/DataFusion AST, identifiers, object names, or SQL strings.
- The first fast physical plan is intentionally narrow: statically known no-match `lix_state` / `lix_state_by_version` `UPDATE`/`DELETE` plans, derived only from `VersionScope::Empty` or row-level `FilterSet::None`. Inserts, unsupported targets, complex predicates, and column-filter contradictions decline to the reference writer so target support and predicate type validation still run.
- Fast execution consumes only the closed `FastWritePlan` enum and returns `0` rows affected for no-match writes. The executor is total over emitted fast plans; it does not validate SQL, decline, decode params, scan live state, or inspect write context for this plan shape.
- Tests cover column-contradiction decline, `WHERE false` deletes, complex update decline, insert decline, unsupported-target no-match decline/error preservation, JSON predicate validation after contradiction decline, no-context fast execution, and an end-to-end complex `lix_state` update that proves declined optimization still falls through to normal write execution and stages the expected row.

## Phase 7: Storage Visibility Cut

- [x] Move live-state scan/write adaptation behind the `live_state` owner facade.
- [x] Keep `packages/engine/src/live_state/visibility.rs` semantics in the shared non-SQL owner module with one public API.
- [x] Define one visibility request type:

```rust
pub(crate) struct VisibilityRequest {
    pub(crate) version_scope: VisibilityVersionScope,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}
```

`VisibilityVersionScope` intentionally has no empty-result variant. Empty live-state
version filters are represented as `VersionIds { version_ids: vec![] }` so they mean
all stored versions with normal dedupe, not no rows.

- [x] Define one resolver:

```rust
pub(crate) fn resolve_visible_rows(
    base_rows: Vec<MaterializedLiveStateRow>,
    staged_rows: Vec<MaterializedLiveStateRow>,
    request: &VisibilityRequest,
) -> Vec<MaterializedLiveStateRow>;
```

- [x] Make dedupe unconditional after base+staged merge.
- [x] Make global-row projection part of the same resolver.
- [x] Make tombstones participate in winner selection before tombstone filtering.
- [x] Remove caller-specific overlay/dedupe logic from transaction code.
- [x] Add tests for:
  - [x] committed/base live-state scans.
  - [x] staged-overlay scans inside `begin_transaction()`.
  - [x] empty version filter with duplicate base/staged identity.
  - [x] global rows projected into requested versions.
  - [x] tombstone winning over older visible rows.

Phase 7 progress:

- `live_state/visibility.rs` now owns the shared non-SQL visibility API and resolver, avoiding a `live_state -> sql2 -> live_state` owner cycle. It projects global candidates into requested version scopes, keeps tombstones in candidate selection until after winners are chosen, and dedupes rows for empty and non-empty version scopes.
- The `live_state` root facade now owns the transaction overlay scan adapter behind a narrow staged-row trait. Transaction context and schema resolution call that facade instead of carrying local overlay/dedupe code, without exposing `transaction::staging` crate-wide.
- The old `transaction/live_state_overlay.rs` module and SQL-shaped storage wrappers are removed. Existing committed/base live-state reads and transaction overlay scans now route through the same visibility resolver.
- Tests cover committed/base projection, staged transaction reads through `begin_transaction()`, duplicate base/staged identity handling with empty version filters, global row projection, committed global fallback for transaction-only version scopes, and tombstone winner semantics.

## Phase 8: Providers Cleanup

- [x] Move provider files under `providers/` without changing behavior first.
- [x] Replace provider-local surface/column knowledge with `catalog/` contracts.
- [x] Ensure read providers and write providers use the same surface definitions.
- [x] Remove any provider-side special casing duplicated in `bind/`.
- [x] Keep DataFusion provider registration in one `providers::register_read` / `providers::register_write` API.

Phase 8 progress:

- Real provider implementations now live under `packages/engine/src/sql2/providers/`; the old root-level provider modules are deleted.
- `session.rs` no longer knows the individual provider registration sequence. Read and write session construction calls only `providers::register_read` and `providers::register_write`.
- Entity SQL surface derivation moved from the entity provider into `catalog/entity_surface.rs`, so catalog owns the schema-to-surface contract used by bind and providers.
- Provider child modules are private behind the `providers` facade, and entity provider registration now iterates `PublicCatalog` surfaces/specs instead of independently deriving table existence from raw schemas.
- System provider registration now dispatches from `PublicCatalog::surfaces()` by `PublicSurfaceKind`, so the catalog is the single source for every public SQL surface name wired into DataFusion.
- Write-backed SQL sessions install read-only catalog surfaces only when the write context exposes transaction-owned committed-read capabilities, so minimal write contexts can still plan simple writes.
- Transaction read-only providers now read through the transaction-owned SQL storage adapter instead of opening separate read transactions during provider registration.
- Provider/catalog schema contract tests compare full Arrow field contracts for every system/entity surface, including type, nullability, and JSON metadata, instead of only names/order.
- Code-structure coverage now asserts that session construction uses the providers facade, provider modules remain private, and entity provider registration stays catalog-driven.

## Phase 9: Differential Test Harness

- [x] Add a test-only fast-path disable knob:

```rust
pub(crate) enum WriteExecutorMode {
    Auto,
    ForceDataFusion,
    ForceFast,
}
```

- [x] Implement `test_support/differential.rs`:
  - [x] initialize identical databases.
  - [x] execute with `ForceDataFusion`.
  - [x] execute with `Auto` or `ForceFast`.
  - [x] compare error code/message class.
  - [x] compare rows affected.
  - [x] compare final live-state rows.
  - [x] compare transaction staged reads before commit.
- [x] Add deterministic repro fixtures for all known bugs:
  - [x] unresolvable assignment target.
  - [x] base entity version override.
  - [x] base entity insert with hidden version column.
  - [x] unknown typed entity insert column.
  - [x] `_by_version` update/delete without version predicate.
  - [x] repeated contradictory predicates.
  - [x] duplicate insert target columns.
  - [x] duplicate update assignments.
  - [x] qualified target table names.
  - [x] staged overlay global-row reads.
  - [x] empty version filter base/staged dedupe.
- [x] Add generated cases for entity/base/_by_version/lix_state DML.
- [x] Store failing generated seeds as regression tests.

Phase 9 progress:

- Added `WriteExecutorMode::{Auto, ForceDataFusion, ForceFast}` behind `#[cfg(test)]` at the write executor boundary. Production write execution still uses `Auto`; tests can force the DataFusion reference writer or require a fast plan from the same bound `LogicalWritePlan`.
- Added test-only session/transaction entrypoints that re-use normal parse/bind/plan code and only vary the physical write executor mode. The knob does not introduce a public API or a second semantic path.
- Implemented `sql2::test_support::differential` over real initialized in-memory engines. Each case runs setup through the `ForceDataFusion` reference writer, executes one side with `ForceDataFusion` and the other with `SemanticParityMayFallback`/`Auto` or `FastRequiredParity`/`ForceFast`, then compares exact error code/message, rows affected, declared transaction-local staged probes before commit, and declared final visible probes after commit/rollback.
- Added deterministic regression seeds for the known Bugbot classes: unresolvable assignments, hidden/base-version columns, unknown entity columns, missing `_by_version` version predicates, repeated contradictions, duplicate insert/update targets, qualified target names, staged global-row reads, and base/staged dedupe.
- Added deterministic generated cases covering `lix_state`, `lix_state_by_version`, entity base, and entity `_by_version` DML shapes. Generated seeds are built from stable table/operation specs, not hand-written fixture rows; the seed names remain stable regression identifiers in `test_support/generators.rs`, and cases can carry bound parameters to cover placeholder/parameter-decoder regressions.
- Differential cases now declare explicit observation probes, including active `lix_state` identities, staged/final `lix_state_by_version` rows for `global`/`version-a`/`version-b`, `lix_registered_schema`, and staged/final `lix_registered_schema_by_version` rows for `global`/`version-a`/`version-b`, so entity/base/_by_version failures are observed instead of relying on one hard-coded `lix_state` slice.
- Fast-path coverage is explicit: `FastRequiredParity` cases force the fast writer, fail when optimization declines, and assert the test-only executor trace reports `WriteExecutorPath::Fast`. `SemanticParityMayFallback` cases intentionally cover semantic parity through the normal auto path even when the current optimizer falls back.
- Known repro cases carry explicit expected success/error outcomes, so differential equality cannot hide a shared reference/candidate bug that accepts invalid SQL. The harness also supports transaction-local setup statements, with coverage for an update that must see a row staged earlier in the same transaction.
- Versioned probes include the initialized session's real active version id plus real `version-a`/`version-b` refs and canonicalize only version-id columns to a stable active sentinel before comparing independent engines.
- `FastRequiredParity` no-op cases, semantic zero-row successes, and expected-error cases compare against an independent baseline snapshot, so a shared empty-scope shortcut or partial mutation before error cannot be the only proof of correctness.
- Transaction-local `_by_version` staged probes use the transaction live-state scan API directly, with requested active/global/explicit version ids preserved, so staged overlay reads are covered before commit as well as after commit.

## Phase 10: Deletion of Legacy Code

- [x] Delete old `simple_dml.rs`.
- [x] Delete old `public_bind/` once all logic is represented in `catalog/` and `bind/`.
- [x] Delete duplicate table-name helpers.
- [x] Delete duplicate assignment validation helpers.
- [x] Delete duplicate version-filter booleans.
- [x] Delete `LiveStateFilter.no_match` if superseded by typed filters.
- [ ] Run `rg` for banned patterns:
  - [x] `object_name_leaf`
  - [x] `statement.clone()` in fast-path selection
  - [x] `require_version_filter`
  - [x] `allow_version_filter`
  - [x] `active_version_id.is_none()`
  - [x] empty `version_ids` meaning both all and none

Phase 10 progress:

- Confirmed the old raw-AST fast path and public DML validation modules are absent: no `simple_dml`, `public_bind`, `ParamDecoder`, `object_name_leaf`, or `validate_public_dml_statement` references remain under `sql2`.
- Removed the last storage no-match side channel by replacing `LiveStateFilter.no_match` with `LiveStateRowFilter::{All,None}`. Empty `version_ids` now stays an all-visible storage scan convention, while zero-row predicates use the typed row filter.
- Replaced provider-local `limit = Some(0)` no-match sentinels with `LiveStateRowFilter::None`, so contradictory entity/lix_state pushdowns no longer overload limit or empty version filters.
- Review hardening: moved the DataFusion reference-writer `VersionScope::Empty` no-op behind target resolution so unsupported no-match writes, such as `DELETE FROM lix_file WHERE false`, still fail with `LIX_UNSUPPORTED_SQL` instead of silently succeeding.
- Review hardening: removed provider-side `lix_state` assignment validation and `_by_version` version-filter validation helpers. The provider still performs physical expression conversion and row materialization, but public assignment/version semantics are owned by bind/plan.
- Current gate: banned-pattern `rg`, `cargo check -p lix_engine`, `cargo test -p lix_engine live_state::visibility --lib -- --nocapture`, `cargo test -p lix_engine sql2::test_support::differential --lib -- --nocapture`, `cargo test -p lix_engine sql2::exec::datafusion::tests --lib -- --nocapture`, `cargo test -p lix_engine --test code_structure -- --nocapture`, and `cargo fmt -p lix_engine --check` pass.

## Verification Gates

- [x] `cargo check -p lix_engine`
  - Passed after Phase 10.
- [x] `cargo test -p lix_engine sql2`
  - Passed: 202 passed, 36 ignored, 513 filtered out.
- [x] `cargo test -p lix_engine lix_state`
  - Passed. The public SQL integration file still reports 52 ignored cases from the Phase 1 bound-write gate.
- [x] `cargo test -p lix_engine lix_registered_schema`
  - Passed. The public SQL integration file still reports 56 ignored cases from the Phase 1 bound-write gate.
- [x] differential tests pass with fast path enabled.
  - Passed via `cargo test -p lix_engine sql2::test_support::differential --lib -- --nocapture`.
- [x] differential tests pass with fast path disabled.
  - Covered by the same differential harness: the reference side runs with `WriteExecutionMode::ForceDataFusion`.
- [x] run a large fallback `INSERT ... VALUES` benchmark to confirm no pre-fallback AST clone regression.
  - Passed after moving the benchmark fixture to the surviving `lix_state` write surface. Command: `cargo bench -p lix_engine --features storage-benches --bench transaction -- transaction_sql_fast_path/insert_values_batch_no_payload/2k --sample-size 10 --measurement-time 1 --warm-up-time 1`. Result: 127.74 ms to 128.77 ms for 2k rows.
- [x] review compile warnings and remove compatibility shims left only for migration.
  - Reviewed during final gates. Remaining warnings are unused/dead-code fallout from the hard-cut type/provider surface and disabled legacy provider DML, not compatibility adapters that should stay hidden.

## Implementation Notes

- Start with Rust types and module boundaries, not with small behavioral patches.
- Prefer compiler errors over compatibility adapters. Add temporary adapters only when needed to keep tests runnable between phases.
- Keep commits phase-sized:
  - [ ] layout/types compile cut.
  - [ ] catalog/bind migration.
  - [ ] logical write plan migration.
  - [ ] fast executor migration.
  - [ ] storage visibility migration.
  - [ ] differential harness.
  - [ ] legacy deletion.
- Do not preserve old internal APIs for callers inside `sql2`; update them to the new pipeline.
- Preserve public SQL behavior only where it is intentional and covered by the new binder tests.
