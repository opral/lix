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
- Review hardening: removed the pre-bind fast-write hook from session execution, changed fast execution to consume `FastWritePlan`, put `bind_statement` on the write planning path, removed old `simple_dml` and `public_bind`, made `VersionScope` the sole entity version authority, made write values bound expressions instead of runtime `Value`s, added planned `FilterSet`s to `LogicalWritePlan`, and routed transaction overlay visibility through the `live_state` visibility owner with `sql2::storage::visibility` as a wrapper.
- Review hardening: transaction overlay candidate scans now remove pre-visibility `limit` and force `include_tombstones = true`, then apply caller limit/tombstone filtering only after shared visibility resolution.
- Review hardening: read planning entrypoints now reject write ASTs before DataFusion planning, live-state point loads and transaction schema point loads route through scan/overlay visibility, empty-version overlay dedupe happens before tombstone filtering, and stale raw-DataFusion write tests are explicitly ignored until the bound write pipeline is implemented.
- Current gate: `cargo check -p lix_engine` passes with warnings from intentionally-unused Phase 1 target types. Write execution intentionally stops at the new binder/planner boundary until Phase 2/3 implement catalog, write binding, and bound write execution; this is the hard cut that prevents falling back to raw-AST DML semantics.
- Current gate: `cargo test -p lix_engine sql2::exec::datafusion::tests --lib -- --nocapture` passes with active SQL2 read coverage restored through non-SQL fixtures; only write/history-write-dependent tests are ignored until the bound write pipeline is implemented.
- Current gate: `cargo test -p lix_engine sql2::lix_state_provider::tests --lib -- --nocapture`, `cargo test -p lix_engine live_state::visibility::tests --lib -- --nocapture`, and `cargo test -p lix_engine overlay_ --lib -- --nocapture` pass. Raw provider DML hooks now fail closed, and shared live-state visibility owns overlay tombstone/dedupe/global projection semantics for both transaction and sql2 callers.
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

- [ ] Implement `bind::bind_statement`.
- [ ] Implement `bind::write::bind_insert`.
- [ ] Implement `bind::write::bind_update`.
- [ ] Implement `bind::write::bind_delete`.
- [ ] Bind assignment targets into resolved column IDs, not strings.
- [ ] Reject duplicate insert target columns during binding.
- [ ] Reject duplicate update assignment targets during binding.
- [ ] Bind params in source-order once into `BoundParamMap`.
- [ ] Bind predicates into `BoundPredicate`.
- [ ] Convert repeated identity predicates into `FilterSet` intersections during planning, not in execution.
- [ ] Remove `ParamDecoder` from fast execution.
- [ ] Delete statement-level DML validation once binding covers the same rules.

## Phase 4: Logical Write Plans

- [ ] Implement `plan::write::plan_write(bound: BoundWrite) -> LogicalWritePlan`.
- [ ] Make `LogicalWritePlan` the common input for DataFusion and fast write execution.
- [ ] Model version requirements in `VersionScope`:
  - [ ] base entity writes use `VersionScope::Active`.
  - [ ] `_by_version` update/delete use `VersionScope::ExplicitRequired`.
  - [ ] `lix_state` can use explicit versions where public semantics allow it.
- [ ] Replace `LiveStateFilter.no_match` with `FilterSet::None` or equivalent.
- [ ] Convert `FilterSet` to storage filters only at the storage boundary.
- [ ] Add tests for contradictory predicates:
  - [ ] repeated equality with different values returns zero matches.
  - [ ] repeated `IN` intersections work.
  - [ ] contradiction does not corrupt param binding.

## Phase 5: DataFusion Path From Bound Plans

- [ ] Move existing `execute.rs` DataFusion plan creation under `exec/datafusion.rs`.
- [ ] Keep DataFusion as the reference physical executor.
- [ ] Build DataFusion sessions from bound/catalog state, not separate public validation.
- [ ] Ensure normal write path and fast write path share the same `LogicalWritePlan`.
- [ ] Remove duplicated calls to `validate_public_dml_statement`.
- [ ] Remove the unconditional full-AST clone in the write fast-path decision.
- [ ] Ensure fallback large `INSERT ... VALUES` does not clone the AST just to decline fast path.

## Phase 6: Fast Write Optimization

- [ ] Rebuild current `simple_dml.rs` as two modules:
  - [ ] `optimize/simple_write.rs`: `LogicalWritePlan -> Option<FastWritePlan>`.
  - [ ] `exec/fast_write.rs`: `FastWritePlan -> rows_affected`.
- [ ] The optimizer may inspect only bound targets, bound predicates, and bound assignments.
- [ ] The executor may inspect only storage-level IDs and values.
- [ ] Unsupported shapes return `Ok(None)` from optimization.
- [ ] Invalid SQL is impossible at this layer; if encountered, treat as internal invariant violation.
- [ ] Add tests that fast optimization declines complex statements without changing normal execution.

## Phase 7: Storage Visibility Cut

- [ ] Move live-state scan/write adaptation into `storage/live_state.rs`.
- [ ] Move `packages/engine/src/live_state/visibility.rs` semantics behind `sql2/storage/visibility.rs` or a shared non-SQL module with one public API.
- [ ] Define one visibility request type:

```rust
pub(crate) struct VisibilityRequest {
    pub(crate) version_scope: VersionScope,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}
```

- [ ] Define one resolver:

```rust
pub(crate) fn resolve_visible_rows(
    base_rows: Vec<MaterializedLiveStateRow>,
    staged_rows: Vec<MaterializedLiveStateRow>,
    request: &VisibilityRequest,
) -> Vec<MaterializedLiveStateRow>;
```

- [ ] Make dedupe unconditional after base+staged merge.
- [ ] Make global-row projection part of the same resolver.
- [ ] Make tombstones participate in winner selection before tombstone filtering.
- [ ] Remove caller-specific overlay/dedupe logic from transaction code.
- [ ] Add tests for:
  - [ ] committed/base live-state scans.
  - [ ] staged-overlay scans inside `begin_transaction()`.
  - [ ] empty version filter with duplicate base/staged identity.
  - [ ] global rows projected into requested versions.
  - [ ] tombstone winning over older visible rows.

## Phase 8: Providers Cleanup

- [ ] Move provider files under `providers/` without changing behavior first.
- [ ] Replace provider-local surface/column knowledge with `catalog/` contracts.
- [ ] Ensure read providers and write providers use the same surface definitions.
- [ ] Remove any provider-side special casing duplicated in `bind/`.
- [ ] Keep DataFusion provider registration in one `providers::register_read` / `providers::register_write` API.

## Phase 9: Differential Test Harness

- [ ] Add a test-only fast-path disable knob:

```rust
pub(crate) enum WriteExecutorMode {
    Auto,
    ForceDataFusion,
    ForceFast,
}
```

- [ ] Implement `test_support/differential.rs`:
  - [ ] initialize identical databases.
  - [ ] execute with `ForceDataFusion`.
  - [ ] execute with `Auto` or `ForceFast`.
  - [ ] compare error code/message class.
  - [ ] compare rows affected.
  - [ ] compare final live-state rows.
  - [ ] compare transaction staged reads before commit.
- [ ] Add deterministic repro fixtures for all known bugs:
  - [ ] unresolvable assignment target.
  - [ ] base entity version override.
  - [ ] base entity insert with hidden version column.
  - [ ] unknown typed entity insert column.
  - [ ] `_by_version` update/delete without version predicate.
  - [ ] repeated contradictory predicates.
  - [ ] duplicate insert target columns.
  - [ ] duplicate update assignments.
  - [ ] qualified target table names.
  - [ ] staged overlay global-row reads.
  - [ ] empty version filter base/staged dedupe.
- [ ] Add generated cases for entity/base/_by_version/lix_state DML.
- [ ] Store failing generated seeds as regression tests.

## Phase 10: Deletion of Legacy Code

- [ ] Delete old `simple_dml.rs`.
- [ ] Delete old `public_bind/` once all logic is represented in `catalog/` and `bind/`.
- [ ] Delete duplicate table-name helpers.
- [ ] Delete duplicate assignment validation helpers.
- [ ] Delete duplicate version-filter booleans.
- [ ] Delete `LiveStateFilter.no_match` if superseded by typed filters.
- [ ] Run `rg` for banned patterns:
  - [ ] `object_name_leaf`
  - [ ] `statement.clone()` in fast-path selection
  - [ ] `require_version_filter`
  - [ ] `allow_version_filter`
  - [ ] `active_version_id.is_none()`
  - [ ] empty `version_ids` meaning both all and none

## Verification Gates

- [ ] `cargo check -p lix_engine`
- [ ] `cargo test -p lix_engine sql2`
- [ ] `cargo test -p lix_engine lix_state`
- [ ] `cargo test -p lix_engine lix_registered_schema`
- [ ] differential tests pass with fast path enabled.
- [ ] differential tests pass with fast path disabled.
- [ ] run a large fallback `INSERT ... VALUES` benchmark to confirm no pre-fallback AST clone regression.
- [ ] review compile warnings and remove compatibility shims left only for migration.

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
