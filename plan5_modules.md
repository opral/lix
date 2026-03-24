# Plan 5: Self-Contained Engine Modules

## Goal

Build five self-contained modules below the SQL layer. Each module has its own contracts, types, and tests — no dependency on `sql/**`. Once complete, the SQL engine hooks into them as a consumer.

```
constraints/           — shared vocabulary: ScanConstraint, ScanField, ScanOperator, Bound
live_untracked_state/  — untracked/helper row access over relational live tables
live_tracked_state/    — tracked row access over relational live tables
effective_state/       — owns all overlay/precedence logic
transaction/           — owns the transaction lifecycle: BEGIN → preflight → write batch → COMMIT
```

Canonical state (lix changes, binary CAS, commit graph, lineage) stays in the existing `state/` module. Transaction depends on it but it doesn't need a new extraction — it's already somewhat separated.

## Why This Order

**Constraints first.** Shared vocabulary that everyone depends on. Small, stable, no logic — just types. Built once, never reopened.

**State stores second.** Each owns its read view trait (`TrackedReadView`, `UntrackedReadView`). Consumers depend on these traits — so they must exist first. No ambiguity about trait ownership.

**Untracked before tracked.** Simpler (pure relational, more scattered call sites to consolidate). Good first module to establish the pattern.

**Effective state fourth.** Pure precedence engine. Consumes `TrackedReadView` and `UntrackedReadView` from the state store modules.

**Transaction last.** Consumes all four. Owns lifecycle, staging, preflight, write-phase orchestration.

## Architectural Alignment with Dolt, DuckDB, and Turso

All three systems keep transaction/session cores below SQL:

- Transaction/session lifecycle owns mutable local state, separate from SQL planning
- State stores are participants that plug into commit through typed boundaries
- Transaction-local mutable state is isolated from shared committed state

SQL entrypoints are thin consumers of these lower layers, not owners. That's the target for Lix.

**Dolt:** layered immutable roots (Head/Working/Staged), statement-level buffering separate from transaction commit, optimistic concurrency with 3-way merge at commit time.

**DuckDB:** per-transaction LocalStorage + UndoBuffer, participant pattern where each table has a LocalTableStorage that participates in commit/rollback.

**Turso:** structured `Constraint` objects (operator enum + column index + selectivity) compiled into `AccessMethodParams::BTreeTable { constraint_refs }`. MVCC with per-transaction versioned row visibility.

## Predicate Pushdown Boundary

All four reference systems (DuckDB, Dolt, Turso, SQLite/libSQL) convert SQL predicates into **structured, SQL-free constraint types** at the planner-storage boundary. The storage layer never sees SQL AST.

| System     | Planner output                                     | Storage input                                |
| ---------- | -------------------------------------------------- | -------------------------------------------- |
| **DuckDB** | `TableFilterSet` with `ConstantFilter`, `InFilter` | Passed to `DataTable::InitializeScan()`      |
| **Dolt**   | `sql.MySQLRange` → `prolly.Range` with byte bounds | Passed to `prolly.Map.IterRange()`           |
| **Turso**  | `Constraint` with `ConstraintOperator` enum        | `RangeConstraintRef` in `AccessMethodParams` |
| **SQLite** | `sqlite3_index_info` with `{iColumn, op}` structs  | Passed to `xFilter(idxNum, argv)`            |

**For Lix, the split is:**

- `sql/**` owns predicate analysis and split into pushed vs residual. Compiles SQL predicates into `ScanConstraint` values from `constraints/`. This matches the current engine (`surface_semantics.rs`, `effective_state_resolver.rs`, `lowerer.rs`).
- `live_tracked_state/` and `live_untracked_state/` own efficient execution of structured constraints.
- `effective_state/` stays SQL-free. It applies overlay precedence and forwards constraints lane-by-lane to the state stores. It does not own pushdown logic.

## Lessons from the lix3 Attempt

**Do:** Keep `PendingTransactionView` as a migration adapter when hooking in. Let it shrink as callers move to `ReadContext`.

**Don't:** Try to eliminate early flush in the same pass. Extract the boundary first, optimize later.

**Don't:** Interleave storage changes with module extraction. The module boundary is the deliverable, not the storage change.

**Don't:** Gold-plate during integration. When hooking modules into `sql/`, keep the adapter layer thin. Resist adding new contract surface during hook-in — the modules should already be complete.

---

## Module 0: `constraints/`

Shared vocabulary for structured, SQL-free scan constraints. Produced by `sql/**`, consumed by state stores, forwarded by `effective_state/`. Closest to DuckDB's `TableFilter` types — a small namespace both planner and storage import.

### Contracts

```rust
/// Which field to constrain. Typed around actual indexed fields.
enum ScanField {
    EntityId,
    FileId,
    PluginKey,
    SchemaVersion,
}

/// Inclusive/exclusive bound for range scans.
struct Bound {
    value: Value,
    inclusive: bool,
}

/// SQL-free structured constraint. Supports eq, in, and range bounds.
/// Aligned with DuckDB TableFilter / Dolt prolly.Range / Turso RangeConstraintRef.
struct ScanConstraint {
    field: ScanField,
    operator: ScanOperator,
}

enum ScanOperator {
    Eq(Value),
    In(Vec<Value>),
    Range { lower: Option<Bound>, upper: Option<Bound> },
}
```

No logic, no dependencies beyond a `Value` type. Built first, consumed by everything.

**Combine semantics:** `Vec<ScanConstraint>` is conjunctive (AND). `schema_key` and `version_id` are required partition selectors on every request — not part of the constraint set. Planner-level OR / disjoint-range logic is either residualized above `effective_state/` or split into multiple scan requests. This matches DuckDB (conjunctive `TableFilterSet`), Dolt (each `prolly.Range` is a single conjunctive key range, disjunctions become multiple ranges), and Turso (conjunctive constraint refs per access method).

**Success criteria:**

- Pure types, no logic
- Zero imports from `sql/**`
- Stable — designed so the contract doesn't need reopening for future storage backends

## Module 1: `live_untracked_state/`

Typed untracked/helper row access. Owns the `UntrackedReadView` trait and its relational implementation.

### Contracts

Point lookup, batch point lookup, and bounded scan are distinct request shapes. Scan requests carry `ScanConstraint` from `constraints/`.

```rust
/// Point lookup by full key.
struct ExactUntrackedRowRequest {
    schema_key: String,
    version_id: String,
    entity_id: String,
    file_id: Option<String>,
}

/// Batch point lookup by entity IDs.
struct BatchUntrackedRowRequest {
    schema_key: String,
    version_id: String,
    entity_ids: Vec<String>,
    file_id: Option<String>,
}

/// Bounded scan with structured constraints.
struct UntrackedScanRequest {
    schema_key: String,
    version_id: String,
    constraints: Vec<ScanConstraint>,
    required_columns: Vec<String>,
}

trait UntrackedReadView {
    fn load_exact_row(&self, request: &ExactUntrackedRowRequest) -> Option<UntrackedRow>;
    fn load_exact_rows(&self, request: &BatchUntrackedRowRequest) -> Vec<UntrackedRow>;
    fn scan_rows(&self, request: &UntrackedScanRequest) -> Vec<UntrackedRow>;
}

trait UntrackedWriteParticipant {
    fn apply_write_batch(&mut self, batch: &[UntrackedWriteRow]) -> Result<()>;
    fn ensure_storage_for_schema(&mut self, schema_key: &str) -> Result<()>;
}
```

Active version, version ref, checkpoint metadata, and helper lookups each implemented once inside this module. The first version may share relational internals with tracked state (both use `lix_internal_live_v1_*` tables) while exposing a clean module-owned API.

**Does not own:** tracked rows, overlay precedence, transaction lifecycle, SQL parsing, predicate analysis.

**Success criteria:**

- Exposes `UntrackedReadView` for `effective_state/` and `transaction/` consumers
- No module outside `live_untracked_state/` builds normalized live-table SQL for untracked rows
- Active version, version ref, and helper metadata lookups implemented once
- Scan requests accept `ScanConstraint` from `constraints/`, not SQL AST or strings
- Zero imports from `sql/**`
- Testable standalone against a real or test backend

## Module 2: `live_tracked_state/`

Typed tracked row access. Owns the `TrackedReadView` trait and its relational implementation. Same request shape pattern as untracked — point lookup, batch, bounded scan with typed constraints.

### Contracts

```rust
struct ExactTrackedRowRequest {
    schema_key: String,
    version_id: String,
    entity_id: String,
    file_id: Option<String>,
}

struct BatchTrackedRowRequest {
    schema_key: String,
    version_id: String,
    entity_ids: Vec<String>,
    file_id: Option<String>,
}

struct TrackedScanRequest {
    schema_key: String,
    version_id: String,
    constraints: Vec<ScanConstraint>,
    required_columns: Vec<String>,
}

trait TrackedReadView {
    fn load_exact_row(&self, request: &ExactTrackedRowRequest) -> Option<TrackedRow>;
    fn load_exact_rows(&self, request: &BatchTrackedRowRequest) -> Vec<TrackedRow>;
    fn scan_rows(&self, request: &TrackedScanRequest) -> Vec<TrackedRow>;
}

trait TrackedWriteParticipant {
    fn apply_write_batch(&mut self, batch: &[TrackedWriteRow]) -> Result<()>;
}
```

Storage is relational (normalized `lix_internal_live_v1_*` tables, `untracked = false`). May share relational internals with untracked state while exposing a clean module-owned API.

**Does not own:** untracked rows, overlay precedence, transaction lifecycle, SQL parsing, predicate analysis.

**Success criteria:**

- Exposes `TrackedReadView` for `effective_state/` and `transaction/` consumers
- No module outside `live_tracked_state/` queries tracked live tables directly
- Scan requests accept `ScanConstraint` from `constraints/`, not SQL AST or strings
- Zero imports from `sql/**`
- Testable standalone against a real or test backend

## Module 3: `effective_state/`

Pure precedence engine. Given read views, resolve which row is visible. Forwards structured constraints lane-by-lane to state stores — does not own pushdown logic.

### Contracts

```rust
enum OverlayLane {
    LocalUntracked,
    LocalTracked,
    GlobalUntracked,
    GlobalTracked,
}

struct EffectiveRowRequest {
    schema_key: String,
    version_id: String,
    entity_id: String,
    file_id: Option<String>,
    include_global: bool,
    include_untracked: bool,
}

struct EffectiveRowsRequest {
    schema_key: String,
    version_id: String,
    constraints: Vec<ScanConstraint>,
    required_columns: Vec<String>,
    include_global: bool,
    include_untracked: bool,
    include_tombstones: bool,
}

/// Lane order: local untracked → local tracked → global untracked → global tracked.
/// Found wins. Tombstone hides. Missing falls through.
fn resolve_effective_row(
    request: &EffectiveRowRequest,
    tracked: &dyn TrackedReadView,
    untracked: &dyn UntrackedReadView,
) -> Option<EffectiveRow>;

fn resolve_effective_rows(
    request: &EffectiveRowsRequest,
    tracked: &dyn TrackedReadView,
    untracked: &dyn UntrackedReadView,
) -> EffectiveRowSet;
```

No planner types, no SQL AST predicates. `EffectiveRowsRequest` carries `ScanConstraint` values from `constraints/` that it forwards to the state stores per lane. The SQL planner is responsible for converting SQL predicates into these constraints before calling in.

**Does not own:** how rows are stored, SQL query building, transaction lifecycle, predicate analysis.

**Success criteria:**

- No module outside `effective_state/` implements overlay precedence
- Lane order and tombstone semantics defined in exactly one place
- Forwards `ScanConstraint` to state stores — no SQL AST, no predicate classification
- Zero imports from `sql/**`
- Testable standalone with mock read views

## Module 4: `transaction/`

Owns the full transaction lifecycle:

```
BEGIN
  1. preflight   — batch read of facts needed to validate/shape writes
  2. write batch — apply all tracked + untracked + internal changes
COMMIT
```

Lix-specific optimization target: two roundtrips (may be chunked for SQLite parameter limits), not N+1.

### Contracts

```rust
/// A batch of state changes to apply in one transaction.
struct TransactionDelta {
    tracked_writes: Vec<TrackedWriteRequest>,
    untracked_writes: Vec<UntrackedWriteRequest>,
    canonical_writes: Vec<CanonicalWriteRequest>,
}

/// Accumulates deltas across statements within a transaction.
struct TransactionJournal {
    staged: Vec<TransactionDelta>,
}

/// Typed outcome of a committed transaction.
/// The engine/session adapter applies these to runtime context.
struct CommitOutcome {
    active_version_id: Option<String>,          // new active version if changed
    committed_changes: Vec<CommittedChange>,     // for commit-stream emission
    schemas_dirty: bool,                         // registry needs refresh
    observe_tick: bool,                          // observation tick emitted
}

/// Write transaction. Owns staged deltas and participant state.
struct WriteTransaction<'a> {
    backend_txn: Box<dyn LixBackendTransaction + 'a>,
    journal: TransactionJournal,
    tracked: TrackedTxnParticipant,
    untracked: UntrackedTxnParticipant,
    canonical: CanonicalTxnParticipant,
}

/// Read-only state access.
struct ReadContext<'a> {
    tracked: &'a dyn TrackedReadView,
    untracked: &'a dyn UntrackedReadView,
    canonical: &'a dyn CanonicalFactsView,
}

impl WriteTransaction {
    fn stage(&mut self, delta: TransactionDelta);
    async fn execute(&mut self) -> Result<()>;  // preflight + write batch
    async fn commit(self) -> Result<CommitOutcome>;
    async fn rollback(self) -> Result<()>;
}
```

The transaction module defines its own write request and outcome types. The SQL layer is responsible for compiling SQL statements into `TransactionDelta` values — that's the input hook-in point. `CommitOutcome` is the output — the engine/session adapter applies those side effects (active version update, commit-stream emission, registry refresh, observe tick) to runtime context. `CommitOutcome` will expand as we implement — the fields shown here are the known core set, not an exhaustive spec. Additional outcomes (e.g. file-cache invalidation targets, plugin change notifications) get added to `CommitOutcome` as the adapter surfaces them during hook-in, rather than staying as raw planner artifacts. Raw planner artifacts like `PlanEffects` do not cross into `transaction/` — the SQL adapter must translate them into module-owned types before staging.

Canonical state (lix changes, binary CAS, commit graph, lineage) stays in `state/` — transaction depends on it via `CanonicalFactsView` and `CanonicalWriteRequest`.

**Does not own:** SQL parsing/lowering, live table layout, overlay precedence, filesystem semantics, predicate analysis.

**Success criteria:**

- Has its own types — not re-exports of `sql/execution/**`
- `WriteTransaction` can stage, execute, commit, and rollback
- Participants call into state store traits, not SQL internals
- Testable standalone with mock backends

---

## Dependency Direction

```
constraints/  ──owns shared scan vocabulary, consumed by everyone──

live_untracked_state/  ──owns──►  UntrackedReadView    ──depends on──►  constraints/
live_tracked_state/    ──owns──►  TrackedReadView       ──depends on──►  constraints/
        │                              │
        ▼                              ▼
effective_state/  ──consumes──►  both read view traits   ──depends on──►  constraints/
        │                        forwards ScanConstraint
        ▼
transaction/  ──consumes──►  all four + canonical from state/

              ┌─────────────────────────────────┐
              │  sql/  hooks into all of these   │
              │  as a consumer at integration    │
              │                                   │
              │  sql/ owns predicate analysis:    │
              │  SQL AST → ScanConstraint         │
              │  pushed vs residual split         │
              └─────────────────────────────────┘
```

One direction. No cycles. `sql/**` is not a dependency of any module. Each state store permanently owns its trait. `constraints/` is the shared vocabulary — depends on nothing, consumed by everything.

## Design Constraints

1. **Two roundtrips, not N+1.** Lix-specific target. One preflight phase, one write phase. Chunking for backend limits is fine, caller-driven per-row loops are not.

2. **Batch-first APIs.** Singleton exact reads for convenience. Transaction-owned preflight and writes use batch/scan as primary interface.

3. **State stores are participants, not orchestrators.** Transaction calls into stores. Stores don't call back into transaction orchestration.

4. **Modules complete before hook-in.** Each module has its own contracts, types, tests, and zero `sql/**` imports. The SQL engine adapts into them, not the other way around.

5. **Extract boundary first, optimize later.** Overlay-backed participant state is fine initially. Don't block on eliminating early flush or replacing PendingTransactionView.

6. **Structured constraints, not SQL.** State store scan requests accept typed `ScanConstraint` values from `constraints/` — aligned with actual indexed fields, supporting eq, in, and range bounds (lower/upper with inclusive/exclusive). Point lookup, batch lookup, and bounded scan are distinct request shapes. The SQL planner converts predicates into `ScanConstraint` at hook-in time. The contract is designed to be stable across storage backend changes.

## Progress log

- 2026-03-24: Plan created. Studied DuckDB, Dolt, Turso/libSQL transaction and pushdown architectures. Established five-module split (constraints, live_untracked_state, live_tracked_state, effective_state, transaction) with build order, dependency direction, structured scan constraints, and CommitOutcome boundary. Reviewed against lix3 attempt lessons. Plan validated through multiple rounds of architectural feedback.
