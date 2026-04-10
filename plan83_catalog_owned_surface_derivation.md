# Plan 83 — Catalog-Owned Surface Derivation And Artifact Splits

## Problem

Plan 70 clarified the intended ownership model:

- `catalog` owns what public named relations mean
- `live_state` owns serving/runtime state
- `sql` owns lowering/planning
- orchestration layers should not absorb owner semantics

That still leaves one awkward seam in the current engine:

- public relation semantics for derived surfaces are split between
  [packages/engine/src/catalog](/Users/samuel/git-repos/lix-2/packages/engine/src/catalog)
  and
  [packages/engine/src/projections](/Users/samuel/git-repos/lix-2/packages/engine/src/projections)
- [packages/engine/src/projections/artifacts.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/projections/artifacts.rs)
  mixes declarative spec types, lifecycle selection, hydrated input
  containers, and derived output rows in one owner-local type bucket
- [packages/engine/src/contracts/artifacts.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/contracts/artifacts.rs)
  is already the cross-owner contract bucket, so the current
  `projections/artifacts.rs` name encourages confusion about what is
  owner-local versus what is a shared interchange contract

The result is conceptual friction:

- `catalog` says what `lix_version` means at the public relation layer
- `projections` says how `lix_version` is derived
- `live_state` hydrates and runs the derivation

That split works mechanically, but it does not match the simpler model
that Plan 70 points toward:

> public relation semantics should stay in `catalog`, while runtime
> execution machinery stays in `live_state`.

The goal of Plan 83 is:

**move declarative surface derivation ownership into `catalog`, keep
execution in `live_state`, and replace generic `artifacts.rs` buckets
with owner-appropriate type roots.**

This plan is intentionally architectural, not just cosmetic:

- no new compatibility owner for `projections`
- no long-lived “temporary” root re-export that preserves the current
  conceptual split
- no growth of `contracts/artifacts.rs` as a miscellaneous sink

---

## Current Evidence

The current code already documents the split clearly enough to show why
it now feels awkward.

### `catalog` owns public relation semantics

[packages/engine/src/catalog/registry.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/catalog/registry.rs)
defines `SurfaceDescriptor`, `SurfaceBinding`, read freshness, read
semantics, capabilities, and the public registry.

[packages/engine/src/catalog/binding.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/catalog/binding.rs)
binds names like `lix_version`, `lix_file`, and `lix_directory` into
typed engine-facing relation bindings.

That is exactly the Plan 70 statement:

- `catalog` owns what public named relations such as `lix_version`,
  `lix_file`, and `lix_directory` mean

as written in
[plan70_sealed_owner_apis_for_scoped_changes.md](/Users/samuel/git-repos/lix-2/plan70_sealed_owner_apis_for_scoped_changes.md).

### `projections` owns declarative derivation definitions

[packages/engine/src/projections/traits.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/projections/traits.rs)
defines `ProjectionTrait` as the declarative boundary:

- which tracked/untracked inputs are needed
- which public surfaces are served
- how hydrated input becomes derived rows

and explicitly says it does not own:

- storage hydration
- replay/catch-up
- readiness/progress/checkpointing
- runtime surface binding

### `live_state` owns runtime derivation execution

[packages/engine/src/live_state/projection/hydration.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/live_state/projection/hydration.rs)
hydrates declared projection input from tracked/untracked storage.

[packages/engine/src/live_state/projection/dispatch.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/live_state/projection/dispatch.rs)
iterates the registry and runs derivation.

[packages/engine/src/live_state/mod.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/live_state/mod.rs)
already presents this as root-level live-state serving machinery.

The current arrangement is therefore:

```text
catalog     -> public relation meaning
projections -> declarative derivation recipe
live_state  -> hydration, replay, execution, readiness
```

This is coherent, but it is one owner too many for the current Lix
shape.

---

## Reference Systems

Plan 83 compares three reference patterns using the local checked-in
reference sources under `artifact/`.

### DuckDB

DuckDB is the strongest example of **catalog-owned named object
semantics**.

See:

- [artifact/duckdb/src/include/duckdb/catalog/catalog_entry.hpp](/Users/samuel/git-repos/lix-2/artifact/duckdb/src/include/duckdb/catalog/catalog_entry.hpp)
- [artifact/duckdb/src/include/duckdb/catalog/catalog.hpp](/Users/samuel/git-repos/lix-2/artifact/duckdb/src/include/duckdb/catalog/catalog.hpp)

Takeaways:

- named object meaning lives under the catalog
- lookup, creation, binding, and object identity stay under the catalog
- runtime planning/execution are separate stages, not siblings that also
  own named-object meaning

DuckDB does **not** introduce a separate top-level owner for “derived
named relation definitions.” Catalog owns those meanings directly.

### Marten

Marten is the strongest example of **projection definitions nested under
their runtime feature owner**.

See:

- [artifact/marten/src/Marten/Events/Projections/IProjection.cs](/Users/samuel/git-repos/lix-2/artifact/marten/src/Marten/Events/Projections/IProjection.cs)
- [artifact/marten/src/Marten/Events/Daemon/ProjectionDaemon.cs](/Users/samuel/git-repos/lix-2/artifact/marten/src/Marten/Events/Daemon/ProjectionDaemon.cs)

Takeaways:

- projection definitions and lifecycle are owned inside the event
  subsystem
- the async daemon that runs projections lives nearby
- the abstraction and the runtime are intentionally coupled

This is a good pattern when derived state is first and foremost a
feature of the event subsystem.

### KurrentDB

KurrentDB is the strongest example of **derived-state consumers as
separate subsystems**.

See:

- [artifact/kurrentdb/src/KurrentDB.Projections.Core/ProjectionsSubsystem.cs](/Users/samuel/git-repos/lix-2/artifact/kurrentdb/src/KurrentDB.Projections.Core/ProjectionsSubsystem.cs)
- [artifact/kurrentdb/src/KurrentDB.SecondaryIndexing/SecondaryIndexingPlugin.cs](/Users/samuel/git-repos/lix-2/artifact/kurrentdb/src/KurrentDB.SecondaryIndexing/SecondaryIndexingPlugin.cs)

Takeaways:

- projection execution is a separate subsystem from the core storage
  engine
- secondary indexing is a different subsystem again
- rebuildable derived-state runtime machinery is isolated from truth
  ownership

This is the right lesson for Lix `live_state`:

- keep serving/runtime machinery separate
- do not let public relation semantics leak into the runtime owner

But KurrentDB does **not** imply that Lix must keep a separate semantic
owner called `projections` forever. It mainly argues that execution and
rebuildable state should remain separate from truth ownership.

---

## Three Refactor Options

### Option 1 — Keep `projections/` top-level, but shrink it

Model:

- `catalog` owns public relation binding
- `projections` owns derivation definitions
- `live_state` owns runtime execution

This preserves the current Plan 60 shape:

- top-level `projections/`
- one-way `live_state -> projections`

Pros:

- smallest migration
- preserves current structure tests and boot wiring
- keeps runtime graph stable

Cons:

- public relation semantics remain split across two roots
- `catalog` still does not fully own derived public relation meaning
- `projections/artifacts.rs` still needs significant cleanup anyway

Best fit:

- only if minimizing churn is more important than conceptual cleanup

### Option 2 — Move declarative surface derivation into `catalog`

Model:

- `catalog` owns public relation descriptors, bindings, and derivation
  specs
- `live_state` owns hydration, replay, execution, readiness
- `sql` lowers catalog-owned relation bindings

Pros:

- matches Plan 70’s intended meaning of `catalog`
- unifies “what this public relation means” and “how this derived public
  relation is declared”
- keeps runtime execution separate in `live_state`
- removes the conceptual need for a standalone `projections` owner

Cons:

- moderate migration effort
- requires careful separation so `catalog` does not absorb runtime or
  backend concerns

Best fit:

- Lix’s current architecture and future Plan 70 direction

### Option 3 — Move projection definitions under `live_state`

Model:

- `catalog` owns only names/bindings
- `live_state` owns both derivation definitions and runtime execution

Pros:

- strongest runtime cohesion
- closest to the Marten shape

Cons:

- weakens the semantic role of `catalog`
- makes `live_state` own more public relation meaning than it should
- increases the chance that serving/runtime details shape public
  semantics

Best fit:

- only if Lix decides that derived public surfaces are fundamentally a
  serving-engine concern rather than a catalog concern

---

## Recommendation

Choose **Option 2**.

That gives Lix the cleanest ownership model:

```text
catalog
  owns public relation semantics
  owns derivation declarations for derived public relations
      │
      ▼
live_state
  owns hydration, replay, execution, readiness, serving state
      │
      ▼
sql / execution / session
  lower, run, and orchestrate through owner APIs
```

This is the best synthesis of the references:

- DuckDB: named relation semantics belong under catalog
- KurrentDB: runtime derived-state machinery stays a separate serving
  subsystem
- Marten: avoid splitting definition and runtime arbitrarily, but only
  when the feature owner is the right semantic owner

For Lix, `catalog` is the right semantic owner and `live_state` is the
right runtime owner.

---

## Reference Fit Check

The recommended model is:

```text
catalog    -> owns public relation meaning + declarative derivation specs
live_state -> owns hydration + replay + runtime derivation execution
sql        -> lowers catalog-owned relation bindings
session    -> orchestrates
execution  -> runs prepared artifacts
```

This is how that recommendation aligns with the local reference systems.

### DuckDB

**Alignment: strong**

DuckDB is the clearest support for making `catalog` the owner of named
relation semantics.

See:

- [artifact/duckdb/src/include/duckdb/catalog/catalog_entry.hpp](/Users/samuel/git-repos/lix-2/artifact/duckdb/src/include/duckdb/catalog/catalog_entry.hpp)
- [artifact/duckdb/src/include/duckdb/catalog/catalog.hpp](/Users/samuel/git-repos/lix-2/artifact/duckdb/src/include/duckdb/catalog/catalog.hpp)

Why it aligns:

- named object meaning lives in catalog-owned entries
- planner/execution are separate stages
- there is no extra semantic root parallel to catalog for describing how
  named relations mean what they mean

Takeaway:

- DuckDB strongly supports moving declarative derived-surface meaning
  into `catalog`

### Turso

**Alignment: partial but positive**

Turso is not a close match to Lix’s state-owner layering, but it does
support the proposed split in one important way.

See:

- [artifact/reference_dependency_models.md](/Users/samuel/git-repos/lix-2/artifact/reference_dependency_models.md)
- [artifact/reference_state_layer_handling.md](/Users/samuel/git-repos/lix-2/artifact/reference_state_layer_handling.md)

Why it aligns:

- parser, translation, execution, and orchestration are treated as
  distinct concerns
- the connection/orchestrator should not become the semantic owner
- strong internal seams are preferred over semantic sprawl

Why it is only partial:

- Turso keeps much of its engine inside one large `core` crate
- it does not model a separate `canonical -> live_state -> public
surfaces` ladder the way Lix does

Takeaway:

- Turso supports keeping `session`/`execution` orchestration-light and
  discourages stray middle-layer semantic owners
- it does not directly answer where derived public relation declarations
  should live

### Marten

**Alignment: mixed**

Marten is the main reference that does **not** point directly to the
recommended Plan 83 shape.

See:

- [artifact/marten/src/Marten/Events/Projections/IProjection.cs](/Users/samuel/git-repos/lix-2/artifact/marten/src/Marten/Events/Projections/IProjection.cs)
- [artifact/marten/src/Marten/Events/Daemon/ProjectionDaemon.cs](/Users/samuel/git-repos/lix-2/artifact/marten/src/Marten/Events/Daemon/ProjectionDaemon.cs)

Why it differs:

- Marten nests projection definitions under the event subsystem
- the daemon that runs them lives nearby
- the abstraction and the runtime are intentionally co-located

What still helps:

- Marten reinforces the rule that definitions should live with their
  true owner
- it argues against a generic shared type sink or a vague neutral helper
  root

Takeaway:

- if Lix believed derivation definitions were fundamentally a serving
  runtime concern, Marten would argue for moving them toward
  `live_state`
- because Lix already treats `catalog` as the owner of public relation
  meaning, Marten is informative but not decisive here

### KurrentDB

**Alignment: strong if runtime stays in `live_state`**

KurrentDB strongly supports separating rebuildable derived-state runtime
machinery from the truth owner.

See:

- [artifact/kurrentdb/src/KurrentDB.Projections.Core/ProjectionsSubsystem.cs](/Users/samuel/git-repos/lix-2/artifact/kurrentdb/src/KurrentDB.Projections.Core/ProjectionsSubsystem.cs)
- [artifact/kurrentdb/src/KurrentDB.SecondaryIndexing/SecondaryIndexingPlugin.cs](/Users/samuel/git-repos/lix-2/artifact/kurrentdb/src/KurrentDB.SecondaryIndexing/SecondaryIndexingPlugin.cs)

Why it aligns:

- projections are a separate runtime subsystem from the core engine
- secondary indexing is another separate derived-state consumer
- rebuildable serving/runtime concerns are kept distinct from truth
  storage

What it does **not** require:

- a separate semantic owner named `projections`

Takeaway:

- KurrentDB supports keeping runtime derivation execution in
  `live_state`
- it is fully compatible with moving declarative derivation ownership
  into `catalog`, as long as runtime machinery stays out of `catalog`

### Summary

Overall fit:

- DuckDB: strong support
- Turso: moderate support
- Marten: mixed
- KurrentDB: strong support

The best synthesis is:

- take **catalog-owned semantics** from DuckDB
- take **runtime derived-state separation** from KurrentDB
- keep **orchestration light** as Turso suggests
- recognize that Marten would only be a better fit if Lix wanted
  derivation declarations to belong to the serving runtime owner rather
  than the public relation owner

That leaves the Plan 83 recommendation unchanged:

**`catalog` should own declarative derived public relation meaning, and
`live_state` should own the runtime machinery that executes it.**

---

## Target Model

### `catalog/*` should own

- public relation names and registry descriptors
- built-in and schema-driven entity public relation binding
- derivation declarations for derived public relations
- derivation lifecycle declarations when they are part of public surface
  semantics
- owner-local catalog contracts for derived surfaces

Examples:

- `lix_version` public surface definition
- `lix_file` and `lix_directory` derived-surface declarations
- schema-driven entity surface declarations

### `live_state/*` should own

- hydration of declared derivation inputs
- read-time derivation execution
- write-time or async replay machinery if introduced later
- readiness, replay cursor, catch-up, serving storage
- runtime row materialization and freshness checks

### `sql/*` should own

- lowering catalog-owned relation bindings into read/write artifacts
- explain/planner support for catalog-owned derived surface bindings
- backend-specific SQL generation only as a lowering technique

### `contracts/*` should own

- only true cross-owner interchange contracts
- prepared artifacts consumed across `sql`, `session`, `execution`, and
  owner facades
- shared row/request/result types that are genuinely cross-owner

`contracts/*` should **not** absorb owner-local derivation declaration
types just because multiple files use them.

---

## Artifact Best Practices

Plan 83 adopts the following `artifact/*` rules.

### Rule 1 — owner-local types live with the owner

Examples:

- catalog-owned derivation declarations live under `catalog/*`
- live-state execution row containers live under `live_state/*`
- version-state facts live under `version_state/*`

### Rule 2 — `contracts/artifacts.rs` is only for cross-owner contracts

Examples that belong there:

- prepared public read/write artifacts
- session snapshots/deltas
- receipts and execution-facing request/response shapes

Examples that should not live there:

- catalog-owned derivation declaration structs
- live-state-only hydrated row containers
- owner-local helper enums that never cross an owner seam

### Rule 3 — do not use `artifacts.rs` as a miscellaneous type sink

If a subsystem grows more than a small handful of related artifact
types, split by role instead of continuing to grow one file.

For the current projection types, the natural split is:

- declarative spec types
- runtime hydrated input types
- runtime derived output row types

### Rule 4 — keep semantic declarations separate from runtime state

The current
[packages/engine/src/projections/artifacts.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/projections/artifacts.rs)
mixes:

- lifecycle declaration
- input specification
- served-surface specification
- hydrated execution input
- derived execution output

Those should not stay collapsed into one file after ownership is moved.

---

## Concrete File Moves

This plan assumes `projections/` is deleted as a long-lived root.

### Chosen `catalog/*` structure

Plan 83 chooses a flat `catalog/` layout instead of introducing
`catalog/views/*` or another new nested subtree.

The recommended target shape is:

- `packages/engine/src/catalog/mod.rs`
- `packages/engine/src/catalog/registry.rs`
- `packages/engine/src/catalog/binding.rs`
- `packages/engine/src/catalog/state.rs`
- `packages/engine/src/catalog/entity.rs`
- `packages/engine/src/catalog/version.rs`
- `packages/engine/src/catalog/file.rs`
- `packages/engine/src/catalog/directory.rs`

Why this shape:

- `catalog` is already the semantic owner of public relations
- the current module is small enough that extra nesting would add
  ceremony without improving clarity
- `state.rs` improves readability by pulling the built-in `lix_state`,
  `lix_state_by_version`, and `lix_state_history` surface definitions out
  of `registry.rs` without changing ownership
- `entity.rs` is more precise than `dynamic.rs` because the current
  runtime-registered catalog additions are schema-driven entity surfaces

### `catalog/*` additions and moves

Add files such as:

- `packages/engine/src/catalog/state.rs`
- `packages/engine/src/catalog/entity.rs`
- `packages/engine/src/catalog/version.rs`
- `packages/engine/src/catalog/file.rs`
- `packages/engine/src/catalog/directory.rs`

These should own:

- `state.rs`
  - built-in `lix_state*` descriptor helpers currently living in
    `registry.rs`
  - state column and type helper functions
- `ProjectionLifecycle` or a renamed catalog-owned lifecycle enum
- `ProjectionRegistration` or a renamed derivation registration type
- `ProjectionInputSpec`
- `ProjectionSurfaceSpec`
- the current declarative `impl ProjectionTrait for ...` logic, likely
  renamed to better reflect catalog ownership

`registry.rs` and `binding.rs` remain the shared catalog infrastructure
files rather than being split into a second layer of nested modules.
`state.rs` is a readability extraction, not a new owner boundary.

### `live_state/*` runtime subtree

Plan 83 keeps the existing runtime subtree name
`packages/engine/src/live_state/projection/*` for now.

Why:

- the current code already uses that name consistently
- renaming the runtime subtree is not required for the ownership move
- names like `derived_runtime` or `view_runtime` add churn without
  resolving the underlying architectural change

That subtree should continue to own:

- hydrated input row containers
- derived runtime row output containers if they are only consumed by
  `live_state` read execution
- runtime executors and replay-specific machinery

### Remove or fold `projections/*`

Delete after migration:

- `packages/engine/src/projections/mod.rs`
- `packages/engine/src/projections/traits.rs`
- `packages/engine/src/projections/artifacts.rs`
- `packages/engine/src/projections/version.rs`
- `packages/engine/src/projections/file.rs`
- `packages/engine/src/projections/directory.rs`

### Keep `contracts/artifacts.rs` focused

Do not move catalog-owned derivation declarations into
[packages/engine/src/contracts/artifacts.rs](/Users/samuel/git-repos/lix-2/packages/engine/src/contracts/artifacts.rs).

If anything moves into `contracts/*`, it should be only truly shared
read/write preparation artifacts, not semantic catalog declaration
structures.

---

## Migration Steps

### Phase A — Introduce catalog-owned derivation declarations

- [x] Add catalog-owned declaration traits/types beside the existing
      `projections/*` types.
- [x] Pull built-in `lix_state*` descriptor helpers out of `registry.rs`
      into flat `catalog/state.rs`.
- [x] Port `lix_version` first into flat `catalog/version.rs`.
- [x] Keep `live_state` runtime reading through the new catalog-owned
      registry while preserving current behavior.

### Phase B — Move runtime-only row containers into `live_state`

- [x] Move hydrated input row containers out of the old projections
      owner.
- [x] Move derived runtime row carriers if they are only used by
      `live_state` and read execution.
- [x] Keep the existing `live_state/projection/*` runtime subtree name
      for now.
- [x] Keep root-level live-state entrypoints stable.

### Phase C — Delete the old `projections` root

- [x] Remove top-level `projections/*` imports from engine startup,
      session, and tests.
- [x] Replace structure tests to enforce the new owner boundary:
      catalog-owned derivation declarations under flat `catalog/*`
      runtime derivation execution under `live_state/*`.
- [x] Delete compatibility re-exports.

### Phase D — Split generic artifact buckets

- [x] Rename or split owner-local `artifacts.rs` files where they are
      now too broad.
- [x] Leave `contracts/artifacts.rs` only with cross-owner contracts.
- [x] Prefer role-based file names over generic `artifacts.rs` once an
      owner has more than one coherent artifact family.

### Phase E — Remove the remaining `catalog` -> `live_state` seam leak

- [x] Stop importing `live_state::projection::*` runtime row carriers
      from `catalog/*`.
- [x] Introduce catalog-owned semantic derive input/output shapes so
      declaration APIs no longer mention runtime hydration carriers.
- [x] Make `live_state` adapt hydrated runtime rows into the
      catalog-owned derive input shape before calling declaration code.
- [x] Restore a truthful sealed owner boundary after the new seam is in
      place.

### Phase F — Finish or explicitly defer filesystem declaration

- [ ] Decide whether `lix_file` and `lix_directory` are in-scope for
      Plan 83 completion.
- [ ] If in-scope, replace placeholder empty derives with real
      declaration-backed behavior.
- [ ] If out-of-scope, mark those declarations as deferred and exclude
      them from the completion claim for Plan 83.
- [ ] Remove transitional migration wording once the authoritative owner
      is settled.

---

## Acceptance Criteria

Plan 83 is complete when all of the following are true:

- `catalog` owns declarative derived-surface meaning for built-in public
  derived relations
- `catalog` declaration APIs do not import or expose
  `live_state::projection::*` runtime carriers
- `live_state` owns hydration and execution of those declarations
- no production derivation-definition implementations live outside
  `catalog/*`
- no production runtime derivation execution logic lives outside
  `live_state/*`
- `lix_version` is catalog-owned without transitional shims or
  migration-only compatibility language
- every Plan 83 surface either has a real catalog-owned declaration path
  or is explicitly deferred from the plan's completion scope
- `contracts/artifacts.rs` does not contain catalog-owned derivation
  declaration types
- `packages/engine/src/projections/*` no longer exists as an
  architectural root
- structure tests enforce the new split

---

## Non-Goals

- changing canonical truth ownership
- moving `sql` lowering into `catalog`
- collapsing `catalog` and `live_state` into one owner
- preserving the `projections` root as a long-lived alias

---

## Recommendation Summary

If Lix wants the cleanest long-term model, the right move is:

- `catalog` owns derived public surface declarations
- `live_state` owns runtime derivation execution
- `contracts/*` stays cross-owner only
- broad `artifacts.rs` files get split by owner role

This keeps the semantic model small and the runtime model boring, which
is the combination the reference systems point toward.
