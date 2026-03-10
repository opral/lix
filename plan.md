# Engine Semantic Rewrite Plan

## Objective

Rebuild `packages/engine` around one coherent semantic architecture that is explicit about:

- logical query meaning
- effective-state resolution
- write legality
- authoritative invariant checking
- commit generation
- projections and materialization

Reasoning:

The current engine still spreads the same semantics across AST rewrites, vtable lowering, followup reconstruction, validation queries, and effect detection. That creates semantic drift, makes concurrency bugs hard to reason about, and forces behavior to be inferred from SQL shape instead of from explicit domain meaning.

## Scope

- This is an intentionally breaking rewrite.
- Backward compatibility with the current internal pipeline is not a goal.
- Obsolete tests should be deleted, not adapted.
- The public SQL entrypoint remains the vtable/public-surface layer.
- Optimization must not bypass the public surface model by teaching callers to hit storage tables directly.

Reasoning:
The engine should keep one public contract and one internal implementation model. Preserving old rewrite boundaries would only lock in the current drift.

## Architectural Thesis

The engine should have:

1. one canonical semantic model before backend-specific lowering
   Why: core semantics should be decided once before SQL is emitted, but day 1 does not require a fully general optimizer IR.
2. one shared effective-state resolver used by both reads and writes
   Why: visibility, overlay, tombstone, and winner semantics must exist exactly once.
3. one authoritative tracked write boundary based on domain changes, not materialized rows
   Why: in an event-sourced system, the commit/event batch is the write truth and projections are derived outputs.
4. one conservative proof engine for the scope, schema, and any target bounds the live path actually needs
   Why: legality and pushdown should come from the same semantic facts, but unsafe inference must fail closed.
5. one explicit concurrency/precondition model for tracked writes
   Why: bounded scope alone does not prevent races or stale writes.

## Non-Goals

- Do not build a general cost-based optimizer in the first rewrite.
- Do not preserve the old multi-pass SQL rewrite engine as the main path.
- Do not use read models or materialized state as the authority for tracked write decisions.
- Do not infer cache invalidation or semantic effects from emitted SQL text.

Reasoning:
The first rewrite should simplify and harden the engine. A small rule-based planner with strong semantics is better than a large optimizer that reintroduces hidden complexity.

## Hard Rules

1. After binding and surface resolution, no phase may infer core semantics by scanning SQL text.
2. Proofs are conservative. If the proof engine cannot prove a property, it must return `Unknown` or `Unbounded`.
3. Tracked writes decide domain changes from authoritative pre-state, not from followup `RETURNING` rows.
4. Materialized tables and read models are outputs, never the source of truth for tracked semantics.
5. Every tracked write carries explicit `CommitPreconditions`.
6. Side effects, cache invalidation, and notifications derive from semantic effects, not SQL inspection.
7. All new SQL rewrite code lives under `src/sql2/` and must not depend on legacy rewrite, followup, classifier, or planning modules in `src/sql/`.
8. Shared parser, binder, AST, lowering, catalog, and contract primitives should only be extracted when `sql2` actually needs them.
9. If shared SQL primitives need to serve both paths during migration, move them into a neutral shared module instead of forcing an upfront `sql2` re-home.
10. `src/sql/` is frozen as a legacy reference during migration; a fully self-sufficient `sql2/` tree is an end state, not a phase-1 prerequisite.

Reasoning:
These rules directly eliminate the classes of drift visible today in AST-shape checks, followup reconstruction, validation re-queries, and post-hoc effect detection.

## Target Execution Model

### Day-1 activation slice

The first live cut of the new planner only needs this authority chain:

1. `SurfaceBinding`
2. canonical state-backed scan or `WriteCommand`
3. `EffectiveStateResolver`
4. `ResolvedWritePlan`
5. `DomainChangeBatch`
6. `CommitPreconditions`
7. `append_commit_if_preconditions_hold(...)`
8. lowered SQL and derived projection updates plus materialization

Reasoning:
This is the smallest slice that makes the new architecture real. It fixes the event-sourced authority problem first and lets larger read-planner abstractions grow later without keeping the old write path alive.

### Read path

1. Parse SQL into AST.
2. Bind parameters into a typed `BoundStatement`.
3. Resolve public surfaces into `SurfaceBinding`s.
4. Lower into a canonical semantic scan/command form.
5. Attach semantic facts and conservative proofs.
6. Resolve effective state through a shared resolver.
7. Perform rule-based pushdown where proven safe.
8. Lower to backend SQL.
9. Execute.
10. Derive read dependencies and cache metadata from the semantic plan.

Reasoning:
Reads need one canonical semantic form before lowering. A broader logical IR can grow later, but the first cut only needs enough structure to share effective-state semantics and preserve residual filtering correctly.

### Tracked write path

1. Parse, bind, and resolve surfaces.
2. Build a `WriteCommand` that describes the target surface, mutation kind, selector, payload, mode, and execution context.
3. Prove scope, schema, and any required target bounds conservatively.
4. Resolve authoritative pre-state through the same effective-state resolver used by reads.
5. Build a `DomainChangeBatch` plus `CommitPreconditions`.
6. Run pure batch-local invariant validation on that batch.
7. Append/generate the commit from the proven batch, re-checking current-state-dependent invariants inside the append transaction.
8. Project derived surfaces and materialize persisted read models from the committed domain changes.
9. Emit semantic effects, invalidations, and notifications.

Reasoning:
This order matches event-sourced best practice. The authoritative write artifact is the domain change batch plus its preconditions, not a patchwork of materialized-row updates that are converted back into changes later.

### Untracked write path

1. Reuse the same `SurfaceBinding`, `WriteCommand`, proof types, and `ResolvedWritePlan` as tracked writes.
2. Diverge only after semantic write resolution.
3. Skip `DomainChangeBatch`, `CommitPreconditions`, and commit append.
4. Lower the resolved untracked write directly to backend SQL.
5. Still derive semantic effects from the resolved write plan, not from SQL text.

Reasoning:
Untracked writes do not need the full event-sourced pipeline, but they must still share the same semantic write path up to `ResolvedWritePlan`. Otherwise the engine will drift back into tracked and untracked write systems with different legality and targeting behavior.

## Core Semantic Model

### 1. Bound statement

Introduce a typed bound representation:

- `BoundStatement`
  - statement kind
  - bound parameters
  - normalized scalar literals
  - execution context

Reasoning:
The binder should be the last phase that knows about placeholders and raw surface syntax. Later phases should consume normalized values, not parameter positions.

### 1a. `sql2` core modules

Before the rewrite goes deep, introduce the `sql2` modules the new planner actually owns directly:

- `src/sql2/core/`
  - parser entrypoints
  - AST utilities
  - placeholder binding
  - generic logical-function parsing/lowering
  - shared contracts that are not rewrite-engine-specific

If `sql2` needs a generic SQL primitive that is currently trapped under legacy `src/sql/**`, extract that primitive into a neutral shared module at that time rather than front-loading a full re-home.

The new rewrite may depend on `sql2/core/**` and neutral shared SQL modules, but must not depend on legacy rewrite, followup, classifier, or planning modules under `src/sql/**`.

Reasoning:
The right clean-slate boundary is between `src/sql2/**` and the legacy rewrite/followup/classifier stack, not between the new rewrite and every reusable primitive that currently happens to live under `src/sql/**`. Fixing the semantic authority chain does not require a second upfront migration of every parser/AST/binder/lowering helper.

### 2. Surface binding

Introduce:

- `SurfaceRegistry`
  - maps public relation names to surface descriptors
  - owns the only live registry of public surfaces in `sql2/catalog/**`
  - contains both builtin surfaces and dynamic schema-derived surfaces
  - tracks a `CatalogEpoch` for dynamic surface descriptors
  - resolves aliases and visible column contracts
  - binds each reference to one surface family plus family-local variant

- `SurfaceDescriptor`
  - public relation name
  - surface family
  - surface variant
  - visible and hidden columns
  - writable/read-only capability
  - default scope semantics
  - surface traits and defaults

- `SurfaceBinding`
  - resolved surface descriptor
  - bound `CatalogEpoch` for dynamic surfaces where applicable
  - exposed columns
  - writable/read-only capability
  - default scope semantics
  - implicit overrides
  - storage/resolution capabilities

- `SurfaceVariant`
  - family-local variants such as `Default`, `ByVersion`, `History`, `Active`, and `WorkingChanges`

Reasoning:
Surface meaning should be resolved once as family plus variant plus defaults. Public surfaces are not syntax sugar for each other, but they also should not each require a unique compiler identity when one canonical family scan is enough.

Hard rule:

- `sql2/catalog/**` owns one `SurfaceRegistry`
- no second live registry, table catalog, or statement classifier may define public-surface meaning in parallel
- legacy registries remain frozen migration references only and are deleted at cutover

Bootstrap rule:

- builtin surface descriptors load first
- dynamic schema-derived surface descriptors load second from an authoritative state source
- that dynamic descriptor load must not go through public-surface planning
- public admin surfaces such as `lix_stored_schema` are planner outputs, not planner bootstrap inputs

Reasoning:
The planner must not need the public surface system to discover the public surface system. Dynamic surfaces come from stored schema state, but the bootstrap path for loading those descriptors must read authoritative state directly and then register the resulting descriptors into `SurfaceRegistry`.

Dynamic-surface invalidation rule:

- commits that change stored schema descriptors must invalidate dynamic surface descriptors
- `SurfaceBinding`s and prepared/bound statements that depend on dynamic surfaces must carry the `CatalogEpoch` they were bound against
- if that epoch no longer matches the live registry, the statement must be rebound before execution
- v1 may satisfy this conservatively by rebinding dynamic surfaces per statement instead of keeping long-lived dynamic-surface bindings

Reasoning:
This is about catalog staleness, not general schema evolution. Dynamic surfaces can appear or change when stored schemas change, so the planner needs one explicit invalidation contract or stale bindings become inevitable.

Reasoning:
The current engine spreads public-surface knowledge across a table registry, surface classifiers, and dynamic entity-view resolution. The new architecture should collapse that into one `sql2/catalog/**` registry so surface meaning cannot drift between subsystems.

### 2a. Surface families

Public surfaces should be grouped into semantic families that share underlying machinery without rewriting into each other as public SQL:

- `StateFamily`
  - `lix_state`
  - `lix_state_by_version`
  - `lix_state_history`
- `EntityFamily`
  - schema-derived entity views such as `lix_key_value`, `lix_entity_label`, and their `_by_version` / `_history` variants
- `FilesystemFamily`
  - `lix_file`
  - `lix_file_by_version`
- `AdminFamily`
  - `lix_version`
  - `lix_active_version`
  - `lix_stored_schema`
  - `lix_active_account`
- `ChangeFamily`
  - `lix_change`
  - `lix_working_changes`

Reasoning:
Families let surfaces share one canonical family scan plus effective-state logic without recreating the old architecture where one public surface is implemented as a rewrite into another public surface.

### 2b. Semantic kernels

Public surface families do not all need distinct semantic kernels.

Day 1 should use these kernels:

- state-backed kernel
  - `CanonicalStateScan`
  - used by `lix_state*`
  - used by `lix_entity*` via schema-driven projection/defaults
  - used by filesystem-style state wrappers where semantics are still state-backed
- admin/version surfaces stay on the frozen legacy path in v1
- add a lightweight `AdminScan` or `CanonicalStateScan + AdminProjectionSpec` only after the state kernel has settled and a concrete simplification case exists

Reasoning:
The current engine already treats entity views as wrappers over state semantics. The cleanest architecture keeps one canonical state kernel for state-backed surfaces in the first cut and only adds admin/version execution semantics after the authority chain is live.

### 2b1. Semantic data model

The planner should make the data model explicit:

- authoritative log
  - commits, change records, and snapshots
  - this is the write truth
  - internal committed change storage such as `lix_internal_change` plus `lix_internal_snapshot` belongs here
  - public change surfaces such as `lix_change` are projections over this log, not the log authority itself
- canonical resolved state
  - effective current rows derived from the authoritative log plus overlay rules
  - this uses one shared state row model across flexible schemas
  - `lix_state*` exposes this directly
- state-backed derived surfaces
  - `lix_entity*` is schema projection/defaults over canonical resolved state
  - filesystem-style surfaces are derived projections over state-backed descriptors plus external payload stores such as CAS/blob storage
  - `lix_version` is an admin projection over global state rows such as version descriptors and version pointers
  - `lix_active_version` is an admin projection over global state rows with schema key `lix_active_version`
  - `lix_stored_schema` is an admin projection over canonical state rows for stored schema descriptors
  - `lix_active_account` is an admin projection over global state rows with schema key `lix_active_account`
  - keep other version/admin surfaces separate only if they are not actually reducible to canonical state semantics

Commit rows may be represented in storage through the same general change machinery, but semantically a commit is still the append boundary that makes a batch of changes authoritative. It should not be treated as just another current-state row.

Reasoning:
The clean architecture is not “everything is one SQL view.” It is “one authoritative log, one canonical resolved state model, and projections over those.” The universal API story comes from the shared row model for state, not from forcing every public surface into the same semantic kernel.

### 2c. Surface compilation rule

The new planner must not model surfaces like this:

- `lix_state` as a SQL rewrite to `lix_state_by_version`
- `lix_version` as a SQL rewrite to `lix_state_by_version`
- entity or filesystem surfaces as SQL rewrites to `lix_state`
- public surfaces as semantic wrappers over `lix_internal_state_vtable`

Instead, it should model them like this:

- each public surface binds directly to a `SurfaceDescriptor`
- family plus variant determine the canonical family scan/spec
- the family-level canonicalization step may canonicalize multiple public surfaces in the same family into one canonical semantic scan/spec
- the canonical family scan/spec then lowers into shared logical semantics
- the backend lowerer then emits physical SQL against internal storage relations

Reasoning:
Public surfaces should be sibling semantic entrypoints over shared machinery, not a chain of SQL rewrites over other public surfaces or internal vtables. Semantic canonicalization within a family is desirable because it reduces compiler duplication while keeping the public contract explicit.

### 2d. Canonical family scans

Each semantic kernel should converge to one canonical scan/spec where practical.

Examples:

- state-backed kernel
  - `lix_state` canonicalizes to `CanonicalStateScan` with:
    - `version_scope = ActiveVersion`
    - `expose_version_id = false`
  - `lix_state_by_version` canonicalizes to the same `CanonicalStateScan` with:
    - `version_scope = Explicit`
    - `expose_version_id = true`
- entity views
  - `lix_entity` canonicalizes to `CanonicalStateScan + EntityProjectionSpec` with:
    - `version_scope = ActiveVersion`
    - schema-driven projection/defaults
    - version columns hidden by default
  - `lix_entity_by_version` canonicalizes to the same `CanonicalStateScan + EntityProjectionSpec` with:
    - `version_scope = Explicit`
    - schema-driven projection/defaults
    - version columns exposed
This canonicalization is semantic, not SQL-shaped:

- it does not rewrite one public surface into another public SQL surface
- it does not force explain output to pretend that one public surface is implemented as another
- it gives proofs, effective-state resolution, and lowering one canonical form per semantic kernel

Reasoning:
This keeps the simplification benefits of canonicalization without recreating the old architecture where behavior is encoded as stacked public-surface rewrites. Entity surfaces stay lightweight wrappers over state semantics instead of becoming a second core scan model.

### 3. Logical relational IR

Longer-term, the planner may grow into one logical algebra:

- `RelExpr`
  - `SurfaceScan`
  - `Project`
  - `Filter`
  - `Join`
  - `Aggregate`
  - `Sort`
  - `Limit`
  - `Union`
  - `Values`

- `ScalarExpr`
  - bound literals
  - column refs
  - normalized comparisons
  - deterministic scalar functions

- `RelProps`
  - output schema
  - uniqueness facts
  - cardinality bounds where known
  - scope facts
  - pushdown-safe predicate classes

Reasoning:
`ReadIntent` alone is too narrow for a mature planner. But this broader algebra is an expansion path, not a day-1 requirement.

`RelExpr::SurfaceScan` should carry a `SurfaceBinding`, not just a table name.

Reasoning:
The logical plan leaf is the public semantic surface, not a backend relation or a rewritten intermediate surface.

### 3a. Day-1 semantic core

The first live slice only needs:

- `CanonicalStateScan`
- `ReadPlan`
- `EntityProjectionSpec`
- `WriteCommand`
- `ResolvedWritePlan`
- `DomainChangeBatch`
- `CommitPreconditions`

Reasoning:
This is enough to establish the new semantic authority chain without requiring a full optimizer framework before the core write-path bug is fixed. The key addition on the read side is a tiny relational shell so query shape does not fall back to ad hoc AST/lowering behavior outside the semantic boundary.

### 4. Read command

Use a tiny day-1 relational shell for reads:

- `ReadPlan`
  - `Scan`
    - canonical state-backed scan root
  - `Filter`
    - residual or pushdown-candidate predicate over a child plan
  - `Project`
    - output column and expression shaping over a child plan
  - `Sort`
    - explicit ordering over a child plan
  - `Limit`
    - limit/offset over a child plan

Reasoning:
The first cut does not need a full optimizer algebra, but it does need an explicit boundary for basic query shape. Otherwise the engine will recreate the current split between semantic state logic and “everything else” handled implicitly in AST rewrites or lowering.

Represent reads as:

- `ReadCommand`
  - `ReadPlan` root
  - `ReadContract`
    - v1: `CommittedAtStart`
  - requested commit mapping

Reasoning:
The read command should capture semantic intent only. Day 1 keeps that intent small but explicit: canonical scan semantics plus filter/project/sort/limit shape. Predicate placement, dependency analysis, and backend capability decisions are planned artifacts derived later.

- `PlannedRead`
  - `ReadCommand`
  - proofs and derived properties
  - chosen `StateSource`
  - pushdown predicates and residual predicates
  - dependency requirements
  - backend pushdown decisions

Reasoning:
Separating `ReadCommand` from `PlannedRead` keeps the semantic contract small while still making planning and explain output explicit.

V1 restriction:

- state-backed reads that fit `Scan -> Filter -> Project -> Sort -> Limit` migrate first
- read shapes that truly require `Join`, `Aggregate`, `Union`, or other broader relational operators may stay on the frozen legacy path until the shell expands

Reasoning:
This keeps the day-1 read model honest. The shell is large enough to prevent semantic leakage back into AST rewriting, but still small enough to avoid building a full optimizer before the authority chain is stable.

### 5. Write command

Represent writes as:

- `WriteCommand`
  - operation kind: `Insert | Update | Delete`
  - target `SurfaceBinding`
  - selector over canonical state rows
  - mutation payload
    - full snapshot replacement
    - patch
    - tombstone
  - tracked vs untracked mode
  - execution context

Reasoning:
Writes still need a selector and a mutation payload, but they are semantic commands over logical rows, not planned or lowered execution artifacts.

- `PlannedWrite`
  - `WriteCommand`
  - scope/schema proofs and any required target proofs
  - chosen `StateSource`
  - `ResolvedWritePlan`
  - optional `CommitPreconditions`
  - residual execution predicates
  - backend pushdown/lowering decisions

Reasoning:
Separating `WriteCommand` from `PlannedWrite` keeps legality, source choice, and append preconditions in one planned artifact instead of smearing them across command and lowering layers. Tracked and untracked writes share the same semantic planning object and diverge only after `ResolvedWritePlan`.

V1 tracked-write restriction:

- a tracked write must bind to exactly one authoritative `WriteLane`
- admissible tracked lanes in v1 are:
  - `ActiveVersion`
  - `SingleVersion(version_id)`
  - `GlobalAdmin`
- tracked writes that bind to multiple version lanes, multiple authority lanes, or any `FiniteVersionSet` with more than one lane are rejected at bind/planning time

Reasoning:
`CommitPreconditions` are intentionally single-lane in v1. The planner must reject multi-lane tracked statements before execution rather than pretending they fit the v1 optimistic-concurrency model.

## Semantic Facts and Proofs

### V1 minimal proofs

Day 1 only needs proof objects that are required for tracked-write legality:

- `ScopeProof`
  - `ActiveVersion`
  - `SingleVersion(version_id)`
  - `FiniteVersionSet(set)`
  - `Unbounded`
  - `Unknown`

- `SchemaProof`
  - exact schema set
  - unknown schema set

- `TargetSetProof`
  - only introduce this in v1 if the tracked write path truly needs it
  - start narrow:
    - exact known target
    - unknown target

Reasoning:
The engine needs to distinguish “safe to prove” from “unsafe to guess,” but the first cut only needs enough proof structure to make tracked writes legal or reject them safely. Commit preconditions are still explicit, but they are derived later from the planned write and enforced at the append boundary rather than living inside the proof lattice.

### V1 conservative proof rules

The v1 proof engine should positively support only:

- conjunctions
- direct equality
- `IN (...)` over explicit literals or bound values
- surface defaults and active-version defaults

The v1 proof engine should treat these as `Unknown` or `Unbounded`:

- `OR`
- `NOT`
- subqueries
- joins
- non-deterministic functions
- user-defined functions
- predicates derived only from emitted SQL shape

Reasoning:
This keeps the proof engine sound and matched to the day-1 scope. It is better to reject some legal writes than to prove a false scope and corrupt data.

### Later proof expansion

After the tracked write path is stable, the proof engine may expand to support:

- `Global` scope where a real write or read path needs it
- richer `SchemaProof` variants
- broader `TargetSetProof` categories
- constant-foldable subqueries
- key-preserving join-derived proofs
- read-pushdown-oriented proof classification

Reasoning:
These are useful planner features, but they should be added because the live engine needs them, not because the first core type section promised a larger proof lattice than day 1 can justify.

### Correctness vs pushdown

The proof engine decides only which predicates may move earlier in the plan. It does not decide whether a predicate is applied at all.

Rules:

- every user predicate must survive into the final logical plan unless it is proven redundant
- predicates that are proven safe for early evaluation become pushdown predicates
- predicates that are not proven safe stay as residual predicates above effective-state resolution
- the backend SQL engine executes both the pushed predicates and the residual predicates in the lowered SQL

Reasoning:
`lix` owns semantic correctness. SQLite/Postgres owns faithful execution of the lowered SQL. A narrow pushdown policy is safe because unproven predicates are retained later in the plan, not dropped.

Example shape:

```sql
WITH effective_state AS (
  -- local/global/untracked/tracked resolution
  -- tombstones and winner selection
)
SELECT *
FROM effective_state
WHERE <residual user predicate>;
```

If part of a predicate is proven safe to push earlier:

```sql
WITH candidate_rows AS (
  SELECT *
  FROM raw_state
  WHERE file_id = 'f'
),
effective_state AS (
  -- resolve overlay and winners from candidate_rows
)
SELECT *
FROM effective_state
WHERE json_extract(snapshot_content, '$.x') = 1;
```

Reasoning:
This is how the engine avoids false results. The backend can evaluate raw SQL predicates correctly, but it does not know `lix` semantics such as active-version defaults, overlay precedence, or tombstone visibility. `lix` must decide where predicates belong relative to semantic resolution. Conservative pushdown only reduces optimization opportunity; it must not change result correctness.

## Backend Execution Contract

### Ownership boundary

- `lix` owns semantic planning
- `lix` lowers a complete SQL program that preserves `lix` semantics
- SQLite/Postgres own faithful execution of that lowered SQL
- backend optimizers may improve execution of the lowered SQL, but they do not discover `lix` semantics

Reasoning:
This is the core planner/backend split. `lix` defines meaning; the backend executes that meaning.

### Lowered program contract

The planner must always emit a complete executable SQL program with:

- pushed predicates that are proven safe below semantic boundaries
- residual predicates retained above semantic boundaries
- backend-specific lowering only after semantic structure is fixed

Reasoning:
Narrow pushdown is safe only if the final lowered SQL still contains all non-redundant user predicates.

### Backend execution modes

- `Plan`: show the `lix` semantic plan and the lowered SQL
- `BackendPlan`: ask the backend to explain the lowered SQL
- `AnalyzeReadOnly`: optionally run backend analyze/explain for read-only queries only

Reasoning:
The explain surface should reflect both levels of planning without conflating them.

## Backend Lowering Contract

### Day-1 contract

Start with a narrow backend contract:

- `BackendLowerer`
  - lowers canonical semantic plans into backend SQL
- `PushdownSupport`
  - `Exact`
  - `Inexact`
  - `Unsupported`
- `PushdownDecision`
  - accepted pushed predicates
  - rejected predicates with reasons
  - required residual predicates

Reasoning:
Mature planners start with a small contract for pushdown and residual filtering. Day 1 does not need a large backend capability taxonomy as long as movable predicates fail closed and residual filtering stays explicit.

### Policy

- the proof engine decides whether a predicate is semantically movable
- the backend contract decides whether the backend can execute that moved predicate exactly
- `Inexact` and `Unsupported` both keep the predicate as a residual filter
- the final lowered SQL always preserves all non-redundant predicates

Reasoning:
Semantic proof without backend equivalence is unsafe, and backend equivalence without semantic proof is also unsafe. A small `Exact / Inexact / Unsupported` contract is enough to keep the first cut correct.

## Shared Effective-State Resolver

### Core abstraction

Create one shared semantic resolver:

- `EffectiveStateRequest`
  - schema set
  - version scope
  - whether global overlay is included
  - whether untracked overlay is included
  - whether tombstones participate
  - predicate classes
  - required columns

- `EffectiveStatePlan`
  - canonical source relations
  - overlay lanes
  - winner-selection semantics
  - pushdown-safe predicates
  - post-resolution predicates

- `ResolvedStateRows`
  - resolved visible rows
  - hidden/shadowed rows where needed for writes
  - lineage metadata for commit mapping and validation

Reasoning:
The engine needs one place that defines visibility and overlay semantics. Reads and writes should both call this resolver, but they should not share a giant all-purpose planner object.

### Source-of-truth contract

The shared resolver must not be just one SQL builder over one physical source. It needs an explicit split between:

- `StateSource`
  - loads candidate rows from a declared authority
  - examples:
    - authoritative committed source for state-backed reads and tracked writes
    - untracked source where relevant
- `OverlayResolver`
  - applies local/global/untracked/tombstone semantics
  - computes winner ordering
  - projects resolved rows into requested surface scope
- `EffectiveStateResolver`
  - orchestrates `StateSource + OverlayResolver`
  - exposes one semantic contract to reads and writes without forcing them to share one physical read path

Reasoning:
If the shared resolver is just a shared SQL builder over a projection-backed source, the rewrite can accidentally preserve today’s projection-based authority model. The shared part must be the overlay and winner semantics; the source of candidate rows must remain explicit.

### Authority rules

- tracked writes must use an authoritative `StateSource`
- tracked write legality and commit generation must not depend on projection/materialized state as authority
- v1 planner reads use `CommittedAtStart` consistency only
- each semantic kernel chooses exactly one authoritative `StateSource` in v1
- planner-managed reads must use that one authoritative `StateSource` in v1
- proving equivalence between alternative read sources is out of scope for v1
- best-effort or lagging projection reads are out of scope until the planner introduces an explicit freshness contract
- all `StateSource`s must feed the same `OverlayResolver` semantics

Reasoning:
This removes the hardest hidden correctness burden from v1. The first cut should not depend on proving that two different read sources are semantically equivalent for the same semantic kernel.

### V1 read consistency

For the first version of the new query planner, adopt one explicit read-consistency model:

- `CommittedAtStart`
  - a read must reflect every commit durably committed before the read began
  - it does not need to include commits that finish after the read starts

This means:

- exactly one authoritative source qualifies per semantic kernel in v1
- lagging, projected, async, or alternative read sources do not qualify in v1
- alternative read sources can be introduced later only behind an explicit equivalence/freshness contract

Reasoning:
`CommittedAtStart` is only simple if the planner reads from one clearly defined authority. Allowing multiple “equivalent” authorities in v1 would reintroduce exactly the semantic burden the rewrite is trying to remove.

### Required semantics

Define once:

- local tracked rows vs global tracked rows
- local untracked rows vs global untracked rows
- tombstone visibility and winner effects
- overlay precedence
- projection of resolved rows onto requested surface/version scope
- lineage needed for commit mapping and mutation planning

Reasoning:
These semantics are the core of the engine. If they exist in more than one place, the rewrite will fail regardless of file layout.

### Separation of concerns

- `EffectiveStateResolver` owns semantic resolution.
- `ReadPlanner` consumes resolved state for reads.
- `WriteResolver` consumes resolved state for writes.

Reasoning:
One shared semantic kernel is good. One giant shared planner is not. Reads and writes have different downstream concerns and should not be forced through identical physical planning.

## Authoritative Tracked Write Boundary

### New authority model

All writes should produce:

- `ResolvedWritePlan`
  - authoritative pre-state rows
  - intended post-state rows
  - tombstones
  - lineage and target metadata

Tracked writes should additionally produce:

- `DomainChangeBatch`
  - change records to commit
  - affected write lane
  - writer metadata
  - semantic effects

- `CommitPreconditions`
  - `WriteLane`
  - `ExpectedTip`
  - `IdempotencyKey`

Reasoning:
The write truth must be the change batch plus its preconditions. Materialized rows are projections; they should not be the object that commit generation has to rediscover semantics from.

### Commit boundary

`generate_commit()` should remain a pure commit-construction step. It should accept only:

- already-proven `DomainChangeBatch`
- already-validated invariants
- authoritative current boundary context loaded at append time

It should not:

- discover scope
- discover target versions
- compensate for missing validation
- inspect materialized SQL rewrites

Reasoning:
Commit generation should be mechanical. If it is still repairing missing semantic information, the architecture is not actually separated.

### Atomic append boundary

Add one explicit transactional boundary:

- `append_commit_if_preconditions_hold(tx, validated_batch, commit_preconditions) -> AppendCommitResult`

This boundary is responsible for:

1. loading the authoritative current tip for the command's `WriteLane` inside the same transaction that will append the commit
2. rejecting unknown or missing versions instead of seeding optimistic defaults
3. checking `ExpectedTip` atomically against current storage state
4. enforcing idempotency keys atomically
5. re-running every current-state-dependent invariant check inside that same transaction snapshot
6. calling `generate_commit()` only after preconditions and append-transaction invariants hold
7. appending commit/change rows and updating version/global pointers in the same transaction
8. making materialization consume the committed batch, not participate in deciding whether the commit is valid

Reasoning:
Without one explicit append boundary, preconditions are only documentation. Correctness requires one place that closes stale-write races by checking preconditions and performing commit append under the same transaction.

## Concurrency and Idempotency

### Required model

Every tracked write must produce one `CommitPreconditions` object:

- `WriteLane`
  - `ActiveVersion`
  - `ExplicitVersion(version_id)`
  - `GlobalAdmin`

- `CommitPreconditions`
  - exactly one `WriteLane` per command
  - `ExpectedTip(commit_id)` or an explicit create-if-missing variant where the command is allowed to create the lane
  - `IdempotencyKey(key)`

Reasoning:
Scope legality answers “may this write target these rows?” Concurrency answers “is this write still valid against the current tip of one concrete write lane?” Both are required.

V1 legality rule:

- `FiniteVersionSet` may remain a useful proof result for reads and later planner growth
- but tracked writes in v1 may only continue if planning can collapse them to exactly one authoritative `WriteLane`
- otherwise the planner must reject the statement before execution begins

Reasoning:
This keeps the narrowed concurrency model honest. A bounded multi-version proof is not enough if the append boundary only supports one authoritative lane per tracked command.

### Atomic enforcement point

Preconditions must be enforced at exactly one point: the transactional append boundary that both verifies the current tip for the command's `WriteLane` and writes the new commit/pointer state.

Required ordering:

1. start transaction
2. load the current authoritative tip for the command's `WriteLane`
3. reject missing versions or missing lanes unless the command is explicitly a creation path that proved it may create them
4. compare the current tip with `CommitPreconditions`
5. enforce idempotency key uniqueness or replay semantics
6. re-run current-state-dependent invariant checks against that same transaction snapshot
7. invoke `generate_commit()` using the authoritative current context that was just checked
8. append commit/change rows
9. update version/global pointers in the same transaction
10. commit transaction
11. run materialization and post-commit effects from the committed batch

Reasoning:
This is the actual race-closing sequence. If preconditions are checked before this boundary or pointer updates happen outside it, stale writes are still possible.

### Later expansion

If later commands truly need broader coordination, the planner may add:

- multi-boundary preconditions
- revision-based preconditions in addition to `ExpectedTip`
- explicit no-intervening-write constraints across multiple lanes

Reasoning:
These are useful end-state tools, but they should be earned by real commands that need them. V1 should stay small and centered on one optimistic-concurrency lane per command.

### Write behavior

- Reject tracked writes that cannot produce a safe `CommitPreconditions` set.
- Reject tracked writes when the transactional append boundary observes tip drift on the selected `WriteLane`.
- Make retries idempotent where possible.
- Never rely on read-model staleness windows for write correctness.

Reasoning:
Without this, the rewrite hardens syntax but still allows stale semantic decisions under concurrent writers.

## Invariant Enforcement

### V1 invariant classes

Model v1 invariants in three classes:

1. Pure batch-local checks
   - payload/schema JSON validation
   - `packages/engine/src/schema/definition.json` validation for stored schema registration payloads
   - schema registration metadata shape checks
   - primary-key/entity-id consistency derived from `x-lix-primary-key`
   - any other invariant that depends only on the intended batch plus already-bound schema metadata

2. Append-transaction checks
   - uniqueness derived from `x-lix-primary-key` and `x-lix-unique`
   - immediate foreign key existence derived from `x-lix-foreign-keys` with immediate mode
   - version existence for resolved version scopes
   - immutable update/delete checks and any other invariant that depends on current committed state
   - these must be re-read inside `append_commit_if_preconditions_hold(...)` against the same transaction snapshot used for tip checks

3. Physical constraints and triggers
   - backend uniqueness constraints, FK constraints, indexes, and triggers used as defense in depth or as selected primary enforcement where simpler

Reasoning:
Not every invariant has the same race profile. Batch-local checks should happen before append because they do not depend on current storage state. State-dependent checks must be re-run inside the append transaction or they still race with concurrent writers.

### Authoritative invariant checker

Create `AuthoritativeInvariantChecker` over the final `DomainChangeBatch` and authoritative pre-state:

- pure batch-local checks before append
- append-transaction checks inside `append_commit_if_preconditions_hold(...)`
- any generated backend constraint/triggers needed for race resistance

Reasoning:
Invariants should be checked against the actual intended mutation set, not against partially rewritten SQL or post-hoc followup rows.

### Physical constraints

Use backend constraints, indexes, and triggers as defense in depth where they simplify enforcement.

Reasoning:
The semantic checker remains the source of truth for intended mutations, but backend constraints are still valuable for race resistance and operational safety.

## Read Planning and Lowering

### Read optimization

Keep the first version rule-based:

- normalize predicates
- push predicates only when proven safe
- exploit exact/bounded schema and target proofs
- use uniqueness facts to simplify joins and limits where safe

Reasoning:
A deterministic rule-based planner is enough to gain most of the simplification. Cost-based planning can be added later if the logical boundary is clean.

### Proofs plus residuals

Every lowered read plan must explicitly track:

- pushdown predicates
- residual predicates
- backend capability rejections

Every write selector must explicitly track:

- legality proofs
- execution predicates that remain in the final lowered SQL
- backend capability rejections that force residual evaluation

Reasoning:
This makes correctness mechanical. Narrow pushdown changes where a predicate runs, not whether it runs.

### Lowering

Lower only after semantics are fixed:

- backend SQL generation
- backend-specific expression lowering
- adapter/enforcer insertion where needed
- residual predicate placement
- backend pushdown decisions

Reasoning:
Lowering is a backend concern. It should not invent semantics or compensate for missing logical information.

## Explainability and Plan Inspection

### Explain API

Day 1 only needs planner/debug traces. A stable explain API can land once the new path is live.

Initial debug output should cover:

- bound statement
- surface bindings
- canonical scan/command
- proofs
- effective-state plan
- pushdown vs residual split
- lowered SQL
- commit preconditions and invariant checks for writes
- semantic effects

Later, provide a structured explain API and render SQL `EXPLAIN` on top of it:

- `Engine::explain(sql, params, options) -> ExplainReport`
- SQL forms:
  - `EXPLAIN <query>`
  - `EXPLAIN (FORMAT JSON) <query>`
  - `EXPLAIN BACKEND <query>`
  - later: `EXPLAIN ANALYZE <read-only query>`

Reasoning:
The explain path should reuse the real planner pipeline. But explainability should not block the day-1 write-path cutover.

### Explain payload shape

Expose:

- `ExplainOptions`
  - mode
  - verbosity
  - requested stages
  - redact literals
  - debug row limit
  - include backend explain
- `ExplainReport`
  - stable summary
  - versioned stage payloads
  - warnings
- `ExplainStageReport`
  - parsed/bound statement
  - canonical scan/command or logical plan
  - proofs
  - effective-state plan
  - pushdown vs residual split
  - lowered SQL
  - backend explain
  - effects

Reasoning:
The stable summary gives callers a durable contract. Versioned stage payloads let the planner evolve without freezing every internal debug shape forever.

### Inspection requirements

Require debug output for:

- bound statement
- surface bindings
- canonical scan/command
- proofs
- effective-state plan
- pushdown vs residual split
- lowered SQL
- commit preconditions and invariant checks for writes
- semantic effects

Reasoning:
Every serious query-planner migration depends on explainability. Without it, correctness and performance regressions are much harder to diagnose.

## Projections and Materialization

### Terminology

- a `projection` is a derived semantic surface or read model shape
- `materialization` is the persisted/runtime form of a projection
- v1 keeps this distinction explicit so planner semantics stay separate from rebuild/apply mechanics

### Model

- Projections are derived from committed domain changes.
- Materialization is the persisted/runtime application of those projections.
- Read models are disposable and rebuildable from commits.
- Materialization lag must not affect tracked write correctness.

### V1 replay contract

- the authoritative replay source is the internal committed change contract, not a public query surface
- in v1 that means internal committed change storage such as `lix_internal_change` plus `lix_internal_snapshot`
- `lix_change` remains a public projection over that internal log
- v1 rebuildability assumes stable internal `lix_*` change, commit, and version schemas
- rebuild tools only need to understand those stable internal schemas in v1
- general schema drift, event upcasters, and unknown historical plugin/custom change handling are out of scope for now

Reasoning:
The rewrite needs a replay contract, but v1 does not need a full event-versioning framework. The first cut only promises that derived state can be rebuilt from the stable internal `lix_*` committed-change storage the engine already owns, without routing replay through a public surface.

Reasoning:
This is the event-sourcing boundary that most directly simplifies the architecture. Once materialization stops being authoritative, the write path becomes easier to reason about and easier to rebuild.

### Consequences

- Delete followup reconstruction of tracked writes from `RETURNING` rows.
- Stop using materialized tables as the authority for commit generation.
- Keep rebuild tools for materialized projections and read models.

Reasoning:
This removes one of the main current sources of drift.

## Side Effects and Observation

### New model

Produce side effects from committed semantic results:

- cache invalidation targets
- dynamic surface catalog invalidation when stored-schema commits change public descriptors
- file refresh targets
- state commit stream changes
- post-commit notifications

Reasoning:
Effects should be attached to committed semantic changes, not guessed from SQL text or AST fragments after the fact.

## Proposed File Structure

`packages/engine/src/sql2/`

- `core/`
  - parser entrypoints
  - AST utilities
  - placeholder binding
  - generic logical-function parsing/lowering
  - shared SQL-facing contracts
- `catalog/`
  - builtin surface descriptors
  - dynamic schema-derived descriptor bootstrap
  - dynamic descriptor invalidation and `CatalogEpoch`
  - `SurfaceRegistry`
  - `SurfaceDescriptor`
  - `SurfaceBinding`
- `planner/`
  - canonicalization helpers
  - `ir/`
    - `canonical_state_scan.rs`
    - `read_plan.rs`
    - `admin_projection_spec.rs`
    - `entity_projection_spec.rs`
    - `read_command.rs`
    - `write_command.rs`
    - `planned_write.rs`
    - `proofs.rs`
  - `semantics/`
    - `effective_state_resolver.rs`
    - `proof_engine.rs`
    - `write_resolver.rs`
    - `domain_changes.rs`
  - `backend/`
    - `pushdown.rs`
    - `lowerer.rs`
- `runtime/`
  - SQL request orchestration
  - explain/debug trace assembly
  - handoff to commit/materialization/effects runtime
- `backend/`
  - SQL execution runner
  - transaction coordination
  - dialect/runtime adapters

Top-level domain/runtime modules remain separate from `sql2/**`:

- `packages/engine/src/commit/`
  - `generate_commit()`
  - commit append support
- `packages/engine/src/materialization/`
  - materialized projections
  - rebuild tools
- `packages/engine/src/effects/`
  - cache invalidation
  - file refresh targets
  - state commit stream changes
  - post-commit notifications

Optional shared migration support, only when needed:

- `packages/engine/src/sql_shared/`
  - generic parser, binder, AST, lowering, catalog, or contract primitives extracted out of legacy-only modules so both paths can use them during migration

Later expansion modules may add:

- `ir/canonical_change_scan.rs`
- `ir/rel_expr.rs`
- `ir/scalar_expr.rs`
- `ir/planned_read.rs`
- `ir/properties.rs`
- `planner/`
- `explain/`

`packages/engine/src/sql/`

- legacy planner/execution tree during migration
- not a dependency target for new `sql2/**` code
- extract genuinely shared SQL primitives out only when `sql2` actually needs them, and place them in a neutral shared module instead of duplicating them
- deleted entirely after `sql2` cutover

Reasoning:
The file structure should reflect the day-1 semantic authority chain first. Broader planner and explain modules can grow later without forcing the first cut to build the entire future architecture up front.

## Legacy SQL Modules To Delete After `sql2` Cutover

- `sql/planning/rewrite_engine/**`
- `sql/execution/followup.rs`
- any surface-specific module whose only job is to rewrite one public SQL surface into another
- any effect detector that scans emitted SQL text or AST shape after planning
- the old validation path that re-queries rewritten tables to discover update semantics

Reasoning:
Keeping these modules around as active alternatives will preserve drift and make the new architecture optional instead of authoritative. During migration they may remain as frozen reference code, but they must not remain live peers of `sql2/**` in production execution. Only the legacy planner/execution/classifier tree is a forbidden dependency and deletion target; generic SQL primitives should be extracted only when the semantic cutover actually needs them.

## Migration Plan

### Migration oracle

- treat the existing integration suite under `packages/engine/tests/**` as the primary migration oracle
- use the existing simulation harness as the main semantic gate across backend and materialization behavior
- require the relevant integration tests to pass on the new `sql2` path before each production-facing cutover
- add targeted planner/unit/differential tests only when the integration suite is too coarse to localize a regression
- if an existing integration test encodes obsolete or unsound behavior, replace it deliberately with a corrected semantic contract test and document why in the same change
- do not invent a separate shadow-execution or shadow-rebuild protocol for v1

Reasoning:
The engine already has broad end-to-end coverage for state, entity, filesystem, version/admin, materialization, validation, observation, and transaction behavior. That suite is a better source of truth for this refactor than a parallel operational cutover framework.

### Phase 1: Establish the clean-slate boundary

Work:

- create `src/sql2/` with the day-1 modules
- define `BoundStatement`, `SurfaceBinding`, `CanonicalStateScan`, `ReadPlan`, `EntityProjectionSpec`, `ReadCommand`, `WriteCommand`, and `PlannedWrite`
- freeze the legacy planner/execution parts of `src/sql/`
- add guardrails so new `sql2/**` code does not depend on legacy rewrite, followup, classifier, or planning modules
- extract shared SQL primitives only when `sql2` actually needs them, using a neutral shared module instead of duplicating or front-loading a full re-home

Reasoning:
The rewrite needs hard cut lines first. Otherwise new behavior will continue landing in the old pipeline.

Exit condition:

- no new engine behavior is added directly to the old rewrite or followup stack

### Phase 2: Bind public surfaces to canonical semantic kernels

Work:

- implement `SurfaceBinding`
- map `lix_state*` onto `CanonicalStateScan`
- map `lix_entity*` onto `CanonicalStateScan + EntityProjectionSpec`
- lift simple state-backed read shape into `ReadPlan::Scan`, `Filter`, `Project`, `Sort`, and `Limit`
- preserve surface-local defaults, visible columns, and schema-driven overrides

Reasoning:
This is the smallest clean-slate semantic boundary that matches the current engine. It removes SQL-surface rewriting without forcing a full optimizer model on day 1, while still making basic read shape explicit.

Exit condition:

- state-backed surfaces bind to one canonical state kernel before lowering

### Phase 3: Build the shared effective-state resolver

Work:

- implement `EffectiveStateRequest`, `EffectiveStatePlan`, and `ResolvedStateRows`
- move overlay, tombstone, and winner semantics into the resolver
- make canonical state-backed scans consume it
- expose effective-state debug output through planner traces

Reasoning:
This is the semantic heart of the engine. It must exist before proofing and writes can be trusted.

Exit condition:

- tracked writes and state-backed reads can resolve visibility through the shared resolver

### Phase 4: Add minimal write-side proofing

Work:

- implement the minimal `ScopeProof` and `SchemaProof` needed for tracked writes
- add a narrow `TargetSetProof` only if the tracked write path proves it actually needs one
- support active-version, single-version, and bounded explicit version-set proofs
- reject tracked writes that cannot be collapsed to exactly one authoritative `WriteLane`
- reject writes that remain `Unknown` or `Unbounded`
- preserve all other conditions as residual execution predicates
- emit proof and residual data through planner traces

Reasoning:
The first correctness requirement is write legality. A larger proof and pushdown framework can grow later once the tracked write path is stable.

Exit condition:

- no tracked write legality decision depends on “does the AST mention this column”

### Phase 5: Build the tracked write authority chain

Work:

- resolve authoritative pre-state through the shared resolver
- build `ResolvedWritePlan`, `DomainChangeBatch`, and `CommitPreconditions`
- split invariants into pure batch-local checks, append-transaction checks, and physical-constraint support
- keep payload/schema validation, stored-schema-definition validation, and primary-key/entity-id consistency as batch-local checks
- move uniqueness, immediate FK, version existence, and other current-state-dependent checks into the append transaction
- keep commit generation pure
- implement `append_commit_if_preconditions_hold(...)`
- perform append-time write-lane tip, idempotency, and current-state-dependent invariant enforcement in the same transaction as commit append and pointer updates
- emit dynamic catalog invalidation when committed stored-schema changes affect public surface descriptors

Reasoning:
This is the first truly valuable cutover. It fixes the core event-sourcing bug class by moving authority to one semantic write path and one atomic append boundary, and it must include append-transaction invariants before any production cutover.

Exit condition:

- tracked writes can execute end to end without followup reconstruction or optimistic version seeding, and append-time invariants are enforced inside the transactional boundary
- tracked writes can execute end to end without followup reconstruction or optimistic version seeding, append-time invariants are enforced inside the transactional boundary, and every tracked command binds to exactly one authoritative `WriteLane`

### Phase 6: Cut tracked `INSERT` / `UPDATE` / `DELETE` over to the new path

Work:

- lower tracked `INSERT`, `UPDATE`, and `DELETE` into the new `WriteCommand`
- route state-backed entity writes through the same canonical state write path
- keep the existing tracked-write integration coverage green on the new path, especially `vtable_write.rs`, `state_view.rs`, `entity_view.rs`, `on_conflict_views.rs`, `transaction_execution.rs`, `commit.rs`, `schema_definition_validation.rs`, and `snapshot_content_validation.rs`
- use targeted `DomainChangeBatch` differential fixtures only when the integration suite is too coarse to isolate a mismatch
- add targeted regression fixtures for known drift bugs

Reasoning:
This is the highest-value simplification in the current engine. It removes the split between tracked insert rewriting and update/delete followup reconstruction, but it is only safe after phase 5 has already landed append-time invariants and the existing tracked-write integration suite stays green.

Exit condition:

- tracked insert/update/delete share one authoritative commit-generation path, and the relevant tracked-write integration tests pass on the new path

### Phase 7: Land derived materialization and post-commit cleanup

Work:

- align materialization so it only consumes committed changes
- keep materialization derived and non-authoritative for v1 surfaces
- keep the existing derived-state integration coverage green, especially `materialization.rs`, `file_materialization.rs`, `state_commit_stream.rs`, and `observe.rs`
- surface invariant-check plans and write-phase traces

Reasoning:
This closes the loop on the event-sourced boundary after cutover: materialized state becomes a derived output again, post-commit runtime behavior is aligned with the authoritative write path, and the existing materialization/rebuild tests remain the acceptance criteria instead of a separate shadow rollout protocol.

Exit condition:

- materialized state is no longer write authority, post-commit runtime behavior is aligned with the authoritative write path, and the relevant materialization/observation integration tests pass on the new path

### Phase 8: Adopt the new path for state-backed reads and narrow pushdown

Work:

- route `lix_state*` and `lix_entity*` reads through the new canonical state kernel
- route migrated read shape through the day-1 `ReadPlan` shell
- keep residual filtering explicit
- implement the narrow `Exact / Inexact / Unsupported` backend pushdown contract
- keep the existing state-backed read integration coverage green on the new path, especially `state_view.rs`, `state_by_version_view.rs`, `state_history_view.rs`, `state_inheritance.rs`, `entity_view.rs`, `entity_by_version_view.rs`, and `entity_history_view.rs`
- use targeted old-vs-new differential fixtures only when the integration suite is too coarse to isolate a mismatch
- keep broader read shapes on the frozen legacy path until the read shell expands
- derive read dependencies and cache metadata from the semantic plan

Reasoning:
Once the write path is stable, the read path can adopt the same state semantics without requiring a full optimizer framework. The main cutover gate is the existing integration suite, not a separate shadow-read protocol.

Exit condition:

- state-backed reads share the new semantic kernel, preserve correctness with residual filtering, and the relevant state-backed read integration tests pass on the new path

### Public surface migration matrix

- `lix_state*`
  - migrate in v1
  - bind in phase 2 and cut reads over in phase 8
- `lix_entity*`
  - migrate in v1
  - bind in phase 2 and cut reads/writes over with the canonical state kernel
- `lix_version`
  - stay on the frozen legacy path in v1
  - decide after the state kernel settles whether it should use a lightweight `AdminScan` or `CanonicalStateScan + AdminProjectionSpec`
  - phase 10 must not delete legacy support for it until it is migrated or intentionally removed
- `lix_active_version`
  - stay on the frozen legacy path in v1
  - decide after the state kernel settles whether it should use a lightweight `AdminScan` or `CanonicalStateScan + AdminProjectionSpec`
  - phase 10 must not delete legacy support for it until it is migrated or intentionally removed
- `lix_stored_schema`
  - stay on the frozen legacy path in v1
  - decide after the state kernel settles whether it should use a lightweight `AdminScan` or `CanonicalStateScan + AdminProjectionSpec`
  - phase 10 must not delete legacy support for it until it is migrated or intentionally removed
- `lix_active_account`
  - stay on the frozen legacy path in v1
  - decide after the state kernel settles whether it should use a lightweight `AdminScan` or `CanonicalStateScan + AdminProjectionSpec`
  - phase 10 must not delete legacy support for it until it is migrated or intentionally removed
- filesystem surfaces
  - stay on the frozen legacy path until a dedicated projection spec exists
  - phase 10 must not delete legacy support for them until they are migrated or intentionally removed
- `lix_change`
  - defer to phase 9
  - phase 10 must not delete legacy support for it until it is migrated or intentionally removed
- `lix_working_changes`
  - stay on the frozen legacy path until it is explicitly modeled or intentionally removed
  - phase 10 must not delete legacy support for it until that decision is made

Reasoning:
The state-backed kernel lands first, but the plan still needs an explicit disposition for every remaining public surface. Otherwise legacy deletion becomes ambiguous and the migration stops being mechanically checkable.

### Phase 9: Expand planner surfaces where needed

Work:

- add richer read planning only where the engine truly needs it
- add `CanonicalChangeScan` and move `lix_change` onto the new planner if still valuable
- expand explain from debug traces into a structured API if still valuable
- introduce broader relational IR nodes only when joins/subqueries or optimizer behavior demand them
- add larger backend capability descriptions only when the small pushdown contract is insufficient

Reasoning:
This keeps the architecture honest. Bigger planner abstractions should be earned by demonstrated need, not built before the core authority chain exists.

Exit condition:

- richer planner abstractions exist only where they simplify real behavior instead of becoming a second framework

### Phase 10: Delete legacy planner/execution code after cutover

Work:

- verify that `sql2/catalog/**` owns the only live `SurfaceRegistry` for public surfaces
- verify that every public surface in the migration matrix has either migrated or been intentionally removed
- remove followup reconstruction from production execution
- remove the old rewrite engine from production execution
- remove duplicate validation and lowering paths
- delete tests that only assert obsolete rewrite strings or pipeline internals
- delete the legacy planner/execution tree once `sql2/**` is the only live SQL path and reusable SQL-facing pieces have been re-homed under `sql2/*`

Reasoning:
A rewrite is not done when the new path exists. It is done when the old planner/execution path is gone, and the new planner no longer carries hidden dependencies on legacy modules or duplicate generic infrastructure.

Exit condition:

- the semantic architecture is the only execution path

## Testing Strategy

### Primary oracle

- treat `packages/engine/tests/**` as the primary migration oracle
- keep the existing simulation-backed end-to-end coverage green as behavior moves from `sql/**` to `sql2/**`
- prefer strengthening or correcting existing integration tests over inventing parallel migration-only harnesses
- add narrow planner/unit tests only where the integration suite is too coarse to explain a failure
- if a test has a soundness bug, replace it deliberately with a corrected semantic contract and a short rationale

Reasoning:
The current suite already covers the engine at the level users actually depend on. It is broad enough to guide the refactor, and it is harder to accidentally game than a separate cutover protocol.

### Keep

Keep tests that encode stable semantic contracts:

- effective-state visibility
- local/global overlay and shadowing
- tombstone behavior
- commit generation
- writer attribution
- invariant enforcement
- materialization rebuild correctness
- observation and cache effects

Reasoning:
These are the behaviors the engine actually promises.

### Delete

Delete tests that only assert:

- exact intermediate rewritten SQL strings
- old module boundaries
- old multi-pass rewrite behavior
- followup reconstruction details
- AST-shape-based legality checks

Reasoning:
Those tests lock the implementation to the architecture being removed.

### Add

Add new tests only where the existing integration suite is not precise enough:

1. canonical binding/normalization tests
   Why: protects canonical scan binding and future IR growth where introduced.
2. effective-state resolver contract tests
   Why: this is the semantic core shared by reads and writes.
3. proof-engine contract tests
   Why: v1 write legality and later pushdown both depend on conservative proofing.
4. tracked write resolver tests
   Why: verifies one unified write path.
5. invariant checker tests
   Why: enforces write-side safety on the authoritative batch.
6. concurrency and idempotency tests
   Why: stale writes and duplicate retries must become explicit failures or safe retries.
7. materialization rebuild tests
   Why: proves persisted read models are derived and disposable.
8. surface-equivalence tests
   Why: state-backed surfaces must agree on semantics when scoped equivalently.
9. end-to-end bug reproductions for current state-drift bugs
   Why: these become the canaries that justify the rewrite.
10. targeted old-vs-new differential fixtures
   Why: useful for isolating regressions during migration, but they are a diagnostic aid rather than the primary oracle.
11. debug trace tests for proof, effective-state, and lowered-SQL visibility
   Why: the day-1 planner still needs inspection coverage even before a stable explain contract exists.

## Definition of Done

The rewrite is done when all of the following are true:

1. one canonical semantic model exists before lowering
   Why: planner semantics are no longer encoded in SQL rewrites.
2. one effective-state resolver is shared by reads and writes
   Why: visibility semantics exist once.
3. write legality uses conservative semantic proofs
   Why: no legality decision depends on syntactic column mention.
4. tracked writes produce `DomainChangeBatch` plus `CommitPreconditions`
   Why: the authoritative write boundary is explicit.
5. tracked integrity enforcement is split between batch-local validation and append-transaction rechecks on the final intended batch
   Why: invariants apply to what will actually be committed, and current-state-dependent checks must not race the append boundary.
6. tracked commit append happens only through one transactional `append_commit_if_preconditions_hold(...)` boundary
   Why: preconditions are enforced atomically relative to commit append and pointer updates.
7. projections are derived from committed changes and materialization is non-authoritative
   Why: read models are not write authority.
8. tracked writes enforce one write-lane optimistic-concurrency contract plus idempotency
   Why: correctness under concurrent writers is part of the architecture, not an afterthought, but v1 stays intentionally small.
9. tracked writes that would span multiple authoritative lanes are rejected during binding/planning in v1
   Why: the single-lane optimistic-concurrency model must be enforced before execution, not only implied at append time.
10. migrated reads use an explicit day-1 `ReadPlan` shell instead of falling back to implicit AST/lowering behavior
   Why: semantic state logic must not be separated from basic query shape again.
11. read lowering is governed by an explicit backend pushdown contract and residual predicates
   Why: planner/backend correctness is explicit instead of implicit.
12. debug traces or explain output expose the semantic plan, proof state, predicate placement, and lowered SQL
   Why: migrations and regressions are diagnosable.
13. the old rewrite and followup architecture is deleted, and no `sql2/**` code depends on legacy planner/execution/classifier modules
   Why: there is only one execution path left and the clean-slate boundary is real.
14. `sql2/catalog/**` owns one `SurfaceRegistry` for builtin and dynamic public surfaces
   Why: public-surface meaning is defined once instead of drifting across multiple registries.
15. dynamic-surface bindings are invalidated and rebound after schema-affecting stored-schema commits
   Why: public surface meaning must not go stale across commits that change dynamic descriptors.
16. the remaining tests describe semantic behavior, not obsolete pipeline structure
   Why: the test suite now protects the new architecture instead of the old one.
17. the relevant existing integration suite passes on the `sql2` path, and any changed expectations are deliberate soundness fixes
   Why: cutover is gated by the engine's real behavioral contract rather than by a parallel migration-only protocol.

## Immediate Implementation Order

1. scaffold `src/sql2/` and freeze `src/sql/`
   Reasoning: this creates the cut lines first.
2. bind state-backed surfaces to `CanonicalStateScan`, entity projection specs, and the day-1 `ReadPlan` shell
   Reasoning: one canonical state kernel removes the current surface-rewrite layering, and the read shell keeps basic query shape inside the semantic boundary.
3. land the shared effective-state resolver
   Reasoning: reads and writes must agree on visibility semantics before anything else.
4. land minimal write-side proofing plus `ResolvedWritePlan`
   Reasoning: tracked write legality is the first correctness boundary.
5. land `DomainChangeBatch`, `CommitPreconditions`, and `append_commit_if_preconditions_hold(...)`
   Reasoning: this is the highest-value simplification and hardening step.
6. cut tracked insert/update/delete over to the new path
   Reasoning: this removes the followup split and makes the authority chain live.
7. land authoritative invariant checking and derived, non-authoritative materialization alignment
   Reasoning: this finalizes the event-sourced write boundary without implying a second read-authority model.
8. adopt the new path for state-backed reads with the day-1 `ReadPlan` shell, narrow pushdown, and residual filtering
   Reasoning: once the write path is correct, reads can follow the same semantic kernel without falling back to implicit AST/lowering behavior.
9. remove followup and old rewrites from production execution, then delete the legacy planner/execution tree after any genuinely shared SQL primitives needed during migration have been extracted out of legacy-only modules
   Reasoning: the rewrite is only complete once the old semantics are no longer live, but that does not require a second blanket re-home of every generic SQL helper up front.

## Progress Log

- 2026-03-06: Replaced the previous inheritance/global-lane plan in this file with a semantic rewrite plan centered on a canonical semantic model, a shared effective-state resolver, conservative proofs, authoritative domain-change batches, and explicit commit preconditions.
- 2026-03-06: Added a `Correctness vs pushdown` section clarifying that conservative proofing only limits early predicate movement; unproven predicates remain as residual filters in the lowered SQL executed by the backend.
- 2026-03-06: Updated the plan to use a clean-slate `src/sql2/` tree, added an explicit backend execution contract, strengthened pushdown into proofs-plus-residuals, defined an explain/debug direction, added a differential migration phase, and changed old `src/sql/` cleanup language from immediate deletion to frozen legacy reference removed from production execution.
- 2026-03-06: Narrowed the clean-slate boundary after review: `sql2/**` must not depend on legacy rewrite/followup/classifier modules, but shared parser/binder/AST/lowering primitives are extracted only when needed instead of front-loading a full self-sufficient `sql2` tree.
- 2026-03-06: Strengthened the concurrency plan with one explicit transactional append boundary, `append_commit_if_preconditions_hold(...)`, so write-lane tip/idempotency checks are enforced atomically relative to commit append and pointer updates rather than remaining architectural intent only.
- 2026-03-06: Simplified the planner model further by collapsing surface identity to family plus variant, separating `ReadCommand`/`WriteCommand` from `PlannedRead`/`PlannedWrite`, and unifying tracked-write concurrency requirements under one `CommitPreconditions` object.
- 2026-03-06: Narrowed the architecture around one canonical state-backed kernel for `lix_state*` and `lix_entity*`, kept `lix_change` as a separate log kernel, replaced the broad backend capability layer with a small pushdown contract, and reordered the migration so the tracked-write authority chain lands before broader planner expansion.
- 2026-03-06: Tightened the v1 read-source policy so each semantic kernel has exactly one authoritative `StateSource`; proving equivalence between alternative read sources is now explicitly out of scope for v1.
- 2026-03-06: Moved append-transaction invariant enforcement into phase 5 so tracked-write cutover cannot happen before current-state-dependent checks are enforced inside `append_commit_if_preconditions_hold(...)`.
- 2026-03-06: Pushed `lix_change` planning and structured explain snapshots out of the day-1 slice so the first implementation stays focused on the tracked-write authority chain and state-backed surface cutover.
- 2026-03-06: Re-guided the migration around the existing `packages/engine/tests/**` integration suite as the primary source of truth, with targeted differential/unit tests as secondary diagnostics and no separate shadow rollout protocol for v1.
- 2026-03-06: Added a tiny day-1 `ReadPlan` shell (`Scan`, `Filter`, `Project`, `Sort`, `Limit`) so migrated reads keep basic query shape inside the semantic boundary without forcing a full optimizer algebra.
- 2026-03-06: Landed the first implementation checkpoint: scaffolded `packages/engine/src/sql2/`, added the initial `BoundStatement`/`SurfaceRegistry`/day-1 IR and effective-state contract types, and replaced the old “`sql2` must stay removed” guardrail with migration guardrails that keep `sql2` isolated from legacy rewrite, followup, and classifier code.
- 2026-03-06 13:16 PST: Committed the first implementation checkpoint as `a4d8ec45` (`Add initial sql2 semantic rewrite scaffold`) before starting the next slice so the clean-slate boundary and migration guardrails are preserved as an explicit checkpoint.
- 2026-03-06 13:19 PST: Landed the next checkpoint in `sql2` by adding a day-1 read canonicalizer for simple `SELECT` statements over `lix_state*` and schema-derived entity surfaces, producing `ReadCommand` plans rooted in `CanonicalStateScan` with explicit `Filter`/`Project`/`Sort`/`Limit` nodes instead of leaving that shape implicit in legacy rewrite code.
- 2026-03-06 13:28 PST: Landed the next checkpoint by bootstrapping `SurfaceRegistry` from builtin schemas plus authoritative stored-schema state, adding a `sql2` runtime read-preparation path for eligible single-statement reads, and invoking that semantic preparation from the live `shared_path` execution shim while preserving legacy lowering/execution as the backend path.
- 2026-03-06 13:38 PST: Landed the next checkpoint by extracting `DependencySpec` into a neutral `sql_shared` contract, deriving semantic dependency specs from eligible `sql2` canonical reads without falling back to legacy rewrite/planning code, threading that override into the live execution-plan builder, and tightening the day-1 `sql2` shell so subqueries and derived tables stay on the frozen legacy path.
- 2026-03-06 13:40 PST: Landed the next checkpoint by turning the `sql2` effective-state resolver stubs into a real prepared-read artifact, building explicit `EffectiveStateRequest` and conservative `EffectiveStatePlan` values from canonical state-backed reads, and exposing those semantic visibility plans through the `sql2` runtime trace alongside the canonical read and dependency spec.
- 2026-03-06 13:45 PST: Review-adjusted the migration boundary by extracting placeholder-state and placeholder-index resolution into a neutral `sql_shared` module, switching `sql2` semantic dependency derivation to that shared primitive instead of legacy `sql::ast::utils`, and extending the guardrail test so new `sql2` code cannot regress back to legacy AST-helper imports.
- 2026-03-06 13:53 PST: Moved dynamic stored-schema bootstrap onto an explicit internal authority by adding `lix_internal_stored_schema_bootstrap` initialization and seeding, loading `SqlStoredSchemaProvider` and stored-schema metadata discovery from that bootstrap table, rewriting stored-schema inserts to target the bootstrap table first while mirroring legacy materialized rows as supplemental writes, and updating the relevant `sql2`, rewrite, and integration tests to assert the new authority path.
- 2026-03-06 14:10 PST: Landed the next `sql2` write-planning checkpoint by adding a day-1 semantic write path for simple single-row `lix_state*` inserts, introducing conservative write proof and write-resolution modules, preparing `PlannedWrite` plus `ResolvedWritePlan` artifacts with explicit scope/schema/target proofs and write-lane resolution, and threading the prepared-write trace through the shared execution-preparation path while leaving entity/admin/filesystem writes on the frozen legacy path.
- 2026-03-06 14:14 PST: Landed the next tracked-write artifact checkpoint by deriving `DomainChangeBatch` and explicit `CommitPreconditions` for supported `sql2` tracked state inserts, loading `ExpectedTip(commit_id)` conservatively from authoritative `lix_version_pointer` state instead of inventing optimistic defaults, surfacing those artifacts through `sql2` prepared-write traces, and keeping unsupported/missing-tip cases on the legacy path until the append boundary exists.
- 2026-03-06 14:18 PST: Expanded the day-1 `sql2` write shell from inserts to explicit-version `lix_state_by_version` updates and deletes, reusing the same conservative proof, write-resolution, domain-change, and commit-precondition pipeline so those mutations now prepare through one semantic path when the selector proves a single concrete version lane and target row.
- 2026-03-06 14:34 PST: Landed the append-boundary checkpoint by extracting reusable commit statement/runtime helpers into top-level `commit/`, adding `lix_internal_commit_idempotency`, introducing `append_commit_if_preconditions_hold(...)` with transactional tip checks plus replay-or-drift handling for single-version lanes, and strengthening `sql2` `DomainChangeBatch` from synthetic change IDs into real proposed domain changes so the semantic write path now has a concrete commit-ready batch shape even before the live sql2 execution cutover.
- 2026-03-06 14:40 PST: Landed the first live `sql2` write cutover by routing eligible tracked `INSERT` statements through the shared-path transition shim into `append_commit_if_preconditions_hold(...)` during normal engine execution and transaction execution, keeping the cutover intentionally narrow to no-`RETURNING`, no-filesystem-effect state inserts while preserving legacy execution for updates, deletes, and broader write shapes; added an end-to-end regression that proves public `lix_state_by_version` inserts now populate `lix_internal_commit_idempotency`.
- 2026-03-06 14:53 PST: Expanded the live `sql2` tracked-write slice to explicit-version `lix_state_by_version` updates and deletes by resolving authoritative pre-state rows before commit generation, merging update patches onto that committed row shape, treating missing targets as semantic no-ops instead of followup-driven writes, deriving state-commit-stream changes from committed domain changes, and adding end-to-end regressions that prove public update/delete operations now populate `lix_internal_commit_idempotency` on the new append path.
- 2026-03-06 15:02 PST: Review-adjusted the live `sql2` explicit-version update/delete slice before further expansion by requiring exact conjunctive selectors for the new authority path, honoring exact `file_id` and other supported row filters during authoritative pre-state resolution so residual mismatches cannot mutate the wrong row, correcting tracked writer attribution to use the current command's `writer_key` unless the mutation explicitly sets it, and adding targeted regression coverage for both selector scoping and writer-key attribution.
- 2026-03-06 15:16 PST: Introduced a focused write-side committed-state source seam under `commit/` so tracked version-tip and version-info lookups no longer read `lix_version_pointer` from materialized state tables directly; routed `sql2` commit-precondition derivation, append-boundary tip checks, and shared commit runtime version loading through that seam using `lix_internal_change` plus `lix_internal_snapshot`, while moving exact tracked row lookup behind the same explicit source module and keeping targeted `sql2`, append-boundary, state-by-version, guardrail, and legacy writer-key coverage green.
- 2026-03-06 15:29 PST: Landed the next correctness checkpoint for the live `sql2` tracked-write slice by adding `sql2`-native batch-local validation and append-time rechecks in `validation.rs`, running those checks once during shared-path preparation and again inside the transaction immediately before `append_commit_if_preconditions_hold(...)`, reusing the transaction backend adapter for append-snapshot reads, and adding end-to-end `lix_state_by_version` regressions for invalid snapshot-content inserts and immutable-schema updates so the migrated path no longer relies solely on pre-append legacy validation.
- 2026-03-06 15:34 PST: Tightened the append boundary itself by teaching `append_commit_if_preconditions_hold(...)` to accept an explicit invariant-checker hook and run append-time rechecks only after tip/idempotency preconditions pass but before commit generation and pointer updates, moving the live `sql2` append validation call out of the shared-path prelude and into the transactional boundary while adding unit coverage that proves successful appends run the checker, replays/tip-drift skip it, and invariant failures abort before commit persistence begins.
- 2026-03-06 16:05 PST: Extended the write side to resolve existing targets through the shared `sql2` effective-state layer by adding exact effective-state winner resolution over tracked and untracked lanes, using that shared visibility decision in `write_resolver.rs` for update/delete target selection, and widening the live `sql2` cutover so tracked active-version `lix_state` updates and deletes now execute on the new append path only when the visible winner is a local tracked row instead of falling back to committed-row-only targeting.
- 2026-03-06 16:05 PST: Routed a narrow state-backed entity write slice through the same canonical state write path by teaching `sql2` write canonicalization to normalize entity `lixcol_*` columns into state semantics, resolving entity inserts and exact single-row update/delete selectors through schema metadata plus the shared effective-state write resolver, and keeping unsupported global or untracked entity-override cases on the frozen legacy path so the new write cutover stays sound while `entity_view.rs` coverage migrates.
- 2026-03-06 16:23 PST: Closed the next stored-schema authority gap in the live `sql2` path by mirroring committed `lix_stored_schema` rows from the `sql2` append result into `lix_internal_stored_schema_bootstrap` inside the same transaction, tightening `sql2` stored-schema validation so committed rows must already satisfy the bootstrap/global identity contract, and adding a transaction regression that proves a schema registered through `lix_state_by_version` on the new append path becomes usable as a dynamic public surface after commit.
- 2026-03-06 16:42 PST: Started the state-backed read cutover by adding a `sql2` read lowerer that turns eligible canonical entity/state reads into executable lowered statements, teaching `SurfaceRegistry` to carry evaluated schema override predicates and fixed version overrides for entity surfaces, routing prepared execution plans through those lowered read statements when present, and adding runtime plus transaction coverage that proves dynamic surfaces created on the live `sql2` write path can now be queried inside the same transaction while the migrated `state_view`, `entity_view`, `entity_by_version_view`, `entity_history_view`, and guardrail suites stay green.
- 2026-03-06 16:59 PST: Pushed phase-7 runtime cleanup further by letting live `sql2` tracked writes override legacy `plan.effects` with authoritative post-commit effects derived from committed domain changes, so state-commit-stream batches, active-version followup state, and direct file-cache refresh targets now come from the semantic append result instead of being merged back in from legacy planner effects; added end-to-end `state_commit_stream` and `observe` coverage that proves `sql2` entity writes emit exactly one semantic change batch and drive follow-up reads through the new semantic path.
- 2026-03-06 17:07 PST: Filled the remaining phase-7 debug gap by extending `sql2` write traces with explicit invariant-check plans and write-phase sequencing, so prepared writes now expose batch-local checks, append-time rechecks, physical defense-in-depth expectations, and the planned authority-chain phases from canonicalization through `append_commit_if_preconditions_hold(...)`; added runtime unit coverage for normal tracked writes and stored-schema writes so invariant/debug artifacts stay visible as the write path expands.

(use timestamps e.g. hour minuate from now on in logs)
- 2026-03-06 17:15 PST: Landed the next phase-8 read-planning checkpoint by threading an explicit `PushdownDecision` through live `sql2` read lowering and debug traces, lowering eligible `lix_state*` reads through an explicit derived source boundary instead of handing raw public-surface SQL straight to the backend, and conservatively falling invalid or non-exposed state-column reads back to the legacy path so state read errors and residual filtering semantics stay stable while the migrated `lowerer`, `runtime`, `state_view`, `entity_view`, and guardrail suites remain green.
- 2026-03-06 17:21 PST: Made the narrow backend pushdown contract real for the first live `sql2` read slice by proving top-level conjunctive `schema_key`, `entity_id`, and `file_id` filters on `lix_state*` as exact pushdown candidates, splitting accepted vs residual predicates inside the semantic effective-state plan, moving accepted conjuncts into the inner derived state-source query while preserving explicit residual filtering above it, and extending planner/runtime coverage so the lowered SQL, `PushdownDecision`, and migrated `state_view`, `entity_view`, and guardrail suites all agree on the same predicate placement.
- 2026-03-06 17:25 PST: Finished the next state-backed read cutover checkpoint by broadening exact state-surface pushdown from a hardcoded base-column list to exposed state-surface columns, which fixed `lix_state_history` root-commit queries on the live `sql2` path and let versioned/history selectors such as `version_id`, `root_commit_id`, and `depth` stay inside the derived source query where the public history/by-version surfaces need them; added direct `sql2` runtime coverage for `lix_state_by_version` and `lix_state_history` reads and re-ran the relevant integration suites (`state_by_version_view`, `state_history_view`, and `state_inheritance`) to keep the phase-8 state-backed read matrix green.
- 2026-03-06 17:27 PST: Closed the remaining phase-8 entity-variant visibility gap by adding direct `sql2` runtime coverage for `lix_key_value_by_version` and `lix_key_value_history` reads, pinning those variants to the live state-backed entity lowering path and verifying that by-version/history entity projections still lower through the expected `lix_state_by_version` and `lix_state_history` sources while their user predicates remain explicit residual filters; re-ran the `sql2::runtime`, `entity_by_version_view`, and `entity_history_view` suites to keep the entity half of the state-backed read matrix green.
- 2026-03-06 17:31 PST: Started phase 9 by adding a narrow `CanonicalChangeScan` path for `lix_change`, teaching `sql2` read preparation to handle non-state scans without forcing fake effective-state artifacts, and lowering `lix_change` through a self-contained derived query over `lix_internal_change` plus `lix_internal_snapshot` instead of the legacy rewrite step; added planner/runtime coverage for the new change-family slice and kept the `change_view` integration suite green while leaving `lix_working_changes` and all change-family writes on the frozen legacy path.
- 2026-03-06 17:36 PST: Bridged `EXPLAIN` onto the migrated `sql2` read path by unwrapping `EXPLAIN <query>` during `sql2` preparation, lowering the inner query through the same semantic canonicalization and backend-lowering pipeline as normal execution, then re-wrapping the lowered statement in `EXPLAIN` so backend planning now reflects the real `sql2` lowered program for migrated reads instead of the legacy explain rewrite path; added runtime coverage for `EXPLAIN` over `lix_state` and kept the existing execute-level `EXPLAIN` integration test green across backends.
- 2026-03-06 18:07 PST: Removed the next two major semantic leaks in the migrated path by switching exact tracked pre-state lookup from direct `lix_internal_state_materialized_v1_*` reads to an authoritative log-backed `materialization_plan(...)` over committed change/snapshot storage, and by changing `sql2` state/entity/history read lowering to emit internal-source SQL over internal state, untracked, commit, change-set, version-pointer, and commit-ancestry relations instead of lowering back into public `lix_state*` SQL; tightened read pushdown so only source-safe state predicates move below effective-state resolution, preserved the existing `LIX_ERROR_SCHEMA_NOT_REGISTERED` behavior for unknown state schemas by falling those reads back to the legacy validator path, and re-ran the relevant `sql2`, `state_view`, `state_by_version_view`, `state_history_view`, `entity_view`, `entity_by_version_view`, `entity_history_view`, and guardrail suites to keep the checkpoint green.
- 2026-03-07 12:46 PST: Stabilized the post-cutover branch to a fully green `cargo test -p lix_engine` run by boxing the observe-path query futures to avoid async stack overflow in late-subscriber polling, updating the legacy entity-view rewrite test backend to recognize the new authoritative version-pointer query shape, marking builtin admin surfaces with fixed schema keys so valid active-version `sql2` reads no longer fall back as unknown schemas, and fixing entity-history lowering/pushdown so `lixcol_root_commit_id` is pushed through the shared effective-state/history source correctly for filesystem-history parity tests.
- 2026-03-07 13:40 PST: Landed the first deferred admin-surface migration checkpoint by adding `CanonicalAdminScan` lowering for `lix_active_version`, `lix_active_account`, `lix_stored_schema`, and `lix_version` directly over internal authoritative relations, enabling `lix_version` tracked `INSERT`/`UPDATE`/`DELETE` on the live `sql2` path with `WriteLane::GlobalAdmin`, teaching the append boundary and committed-state seam to enforce global-admin tip preconditions through authoritative `lix_version_pointer` history, and moving `lix_version` last-checkpoint side effects onto the `sql2` runtime path so migrated admin reads and version writes no longer depend on legacy public-surface rewrites or followup reconstruction.
- 2026-03-07 13:52 PST: Completed the untracked admin-write slice by routing `lix_active_version` `UPDATE` and `lix_active_account` `INSERT`/`DELETE` through `sql2` canonicalization, untracked admin-row resolution, and a new direct internal `lix_internal_state_untracked` execution path instead of legacy admin-view rewrites, while preserving active-version cache effects from semantic write results and adding a dedicated `active_account.rs` integration suite plus targeted stable-backend `active_version` checks to keep the new singleton-admin contract explicit.
- 2026-03-07 14:04 PST: Migrated the conservative `lix_working_changes` read slice onto `sql2` by adding `CanonicalWorkingChangesScan`, wiring `sql2` catalog/canonicalization/dependency derivation for the working-changes surface, and lowering supported single-surface reads directly to the internal last-checkpoint, commit-ancestry, commit, change-set-element, and change/snapshot relations instead of the legacy view rewrite; kept all predicates residual above the derived query, added runtime coverage that pins `lix_working_changes` to the live `sql2` path, and intentionally left nested public-surface subquery shapes on the frozen legacy path until the read shell expands.
- 2026-03-07 14:33 PST: Re-stabilized the branch after the deferred-admin cutover by fixing Postgres `lix_version` admin lowering so `hidden` defaults stay type-stable during init/bootstrap reads, emitting semantic untracked state-commit-stream changes for live `sql2` admin writes so active-version switches wake observe/state listeners correctly, and repairing `sql2` update placeholder ordering so `SET` and `WHERE` clauses share one placeholder stream; re-ran `cargo test -p lix_engine` to a fully green result before continuing deferred-surface migration.
- 2026-03-07 15:03 PST: Landed the first filesystem `sql2` migration checkpoint by expanding `sql2/catalog` to cover the full public filesystem surface set, replacing the live current/by-version file and directory projection builders with internal-source SQL over authoritative descriptor/blob relations, cutting supported `lix_file`, `lix_file_by_version`, `lix_directory`, and `lix_directory_by_version` reads over to `sql2` lowering with residual-only predicate handling, and adding direct `sql2` lowerer/runtime coverage plus full `filesystem_view` integration verification to pin those current/by-version reads to the new path while intentionally leaving filesystem history and filesystem writes on the frozen legacy fallback for the next checkpoints.
- 2026-03-07 15:17 PST: Finished stabilizing the first filesystem `sql2` checkpoint by preserving tracked `writer_key` values in the new internal filesystem live projections so observe suppression and writer-key-aware filesystem filters keep matching legacy semantics, then re-ran `cargo test -p lix_engine` to a fully green package result before moving on to filesystem history and write-path migration.
- 2026-03-07 15:29 PST: Landed the next filesystem `sql2` read checkpoint by routing active `lix_file_history` and `lix_directory_history` reads through direct internal history lowering, adding root/version-aware history-source predicate pushdown below the derived history source without changing residual user filtering, and pinning the new path with runtime coverage plus `file_history_view` and `state_history_view` integration verification while intentionally leaving `lix_file_history_by_version` on the frozen legacy fallback until explicit version-lane history mapping can be migrated without destabilizing the shared state-history kernel.
- 2026-03-07 15:40 PST: Landed the first filesystem `sql2` write checkpoint by routing supported `lix_directory` and `lix_directory_by_version` inserts and updates through semantic `sql2` write planning, extending write canonicalization/proofing for filesystem descriptor surfaces, resolving normalized path and parent/name consistency against internal filesystem projections, explicitly planning missing ancestor directory rows instead of relying on rewrite side effects, and widening the live tracked-write gate so pure directory-descriptor side effects no longer block the migrated path while targeted `filesystem_view` integration coverage stays green across backends.
- 2026-03-07 16:59 PST: Stabilized the live filesystem migration branch to a fully green `cargo test -p lix_engine` run by threading an active-version hint through the remaining legacy preprocessing path for parameterized filesystem statements inside explicit transactions, lowering `sql2` filesystem helper projection SQL through backend-specific AST lowering before executing internal Postgres lookups, and moving the parser/lowering entrypoints that `sql2` still needs behind a neutral `sql_shared/ast` wrapper so the filesystem write resolver stays isolated from legacy rewrite modules while `observe`, transaction, guardrail, and full-package verification remain green.
- 2026-03-07 17:14 PST: Landed the next filesystem `sql2` write cutover by widening the live tracked-write gate from directory-only side effects to authoritative file-descriptor and blob-ref domain changes, which puts supported tracked `lix_file` and `lix_file_by_version` inserts, updates, and deletes onto the semantic append path with real append-idempotency coverage; verified the new slice against the full `filesystem_view`, `file_history_view`, `observe`, `state_commit_stream`, `sql2::runtime`, and guardrail suites, and explicitly kept `lix_file_history_by_version` on the deferred legacy fallback after validating that the first live lowering attempt was not yet semantically correct.
- 2026-03-08 16:34 PDT: Landed the next filesystem sql2 write checkpoint by routing supported tracked `lix_directory_by_version` deletes through semantic write resolution and the live append path, resolving authoritative directory/file/blob cascades from internal filesystem projections, rejecting unsupported tracked/untracked mixed-winner cascades instead of falling back unsafely, and adding append-idempotency plus cascade regression coverage while re-running the full `filesystem_view`, `sql2::runtime`, and guardrail suites to keep the new directory-delete slice green.
- 2026-03-09 10:31 PDT: Landed the untracked filesystem `sql2` write checkpoint by widening the live untracked gate to `lix_file*` and `lix_directory*`, persisting authoritative pending file payload writes before applying resolved untracked rows, deriving file-cache refresh and untracked state-commit-stream effects from the semantic write plan instead of legacy SQL-shape detection, and surfacing mixed tracked/untracked directory-delete cascades as resolver failures instead of silently falling back; also hardened the embedded Postgres simulation harness with retry-on-deadline database creation so the full `cargo test -p lix_engine` package suite stays green under the heavier deferred-surface migration load while global plugin-archive filesystem writes remain on the intentional legacy fallback.
- 2026-03-09 14:22 PDT: Re-stabilized the deferred-surface branch after the nested-subquery and history work by removing the temporary day-1 `sql2` rejection of nested subqueries, teaching the `sql2` lowerer to rewrite nested filesystem public surfaces into internal-source derived queries across migrated read families, and fixing tracked effective-state lowering to preserve `writer_key` instead of projecting `NULL`; re-ran `cargo test -p lix_engine --test writer_key`, `cargo test -p lix_engine --lib sql2::runtime`, and a fully green `cargo test -p lix_engine` to pin the checkpoint before continuing the remaining filesystem history and cleanup work.
- 2026-03-09 14:45 PDT: Locked down the aligned history-lineage contract at the public boundary by adding regression coverage that `lix_state_history` `SELECT *` and unknown-column diagnostics expose `commit_created_at`, that `lix_file_history_by_version` `SELECT *` and diagnostics expose `lixcol_commit_created_at`, and that `lix_directory_history` `SELECT *` and diagnostics expose `lixcol_commit_created_at`; verified the shape across `state_history_view`, `file_history_view`, and `filesystem_view` without changing the already-correct catalog/registry column contracts.
- 2026-03-09 15:01 PDT: Removed the dead legacy filesystem surface-classifier layer by deleting the unused `sql/surfaces/filesystem` module and unhooking it from `sql/surfaces/mod.rs`, keeping the legacy surface registry filesystem-blind while the existing guardrails and `sql::surfaces::registry` tests stay green.
- 2026-03-09 15:08 PDT: Removed the dead legacy filesystem read-rewrite adapter layer by deleting the unused `pipeline/rules/query/canonical/filesystem_views.rs` entrypoint, unhooking it from the canonical query-rule module tree, and stripping the read-rewrite wrappers out of `steps/filesystem_step.rs` so the legacy rewrite engine keeps only filesystem write helpers while the guardrails and rewrite-pipeline sanity checks remain green.
- 2026-03-09 15:17 PDT: Removed the remaining filesystem-specific postprocess followup branch by deleting the legacy directory-delete cascade logic from `sql/execution/followup.rs`, leaving vtable postprocess followup responsible only for generic tracked row followup plus explicit detected file-domain changes while the full `filesystem_view` integration suite and filesystem guardrails stay green.
- 2026-03-09 22:18 PDT: Collapsed one more live public-surface metadata dependency onto `sql2/catalog/**` by replacing error normalization and user-facing unknown-table/unknown-column diagnostics that still read `lix_table_registry` with builtin and backend-bootstrapped `SurfaceRegistry` lookups, restoring the full public `lix_stored_schema` column contract in `sql2/catalog`, deleting `src/lix_table_registry.rs`, and adding a guardrail that the duplicate registry stays removed while targeted diagnostics and guardrail tests remain green.
- 2026-03-09 22:39 PDT: Removed one more live legacy filesystem rewrite dependency from the shared execution path by teaching `prepare_execution_with_backend(...)` to skip the legacy filesystem update-side-effect detector when a supported non-`RETURNING` `sql2` file or directory write already owns execution, while still collecting neutral pending payload/delete helpers for the semantic path; added transition-shim unit coverage for the new policy and kept targeted sqlite filesystem simulations plus guardrails green before the full package rerun.
- 2026-03-09 23:07 PDT: Deleted the dead `rewrite_engine/steps/filesystem_step.rs` wrapper, rewired the remaining legacy canonical filesystem write rule directly onto the neutral `filesystem::mutation_rewrite` helpers, removed the now-unused legacy surface-classifier filesystem coverage bucket, and added guardrails that the wrapper stays removed while `logical_views.rs` and `vtable_read.rs` remain filesystem-blind; this keeps the production execution path from regressing back through one more legacy filesystem bridge layer without touching the still-reusable non-`src/sql/**` filesystem runtime helpers.
- 2026-03-09 23:19 PDT: Deleted the dead `filesystem/select_rewrite.rs` module and its obsolete legacy filesystem-read tests, unhooked it from `filesystem/mod.rs`, and added a guardrail that the removed module stays gone now that migrated filesystem reads lower exclusively through `sql2`; this removes another parallel legacy filesystem read implementation that was no longer on the production path.
- 2026-03-09 23:27 PDT: Deleted the dead `sql/planning/rewrite_engine/rewrite.rs` duplicate filesystem coalescer, leaving `sql/planning/script.rs` as the only live transaction-script coalescing path and adding a guardrail that the removed dead file stays gone; this trims one more unused legacy filesystem bridge artifact from the rewrite-engine tree without changing production behavior.
- 2026-03-09 23:36 PDT: Removed filesystem public-surface awareness from the legacy rewrite-engine query analysis and validation scaffolding by deleting the filesystem logical-view lists from `pipeline/context.rs` and `pipeline/validator.rs`, and added a guardrail that those files stay filesystem-blind; this prevents the remaining legacy query engine from claiming filesystem surfaces as part of its logical-read rewrite boundary now that migrated filesystem reads belong exclusively to `sql2`.
- 2026-03-09 23:48 PDT: Deleted the last live sqlite `lix_file` transaction-script coalescer from `sql/planning/script.rs` and `sql/scripts.rs`, removed its obsolete engine tests, and added a guardrail that the helper stays gone; this removes another production filesystem bridge that was still rewriting public filesystem SQL before execution even though migrated filesystem writes now belong directly to `sql2`.
- 2026-03-09 23:55 PDT: Deleted the dead `sql/planning/rewrite_engine/analysis.rs` duplicate filesystem analysis helper and added a guardrail that it stays removed; the live runtime already uses `sql/history/plugin_inputs.rs` and `sql/semantics/state_resolution/optimize.rs` for filesystem materialization/cache analysis, so keeping a second rewrite-engine copy would only preserve stale bridge logic.
- 2026-03-10 00:02 PDT: Deleted the dead `pipeline/rules/statement/canonical/filesystem_write.rs` wrapper and rewired the remaining legacy canonical statement rule directly onto the neutral `filesystem::mutation_rewrite` helpers, plus added a guardrail that the wrapper stays removed; this trims another dedicated legacy filesystem adapter layer without touching the reusable non-`src/sql/**` mutation logic itself.
- 2026-03-10 00:14 PDT: Removed the last legacy canonical filesystem write branches from the production shared path by making top-level public filesystem writes require `sql2` preparation in `execution/shared_path.rs`, then deleting the now-dead filesystem insert/update/delete rewrite logic from the legacy canonical statement rule and adding guardrails that `canonical/mod.rs` stays filesystem-blind; this is the first cut that stops the production runtime from silently falling back into legacy filesystem write rewriting when `sql2` owns the surface.
- 2026-03-10 00:28 PDT: Deleted the remaining no-op legacy filesystem update-detector bridge by removing the `skip_legacy_filesystem_update_side_effect_detection` policy from execution intent collection, stripping the dead tracked/untracked update-detection hook out of `sql/side_effects.rs`, and rerouting pending auto-id file resolution through internal live-projection SQL rather than `filesystem::mutation_rewrite`; added guardrails that the live runtime stays off the removed detector plumbing so migrated filesystem writes either execute through `sql2` or fail instead of retaining a fallback switch.
- 2026-03-10 00:41 PDT: Deleted the dead `filesystem/mutation_rewrite.rs` module and its obsolete legacy filesystem-write tests, unhooked it from `filesystem/mod.rs`, and replaced the remaining guardrails with checks that the removed mutation-rewrite layer stays gone and that no source path reintroduces fake filesystem no-op SQL synthesis; kept the fast sqlite filesystem integration suite and guardrails green to confirm `sql2` owns the public filesystem write path without a legacy rewrite fallback.
- 2026-03-10 00:28 PDT: Deleted the remaining no-op legacy filesystem update-detector bridge by removing the `skip_legacy_filesystem_update_side_effect_detection` policy from execution intent collection, stripping the dead tracked/untracked update-detection hook out of `sql/side_effects.rs`, and rerouting pending auto-id file resolution through internal live-projection SQL rather than `filesystem::mutation_rewrite`; added guardrails that the live runtime stays off the removed detector plumbing so migrated filesystem writes either execute through `sql2` or fail instead of retaining a fallback switch.
- 2026-03-10 09:02 PDT: Extended `sql2` filesystem selector canonicalization so singleton `IN (...)` filters count as exact selectors on supported write keys, which keeps parameterized transaction-script `DELETE FROM lix_file WHERE id IN (?)` on the live `sql2` path instead of failing back to the removed legacy filesystem bridge; re-ran the narrow canonicalizer test, the sqlite transaction-script regression, `sql_guardrails`, and the sqlite `filesystem_view` suite to confirm the stricter no-fallback runtime still executes the supported script shape.
- 2026-03-10 09:18 PDT: Removed the remaining dead legacy-duplicate checks from the live `sql2` filesystem write gate in `execution/shared_path.rs`, so tracked and untracked file/directory writes no longer consult empty legacy detected-change vectors before executing; re-verified the same sqlite transaction-script regression, `sql_guardrails`, and the sqlite `filesystem_view` suite to keep the no-fallback production path green.
- 2026-03-10 09:12 PDT: Deleted the still-threaded but empty `untracked_filesystem_update_domain_changes` path from runtime intent/effect handling by removing it from collected/deferred side-effect structs, transaction/apply-effect call sites, and `sql/side_effects.rs`, then simplifying the side-effect collector so it no longer fabricates per-statement filesystem change vectors from an empty source; kept sqlite `filesystem_view`, `observe`, `state_commit_stream`, and `sql_guardrails` green to confirm the live `sql2` filesystem path still emits the right post-commit/runtime behavior without the dead legacy hook.
- 2026-03-10 09:19 PDT: Removed the dead `detected_file_domain_changes_by_statement` bridge from the production plan build by deleting it from collected intent, `build_execution_plan`, `preprocess_with_surfaces`, and the shared runtime caller, while preserving the lower legacy vtable-insert hook as an explicit local `&[]` until that rewrite path is deleted entirely; kept sqlite `filesystem_view`, `transaction_execution`, `observe`, and `sql_guardrails` green so the stricter no-fallback runtime still handles migrated filesystem writes and transaction scripts correctly.
- 2026-03-10 09:23 PDT: Deleted the remaining lower legacy vtable-insert/file-domain-change hook from the rewrite engine by removing `DetectedFileDomainChange` threading from the backend statement pipeline, canonical statement-rule context, helper wrappers, and backend vtable-insert entrypoints, then deleting the legacy loop in `steps/vtable_write.rs` that used to append file-derived domain changes into tracked insert commit batches; this leaves migrated filesystem effects owned by `sql2` and the runtime path instead of legacy rewrite-engine commit synthesis.
- 2026-03-10 09:37 PDT: Removed the dead runtime/followup `DetectedFileDomainChange` bridge for migrated surfaces by deleting it from execution intent, deferred side-effect structs, `execute_plan_sql*`, and legacy vtable update/delete followup builders, while keeping payload-derived blob-ref persistence as the only remaining filesystem side-effect path in `apply_effects_tx.rs` and `sql/side_effects.rs`; kept sqlite `filesystem_view`, sqlite `transaction_execution`, and `sql_guardrails` green so the no-fallback runtime still handles migrated filesystem writes without carrying an always-empty detected-change vector through legacy execution.
- 2026-03-10 09:42 PDT: Deleted the last dead engine-level helper code that only supported the removed detected-file bridge by removing unused `DetectedFileChange` conversion/deduplication helpers, their obsolete writer-key tests, and the dead binary-blob target collector from `engine.rs`, while keeping the live schema-registration runtime path intact and verifying with sqlite `filesystem_view`, sqlite `transaction_execution`, and `sql_guardrails`.
- 2026-03-10 09:46 PDT: Renamed the remaining live filesystem payload-side effect path away from the removed detected-file bridge vocabulary by replacing `DetectedFileDomainChange` and its persistence helpers with `FilesystemPayloadDomainChange` terminology across contracts, runtime side-effect collection, transaction flushing, and state insert SQL builders; kept sqlite `filesystem_view`, sqlite `transaction_execution`, and `sql_guardrails` green to confirm the cleanup was purely structural.
- 2026-03-10 09:58 PDT: Deleted the fully dead plugin-side filesystem detect/materialize branch that predated `sql2` authority by removing the unused `detect-changes` request/output path, dead current-file cache-fill materialization helpers, and their obsolete unit coverage from `plugin/runtime.rs`, while reusing the shared component-cache helper in the still-live plugin apply/materialization loops; kept sqlite `filesystem_view` and `sql_guardrails` green to confirm the migrated filesystem path no longer depends on those legacy plugin-side diff helpers.
- 2026-03-10 10:02 PDT: Deleted the dead file-read materialization-scope analyzer that no longer participates in the live `sql2` filesystem path by removing `FileReadMaterializationScope`, its AST-scanning helpers, and the obsolete engine/history tests that only exercised that legacy pre-materialization heuristic; kept sqlite `filesystem_view` and `sql_guardrails` green to confirm history-read materialization detection still works while the unused non-history scope path stays gone.
- 2026-03-10 10:10 PDT: Removed another dead helper cluster from the migrated path by deleting unused intent-verification telemetry from `engine.rs`, dropping the stale `sql2_read` field from the shared execution context, deleting the dead side-effect placeholder wrapper and switching the engine regression to the underlying AST visitor helper, trimming unused rewrite-session caches and legacy rewrite helpers (`projected_columns_for_target`, optional-string parsing, effect-only statement-context output), and dropping unused filesystem/materialization helpers (`build_live_file_projection_sql`, `path_depth`, `file_ancestor_directory_paths`, and the unused version-descriptor snapshot field); kept sqlite `sql_guardrails`, sqlite `filesystem_view`, and sqlite `transaction_execution` green to confirm the cleanup stayed structural.
- 2026-03-10 10:16 PDT: Deleted the dead `active_version_id_hint` passthrough from the legacy statement-rewrite/preprocess stack by removing it from the statement rule engine, canonical vtable-write rule entrypoints, statement pipeline, preprocess wrapper, surface-preprocess shim, and execution-plan builder; kept sqlite `sql_guardrails`, sqlite `filesystem_view`, and sqlite `transaction_execution` green to confirm the legacy rewrite path still preprocesses migrated statements without carrying an inert active-version compatibility thread.
- 2026-03-10 10:21 PDT: Trimmed another cleanup batch around the fast sqlite loop by keeping the legacy surface-coverage keepalive only where it suppresses irrelevant warning noise in non-migrated surface modules, while adding explicit `#[allow(unused_imports)]` / `#[allow(dead_code)]` guards to backend-specific simulation helpers so sqlite-only integration runs stop flooding on postgres/sqlite support exports that are still needed by other suites; re-verified sqlite `sql_guardrails`, sqlite `filesystem_view`, and sqlite `transaction_execution`.
- 2026-03-10 10:55 PDT: Removed the dead `src/sql/surfaces` classifier layer entirely by wiring plan-build straight to `preprocess_with_surfaces_to_plan(...)`, deleting the legacy surface registry/matcher/per-surface lowering modules, and adding a guardrail that `src/sql/surfaces` stays gone; also pruned the fully unused legacy `src/sql/vtable/**` subtree and the dead select/table walker helpers in `sql/ast/walk.rs`, since migrated `sql2` surfaces no longer depend on legacy vtable classification or those traversal utilities.
- 2026-03-10 11:11 PDT: Deleted the legacy public-read canonical rewrite bridge for already-migrated `sql2` surfaces by removing the old query canonicalization directory and the state pushdown rule registrations from the rewrite-engine pipeline, plus deleting the now-unused entity/state/change/working-changes read rewriters; legacy read planning now fails closed on unresolved public logical views instead of silently rewriting them behind `sql2`, while the remaining internal admin write helpers stay intact until their own write-side cleanup lands.
- 2026-03-10 11:43 PDT: Replaced that deleted canonical-read bridge with a smaller `sql2`-backed relation rewrite in `sql/planning/preprocess.rs` that lowers migrated admin/change/working-changes reads and nested filesystem surfaces to internal-source derived queries before the remaining legacy plan-build sees them; this keeps joins/subqueries and internal helper prefetches on the `sql2` semantics path without restoring the old logical-view rewrite layer.
- 2026-03-10 11:49 PDT: Deleted another orphaned rewrite-engine helper cluster by removing `ast_ref.rs`, the dead `column_usage.rs` read-helper module, and the stale reexports that only existed for the removed public-read bridge; the remaining warnings are now centered on still-live legacy write helpers rather than dead migrated-surface read infrastructure.
