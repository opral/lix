# Public SQL Lowering Plan

## Objective

Redesign the public SQL pipeline around one rule:

`The planner's first job is to lower valid public SQL into backend-executable SQL.`

Optimization is a separate, optional phase. The planner must not reject a query just because an optimizer or canonicalizer does not understand its shape.

## Core Principle

The correct pipeline is:

1. Parse SQL once.
2. Bind it into structured semantic state once.
3. Lower public surfaces recursively into a backend-executable relational program.
4. Optionally optimize that lowered program.
5. Execute it on the SQL backend.

The important consequence is:

- lowering must be total for the supported public SQL surface
- optimization is best-effort
- optimizer gaps must degrade to less optimized SQL, not to planner failure

## Why This Plan Exists

The current engine still has multiple competing paths:

- a canonicalizer-led generic read path
- a surface-expansion fallback path
- a selector-specific path for write resolution

Those paths disagree on what the source of truth is:

- sometimes the original `Statement`
- sometimes stringified predicates
- sometimes a narrower structured selector model

That is the root cause behind failures like nested public subqueries in write selectors. The engine can understand the top-level surface but still fail to lower nested public reads consistently.

## Non-Goals

This plan does not optimize first.

It explicitly avoids:

- teaching every optimizer rule every query shape before lowering can proceed
- keeping reject-on-noncanonical behavior as the default
- preserving old `sql2` naming on new APIs

## Target Architecture

### 1. One Semantic Boundary

After parsing, convert SQL into one structured public-query model.

That model should carry:

- resolved public/internal relations
- resolved columns and aliases
- subqueries and CTEs
- parameter references
- query shape information needed for reads and writes

It should not treat raw `Statement` text as the long-term semantic source of truth.

### 2. Recursive Surface Lowering

Lowering must rewrite public surfaces everywhere they appear:

- top-level reads
- nested subqueries
- `IN (...)`
- `EXISTS (...)`
- CTEs
- write selectors
- any derived query used during write resolution

The output of lowering is a backend-executable relational query/program.

For example, a delete like:

```sql
DELETE FROM lix_delete_variant_schema
WHERE message_id IN (
  SELECT id
  FROM lix_delete_message_schema
  WHERE bundle_id = 'bundle.cleanup'
)
```

must lower both public surfaces in one recursive pass. If the optimizer cannot improve that shape, the unoptimized lowered query should still execute correctly.

### 3. Optional Optimization Layer

After lowering, run optimization passes such as:

- effective-state pushdown
- dependency narrowing
- overlay pruning
- exact-filter fast paths
- selector specialization

If an optimization pass cannot prove something, it must return the input program unchanged.

It must not fail the whole query unless it detects a real semantic error.

### 4. Backend-Lowering as the Final Step

Only at the backend boundary should the engine emit:

- backend SQL text
- dense parameter arrays
- schema registrations / prerequisites

This keeps parameter identity, nested query structure, and public-surface semantics intact until the last responsible moment.

## Design Rules

### Rule 1: Never Reject for Lack of Optimization

Reject only for:

- invalid SQL
- invalid public-surface semantics
- explicit unsupported product constraints

Do not reject for:

- unfamiliar predicate shape
- nested public subquery shape
- inability to derive a narrow dependency spec
- inability to build a specialized effective-state plan

Those cases should fall back to generic lowered execution.

### Rule 2: Lowering Owns Correctness

Correctness must come from lowering, not from optimization passes.

The optimizer may improve:

- performance
- dependency precision
- pushdown
- fewer joins / smaller source scans

But the base lowered query must already be semantically correct.

### Rule 3: One Path for Reads and Selector Reads

Write selectors are not a separate semantic world.

They should use the same public-query lowering machinery as ordinary reads, with a narrower output contract such as:

- projected selector column(s)
- row ids / entity ids
- exact current row lookup

### Rule 4: No Internal SQL Text Transport

Do not build helper SQL strings and then reparse them to continue planning.

Internal planner/runtime boundaries should exchange structured query state, not SQL text.

## Proposed Pipeline

### Phase A: Parse

Input:

- SQL text
- raw parameters
- execution context

Output:

- parsed AST

### Phase B: Bind

Output:

- bound semantic query model

Responsibilities:

- resolve parameter references
- resolve relation names
- resolve column references
- record scope and alias information
- classify top-level operation kind

### Phase C: Public-Surface Lowering

Output:

- generic backend-executable relational program

Responsibilities:

- replace public surfaces with relational source programs
- recurse through subqueries and CTEs
- carry schema/dependency requirements alongside the lowered program
- preserve parameter identity until final backend emission

This phase should be able to lower both:

- ordinary reads
- read-like selectors used by writes

### Phase D: Optional Optimization

Output:

- same relational program, maybe improved

Responsibilities:

- state/entity optimization
- dependency narrowing
- pushdown analysis
- exact-filter recognition
- overlay-source minimization

If no optimization applies, return the program unchanged.

### Phase E: Backend Emission

Output:

- SQL statement batch
- dense parameter arrays
- schema registration requirements

Responsibilities:

- dialect-specific SQL generation
- placeholder compaction / renumbering
- backend-specific syntax choices

## Migration Strategy

## Phase 1: Define the New Contracts

### Goal

Introduce neutral, non-`sql2` contracts for:

- bound public query
- lowered public program
- optional optimizer input/output

### Tasks

1. Define the semantic query model in `packages/engine/src/sql/public`.
2. Define a lowered relational program type that is executable even without optimization.
3. Define optimizer traits/functions that accept the lowered program and return either:
   - an improved program, or
   - the unchanged program

### Deliverable

A contract split that makes "lower first, optimize second" explicit in the types.

## Phase 2: Make Generic Read Lowering Total

### Goal

Any supported public read should lower even if it cannot be canonicalized into a specialized plan.

### Tasks

1. Replace canonicalizer-led rejection for generic reads with:
   - structured lowering if recognized
   - otherwise generic public-surface expansion in the new semantic model
2. Ensure nested public subqueries and CTEs lower recursively.
3. Keep the output backend-executable without requiring effective-state specialization.

### Deliverable

A generic read path where planner understanding controls optimization quality, not correctness.

## Phase 3: Move Selector Reads Onto the Same Lowerer

### Goal

Delete the separate selector semantics path.

### Tasks

1. Express write selectors as bound semantic queries.
2. Lower them through the same public-surface lowering machinery as ordinary reads.
3. Keep selector-specific projection/result handling as a thin wrapper only.

### Deliverable

One read lowering path for both ordinary reads and write-resolution selectors.

## Phase 4: Make Writes Depend on Lowered Selector Programs

### Goal

Write resolution should consume lowered selector programs, not special-case planner helpers.

### Tasks

1. Replace selector helper paths in write resolution with the shared lowerer.
2. Ensure nested public subqueries in `UPDATE` and `DELETE` predicates lower correctly.
3. Register schema dependencies from the shared lowered program.

### Deliverable

The failing nested-subquery delete shape works because it is no longer special-cased.

## Phase 5: Isolate Optimization

### Goal

Turn the current specialized logic into optional optimization passes.

### Candidates

- effective-state planning
- dependency narrowing
- pushdown-safe predicate extraction
- exact-row fast paths
- specialized state/entity source building

### Tasks

1. Move these passes behind an optimizer boundary.
2. Ensure each pass can decline safely and return the input unchanged.
3. Remove any remaining "unsupported query shape" errors that are only optimization failures in disguise.

### Deliverable

The optimizer can be wrong about opportunity without being wrong about correctness.

## Phase 6: Remove Statement-Centric Semantic Dependencies

### Goal

Stop using raw `Statement` trees as the planner's semantic truth after binding.

### Tasks

1. Remove planner dependencies on `CanonicalizedRead.bound_statement.statement`.
2. Stop storing predicate semantics as strings.
3. Move dependency and schema derivation onto structured semantic query nodes.
4. Lower to backend SQL only at the final emission phase.

### Deliverable

No planner stage needs to re-derive meaning from raw SQL AST text after binding.

## Phase 7: Remove Transitional APIs and Naming

### Goal

Clean out the old split-path language and temporary compatibility seams.

### Tasks

1. Remove `sql2` prefixes from new APIs.
2. Delete surface-expansion fallback terminology once lowering is the main path.
3. Delete selector-specific planning seams that are no longer needed.
4. Shrink or remove old canonicalizer types if they are now optimizer-only helpers.
5. Rename planner analysis APIs away from theorem-prover language.
   - `prove_write()` should become `analyze_write()`.
   - `proof_engine.rs` should become `write_analysis.rs` or equivalent.
   - Keep `ScopeProof` / `SchemaProof` / `TargetSetProof` temporarily if needed, but prefer follow-up renames toward fact/analysis terminology once the API churn is worth it.
   - Principle: names should describe fact derivation and partial knowledge, not imply correctness proofs stronger than what the planner actually establishes.

## Immediate Next Implementation Plan

The next concrete implementation target is replacing the broad recursive rewrite path in
`planner/backend/lowerer/expand.rs` with a recursive public-query IR and direct lowering from that
IR.

This should happen in incremental slices.

### Slice 1: Define Broad Public-Query IR

#### Goal

Introduce a recursive IR that can represent the shapes the broad path currently handles via AST
rewrite.

#### Types

Add a new broad-query IR alongside the specialized read IR, for example:

- `BroadPublicQuery`
- `BroadQueryExpr`
- `BroadQuerySource`
- `BroadQueryTableRef`
- `BroadQueryJoin`
- `BroadQueryCte`
- `BroadQueryOrder`
- `BroadQueryLimit`

#### Required Semantics

The IR must represent:

- top-level `SELECT`
- nested subqueries
- `IN (subquery)`
- `EXISTS`
- derived tables
- CTEs
- multiple public and internal relations in one query
- relation aliases
- expression trees with preserved placeholder identity

#### Constraint

Do not try to make this IR optimizer-aware yet.

It only needs enough semantics to support correct lowering and recursive dependency extraction.

### Slice 2: Bind Broad Public Reads into the IR

#### Goal

Replace `summarize_bound_public_read_statement(...)` as the semantic starting point for broad reads.

#### Tasks

1. Build a binder from parsed `Query` AST to `BroadPublicQuery`.
2. Preserve relation classification in the bound IR:
   - public surface
   - internal table
   - external relation
3. Preserve recursive subquery structure in the IR instead of flattening to relation-name sets.
4. Move broad read metadata extraction onto the IR:
   - public surface bindings
   - internal relation detection
   - requested history root commit ids

#### Deliverable

Broad public-read preparation should be able to reason about a query without re-traversing the raw
`Statement`.

### Slice 3: Derive Broad Dependencies from the IR

#### Goal

Replace relation-summary heuristics with recursive dependency derivation from the bound IR.

#### Tasks

1. Add a dependency visitor over `BroadPublicQuery`.
2. Collect:
   - bound public relations
   - schema requirements
   - active-version dependencies
   - history root references
3. Keep the output conservative by default.
4. Do not reject complex shapes just because precision is poor.

#### Deliverable

The broad path gets schema/dependency requirements from the same recursive structure that lowering
uses.

### Slice 4: Lower Broad IR Directly

#### Goal

Replace AST mutation in `expand.rs` with recursive lowering from `BroadPublicQuery` to
backend-executable SQL.

#### Tasks

1. Lower public table refs to derived backend queries.
2. Recurse through:
   - subqueries
   - CTE bodies
   - derived tables
   - `IN`/`EXISTS` expressions
3. Preserve aliases and placeholder identity through lowering.
4. Emit:
   - lowered `Statement`/`Query`
   - required schema registrations

#### Constraint

If a public relation can be lowered but not optimized, emit the slower lowered query anyway.

### Slice 5: Route Broad Public Reads Through the New IR Path

#### Goal

Make the broad IR lowerer the default non-specialized public-read path.

#### Tasks

1. Change `prepare_public_read_via_surface_lowering(...)` to:
   - bind to `BroadPublicQuery`
   - derive dependencies from the IR
   - lower from the IR
2. Keep `expand.rs` only as a temporary fallback/debug cross-check during rollout.
3. Add assertions that the IR-lowered query no longer references public surfaces.

#### Deliverable

Broad public reads stop depending on raw AST rewrite for correctness.

### Slice 6: Delete the Broad Rewrite Engine

#### Goal

Remove `expand.rs` as the semantic public-read lowering path.

#### Tasks

1. Delete or quarantine `rewrite_supported_public_read_surfaces_in_statement*`.
2. Remove broad-path callers that only exist to support AST rewriting.
3. Keep only parse-boundary statement inspection helpers that are still needed for top-level
   classification.

#### Deliverable

The only lowering path is IR-based.

### Validation Sequence for These Slices

For each slice, run at least:

- nested public subqueries in broad reads
- CTEs referencing public surfaces
- mixed public/internal broad reads
- SQLite entity/history/filesystem cases that currently route through broad lowering
- existing selector/update/delete regression tests to ensure the shared contracts still line up
- `cargo test -p lix_engine sqlite --no-fail-fast`
- when that SQLite suite finds regressions, fix them immediately and rerun the same command in a loop until it passes cleanly

## Tests and Guardrails

### Must Pass

- nested public subqueries in reads
- nested public subqueries inside write selectors
- CTEs over public surfaces
- `IN`, `EXISTS`, and derived-table shapes over public surfaces
- selector-based `UPDATE` and `DELETE` over entity/state/filesystem surfaces

### Guardrails

Add tests that assert:

- lowering succeeds for supported queries even when no optimization applies
- optimizer declines do not change results
- schema dependencies are collected recursively from nested public reads
- write selectors and ordinary reads use the same lowering machinery

## Success Criteria

The redesign is successful when:

- supported public SQL lowers to executable backend SQL without needing optimizer recognition
- nested public surfaces are handled recursively and uniformly
- write selectors no longer require their own semantic pipeline
- optimization failures degrade to slower plans, not planner errors
- new planner APIs no longer use `sql2` naming

## Progress Log

- 2026-03-12: Tightened plan4 validation to require a SQLite-first no-fail-fast loop. For every remaining planner/runtime slice, run `cargo test -p lix_engine sqlite --no-fail-fast`, fix the reported regressions, and rerun until the full SQLite-focused engine suite is green.

- 2026-03-11: Created `plan4.md` around the principle that lowering must produce executable backend SQL for supported public queries, while optimization is optional and non-blocking.
- 2026-03-12: Implemented the first lowering-first slice for nested selector subqueries. Nested public reads inside selector predicates now lower through the shared public-read lowering path, carry required schema registrations back out, and no longer fail just because the selector-specialized path only understood top-level surfaces.
- 2026-03-12: Implemented a generic read fallback slice: reads that bind multiple public surfaces now prefer broad recursive surface lowering over the specialized canonicalized path, and specialized read prep no longer returns a prepared public read with no lowered SQL. This shifts more shapes from “optimizer-dependent” to “lowering-first”.
- 2026-03-12: Made lowered public reads carry explicit schema requirements. Direct execution call sites that consume lowered public queries now register those schema tables before executing, aligning the lowering contract more closely with the plan4 principle that lowering should produce an executable backend program plus its requirements.
- 2026-03-12: Renamed the main runtime preparation contracts away from `sql2` (`PreparedPublicRead`, `PreparedPublicWrite`, `PreparedPublicExecution`) and made `PreparedPublicRead.lowered_read` mandatory rather than optional. This makes “prepared but not lowered” unrepresentable for public reads and pushes the runtime seam further toward lowering-first semantics.
- 2026-03-12: Renamed the top execution seam for public writes away from `sql2` in `shared_path.rs`, `api.rs`, and transaction/session routing. The runtime now talks in terms of `public_write`, `PendingPublicAppendSession`, and `maybe_execute_public_write_*`, which aligns the execution boundary with the lower-first planner contract instead of the old split-path naming.
- 2026-03-12: Renamed the public-write preparation helpers in `sql/public/runtime/mod.rs` away from `sql2` (`build_public_write_execution`, `public_write_preparation_error*`, `public_write_phase_trace`, `build_public_write_invariant_trace`, etc.). This keeps the new planner/runtime contracts consistently product-named even though some deeper validation helpers still retain legacy identifiers.
- 2026-03-12: Moved specialized read-planning artifacts behind an explicit optional optimization bundle on `PreparedPublicRead`. The base prepared-read contract is now `dependency_spec + lowered_read + debug_trace`, while canonicalized/effective-state data is exposed only as optional optimization state. This is the first real type-level step toward “lower first, optimize second.”
- 2026-03-12: Introduced `StructuredPublicRead` and moved dependency derivation, effective-state planning, and backend read lowering onto that neutral read model instead of `CanonicalizedRead`. Canonicalization is now only one producer of the structured read model, rather than the semantic owner of those planner stages.
- 2026-03-12: Added a normalized top-level read shape to `StructuredPublicRead` (`source_alias`, `projection`, `selection`, conjunctive predicates, `order_by`, `limit_clause`) and moved specialized dependency extraction, effective-state analysis, and lowered-query construction onto that normalized model. The specialized planner no longer reopens `structured_read.bound_statement.statement` as its semantic source of truth; it rebuilds executable queries from the normalized read shape plus lowered source queries.
- 2026-03-12: Removed the remaining semantic raw-statement reads outside the broad rewrite boundary. Active-history root binding now updates the normalized read model directly, public-read history root materialization flows through structured/summarized read metadata instead of re-reading `Statement`, and public write proof/execution no longer recovers selector/result semantics from `Statement::Update`/`Delete`. The remaining `Statement` access in `sql/public` is now parse-boundary classification and the broad recursive rewrite engine itself.
- 2026-03-12: Expanded `plan4.md` with an execution plan for the remaining work: define a recursive broad public-query IR, bind broad reads into that IR, derive dependencies from it, lower broad reads directly from it, route broad public reads through that path, and then delete the AST rewrite engine in `expand.rs`.
- 2026-03-12: Replaced the broad public-read AST rewrite engine with a recursive broad-read lowering path. Public read surface expansion now binds queries into a broad bound model, lowers public table references from that model, recursively lowers nested subqueries found in expressions, and no longer depends on `expand.rs` for correctness.
- 2026-03-12: Removed the selector-specific execution seam. `write_resolver` now builds an ordinary public `SELECT` query for selector lookups and executes it through the same `execute_public_read_query_strict` path used by other lowered public reads. The old selector-only runtime/lowering helpers were deleted, and the sparse-placeholder regression is now covered by a real SQLite engine test instead of a fake backend unit stub.
- 2026-03-12: Isolated specialized read optimization behind an explicit decline path. Canonicalization, effective-state setup, and specialized lowering now act as best-effort optimizers: when they decline or hit non-semantic lowering errors, `try_prepare_public_read_with_internal_access` falls back to the broad lowered path instead of failing the query. Semantic read errors like unknown columns still remain fatal and keep their existing diagnostics.
- 2026-03-12: Extended the same “optimize when possible, lower when needed” principle to the state write live slice. State `UPDATE`/`DELETE` resolution no longer collapses selector-driven writes down to `entity_id` only; it now resolves full visible-row identity through public selector reads and can handle broader selectors such as `OR` disjunctions on `lix_state_by_version`, while semantic constraints like required `version_id` predicates still remain fatal.
- 2026-03-12: Extended the entity write slice so it no longer rejects all non-property state-column updates as a live-slice limitation. Entity `UPDATE` now supports non-identity state columns that can be persisted directly (`lixcol_plugin_key`, `lixcol_schema_version`, `lixcol_metadata`, and same-value identity columns), while true identity changes and primary-key mutations still remain fatal.
- 2026-03-12: Removed the exact-filter-only admin write shortcut. `lix_active_version`, `lix_active_account`, and `lix_version` update/delete targeting now uses the same public selector-read approach as other write families, so OR selectors and broader public predicates degrade to slower row targeting instead of failing at the resolver boundary.
- 2026-03-12: Fixed a remaining admin insert live-slice gap in `lix_version`: bulk `VALUES (...), (...)` inserts now route through `payload_maps()` and produce the same descriptor/pointer writes as repeated single-row inserts. This keeps `lix_version` aligned with the lowering-first contract instead of silently falling back to a single-row-only resolver.
- 2026-03-12: Removed the exact-selector-only shortcut for filesystem file/directory update/delete targeting. Filesystem writes now use public selector reads to resolve target ids, which allows broader predicates like OR selectors; the only remaining guardrail in this slice is multi-row structural renames, which still stay explicit until the planner can validate cross-target path conflicts correctly.
- 2026-03-12: Extended the filesystem insert live slice from single-row to true multi-row `VALUES (...), (...)` planning. File and directory inserts now plan through a shared pending batch that reuses shared ancestor directories, lets later explicit directory rows override auto-created ancestors in the same statement, and keeps the new selector-regression fixtures on real multi-row public inserts instead of single-row seeding workarounds.
- 2026-03-12: Removed the remaining fake live-slice guard on multi-row directory structural updates. Batch directory renames/moves now resolve parent links and final paths across the whole target set before validating uniqueness, including descendant rows whose final path depends on another targeted parent’s rename. Multi-row file path rewrites were also reclassified from a misleading “not yet support” planner error to the real semantic uniqueness failure they represent.
- 2026-03-12: Reworked public write execution around physical partitions instead of a single global execution mode. `ResolvedWritePlan` and `PublicWriteExecution` now execute multiple partitions inside one outer engine transaction, which removes the old hard failure for mixed tracked/untracked winners and lets tracked state/entity writes split by concrete version lane instead of collapsing to one write lane.
- 2026-03-12: Relaxed explicit-version write proof from “must prove one literal version_id up front” to “must stay on an explicit-version surface and discover concrete lanes from selected rows.” `lix_state_by_version` update/delete selectors can now target multiple version lanes in one statement, and the real SQLite regressions cover both mixed tracked/untracked state writes and multi-version tracked writes instead of relying on fake-backend runtime tests.
- 2026-03-12: Added a Phase 7 naming task to replace theorem-prover framing in write analysis. `prove_write()` / `proof_engine.rs` should be renamed toward `analyze_write()` / `write_analysis.rs`, because the planner derives partial facts and bounded classifications rather than establishing correctness proofs in the strong sense.
- 2026-03-12: Renamed the write “proof” seam to write analysis. `proof_engine.rs` is now `write_analysis.rs`, `prove_write()` is now `analyze_write()`, the runtime phase trace now records `analyze_write`, and the touched planner/runtime tests were updated accordingly. This keeps the write pipeline aligned with plan4’s “derive facts, then lower” model instead of theorem-prover terminology.
- 2026-03-12: Removed the remaining backward-compat wording shims from the active write path. Runtime error normalization now prefers the clean public-lowering vocabulary, the write resolver and domain-change derivation use `public` instead of `sql2`/`day-1` in the touched diagnostics, and the SQL guardrail for the runtime seam now asserts the current `prepare_public_execution_with_internal_access` entrypoint instead of the removed `prepare_sql2_*` names.
- 2026-03-12: Closed the SQLite validation loop. `cargo test -p lix_engine sqlite --no-fail-fast` now passes cleanly after fixing selector-query placeholder rebinding for derived public selector reads, exact-filter selector predicate reconstruction for exact writes, and the remaining SQLite stack-overflow harness gap in `state_by_version_view`.
