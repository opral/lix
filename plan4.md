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

- 2026-03-11: Created `plan4.md` around the principle that lowering must produce executable backend SQL for supported public queries, while optimization is optional and non-blocking.
