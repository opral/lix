# Plan 6: Refactor Sql2 Around Broad Public-SQL Acceptance

## Objective

Refactor `packages/engine/src/sql2/**` so public-SQL ownership is based on semantic surface binding, not on a narrow day-1 planner shape whitelist.

Core rule:

- if sql2 can bind all referenced public surfaces, rewrite them to backend-real sources, preserve semantics, and hand the resulting SQL to the backend, the query should run
- lack of advanced optimization must not be a rejection reason by itself

This plan is broader than `plan5.md`.

- `plan5.md` is the immediate breadth gap around joined public reads
- `plan6.md` is the refactor needed so sql2 stops re-creating that same class of problem as new public query shapes appear

## Why A New Plan

The current sql2 pipeline still reflects a day-1 migration shape:

- `packages/engine/src/sql2/runtime/mod.rs` owns dispatch, compatibility behavior, read preparation, write preparation, error mapping, and a large amount of test-only inspection logic
- `packages/engine/src/sql2/planner/canonicalize.rs` still acts as a shape gatekeeper for reads, especially with errors like:
  - `requires a single surface scan without joins`
  - `only supports SELECT bodies`
  - `only supports Scan->Filter->Project->Sort->Limit read shapes`
- `packages/engine/src/sql2/planner/backend/lowerer.rs` is simultaneously:
  - family-specific surface lowering
  - read SQL rewriting
  - pushdown logic
  - backend query assembly
- `packages/engine/src/sql2/planner/ir/mod.rs` is still centered on a single-scan read shell rather than a general “bound public query” model

That is why sql2 currently rejects bindable public queries for planner-shape reasons even when the backend database could execute the composed lowered query correctly.

## Current Diagnosis

The main problem is not that sql2 lacks a cost-based optimizer.

The main problem is that sql2 still collapses five separate concerns into one path:

1. public ownership
2. public surface binding
3. semantic validation
4. optional optimization and pushdown
5. backend SQL assembly

That coupling creates the wrong failure mode:

- if optimization or canonical-shape modeling is incomplete, the query gets rejected

The desired failure mode is narrower:

- if sql2 cannot bind the public surfaces, cannot preserve public semantics, or encounters a truly unsupported semantic rule, reject
- otherwise, lower conservatively and let the backend execute

## Governing Principles

1. Public ownership is determined by semantic surface references, not by whether the statement is valid.
2. Invalid public statements are still sql2-owned and must fail with sql2 semantic errors.
3. Sql2 may be the universal front door for statement classification, but it only takes semantic ownership of public-surface work.
4. Broad public-query acceptance comes before advanced optimization.
5. Surface expansion is the primary read primitive; relational execution is the backend database's job.
6. Semantic enrichment must be conservative and leaf-local where possible.
7. If sql2 cannot prove a pushdown or rewrite is safe, keep the predicate/project/join at the composed query level rather than rejecting.
8. No legacy fallback or public-pipeline bypass may return for public queries.
9. `query_runtime/**` may execute sql2 artifacts, but it must not decide public-surface semantics.

## Target End State

The public read path should look like this:

1. Parse and bind SQL.
2. Discover and bind every referenced public surface recursively, including joins, subqueries, and CTE bodies.
3. If no public surfaces are referenced, sql2 may still be the top-level dispatch/classification entrypoint, but it should classify the statement as non-public and pass it through without taking semantic ownership.
4. If any public surfaces are referenced, sql2 owns the statement.
5. Sql2 expands each public surface reference into a semantics-correct derived backend query.
6. Sql2 preserves the original relational composition around those expanded leaves.
7. Sql2 optionally pushes filters, projections, and ordering into individual leaves only when that is proven safe.
8. The backend database executes the composed query.
9. Dependencies, effective-state metadata, and observation data derive from the bound public leaves and their semantic expansions, not from SQL text inspection.

The public write path should look like this:

1. Parse and bind SQL.
2. Bind the public target surfaces.
3. Build explicit write intent.
4. Prove scope/schema/target facts conservatively.
5. Resolve authoritative pre-state and execution mode.
6. Produce one stable prepared write artifact for tracked or untracked execution.
7. Execute without re-deriving public semantics outside sql2.

## Architectural Changes

### 1. Split Ownership From Validation

Current problem:

- `runtime/mod.rs` still mixes “is this public?” with “can sql2 fully plan this exact shape right now?”

Refactor:

- introduce one explicit public ownership/binding phase for top-level statements and nested public references
- ownership should answer only:
  - does this statement reference public surfaces?
  - if yes, which ones?
  - are there mixed public/internal references that must be rejected?

Not:

- can the optimizer already model the full shape?

Deliverable:

- a `BoundPublicStatement` or equivalent contract that records:
  - statement kind
  - bound public surface references
  - nested reference locations
  - whether the statement is read or write
  - whether the statement is mixed public/internal in a disallowed way

### 2. Replace The Read Shape Whitelist With Compositional Binding

Current problem:

- `canonicalize_read()` assumes one surface scan plus a very narrow wrapper stack

Refactor:

- split read preparation into:
  - public-surface binding
  - semantic surface expansion
  - optional optimization/pushdown
  - backend SQL assembly

The key change:

- canonicalization must stop being a broad rejection gate for bindable public query shapes
- instead, it should become a binder/annotator that identifies:
  - public relation leaves
  - aliases
  - correlation boundaries
  - residual predicates and projections

Deliverable:

- a read preparation model that can preserve arbitrary relational composition over bound public leaves, even when sql2 does not yet deeply optimize that composition

### 3. Make Surface Expansion The Core Read Primitive

Sql2 does not need to own relational execution.

It needs to own semantic leaf expansion.

Refactor:

- for each public surface leaf, produce a semantics-correct derived backend query
- reinsert that derived query back into the original statement structure
- preserve:
  - join structure
  - subquery boundaries
  - aliases
  - correlation
  - `ON` vs `WHERE`
  - `ORDER BY`
  - `LIMIT`
  - `EXISTS`
  - CTE reference structure where possible

Initial rule:

- if the composed SQL can be preserved safely after surface expansion, accept it even if no additional optimization is performed

### 4. Move Semantic Read Enrichment To Leaf-Oriented Modules

Current problem:

- read dependency and effective-state preparation still assume a single canonical root

Refactor:

- derive semantic metadata from the set of bound public leaves instead of from a single root scan shape
- keep family-specific logic per leaf:
  - state/entity leaves
  - filesystem leaves
  - admin/version leaves
  - change/working-changes leaves

Conservative rule:

- if a predicate cannot be proven safe to push into a leaf expansion, keep it outside the leaf
- do not reject the query merely because pushdown is incomplete

### 5. Split Lowering Into Expansion, Pushdown, And SQL Assembly

Current problem:

- `lowerer.rs` is too large because it owns too many unrelated steps

Refactor target:

- `sql2/planner/backend/expand/`
  - family-specific public leaf expansion
- `sql2/planner/backend/pushdown/`
  - proven-safe leaf pushdown and residual handling
- `sql2/planner/backend/assemble/`
  - statement reconstruction with expanded leaves

The decomposition rule:

- expansion is required for correctness
- pushdown is optional optimization
- assembly is generic and should not encode family semantics

### 6. Shrink Runtime To A Thin Facade

Current problem:

- `runtime/mod.rs` is carrying too many responsibilities

Refactor target:

- `sql2/runtime/dispatch.rs`
  - public ownership and batch classification
- `sql2/runtime/read.rs`
  - public read preparation
- `sql2/runtime/write.rs`
  - public write preparation
- `sql2/runtime/errors.rs`
  - public error normalization
- `sql2/runtime/debug.rs`
  - debug trace and test-only inspection helpers

`runtime/mod.rs` should become a thin facade over those modules.

### 7. Finish The Write-Side Decomposition

The write-side correctness model is better now than the read side, but it is still too concentrated.

Refactor target:

- split `write_resolver.rs` by concern:
  - selector resolution
  - winner/effective-state targeting
  - family-specific write resolution
  - tracked/untracked execution-mode resolution
  - `ON CONFLICT` resolution

Keep the architectural rule already established:

- canonicalization records write intent
- proofs establish conservative facts
- resolver decides authoritative target rows and execution mode
- runtime executes the resolved artifact

## Phases

## Phase 1: Introduce Bound Public Statement Ownership

Goal:

- make public ownership recursive and structural

Work:

- add a binder that walks full statement trees and records every public-surface reference
- classify top-level statements as:
  - non-public
  - public-read
  - public-write
  - invalid mixed public/internal
- use this binder as the only ownership gate for sql2

Exit criteria:

- sql2 no longer decides ownership from read/write capability checks or single-scan assumptions
- invalid public statements are still sql2-owned

## Phase 2: Replace Read Canonicalization With Public-Leaf Expansion Prep

Goal:

- stop rejecting bindable read shapes for planner-shape reasons

Work:

- replace `canonicalize_read()` as the main read admission gate
- introduce a compositional read-preparation contract centered on bound public leaves and preserved SQL structure
- allow joins, subqueries, `EXISTS`, and CTE-contained public reads to proceed when leaf expansion is possible

Exit criteria:

- errors like `requires a single surface scan without joins` are gone for bindable public queries
- sql2 accepts broad composed public reads, even if initially unoptimized

## Phase 3: Add Leaf Expansion For All Public Families

Goal:

- ensure every public surface family can participate in composed reads through the same mechanism

Work:

- implement leaf expansion modules for:
  - state/entity
  - filesystem
  - admin/version
  - change
  - working changes
- make nested public reads use the same expansion entrypoint as top-level reads

Exit criteria:

- composed queries over mixed public families lower through one sql2 read path
- no top-level public read query requires bridge logic

## Phase 4: Rebuild Dependency And Effective-State Derivation Around Leaves

Goal:

- make semantic read metadata independent from a single canonical scan root

Work:

- derive dependency specs from bound public leaves plus their expansions
- derive effective-state requests per leaf
- preserve residual composition at the statement level

Exit criteria:

- dependency/effective-state derivation no longer assumes one scan root
- composed public reads can still report semantic dependencies correctly

## Phase 5: Split Lowerer And Runtime

Goal:

- remove the current concentration risk

Work:

- split `lowerer.rs` into expansion, pushdown, and assembly modules
- split `runtime/mod.rs` into dispatch/read/write/errors/debug modules
- keep behavior identical while reducing module concentration

Exit criteria:

- read lowering no longer depends on one monolithic file
- runtime orchestration is distinct from semantic planning

## Phase 6: Split The Write Resolver

Goal:

- keep write correctness while removing the current concentration bottleneck

Work:

- split `write_resolver.rs` by family and resolution concern
- keep resolver-driven execution-mode selection as the canonical model
- make selector reads and write-family handling explicit modules rather than internal regions of one file

Exit criteria:

- no single sql2 file remains the default home for unrelated write concerns
- write resolution stays explicit and testable

## Acceptance Criteria

1. A public query should be rejected only for semantic unsafety, unsupported public semantics, or disallowed public/internal mixing, not for missing optimizer sophistication.
2. Public joins and subqueries over bindable surfaces should run through sql2 without fallback.
3. Checkpoint/version/admin query families should pass without special-case bypasses.
4. Existing public write guarantees must remain:
   - transaction-scoped tracked commits
   - resolver-driven tracked/untracked execution mode
   - sql2-owned read-only write rejection
5. `query_runtime/**` must remain execution plumbing, not a second semantic planner.
6. `internal_state/**` must remain internal-only.

## Test Strategy

### Structural tests

- public ownership classification for:
  - read-only public writes
  - joined public reads
  - nested public subqueries
  - mixed public/internal rejection

### Read-breadth tests

- joins across admin/version surfaces
- joins across state/entity/version surfaces
- outer joins
- correlated `EXISTS`
- public reads inside CTE bodies

### Write-regression tests

- transaction batching
- `ON CONFLICT`
- resolver-driven tracked vs untracked updates
- read-only public write rejection

### Suite gates

- `packages/engine/tests/checkpoint.rs`
- `packages/engine/tests/version_api.rs`
- `packages/engine/tests/working_changes_view.rs`
- `packages/engine/tests/transaction_execution.rs`
- `packages/engine/tests/state_view.rs`
- `packages/engine/tests/entity_view.rs`
- `packages/engine/tests/change_view.rs`

## Non-Goals

- do not build a cost-based optimizer in this phase
- do not bypass the public SQL pipeline for engine internals
- do not reintroduce legacy fallback for public queries
- do not require deep pushdown before accepting a query the backend can execute correctly after surface expansion

## Progress

- 2026-03-11: created `plan6.md` after reviewing the current sql2 runtime, canonicalizer, IR, lowerer, and existing `plan4.md`/`plan5.md`
- 2026-03-11: implemented conservative public-read surface expansion as a fallback execution path when single-scan read canonicalization rejects a bindable public query shape; sql2 now prepares joined admin reads like `lix_active_version JOIN lix_version` without reopening fallback
- 2026-03-11: routed plugin/runtime history materialization reads through sql2 lowering instead of legacy preprocess, which restored the `file_history_view` family under full sql2 public-read ownership
- 2026-03-11: fixed exact `lix_file` delete-target collection to keep only rows that actually exist in the live or overlay view, eliminating bogus payload tombstones and restoring parameterized multi-statement transaction scripts
- 2026-03-11: threaded bound write parameters through sql2 selector-read resolution so write-family selector reads can execute lowered public queries with placeholders intact; `working_changes_view` is green again
- 2026-03-11: normalized state-history exposed-column errors to use the public `LIX_ERROR_SQL_UNKNOWN_COLUMN` contract on sql2-owned reads
