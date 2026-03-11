# Plan 7: Sql2 Priority Backlog For Broad Public-SQL Acceptance

## Objective

Turn the sql2 refactor direction from `plan6.md` into a ranked implementation backlog.

Core rule:

- if sql2 can bind all referenced public surfaces, rewrite them to backend-real sources, preserve semantics, and hand the resulting SQL to the backend, the query should run
- lack of advanced optimization must not be a rejection reason by itself

This plan is intentionally prioritized from highest to lowest engineering value.

## Ranking Principles

Rank items by this order:

1. removes current false rejections for bindable public queries
2. preserves public semantics without reopening legacy fallback
3. reduces architectural coupling that keeps reintroducing planner-shape rejection
4. improves optimization only after acceptance/correctness are stable

## HIGH Priority

### 1. Replace Read-Shape Gating With Recursive Public-Surface Binding

Current blockers live in:

- `packages/engine/src/sql2/planner/canonicalize.rs`
- `packages/engine/src/sql2/runtime/mod.rs`

Current failure mode:

- sql2 still rejects bindable reads with messages like:
  - `requires a single surface scan without joins`
  - `only supports SELECT bodies`
  - `only supports Scan->Filter->Project->Sort->Limit read shapes`

Required change:

- bind public surfaces anywhere in the read AST
- stop using canonical read shape as the primary admissibility gate
- treat joins, subqueries, CTE bodies, `EXISTS`, `IN`, and derived tables as normal composition if their public leaves can be bound

Why this is highest priority:

- this is the main place where sql2 violates the core rule today

Done when:

- bindable public reads stop failing purely because the outer query is not a day-1 single-scan shape

### 2. Make Surface Expansion The Required Read Primitive

Current blockers live in:

- `packages/engine/src/sql2/planner/backend/lowerer.rs`
- `packages/engine/src/sql2/planner/ir/mod.rs`

Required change:

- lower each public surface leaf into a semantics-correct backend subquery
- preserve the original relational shell around those expanded leaves
- treat expansion as correctness-critical and pushdown as optional

Why this is highest priority:

- sql2 should own semantic leaf expansion, not relational execution
- this is the mechanism that lets the backend run broad SQL without sql2 having to model every read shape first

Done when:

- a query with multiple bound public leaves can be reconstructed as valid backend SQL without falling back to legacy planning

### 3. Add A Generic Read Assembly Layer

Current blockers live in:

- `packages/engine/src/sql2/planner/backend/lowerer.rs`

Required change:

- assemble expanded public leaves back into the original query structure
- preserve:
  - aliases
  - join conditions
  - subquery boundaries
  - correlation
  - `GROUP BY`
  - `DISTINCT`
  - `ORDER BY`
  - `LIMIT`

Why this is highest priority:

- surface expansion is not enough unless sql2 can safely reconstruct the full statement around the expanded leaves

Done when:

- sql2 can emit backend SQL for broad read shapes without flattening them into a single-scan IR

### 4. Move Semantic Read Correctness To Leaf Contracts

Current blockers live in:

- `packages/engine/src/sql2/planner/semantics/effective_state_resolver.rs`
- `packages/engine/src/sql2/catalog/mod.rs`
- `packages/engine/src/sql2/runtime/mod.rs`

Required change:

- define per-family public leaf contracts for:
  - state/entity
  - filesystem
  - admin/version
  - change/working-changes
- compute dependencies, visibility, exposure, and observation metadata from bound leaves, not from one canonical root scan

Why this is highest priority:

- without this, sql2 can execute broad SQL but still lose the public semantics that make the result correct

Done when:

- correctness metadata derives from the set of bound public leaves and their expansions instead of from a single read shell

## MEDIUM-HIGH Priority

### 5. Define Explicit Mixed Public/Non-Public Composition Rules

Current blockers live in:

- `packages/engine/src/sql2/runtime/mod.rs`
- `packages/engine/src/query_runtime/preprocess.rs`

Required change:

- make mixed queries a semantic decision, not an accidental rejection
- explicitly allow mixed composition when:
  - all public leaves can be expanded safely
  - the remaining non-public relations are backend-real
- reject only when public semantics cannot be preserved or when internal-only relations would leak unsupported behavior

Why this matters:

- many realistic SQL queries combine public surfaces with ordinary backend relations or derived subqueries

Done when:

- sql2 has a principled contract for mixed queries instead of rejecting them because they are mixed

### 6. Turn Pushdown Into Optimization, Not Admissibility

Current blockers live in:

- `packages/engine/src/sql2/planner/backend/lowerer.rs`
- `packages/engine/src/sql2/backend/mod.rs`

Required change:

- if a filter, projection, ordering, or limit cannot be proven safe to push into a leaf, keep it above the leaf expansion
- do not reject the query merely because pushdown is incomplete

Why this matters:

- this is the exact boundary between correctness-first acceptance and optimizer-first rejection

Done when:

- rejected pushdowns become residual SQL structure, not planner errors

### 7. Remove Remaining Day-1 Read Rejection Paths

Current blockers live in:

- `packages/engine/src/sql2/planner/canonicalize.rs`

Representative current messages:

- `only supports SELECT bodies`
- `does not support GROUP BY`
- `requires a single surface scan without joins`

Required change:

- convert these from broad product-level rejection paths into:
  - binding failures
  - true semantic unsupportedness
  - or successful conservative lowering

Why this matters:

- the user-visible contract should be “bindable public query runs,” not “planner shell accepts this AST shape”

Done when:

- these day-1 read-shape messages disappear for bindable public queries

## MEDIUM Priority

### 8. Bring Write-Side Admission Fully In Line With The Same Rule

Current blockers live in:

- `packages/engine/src/sql2/planner/canonicalize.rs`
- `packages/engine/src/sql2/planner/semantics/write_resolver.rs`

Required change:

- keep ownership based on public surface binding
- keep validity based on proof/resolution, not dispatch shape
- remove remaining write rejections that exist only because sql2 has not modeled a syntax variant yet, where backend-preserving lowering is possible

Why this matters:

- the read side is the biggest breadth gap, but the write side should obey the same architectural rule

Done when:

- public writes are rejected only for real semantic unsupportedness, not planner day-1 shape limits

### 9. Shrink Runtime Into Small Ownership-Focused Modules

Current blockers live in:

- `packages/engine/src/sql2/runtime/mod.rs`

Refactor target:

- `sql2/runtime/dispatch.rs`
- `sql2/runtime/read.rs`
- `sql2/runtime/write.rs`
- `sql2/runtime/errors.rs`
- `sql2/runtime/debug.rs`

Why this matters:

- ownership, read prep, write prep, and error normalization are still too concentrated
- that concentration makes it easy for shape rejection and semantic ownership to become entangled again

Done when:

- `runtime/mod.rs` is a thin facade rather than the main policy surface

## LOW Priority

### 10. Split Lowerer Into Expansion, Pushdown, And Assembly Modules

Current blockers live in:

- `packages/engine/src/sql2/planner/backend/lowerer.rs`

Refactor target:

- `sql2/planner/backend/expand/`
- `sql2/planner/backend/pushdown/`
- `sql2/planner/backend/assemble/`

Why this is lower priority:

- this is important cleanup, but it should follow the acceptance-model fix instead of leading it

### 11. Improve Optimizer Quality After Broad Acceptance Is Stable

Examples:

- narrower dependency derivation
- deeper pushdown
- better composed explain/debug traces
- smarter leaf pruning

Why this is lower priority:

- these changes improve efficiency and debuggability, not the basic “query should run” contract

### 12. Build A Permanent Broad-Acceptance Regression Suite

Coverage should include:

- joins across multiple public surfaces
- joins plus derived tables
- subqueries, `EXISTS`, and `IN`
- `GROUP BY`, `DISTINCT`, and aggregates
- CTEs that reference public surfaces
- mixed public/non-public queries
- public read-only surfaces rejecting writes semantically through sql2

Why this is lower priority:

- this should lock in the new behavior after the architectural acceptance path exists

## Recommended Execution Order

Implement in this order:

1. `HIGH` 1 through 4 as one acceptance-model cut
2. `MEDIUM-HIGH` 5 through 7 to remove remaining false rejection classes
3. `MEDIUM` 8 so write-side admission matches the same principle
4. `MEDIUM` 9 to reduce concentration and make the new model durable
5. `LOW` 10 through 12 after correctness and acceptance are stable

## Guardrails

Do:

- reject only for real semantic unsafety, unsupported public semantics, or disallowed internal leakage
- preserve public semantics at the leaf level
- let the backend execute broad relational composition

Do not:

- reintroduce legacy fallback for public queries
- require optimizer sophistication before accepting a bindable public query
- keep planner-shape error messages as product behavior for bindable public reads

## Acceptance Criteria

This plan succeeds when:

1. sql2-owned public reads no longer fail because the outer query is “too complex” if all public leaves can be bound and expanded safely
2. sql2 preserves public semantics while handing broad relational composition to the backend
3. the remaining user-visible rejections are about actual semantics, not missing planner shell coverage
4. runtime ownership and planner validation stay cleanly separated

## Progress log

- 2026-03-11 14:22 PST - initial plan draft
- 2026-03-11 15:08 PST - implemented the first HIGH-priority acceptance cut in the existing broad-read path: sql2 public read surface expansion is now scope-aware for CTE shadowing and recursively rewrites bindable public leaves in `WITH`, join constraints, grouping/order/limit expressions, functions, `EXISTS`, `IN`, and derived subqueries. Expanded fallback leaf support now covers state/admin/change/filesystem/entity families through backend-real derived queries instead of the old narrow admin/filesystem subset.
- 2026-03-11 15:08 PST - validated the slice with focused sql2 unit coverage plus integration canaries. New/runtime-updated coverage proves bindable CTE+JOIN+GROUP BY public reads prepare through sql2 and that CTE names shadowing public surfaces stay non-public. Broader canaries passing on this checkpoint: `checkpoint`, `version_api`, and `working_changes_view`.
- 2026-03-11 15:24 PST - broadened validation after formatting and stabilized brittle lowered-SQL assertions to check semantic markers instead of exact `WITH` prefixes. Full `sql2::runtime` unit slice is green again, and heavier public-read suites validating the same expansion model are green at this checkpoint: `file_history_view`, `file_materialization`, and `sql_guardrails`.
- 2026-03-11 15:46 PST - implemented the next semantic-ownership slice on top of the broad expansion path: sql2 now builds an explicit bound-public-leaf summary for broad reads, derives conservative dependency specs from those bound leaves instead of returning `None`, and rejects mixed public-plus-`lix_internal_*` reads with `LIX_ERROR_INTERNAL_TABLE_ACCESS_DENIED` while still allowing public queries to compose with ordinary backend-real tables. This also brings `lix_change` canonical reads onto an explicit conservative dependency contract instead of the old `None`.
- 2026-03-11 15:46 PST - validated the semantic slice with the full `sql2::runtime` lib suite plus the main public-read canaries. New policy tests cover both allowed public/external joins and rejected public/internal joins. Integration canaries still green at this checkpoint: `checkpoint` and `version_api`.
- 2026-03-11 17:08 PST - fixed another `plan7` ownership gap in `sql2::runtime`: `lower_public_read_query_with_sql2_backend()` now boots the backend surface registry before deciding whether a query is public, so backend-registered public surfaces are lowered through sql2 instead of slipping past the broad-read path. Added regression coverage for dynamic stored-schema surfaces plus a `GROUP BY/HAVING` public read that should run through conservative surface expansion rather than day-1 canonical read gating.
- 2026-03-11 17:08 PST - started the structural cleanup phase from the same checkpoint by extracting the broad public read-surface expansion machinery into `sql2/planner/backend/lowerer/expand.rs`. `lowerer.rs` now keeps live lowering and assembly responsibilities while the recursive public-leaf expansion path has its own module boundary. Validation green at this checkpoint: `sql2::planner::backend::lowerer`, full `sql2::runtime`, and the dynamic-surface `transaction_execution` canary.
- 2026-03-11 17:34 PST - continued the runtime decomposition step from `plan7` without changing behavior: the top-level sql2 read ownership/preparation entrypoints now live in `sql2/runtime/read.rs`, while `runtime/mod.rs` keeps the shared types, dispatch facade, and write-side path. This keeps public-read ownership logic out of the main mixed read/write module without reopening any fallback path.
- 2026-03-11 18:03 PST - fixed a provenance/access bug in the broad-read path: sql2 was applying the mixed public-plus-`lix_internal_*` guard after engine-generated lowering and without honoring call-site internal access. The read-preparation path now takes explicit internal-table access at the user boundary, internal engine lowering (`lower_public_read_query_with_sql2_backend`) opts into internal sources intentionally, and statement-script / transaction execution now preserve that access bit through shared preparation.
- 2026-03-11 18:03 PST - validated the fix with focused runtime policy tests plus the full `file_history_view` suite. The restored contract is: user-authored mixed public/internal reads are still rejected by default, they prepare when internal access is explicitly enabled, and engine-generated file-history/materialization reads no longer false-positive on their own lowered internal ancestry/change sources.
