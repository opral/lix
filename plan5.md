# Plan 5: Extend Sql2 Public Reads To Composed Surface Queries

## Goal

Close the gap between sql2 public-read ownership and sql2 public-read breadth by supporting composed public-read query shapes without reintroducing fallback.

The immediate failing family is:

- `lix_active_version` joined to `lix_version`

The goal of this phase is not to build a full general relational optimizer. The goal is:

- add the minimal sql2 semantic machinery needed to compose lowered public surfaces inside joins and subqueries

This means sql2 stays the semantic owner for public read composition, while the SQL backend continues to do the actual relational execution.

## Problem Statement

After the Phase 5 bridge deletion, public reads are structurally owned by sql2. That is correct.

The current limitation is that sql2 read canonicalization in `packages/engine/src/sql2/planner/canonicalize.rs` still rejects join shapes with:

- `sql2 day-1 canonicalizer requires a single surface scan without joins`

This creates an ownership/breadth mismatch:

- dispatch is correct
- implementation breadth is incomplete

The fix should be to widen sql2 read support, not to reintroduce bridge or fallback logic.

## What The SQL Backend Should Do

The backend database already knows how to execute:

- joins
- subqueries
- `EXISTS`
- aliasing
- `WHERE`
- `ORDER BY`
- `LIMIT`

That is not the hard part.

The hard part in `lix` is that public surfaces are semantic projections, not plain backend tables. So sql2 must first lower each public surface reference into a semantics-correct derived query.

For example, a query like:

```sql
SELECT *
FROM lix_state s
WHERE EXISTS (
  SELECT 1
  FROM lix_key_value kv
  WHERE kv.id = s.entity_id
)
```

should become conceptually:

```sql
SELECT *
FROM ( ...lowered lix_state... ) s
WHERE EXISTS (
  SELECT 1
  FROM ( ...lowered lix_key_value... ) kv
  WHERE kv.id = s.entity_id
)
```

The database handles the join/subquery execution. sql2 is responsible for:

- expanding each public surface into the right semantic source query
- preserving aliasing and correlation
- preserving `ON` versus `WHERE` predicate placement
- keeping residual predicates explicit when they cannot be pushed into a surface expansion safely

This phase should therefore focus on semantics-preserving public-surface expansion under composition, not on replacing the SQL backend's relational engine.

## First-Principles Model

1. sql2 owns public-surface semantics
2. ownership is independent from statement validity
3. unsupported public read shapes should become narrower over time by expanding sql2, not by sending them back to legacy planners
4. the SQL backend should execute relational composition after sql2 lowers each public surface into a semantics-correct derived query
5. any new semantic IR added here should be the smallest extension needed for composition

For this phase, the next semantic expansion is:

- composed public-surface reads with explicit join/subquery-aware relation expansion

Not:

- legacy fallback for joined public queries
- a full general read optimizer
- a broad final-form relational algebra

## Scope

### In scope

- top-level `SELECT`/`EXPLAIN` reads with joins across public surfaces
- public-surface references inside subqueries where sql2 already owns the referenced surfaces
- multiple join operands
- inner and outer joins
- equi-joins and non-equi join predicates
- projection, `WHERE`, `ORDER BY`, `LIMIT`, and join aliasing above the join tree
- initial must-pass family:
  - `lix_active_version` joined to `lix_version`
- immediate follow-on family:
  - representative `EXISTS`/subquery shapes over lowered public surfaces

### Out of scope

- aggregate/group-by/having expansion unless a concrete must-pass query demands it
- non-public internal-only query forms
- cost-based optimization
- a final-form general `RelExpr` framework

## Target Query Shapes

Initial concrete shape:

```sql
SELECT av.version_id, v.commit_id
FROM lix_active_version av
JOIN lix_version v ON v.id = av.version_id
ORDER BY av.id
LIMIT 1
```

Additional required coverage:

```sql
SELECT ...
FROM lix_active_version av
JOIN lix_version v ON v.id = av.version_id
WHERE ...
```

Broader representative coverage:

```sql
SELECT wc.id, av.version_id, v.commit_id
FROM lix_working_changes wc
JOIN lix_active_version av ON av.version_id = wc.lixcol_version_id
JOIN lix_version v ON v.id = av.version_id
```

```sql
SELECT *
FROM lix_state s
JOIN lix_change c ON c.entity_id = s.entity_id
LEFT JOIN lix_version v ON v.id = s.lixcol_version_id
```

```sql
SELECT *
FROM lix_state s
WHERE EXISTS (
  SELECT 1
  FROM lix_key_value kv
  WHERE kv.id = s.entity_id
)
```

## Architecture Changes

### 1. Canonicalization And Relation Expansion

File:

- `packages/engine/src/sql2/planner/canonicalize.rs`

Change:

- extend read canonicalization from single-scan roots to composed read roots
- represent the minimum relation structure needed to preserve composed public-surface semantics
- support public-surface references in joins and subqueries without routing them back through legacy preprocess
- preserve aliasing and predicate placement as planner data rather than flattening them back into SQL strings
- keep the shape minimal and driven by concrete failing queries

Deliverable:

- a canonical read form that can represent:
  - public scan leaves
  - join nodes where needed
  - relation aliasing
  - outer projection/filter/sort/limit
  - enough information to lower public-surface subqueries compositionally

### 2. Read IR

File:

- `packages/engine/src/sql2/planner/ir/mod.rs`

Change:

- add the smallest join-capable semantic extension to the current read shell
- keep scan leaves semantic surface scans, not backend table names
- do not introduce a full general optimizer algebra in this phase

Suggested shape:

- extend `ReadPlan` with a join-capable relation node such as:
  - `Join { left, right, kind, predicate, alias data }`

The important property is:

- sql2 models composition between semantic surface expansions
- the backend still executes the actual join after lowering

### 3. Dependency And Effective-State Preparation

Files:

- `packages/engine/src/sql2/runtime/mod.rs`
- `packages/engine/src/sql2/planner/semantics/dependency_spec.rs`
- `packages/engine/src/sql2/planner/semantics/effective_state_resolver.rs`

Change:

- derive dependencies across composed public-scan leaves
- prepare effective-state inputs per public scan node
- compose lowering over multiple semantic scan leaves without assuming a single scan root

Constraint:

- do not assume arbitrary mixed-family joins are valid by default
- mixed-family composition must stay explicit about the authority and semantic source of each leaf
- predicates moved into a leaf expansion must remain conservative and fail closed

- do not reintroduce generic preprocess fallback

### 4. Lowering

File:

- `packages/engine/src/sql2/planner/backend/lowerer.rs`

Change:

- lower each public surface scan to its backend source query
- lower composed relation nodes over those derived public scan sources
- preserve:
  - selected columns
  - aliases
  - where predicates
  - order by
  - limit
  - join kinds and join predicates

Key principle:

- sql2 lowers public surfaces into derived backend queries
- the SQL database then performs the join, subquery execution, and remaining relational work

### 5. Runtime Entry

File:

- `packages/engine/src/sql2/runtime/mod.rs`

Change:

- `prepare_sql2_read()` and `prepare_sql2_public_execution()` should accept the new joined-read canonical form
- error messages for unsupported composed reads should stay explicit and sql2-owned
- unsupported shapes should fail inside sql2 rather than routing elsewhere

## Testing Plan

### Direct sql2 unit coverage

Add read-preparation tests for:

- `lix_active_version JOIN lix_version`
- same shape with `ORDER BY` and `LIMIT`
- a representative multi-join shape
- an outer join shape
- a non-equi join shape
- a representative `EXISTS` subquery over a lowered public surface

Keep this phase concrete:

- add only the additional fixtures needed to prove the minimal composition model works
- do not broaden the matrix to "all public families" up front

### Integration suites

Must pass:

- `packages/engine/tests/checkpoint.rs`
- `packages/engine/tests/version_api.rs`

Should re-run:

- `packages/engine/tests/working_changes_view.rs`
- `packages/engine/tests/transaction_execution.rs`
- `packages/engine/tests/file_history_view.rs`
- `packages/engine/tests/state_history_view.rs`

### Structural guardrails

Preserve:

- no new fallback path for joined public reads
- no new dependency from sql2 back into generic preprocess for public read ownership

## Acceptance Criteria

- checkpoint tests pass without reintroducing legacy bridge behavior
- version-facing public join queries pass through sql2
- representative multi-join and subquery-based public queries pass through sql2
- sql2 remains the sole semantic owner for these public joined reads
- joined public reads no longer depend on the legacy preprocess bridge
- the lowered shape is "expand public surfaces, let the backend execute the relation"

## Suggested Implementation Order

1. add failing sql2 unit tests for `lix_active_version JOIN lix_version`
2. add one minimal join-capable semantic extension to the current read IR
3. lower composed public reads by expanding each public surface into a derived source query
4. add one representative public-surface subquery shape
5. run checkpoint, version, and representative composition-heavy suites
6. expand only the next unsupported shapes revealed by those suites

## Non-Goals

- do not special-case checkpoint by bypassing the public SQL pipeline
- do not reintroduce fallback in `query_runtime/shared_path.rs`
- do not build the final general relational algebra in this phase
