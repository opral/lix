# Plan 27: Support Public `INSERT ... SELECT` Canonicalization

This pass takes the canonicalization half of the original public state-write cleanup and treats it as its own SQL-planning cut.

This is the follow-on cut after [plan25_public_state_write_semantics.md](/Users/samuel/git-repos/lix-2/plan25_public_state_write_semantics.md).

## Problem

The public canonicalizer still rejects `INSERT ... SELECT` for state-backed writes.

Today:

- [`sql/public/planner/canonicalize.rs`](/Users/samuel/git-repos/lix-2/packages/engine/src/sql/public/planner/canonicalize.rs)
  still assumes a `VALUES`-shaped insert source
- parameterized source forms like:

```sql
INSERT INTO lix_state_by_version (...) SELECT $1, $2, $3, ...
```

fail even though they are normal SQL and used to work through the internal state path

That leaves the public write surface artificially narrow and makes the planner depend on one AST shape instead of owning row-source canonicalization.

## Goal

Support `INSERT ... SELECT` in the public canonicalizer for state-backed public writes.

The intended shape is:

```text
INSERT source
  -> canonicalize source rows
  -> produce payload rows
  -> preserve tracked/untracked mode semantics
  -> feed normal write resolver flow
```

The important move is:

- canonicalization operates on evaluated row payloads, not a hardcoded `VALUES` AST form

## Scope

At minimum, this plan should support the simple cases already exercised by tests:

- parameterized `SELECT $1, $2, ...`
- single-row projections with no table reads

If broader support is coherent in one pass, accept it. If not, land a crisp first cut and document the deferred shapes explicitly.

## Work Breakdown

### A. Replace `VALUES`-only canonicalization

- [x] Replace the `VALUES`-only insert-source branch in [`canonicalize.rs`](/Users/samuel/git-repos/lix-2/packages/engine/src/sql/public/planner/canonicalize.rs)
- [x] Canonicalize supported row-source forms into payload rows
- [x] Keep the resulting payload contract identical to the normal write resolver input

### B. Support the immediate public state-write cases

- [x] Support parameterized `INSERT ... SELECT $1, $2, ...`
- [x] Support direct `lix_state_by_version` public writes through that path
- [x] Ensure tracked/untracked mode rules still behave correctly

### C. Make the limits explicit if needed

- [x] Decide first-cut support for:
  - multi-row source
  - table-backed source
  - expressions
  - mixed tracked/untracked rows
- [x] If any of those remain deferred, fail with precise user-facing messages

### D. Add focused coverage

- [x] parameterized `INSERT ... SELECT` into `lix_state_by_version`
- [x] default application still works through that path once Plan 26 lands
- [x] unsupported source shapes fail with a precise message if still deferred

## Exit Criteria

Plan 27 is done when:

- the public canonicalizer supports the intended `INSERT ... SELECT` state-backed write shapes
- payload generation is no longer tied to `VALUES` AST ownership
- focused coverage exists for the parameterized direct-state insert path
- deferred source shapes, if any, are documented and fail precisely

## Progress Log

- [x] Extracted from the original combined Plan 25 scope
- [x] Reviewed the public insert canonicalizer on 2026-03-26
- [x] Confirmed the current Plan 27 gap is localized to [`packages/engine/src/sql/public/planner/canonicalize.rs`](/Users/samuel/git-repos/lix-2/packages/engine/src/sql/public/planner/canonicalize.rs), which still only accepts `SetExpr::Values` for insert payload canonicalization
- [x] Previous Plan 26 blocker cleared after the branch compiled again on 2026-03-26
- [x] Implemented shared insert-source canonicalization in [`packages/engine/src/sql/public/planner/canonicalize.rs`](/Users/samuel/git-repos/lix-2/packages/engine/src/sql/public/planner/canonicalize.rs)
- [x] Landed first-cut `INSERT` source support:
  - `VALUES`
  - single-row projection `SELECT` sources
  - parameterized `INSERT INTO lix_state_by_version (...) SELECT $1, $2, ...`
- [x] Kept deferred source shapes explicit with precise failures:
  - table-backed `INSERT ... SELECT` sources
  - set-operation / multi-row `INSERT` sources outside `VALUES`
  - `ORDER BY` / `LIMIT` / advanced query clauses in `INSERT` sources
- [x] Verified canonicalizer coverage with `cargo test -p lix_engine --lib sql::public::planner::canonicalize::tests:: -- --nocapture`
- [x] Verified public runtime defaulting through the new path with `cargo test -p lix_engine --test cel_default_values insert_select_applies_cel_default -- --nocapture`
- [x] Implementation complete in this plan
