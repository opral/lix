# sql2 Runtime Flow

`sql2` is the only engine execution path.

Runtime lifecycle:

1. `parse`
2. `bind_once` (script placeholders)
3. `plan`
4. `derive_requirements` and `derive_effects`
5. `lower_sql`
6. `execute`
7. `postprocess`

Postprocess ordering:

1. `postprocess_sql` inside SQL transaction scope
2. `apply_effects_tx` for SQL-backed records
3. commit boundary
4. `apply_effects_post_commit` for non-SQL runtime effects

## Ownership Map

- `ast/`
  SQL AST helpers and parameter binding utilities.
- `planning/`
  Parse/bind/plan orchestration and plan fingerprinting.
- `execution/`
  Transaction-scoped execution, SQL materialization, and postprocess orchestration.
- `surfaces/`
  View/surface classification and lowering entrypoints for logical Lix surfaces.
- `semantics/`
  Stateful semantic derivation (`requirements`, `effects`, and state resolution decisions).
- `vtable/`
  Internal state-vtable capability detection and read/write lowering.
- `storage/`
  SQL text helpers and table/query-specific storage utilities.
- `history/`
  History rewrite and projection helpers used by filesystem/state history surfaces.
- `contracts/`
  Cross-stage data contracts used by planning and execution.
- `contracts/legacy_sql/`
  Temporary adapter edge for legacy `crate::sql` types while runtime semantics live in `sql2`.

## Guardrails

- `tests/sql2_guardrails.rs` ensures `src/execute` stays removed.
- `tests/sql2_guardrails.rs` ensures the removed bridge module and its callsites are not reintroduced.
- `tests/sql2_guardrails.rs` ensures no string-matched fallback helper is reintroduced.
