# sql Runtime Flow

`sql` is the only engine execution path.

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
  Parse/bind/plan orchestration.
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
- `planning/rewrite_engine/`
  Statement/query rewrite engine that canonicalizes and lowers logical views for planning.

## Guardrails

- `tests/sql_guardrails.rs` ensures `src/execute` stays removed.
- `tests/sql_guardrails.rs` ensures `src/sql` stays removed and `crate::sql::*` imports are forbidden.
- `tests/sql_guardrails.rs` ensures no string-matched fallback helper is reintroduced.
- `tests/sql_guardrails.rs` ensures preprocess placeholder binding flows through `planning/bind_once.rs`.
