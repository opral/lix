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

Guardrails:

- `tests/sql2_guardrails.rs` ensures `src/execute` stays removed.
- `tests/sql2_guardrails.rs` ensures no string-matched fallback helper is reintroduced.
