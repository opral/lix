---
type: minor
---

SQL discovery now reports logical types directly, and expressions preserve typed JSON and row references.

This is a breaking catalog change: `information_schema.columns` and `information_schema.table_functions` no longer expose `lix_value_kind`. Read `data_type` directly for `JSONB`, `ROW_REF`, and `TIMESTAMPTZ`. Historical column nullability now accounts for schema changes. CASE and COALESCE preserve logical types, UNION resolves types across its inputs including NULLs, and RETURNING accepts CASE expressions.
