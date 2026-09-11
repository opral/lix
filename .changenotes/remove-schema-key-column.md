---
type: minor
---

Removed the redundant `lixcol_schema_key` SQL column.

Schema tables, historical reads, diff/history results, and SQL metadata no longer expose this column. Queries that explicitly reference it now fail; use the relation name to identify the schema. The `schema_key` column on `lix_change` is unchanged.
