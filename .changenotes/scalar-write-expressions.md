---
type: patch
---

SQL `UPDATE` statements now support scalar expressions such as `replace`, `concat`, `coalesce`, and string concatenation in file and row mutations.

Update assignments, filters, and returned expressions use the same scalar function rules as reads. Invalid expressions reject the mutation atomically. Partial replicas can use resident file content in an exact-path expression update while offline.

These additional scalar expressions, such as `upper` and `concat`, remain unsupported in registered-row `INSERT` values, upsert assignments, and insert `RETURNING`; those statements fail without changing rows.
