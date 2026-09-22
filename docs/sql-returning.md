# SQL RETURNING

`INSERT`, `UPDATE`, `DELETE`, and `INSERT ... ON CONFLICT DO UPDATE` can return the affected rows as part of the write:

```sql
UPDATE lix_file SET content = $1 WHERE id = $2
RETURNING id, OLD.content AS before, NEW.content AS after;
```

`OLD.column` reads the row before the statement's mutation; `NEW.column` reads it afterwards. `OLD.*` and `NEW.*` expand the corresponding image. These qualifiers are scoped to `RETURNING` and work with its supported expressions.

| Operation | OLD | NEW | Unqualified columns / `*` |
| :-- | :-- | :-- | :-- |
| INSERT | SQL NULL | Inserted row | Inserted row |
| UPDATE | Previous row | Updated row | Updated row |
| DELETE | Deleted row | SQL NULL | Deleted row |
| Upsert conflict update | Previous row | Updated row | Updated row |

For a mixed upsert, each inserted row has a null old image and each updated row has its own old image. Absent images retain the column's SQL type. An empty file is an empty BYTEA, not SQL NULL. A statement affecting zero rows still returns the declared columns and types.

RETURNING supports searched and simple `CASE` expressions, including `OLD` and `NEW` values in conditions and result branches. `CASE` and `COALESCE` retain JSONB and row-reference types when their value branches share that logical type; a SQL NULL branch does not change it.

These image semantics follow PostgreSQL 18. Custom image aliases using `RETURNING WITH (OLD AS ..., NEW AS ...)` are not supported.

Returning rows describe a statement, including no-op updates. Two statements that change A to B and then B to A return both transitions, while an endpoint diff can be empty. Use the committed operation's `commit` receipt with `lix_diff` for the transaction's net changes. Rows returned inside an explicit transaction are provisional until commit succeeds. A failing RETURNING expression rolls back that statement's writes.

File bytes are materialized only when needed by a predicate, assignment, or a requested image. Select file IDs or paths when the caller only needs metadata. RETURNING provides accurate write images; overwrite protection additionally requires validating the caller's expected state inside the write transaction.
