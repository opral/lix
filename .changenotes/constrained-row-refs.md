---
type: minor
target: lix
---
Add the Schema v1 column type `row_ref` for complete row references. A `row_ref` column is typed `ROW_REF` in SQL queries, `RETURNING`, `lix_diff`, `lix_history`, and `information_schema`, so it compares directly with `lix_row_ref(...)`; a string literal or TEXT parameter compared with it must be a canonical reference, and other TEXT values require an explicit cast. A written reference must resolve against stored rows and pending writes in the current branch, including cross-file references. The schema-level `row_refs` list sets a column's delete action: `cascade`, `detach` (keep the referencing row and its reference, unenforced, while the target is gone), or, without an entry, `no_action`. Actions apply through ordinary writes, merges, and selective checkpoint/recovery operations, with indexed incoming-reference lookup.
