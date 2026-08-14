---
type: patch
---

Equality lookups on unique and foreign-key columns no longer scan the whole collection.

Any column a schema declares through `x-lix-unique` or `x-lix-foreign-keys` now has an
index behind it, so `WHERE fk_column = ?` reads the matching rows instead of every row
in the table, and inserting into a table with a unique constraint no longer gets slower
as that table grows. Collections written before this release keep their old behaviour
until their next generation is published.
