---
type: minor
---

Added SQL `DELETE ... RETURNING` and `LIKE`/`ILIKE` predicates across writable Lix SQL surfaces.

`DELETE ... RETURNING` returns the pre-delete rows from the same atomic delete operation, including binary file data when requested, while still reporting the full affected-row count for cascading deletes.
