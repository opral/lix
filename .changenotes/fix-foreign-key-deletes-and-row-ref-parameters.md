---
type: patch
---

Fixed foreign-key enforcement when deleting referenced rows and parameterized RowRef construction.

Deleting a referenced parent now rejects the operation instead of leaving orphaned children, including whole-table deletes. `lix_row_ref` now resolves newly registered relations consistently when the relation name is supplied as a SQL parameter.
