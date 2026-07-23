---
type: minor
---

History SQL surfaces now default to the active branch head pinned for the
statement or coherent read batch. Queries no longer need a
`lixcol_as_of_commit_id = lix_active_branch_commit_id()` predicate for the common
case.

The obsolete `LIX_HISTORY_FILTER_REQUIRED` error code is retired.

Exact equality and non-empty `IN` predicates on the history anchor still
override the default for time travel. Other anchor predicates now fail
explicitly instead of silently traversing from the active head.
Validation follows the resolved history relation through aliases, subqueries,
and joins; an unrelated table column with the same name is not an anchor.
