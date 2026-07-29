---
type: minor
---

History relations are now table-valued functions with explicit commit arguments.

Use `example_history()` for history from the active head or
`example_history($commit)` for an explicit head. The former
`lixcol_as_of_commit_id` result column and predicate-based anchor API have been
removed.
