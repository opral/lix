---
type: minor
---

Added `SELECT lix_restore(commit_id)` to move the active branch head to an ancestor commit without creating a new commit.

Restore is available through the existing `execute()` API on local and remote sessions. Orphaned commits remain eligible for ordinary garbage collection, while checkpoint rows are retained.
