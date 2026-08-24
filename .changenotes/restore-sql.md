---
type: minor
---

Added the `lix_restore` command sink to move the active branch head to an ancestor commit without creating a new commit.

Call it with `INSERT INTO lix_restore (commit_id) VALUES (...)`. Restore is available through the existing `execute()` API on local and remote sessions. Orphaned commits remain eligible for ordinary garbage collection, while checkpoint rows are retained.
