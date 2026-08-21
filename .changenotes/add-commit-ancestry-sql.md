---
type: minor
---

Added `lix_commit_ancestry()` for querying commits reachable from the active branch through SQL.

The table function defaults to the active branch head, accepts an explicit commit anchor, and reports each reachable commit with its shortest ancestry depth.
