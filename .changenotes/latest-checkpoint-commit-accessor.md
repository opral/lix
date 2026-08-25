---
type: minor
---

Added `lix_latest_checkpoint_commit_id()` for reading the active branch's latest checkpoint directly in SQL.

The accessor falls back to the repository root when the branch has no checkpoint, allowing reactive working changes to be queried with `lix_diff('lix_file', lix_latest_checkpoint_commit_id(), lix_active_branch_commit_id())`.
