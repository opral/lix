---
type: minor
---

Recovery and apply commands now use explicit top-level `SELECT commit_id FROM ...` functions.

Use `lix_restore` to copy a source state into a new commit, `lix_revert` or `lix_revert_range` to reverse changes, and `lix_apply(before, after, rows)` to replay a selected endpoint diff. Each command returns one commit receipt row; an unchanged or empty recovery/apply selection returns `commit_id = NULL`.

This is a breaking change: the `INSERT INTO lix_restore`, `lix_revert`, and `lix_apply` command sinks are removed without aliases. Restore no longer rewinds the branch or resets its working baseline. Row references select identities; source versions are supplied explicitly, and `lix_diff` is optional for selection.
