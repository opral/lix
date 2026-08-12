---
type: minor
---

Untracked rows now expose a stable `lixcol_change_id`, so every row has an addressable identity.

**Breaking for one query idiom: use `lixcol_untracked` to detect untracked rows.** `lixcol_change_id IS NULL` used to identify untracked state, and it no longer will — the column is now populated for every row. Replace `WHERE lixcol_change_id IS NULL` with `WHERE lixcol_untracked = true`, which has always been the direct way to ask this question.

Previously `lixcol_change_id` was always `NULL` for untracked state, which made untracked rows the only rows in Lix without an id to refer to them by. They now carry a real change id while remaining history-free: `lixcol_commit_id` stays `NULL`, and untracked state is still absent from the changelog, from history views, and from the commit graph.

Treat an untracked row's change id as a local identity only. It is minted per write on the branch that made it, and untracked state is branch-scoped and never reconciled through the commit graph, so these ids are not meaningful to compare across sessions or replicas.
