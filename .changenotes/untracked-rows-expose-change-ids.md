---
type: minor
---

Untracked rows now expose a stable `lixcol_change_id`, so every row has an addressable identity.

Previously `lixcol_change_id` was always `NULL` for untracked state, which made untracked rows the only rows in Lix without an id to refer to them by. They now carry a real change id while remaining history-free: `lixcol_commit_id` stays `NULL`, and untracked state is still absent from the changelog, from history views, and from the commit graph. Queries that relied on `lixcol_change_id IS NULL` to detect untracked rows should use `lixcol_untracked` instead.
