---
type: patch
---

Writing untracked rows no longer gets slower as the number of untracked rows grows, and no longer leaves superseded copies behind on disk.

Untracked rows previously lived under a second serving root of their own, and every untracked write republished the branch's *entire* untracked population under a fresh root. A single-row `UPDATE` against ten thousand untracked rows therefore cost about forty times a tracked one, and the superseded copies were never reclaimed. Untracked rows now live in the branch's one serving generation alongside tracked rows, separated only by the `lixcol_untracked` flag they already carried, so an untracked write is an ordinary in-place mutation. Scans get the larger win: because a single untracked row no longer forces a second serving root, the columnar layout, the immutable-base projection and the schema-presence filter stay switched on, and a full table scan on a branch containing untracked rows returns to full speed. `lixcol_untracked` keeps its exact meaning and SQL surface: untracked rows still carry no `commit_id`, stay out of history, diffs, merges and checkpoints, and are still deleted physically.
