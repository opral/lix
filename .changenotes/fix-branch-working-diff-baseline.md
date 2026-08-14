---
type: patch
---

Fixed branch-local edits being reported as additions in the working diff, which made reverting one delete the row instead of restoring its previous value.

On a newly created branch, editing a row that already existed at the checkpoint was recorded as if the row had just been created. `lix_working_diff` reported it as `added` with no before value, deleting such a row did not appear in the working diff at all, and reverting the edit removed the row rather than restoring the checkpointed value. Branch-local edits now carry their correct before image, so working diffs classify them as modifications and reverts restore the previous value. Merges, merge previews and the main branch were never affected.
