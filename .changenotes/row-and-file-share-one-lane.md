---
type: minor
---

A row and the file that owns it must now be in the same lane: an untracked row can no longer be owned by a tracked file, or the reverse.

Previously an untracked row could name a tracked file as its owner. That combination was quietly lossy — deleting the file, or undoing the commit that created it, would take the untracked rows with it. Undo and redo guarded against this by refusing to run whenever they detected the mixed state, which turned an unenforced invariant into a surprising failure at an unrelated moment. The mixed state can no longer be created, so that guard is gone and undo/redo behave normally again.

Writes that cross the lane boundary are now rejected at the write, and the error names both the row's lane and the file's lane instead of reporting the file as missing.

This applies only to file ownership. A row referencing a schema, an account, or a parent directory in the other lane is unaffected.
