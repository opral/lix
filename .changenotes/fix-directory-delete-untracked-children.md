---
type: patch
---

Deleting a directory now also deletes the untracked files and directories beneath it.

Previously a recursive directory delete only removed children that shared the directory's own
durability lane. Deleting a normal directory left any untracked file below it behind, pointing at a
parent that no longer existed. That made every subsequent read of `lix_file` and `lix_directory` on
the branch fail, the state survived a restart, and the leftover file could not be deleted because the
delete needed the same reads. A directory delete now reaches untracked children as well; deleting an
untracked directory still leaves tracked children alone.
