---
type: patch
---

Kept partial checkpoints on the history-free working-diff path.

Lix now carries unselected changes into the new checkpoint epoch atomically, avoiding cold history reconstruction and sync hydration on the next write or partial checkpoint.
