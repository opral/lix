---
type: patch
---

Moving a branch head — most visibly a fast-forward merge — no longer copies the whole working set into storage.

Creating a branch already published a single reference to the shared, immutable state at its head. Moving an existing branch's head did not: it wrote one row per tracked row into that branch's private serving storage, so a fast-forward merge of a ten-row change in a hundred-thousand-row repository grew the repository by roughly ten times its own size. Head moves now publish the same one-reference form, and the cost of a merge tracks the size of the merge rather than the size of the repository.
