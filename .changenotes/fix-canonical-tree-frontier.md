---
type: patch
---

Fixed tracked-state updates that could drop newly inserted rows or produce inconsistent state roots.

Sparse updates now repair boundaries across neighboring subtrees while skipping unaffected gaps and preserving existing canonical grouping rules. Key-size combinations that cannot form a finite canonical tree now fail explicitly instead of looping indefinitely.
