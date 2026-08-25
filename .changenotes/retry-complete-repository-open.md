---
type: patch
---

Fixed simultaneous repository opens against shared browser storage.

Lix now restarts the complete open lifecycle when a concurrent commit invalidates its coherent read, so another tab finishing sync bootstrap cannot leave the repository half-open.
