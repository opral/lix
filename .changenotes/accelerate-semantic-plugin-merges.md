---
type: patch
---

Improved semantic plugin write and merge performance for large files.

Lix now reuses unchanged binary storage chunks for common fixed-width semantic
edits, reducing the work needed to persist plugin-rendered changes.
