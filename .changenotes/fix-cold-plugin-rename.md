---
type: patch
---

Fixed plugin file renames after switching branches or opening a new session losing the previous filename when no plugin observation was cached.

Cold transitions now resolve the predecessor path from the transaction's repository state. Format-sensitive plugins can correctly handle changes such as CSV to TSV before subsequent semantic edits.
