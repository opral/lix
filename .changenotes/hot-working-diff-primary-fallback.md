---
type: patch
---

Kept browser working-diff reads local when the sparse index cannot certify its coverage.

Lix now falls back to authoritative HOT rows and resolves snapshot-local payloads before hydrating canonical commit history.
