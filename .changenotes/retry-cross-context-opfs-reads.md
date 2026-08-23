---
type: patch
---

Prevented transient cross-tab writes from interrupting coherent SQL reads in browser repositories.

Lix now gives competing OPFS commits time to settle before retrying the complete query, while preserving bounded failure under continuous invalidation.
