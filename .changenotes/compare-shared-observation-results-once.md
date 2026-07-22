---
type: patch
---

Reduced CPU time when identical observations fan out large results.

Lix now compares each new shared query result with its predecessor once and lets subscribers reuse that equivalence decision, while retaining each generation's fresh result metadata.
